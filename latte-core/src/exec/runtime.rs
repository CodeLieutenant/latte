//! A single-threaded task set driven by `pump` calls from the host.
//!
//! The core owns no thread and no event loop. Tasks are polled with a no-op waker. A task
//! becomes ready again only when an op it issued completes, its deadline passes, or a timer
//! it set expires, so the reactor tracks which task waits for what instead of using wakers.
//!
//! All state here belongs to one instance and its owner. It uses no atomic operation and no
//! lock, and after warm-up the per-op path allocates nothing: ops sit in a dense slab indexed
//! by their sequence number, so lookup, completion and deadline checks are O(1).

use crate::clock;
use crate::ops::{Completion, DriverError, Op, OpId, OpKind};
use std::cell::{Cell, RefCell, RefMut};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

pub type TaskId = usize;

thread_local! {
    static CURRENT_TASK: Cell<Option<TaskId>> = const { Cell::new(None) };
    static CURRENT_SEND_AT: Cell<u64> = const { Cell::new(0) };
}

/// Each reactor takes a distinct high part for its op ids, once, when it is made. Op ids of
/// a run are therefore unique and never reused, and each reactor numbers its own ops densely.
static NEXT_REACTOR: AtomicU64 = AtomicU64::new(1);
const SEQ_BITS: u32 = 40;

fn current_task() -> TaskId {
    CURRENT_TASK
        .with(Cell::get)
        .expect("a reactor future was polled outside of a task")
}

/// Asks a run to stop. Clones share one flag. Setting it is a single atomic store, so any
/// thread and a signal handler may call [`CancelToken::cancel`]. Readers use a relaxed load,
/// once per pump call and once per task poll.
#[derive(Clone, Debug, Default)]
pub struct CancelToken(Arc<AtomicBool>);

impl CancelToken {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SlotState {
    /// Made by a task, not handed to the host yet.
    Held,
    /// Handed to the host.
    Released,
    /// Answered; the completion waits for its task.
    Ready,
    /// The task took the completion.
    Taken,
}

struct Slot {
    task: TaskId,
    deadline_ns: u64,
    state: SlotState,
    completion: Option<Completion>,
}

struct ReactorState {
    now_ns: u64,
    id_base: u64,
    next_seq: u64,
    /// Sequence number of `slots[0]`.
    front_seq: u64,
    slots: VecDeque<Slot>,
    outbox: Vec<Op>,
    spare: Vec<Op>,
    released: usize,
    timers: Vec<(u64, TaskId)>,
    woken: Vec<TaskId>,
}

impl ReactorState {
    fn slot_index(&self, id: OpId) -> Option<usize> {
        if id >> SEQ_BITS != self.id_base >> SEQ_BITS {
            return None;
        }
        let seq = id & ((1 << SEQ_BITS) - 1);
        let index = seq.checked_sub(self.front_seq)? as usize;
        (index < self.slots.len()).then_some(index)
    }

    /// Drops the taken slots at the front, so the slab holds only live ops.
    fn compact(&mut self) {
        while self
            .slots
            .front()
            .is_some_and(|s| s.state == SlotState::Taken)
        {
            self.slots.pop_front();
            self.front_seq += 1;
        }
    }
}

/// Op and timer bookkeeping of one instance. Scripts reach it through their `Context`.
pub struct Reactor {
    state: RefCell<ReactorState>,
    request_timeout_ns: u64,
    native: Cell<bool>,
}

impl Reactor {
    /// A reactor whose ops time out `request_timeout_ns` after their send time.
    pub fn new(request_timeout_ns: u64) -> Rc<Reactor> {
        let id_base = NEXT_REACTOR.fetch_add(1, Ordering::Relaxed) << SEQ_BITS;
        Rc::new(Reactor {
            state: RefCell::new(ReactorState {
                now_ns: 0,
                id_base,
                next_seq: 0,
                front_seq: 0,
                slots: VecDeque::new(),
                outbox: Vec::new(),
                spare: Vec::new(),
                released: 0,
                timers: Vec::new(),
                woken: Vec::new(),
            }),
            request_timeout_ns,
            native: Cell::new(true),
        })
    }

    /// True when the host takes native values, rows and JSON; see [`crate::native`].
    pub fn native(&self) -> bool {
        self.native.get()
    }

    /// Switches the op data to the C ABI form: CQL bytes and JSON text.
    pub fn set_native(&self, native: bool) {
        self.native.set(native);
    }

    fn lock(&self) -> RefMut<'_, ReactorState> {
        self.state.borrow_mut()
    }

    /// Returns a future that sends `kind` to the host and resolves to its completion.
    pub fn submit(self: &Rc<Self>, kind: OpKind) -> OpFuture {
        OpFuture {
            reactor: Rc::clone(self),
            kind: Some(kind),
            id: None,
            send_at_ns: 0,
        }
    }

    /// Returns a future that resolves once the clock reaches `deadline_ns`.
    pub fn sleep_until(self: &Rc<Self>, deadline_ns: u64) -> Sleep {
        Sleep {
            reactor: Rc::clone(self),
            deadline_ns,
        }
    }

    pub fn now_ns(&self) -> u64 {
        self.lock().now_ns
    }

    pub fn set_now(&self, now_ns: u64) {
        self.lock().now_ns = now_ns;
    }

    /// Stores a completion and marks the task that waits for it as ready. Returns false for
    /// an unknown id: a late completion of an op that timed out, or one already answered.
    pub fn complete(&self, completion: Completion) -> bool {
        let mut state = self.lock();
        let state = &mut *state;
        let Some(index) = state.slot_index(completion.id) else {
            return false;
        };
        let slot = &mut state.slots[index];
        match slot.state {
            SlotState::Held | SlotState::Released => {
                let was_released = slot.state == SlotState::Released;
                let task = slot.task;
                slot.state = SlotState::Ready;
                slot.completion = Some(completion);
                if was_released {
                    state.released -= 1;
                }
                state.woken.push(task);
                true
            }
            SlotState::Ready | SlotState::Taken => false,
        }
    }

    /// Answers every released op whose deadline passed with a client timeout. Deadlines grow
    /// with the sequence number, so the scan stops at the first released op still in time.
    pub fn expire_deadlines(&self) {
        let mut state = self.lock();
        let state = &mut *state;
        let now = state.now_ns;
        for slot in state.slots.iter_mut() {
            if slot.state != SlotState::Released {
                continue;
            }
            if slot.deadline_ns > now {
                break;
            }
            slot.state = SlotState::Ready;
            slot.completion = Some(Completion {
                id: 0,
                start_ns: 0,
                end_ns: 0,
                result: Err(DriverError::deadline_passed()),
            });
            state.released -= 1;
            state.woken.push(slot.task);
        }
    }

    /// Marks the tasks whose timers expired as ready and returns the next timer deadline.
    pub fn expire_timers(&self) -> Option<u64> {
        let mut state = self.lock();
        let state = &mut *state;
        let now = state.now_ns;
        let mut next = None;
        let woken = &mut state.woken;
        state.timers.retain(|&(deadline, task)| {
            if deadline <= now {
                woken.push(task);
                false
            } else {
                next = Some(next.map_or(deadline, |n: u64| n.min(deadline)));
                true
            }
        });
        next
    }

    pub fn take_woken(&self, into: &mut Vec<TaskId>) {
        into.append(&mut self.lock().woken);
    }

    /// Moves at most `limit` ops whose send time has come to `into`.
    /// Ops due later stay in the core.
    pub fn drain_ops(&self, into: &mut Vec<Op>, limit: usize) {
        let mut state = self.lock();
        let state = &mut *state;
        if state.outbox.is_empty() {
            return;
        }
        let now = state.now_ns;
        let mut released = 0;
        let mut outbox = std::mem::take(&mut state.outbox);
        for op in outbox.drain(..) {
            if released < limit && op.send_at_ns <= now {
                if let Some(index) = state.slot_index(op.id) {
                    if state.slots[index].state == SlotState::Held {
                        state.slots[index].state = SlotState::Released;
                        state.released += 1;
                    }
                }
                into.push(op);
                released += 1;
            } else {
                state.spare.push(op);
            }
        }
        state.outbox = std::mem::replace(&mut state.spare, outbox);
    }

    /// The earliest send time of an op the core still holds.
    pub(crate) fn next_send_at(&self) -> Option<u64> {
        self.lock().outbox.iter().map(|op| op.send_at_ns).min()
    }

    /// The deadline of the oldest op the host is running.
    pub(crate) fn next_op_deadline(&self) -> Option<u64> {
        self.lock()
            .slots
            .iter()
            .find(|s| s.state == SlotState::Released)
            .map(|s| s.deadline_ns)
    }

    /// Number of ops the host received and has not answered.
    pub fn in_flight(&self) -> usize {
        self.lock().released
    }

    /// Forgets every op and timer. Later completions of the dropped ops are ignored.
    pub(crate) fn clear(&self) {
        let mut state = self.lock();
        state.outbox.clear();
        state.slots.clear();
        state.front_seq = state.next_seq;
        state.released = 0;
        state.timers.clear();
        state.woken.clear();
    }
}

/// A completion together with the send time the core gave the op.
pub struct Sent {
    pub completion: Completion,
    pub send_at_ns: u64,
}

pub struct OpFuture {
    reactor: Rc<Reactor>,
    kind: Option<OpKind>,
    id: Option<OpId>,
    send_at_ns: u64,
}

impl Future for OpFuture {
    type Output = Sent;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Sent> {
        if let Some(kind) = self.kind.take() {
            let task = current_task();
            let mut state = self.reactor.lock();
            let seq = state.next_seq;
            state.next_seq += 1;
            let id = state.id_base | seq;
            // The first op of a cycle started ahead of its schedule waits for the scheduled
            // time; any later op goes out as soon as the script makes it.
            let send_at_ns = CURRENT_SEND_AT.with(Cell::get).max(state.now_ns);
            let deadline_ns = send_at_ns.saturating_add(self.reactor.request_timeout_ns);
            if state.slots.is_empty() {
                state.front_seq = seq;
            }
            state.slots.push_back(Slot {
                task,
                deadline_ns,
                state: SlotState::Held,
                completion: None,
            });
            state.outbox.push(Op {
                id,
                send_at_ns,
                deadline_ns,
                kind,
            });
            drop(state);
            self.send_at_ns = send_at_ns;
            self.id = Some(id);
            return Poll::Pending;
        }
        let id = self.id.expect("op future polled after completion");
        let mut state = self.reactor.lock();
        let state = &mut *state;
        let Some(index) = state.slot_index(id) else {
            return Poll::Pending;
        };
        let slot = &mut state.slots[index];
        if slot.state != SlotState::Ready {
            return Poll::Pending;
        }
        slot.state = SlotState::Taken;
        let mut completion = slot
            .completion
            .take()
            .expect("a ready slot holds a completion");
        completion.id = id;
        state.compact();
        Poll::Ready(Sent {
            completion,
            send_at_ns: self.send_at_ns,
        })
    }
}

pub struct Sleep {
    reactor: Rc<Reactor>,
    deadline_ns: u64,
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        let mut state = self.reactor.lock();
        if state.now_ns >= self.deadline_ns {
            return Poll::Ready(());
        }
        let task = current_task();
        state.timers.push((self.deadline_ns, task));
        Poll::Pending
    }
}

pub type BoxTask<T> = Pin<Box<dyn Future<Output = T>>>;

/// Polls and drops a future of one concrete type stored in a [`TaskSlot`].
struct TaskVtable<T> {
    poll: unsafe fn(*mut u8, &mut Context<'_>) -> Poll<T>,
    drop: unsafe fn(*mut u8),
}

trait HasTaskVtable<T> {
    const VTABLE: TaskVtable<T>;
}

impl<F: Future<Output = T>, T> HasTaskVtable<T> for F {
    const VTABLE: TaskVtable<T> = TaskVtable {
        poll: poll_in_place::<F, T>,
        drop: drop_in_place::<F>,
    };
}

/// # Safety
/// `ptr` points to a live, pinned `F`.
unsafe fn poll_in_place<F: Future<Output = T>, T>(ptr: *mut u8, cx: &mut Context<'_>) -> Poll<T> {
    unsafe { Pin::new_unchecked(&mut *(ptr as *mut F)) }.poll(cx)
}

/// # Safety
/// `ptr` points to a live `F` that is not used again.
unsafe fn drop_in_place<F>(ptr: *mut u8) {
    unsafe { std::ptr::drop_in_place(ptr as *mut F) }
}

/// The smallest alignment of slot memory. Larger alignments get memory of their own.
const SLOT_ALIGN: usize = 16;

/// Heap memory for one task future. A finished future leaves its memory in the slot, and the
/// next future that fits is written into the same memory, so starting a task allocates
/// nothing once the slot is warm. The memory never moves while it holds a future, which keeps
/// the future pinned.
struct TaskSlot<T: 'static> {
    mem: std::ptr::NonNull<u8>,
    layout: std::alloc::Layout,
    vtable: Option<&'static TaskVtable<T>>,
    send_at: u64,
}

impl<T: 'static> TaskSlot<T> {
    fn empty() -> Self {
        TaskSlot {
            mem: std::ptr::NonNull::dangling(),
            layout: std::alloc::Layout::from_size_align(0, SLOT_ALIGN).unwrap(),
            vtable: None,
            send_at: 0,
        }
    }

    fn is_live(&self) -> bool {
        self.vtable.is_some()
    }

    /// Stores `future`, growing the memory first when it does not fit.
    fn put<F: Future<Output = T> + 'static>(&mut self, future: F) {
        debug_assert!(!self.is_live());
        let size = std::mem::size_of::<F>().max(1);
        let align = std::mem::align_of::<F>().max(SLOT_ALIGN);
        if size > self.layout.size() || align > self.layout.align() {
            self.release();
            let size = size.next_power_of_two();
            let layout = std::alloc::Layout::from_size_align(size, align).expect("a valid layout");
            // SAFETY: the layout has a non-zero size.
            let mem = unsafe { std::alloc::alloc(layout) };
            self.mem = std::ptr::NonNull::new(mem)
                .unwrap_or_else(|| std::alloc::handle_alloc_error(layout));
            self.layout = layout;
        }
        // SAFETY: the memory is large and aligned enough for `F`, and holds no live value.
        unsafe { std::ptr::write(self.mem.as_ptr() as *mut F, future) };
        self.vtable = Some(&<F as HasTaskVtable<T>>::VTABLE);
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Option<Poll<T>> {
        let vtable = self.vtable?;
        // SAFETY: the slot holds a live future of the type `vtable` was made for, and its
        // memory has not moved since the future was written.
        Some(unsafe { (vtable.poll)(self.mem.as_ptr(), cx) })
    }

    /// Drops the future, if any, and keeps the memory for the next one.
    fn clear(&mut self) {
        if let Some(vtable) = self.vtable.take() {
            // SAFETY: the slot held a live future of the type `vtable` was made for.
            unsafe { (vtable.drop)(self.mem.as_ptr()) };
        }
    }

    fn release(&mut self) {
        self.clear();
        if self.layout.size() > 0 {
            // SAFETY: `mem` was allocated with `layout`.
            unsafe { std::alloc::dealloc(self.mem.as_ptr(), self.layout) };
            self.layout = std::alloc::Layout::from_size_align(0, SLOT_ALIGN).unwrap();
            self.mem = std::ptr::NonNull::dangling();
        }
    }
}

impl<T: 'static> Drop for TaskSlot<T> {
    fn drop(&mut self) {
        self.release();
    }
}

/// Slots for the futures of one worker's context calls. A call stores its future in a free
/// slot and gives Rune a [`SlotFuture`] of a few bytes, so Rune boxes the small handle and
/// not the full call state. A slot keeps its memory for the next call.
pub struct FutureSlots<T: 'static> {
    slots: RefCell<Vec<TaskSlot<T>>>,
    free: RefCell<Vec<usize>>,
}

impl<T: 'static> Default for FutureSlots<T> {
    fn default() -> Self {
        FutureSlots {
            slots: RefCell::new(Vec::new()),
            free: RefCell::new(Vec::new()),
        }
    }
}

impl<T: 'static> FutureSlots<T> {
    /// Stores `future` in a free slot and returns the handle that runs it.
    pub fn spawn<F: Future<Output = T> + 'static>(self: &Rc<Self>, future: F) -> SlotFuture<T> {
        let id = self.free.borrow_mut().pop();
        let mut slots = self.slots.borrow_mut();
        let id = id.unwrap_or_else(|| {
            slots.push(TaskSlot::empty());
            slots.len() - 1
        });
        slots[id].put(future);
        SlotFuture {
            slots: Rc::clone(self),
            id,
            live: true,
        }
    }

    /// Drops the future in slot `id` and makes the slot free.
    fn release(&self, id: usize) {
        // Take the vtable out first, so the drop runs with no borrow of the slot list held.
        let (mem, vtable) = {
            let mut slots = self.slots.borrow_mut();
            let slot = &mut slots[id];
            (slot.mem, slot.vtable.take())
        };
        if let Some(vtable) = vtable {
            // SAFETY: the slot held a live future of the type `vtable` was made for.
            unsafe { (vtable.drop)(mem.as_ptr()) };
        }
        self.free.borrow_mut().push(id);
    }
}

/// Runs a future stored in [`FutureSlots`].
pub struct SlotFuture<T: 'static> {
    slots: Rc<FutureSlots<T>>,
    id: usize,
    live: bool,
}

impl<T: 'static> Future for SlotFuture<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        assert!(self.live, "a slot future polled after it finished");
        let (mem, vtable) = {
            let slots = self.slots.slots.borrow();
            let slot = &slots[self.id];
            (slot.mem, slot.vtable.expect("a live slot holds a future"))
        };
        // SAFETY: the slot holds a live future of the type `vtable` was made for. Its memory
        // stays in place even when the slot list grows, because each slot owns its memory.
        // No borrow of the slot list is held, so the future may start other slot futures.
        let poll = unsafe { (vtable.poll)(mem.as_ptr(), cx) };
        if poll.is_ready() {
            self.live = false;
            self.slots.release(self.id);
        }
        poll
    }
}

impl<T: 'static> Drop for SlotFuture<T> {
    fn drop(&mut self) {
        if self.live {
            self.slots.release(self.id);
        }
    }
}

/// Futures of one instance, indexed by task id. Each id owns a slot whose memory is reused
/// by the next task that gets the id.
pub struct TaskSet<T: 'static> {
    slots: Vec<TaskSlot<T>>,
    free: Vec<TaskId>,
    ready: Vec<TaskId>,
    /// The ready list of the poll in progress. Kept between polls so a poll allocates nothing.
    polling: Vec<TaskId>,
    len: usize,
}

impl<T: 'static> Default for TaskSet<T> {
    fn default() -> Self {
        TaskSet {
            slots: Vec::new(),
            free: Vec::new(),
            ready: Vec::new(),
            polling: Vec::new(),
            len: 0,
        }
    }
}

impl<T: 'static> TaskSet<T> {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Adds a task and marks it ready for its first poll.
    pub fn spawn<F: Future<Output = T> + 'static>(&mut self, task: F) {
        self.spawn_at(task, 0);
    }

    /// Adds a task whose first op must not go out before `send_at_ns`.
    pub fn spawn_at<F: Future<Output = T> + 'static>(&mut self, task: F, send_at_ns: u64) {
        let id = match self.free.pop() {
            Some(id) => id,
            None => {
                self.slots.push(TaskSlot::empty());
                self.slots.len() - 1
            }
        };
        let slot = &mut self.slots[id];
        slot.put(task);
        slot.send_at = send_at_ns;
        self.len += 1;
        self.ready.push(id);
    }

    /// Drops every task without running it further. The slot memory stays for later tasks.
    pub fn clear(&mut self) {
        self.free.clear();
        for (id, slot) in self.slots.iter_mut().enumerate() {
            slot.clear();
            self.free.push(id);
        }
        self.ready.clear();
        self.polling.clear();
        self.len = 0;
    }

    /// Polls every ready task once and passes the output of each finished task to `on_done`.
    /// Returns true if any task was polled.
    /// Stops early once `cancel` is set, so a cancel waits for at most one task poll, which
    /// the Rune budget bounds.
    pub fn poll_ready(
        &mut self,
        reactor: &Reactor,
        cancel: &CancelToken,
        mut on_done: impl FnMut(T),
    ) -> bool {
        reactor.take_woken(&mut self.ready);
        if self.ready.is_empty() {
            return false;
        }
        let mut cx = Context::from_waker(Waker::noop());
        std::mem::swap(&mut self.ready, &mut self.polling);
        let mut index = 0;
        while index < self.polling.len() {
            let id = self.polling[index];
            index += 1;
            if cancel.is_cancelled() {
                break;
            }
            let Some(slot) = self.slots.get_mut(id) else {
                continue;
            };
            if !slot.is_live() {
                continue;
            }
            CURRENT_TASK.with(|c| c.set(Some(id)));
            CURRENT_SEND_AT.with(|c| c.set(slot.send_at));
            let poll = slot.poll(&mut cx);
            CURRENT_TASK.with(|c| c.set(None));
            CURRENT_SEND_AT.with(|c| c.set(0));
            if let Some(Poll::Ready(output)) = poll {
                slot.clear();
                self.free.push(id);
                self.len -= 1;
                on_done(output);
            }
        }
        self.polling.clear();
        true
    }
}

/// The time on the shared clock that the host passes to `pump`, or the clock itself.
pub fn pump_time(now_ns: u64) -> u64 {
    if now_ns == 0 {
        clock::now_ns()
    } else {
        now_ns
    }
}
