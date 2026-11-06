//! C ABI of the core for hosts in other languages.
//!
//! The host calls into the core and the core never calls the host.
//!
//! Threading contract:
//! * Run-level calls (`latte_cancel`, `latte_is_cancelled`, `latte_clock_ns`,
//!   `latte_last_error`, `latte_abi_version`) are safe from any thread at any time.
//! * An instance has one owner at a time. The owner calls `latte_pump` and reads and writes
//!   the instance's two rings. A host may move an instance to another thread between pump
//!   calls; its own handoff (a task continuation, a channel) orders the memory. An
//!   overlapping `latte_pump` call returns `LATTE_ERR_BUSY`.
//! * Completions made on other threads go through `latte_submit`, which takes a whole batch
//!   and is safe from any thread. `latte_reserve` is also safe from any thread.
//!
//! Each instance owns two rings, which hold fixed records and a byte arena:
//! * the op ring, which `latte_pump` fills with the ops the host must run,
//! * the completion ring, which the owner fills before the next `latte_pump`.
//!
//! Every export catches panics and reports them as an error code.

use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::{c_char, c_int, CStr};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::Mutex;

use scylla_cql_core::frame::response::result::{
    CollectionType, ColumnSpec, ColumnType, NativeType, TableSpec, UserDefinedType,
};

use crate::clock;
use crate::exec::instance::{Instance, PumpStatus};
use crate::exec::run::{self, Run, Start};
use crate::exec::runtime::CancelToken;
use crate::ops::{ClientKind, Completion, DriverError, Op, OpKind, Response, Rows};

/// Version of this ABI. A host refuses to run with a core of another version.
pub const LATTE_ABI_VERSION: u32 = 3;

/// `latte_pump` results. Negative values are errors.
pub const LATTE_OK: c_int = 0;
pub const LATTE_DONE: c_int = 1;
pub const LATTE_DRAINING: c_int = 2;
pub const LATTE_CANCELLED: c_int = 3;
pub const LATTE_ERR_PANIC: c_int = -1;
pub const LATTE_ERR_ARG: c_int = -2;
pub const LATTE_ERR_DECODE: c_int = -4;
pub const LATTE_ERR_BUSY: c_int = -5;

pub const LATTE_RING_RECORDS: usize = 4096;
pub const LATTE_RING_ARENA: usize = 4 << 20;

/// Largest result a host may hand over through `latte_reserve`. A bigger result is
/// reported as `LATTE_CLIENT_RESULT_TOO_LARGE`.
pub const LATTE_MAX_RESULT_BYTES: usize = 256 << 20;

/// CQL op kinds in [`LatteRecord::kind`] of the op ring.
pub const LATTE_OP_PREPARE: u32 = 1;
pub const LATTE_OP_QUERY: u32 = 2;
pub const LATTE_OP_EXECUTE: u32 = 3;
pub const LATTE_OP_BATCH: u32 = 4;
pub const LATTE_OP_DATACENTERS: u32 = 5;

/// DynamoDB op kinds. The text range holds the operation name, the data range the request
/// JSON of the DynamoDB wire protocol.
pub const LATTE_OP_DDB_CREATE_TABLE: u32 = 16;
pub const LATTE_OP_DDB_DELETE_TABLE: u32 = 17;
pub const LATTE_OP_DDB_DESCRIBE_TABLE: u32 = 18;
pub const LATTE_OP_DDB_PUT_ITEM: u32 = 19;
pub const LATTE_OP_DDB_GET_ITEM: u32 = 20;
pub const LATTE_OP_DDB_UPDATE_ITEM: u32 = 21;
pub const LATTE_OP_DDB_DELETE_ITEM: u32 = 22;
pub const LATTE_OP_DDB_QUERY: u32 = 23;
pub const LATTE_OP_DDB_SCAN: u32 = 24;
pub const LATTE_OP_DDB_BATCH_GET_ITEM: u32 = 25;
pub const LATTE_OP_DDB_BATCH_WRITE_ITEM: u32 = 26;
pub const LATTE_OP_DDB_DESCRIBE_ENDPOINTS: u32 = 27;

/// Row mode in the low two bits of [`LatteRecord::flags`] of a query or execute op.
/// None: return no row data; set `rows` when the driver knows the count for free.
/// Count: return only the row count in `rows`, with no row data and no arena use.
/// Rows: return the column specs and the cells.
/// Paging is the core's job in every mode: the host runs one page per op and returns the
/// paging state, and the core requests the next page and sums the counts.
pub const LATTE_ROWS_NONE: u32 = 0;
pub const LATTE_ROWS_COUNT: u32 = 1;
pub const LATTE_ROWS_ALL: u32 = 2;

/// Status in [`LatteRecord::kind`] of a completion.
pub const LATTE_STATUS_OK: u32 = 0;
pub const LATTE_STATUS_ERROR: u32 = 1;

/// Response layouts in [`LatteRecord::flags`] of a successful completion.
pub const LATTE_RESP_DONE: u32 = 0;
pub const LATTE_RESP_PREPARED: u32 = 1;
pub const LATTE_RESP_PAGE: u32 = 2;
pub const LATTE_RESP_DATACENTERS: u32 = 3;
pub const LATTE_RESP_JSON: u32 = 4;

/// Client kinds in [`LatteRecord::client_kind`] of an error completion.
pub const LATTE_CLIENT_NONE: u8 = 0;
pub const LATTE_CLIENT_TIMEOUT: u8 = 1;
pub const LATTE_CLIENT_CONNECTION_LOST: u8 = 2;
pub const LATTE_CLIENT_NO_HOST_AVAILABLE: u8 = 3;
pub const LATTE_CLIENT_RESULT_TOO_LARGE: u8 = 4;
pub const LATTE_CLIENT_OTHER: u8 = 5;

/// One op or one completion at a fixed offset in a ring. Hosts read and write the fields in
/// place. Numbers inside the arena are big-endian, as in the CQL native protocol.
///
/// An op:
/// * `kind` is a `LATTE_OP_*` value, `flags` holds a `LATTE_ROWS_*` row mode.
/// * `id` is unique in the run and never reused.
/// * `send_at_ns` is the time on `latte_clock_ns` to send the op at; `latte_pump` only hands
///   out ops whose time has come. `deadline_ns` is the send time plus the request timeout:
///   pass that timeout to the driver. The core times the op out itself at `deadline_ns`
///   and drops a later completion.
/// * `stmt` is the statement handle: the array index to store a prepared statement at, or
///   the statement to execute. `page_size` applies to prepare and query.
/// * The text range holds the CQL of prepare and query, or the DynamoDB operation name.
///   The paging range holds the paging state of the page to fetch.
/// * The data range holds the bound values of execute: the CQL `[value]` sequence of the
///   statement (`i32` length then bytes, -1 null, -2 unset). A batch holds per statement:
///   `u64` handle, `u16` value count, the values. A DynamoDB op holds the request JSON.
///
/// A completion:
/// * `id` repeats the op id. `start_ns` and `end_ns` are `latte_clock_ns` readings around the
///   driver call.
/// * `kind` is `LATTE_STATUS_OK` or `LATTE_STATUS_ERROR`.
/// * On error, `server_code` is the CQL protocol error code (0 if none), `client_kind` a
///   `LATTE_CLIENT_*` value, the text range the exception class name (for DynamoDB, the
///   error type) and the data range its message. The core assigns the error category from
///   `server_code` and `client_kind`, and from the DynamoDB error type.
/// * On success, `flags` is a `LATTE_RESP_*` layout of the data range:
///   prepared: the column specs of the bind markers;
///   page: the column specs, then the cells of `rows` rows, each a CQL `[bytes]`, with the
///   paging state of the next page in the paging range;
///   datacenters: `u16` count, then per name a `u16` length and UTF-8 bytes;
///   json: the response JSON of a DynamoDB op; done: nothing.
/// * Column specs are a `u16` count, then per column a CQL `[string]` name and a CQL
///   `[option]` type, the encoding of result metadata in the native protocol.
/// * `buffer` is null when the ranges point into the ring arena. A result too big for the
///   arena goes into a buffer from `latte_reserve`; `buffer` then holds its address and the
///   ranges point into it.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct LatteRecord {
    pub id: u64,
    pub send_at_ns: u64,
    pub deadline_ns: u64,
    pub start_ns: u64,
    pub end_ns: u64,
    pub stmt: u64,
    pub rows: u64,
    pub buffer: *mut u8,
    pub kind: u32,
    pub flags: u32,
    pub page_size: i32,
    pub server_code: u32,
    pub client_kind: u8,
    pub reserved: [u8; 7],
    pub text_off: u32,
    pub text_len: u32,
    pub paging_off: u32,
    pub paging_len: u32,
    pub data_off: u32,
    pub data_len: u32,
}

impl Default for LatteRecord {
    fn default() -> Self {
        LatteRecord {
            id: 0,
            send_at_ns: 0,
            deadline_ns: 0,
            start_ns: 0,
            end_ns: 0,
            stmt: 0,
            rows: 0,
            buffer: ptr::null_mut(),
            kind: 0,
            flags: 0,
            page_size: 0,
            server_code: 0,
            client_kind: 0,
            reserved: [0; 7],
            text_off: 0,
            text_len: 0,
            paging_off: 0,
            paging_len: 0,
            data_off: 0,
            data_len: 0,
        }
    }
}

/// A ring: `len` valid records and `arena_len` used arena bytes. Plain memory, owned by the
/// owner of the instance.
#[repr(C)]
pub struct LatteRing {
    pub len: u32,
    pub arena_len: u32,
    pub records: [LatteRecord; LATTE_RING_RECORDS],
    pub arena: [u8; LATTE_RING_ARENA],
}

impl LatteRing {
    fn boxed() -> Box<LatteRing> {
        // A ring is over 4 MiB, so it is built on the heap, never on the stack.
        let layout = std::alloc::Layout::new::<LatteRing>();
        unsafe {
            let ptr = std::alloc::alloc_zeroed(layout) as *mut LatteRing;
            if ptr.is_null() {
                std::alloc::handle_alloc_error(layout);
            }
            Box::from_raw(ptr)
        }
    }

    fn clear(&mut self) {
        self.len = 0;
        self.arena_len = 0;
    }

    /// Writes bytes straight into the free arena with `write`. Returns `None` when they do not
    /// fit or `write` fails, and leaves the arena as it was.
    fn put_with(
        &mut self,
        write: impl FnOnce(&mut dyn std::io::Write) -> std::io::Result<()>,
    ) -> Option<(u32, u32)> {
        let off = self.arena_len as usize;
        let mut free = &mut self.arena[off..LATTE_RING_ARENA];
        let room = free.len();
        write(&mut free).ok()?;
        let len = room - free.len();
        self.arena_len = (off + len) as u32;
        Some((off as u32, len as u32))
    }

    fn put(&mut self, bytes: &[u8]) -> Option<(u32, u32)> {
        let off = self.arena_len as usize;
        let end = off.checked_add(bytes.len())?;
        if end > LATTE_RING_ARENA {
            return None;
        }
        self.arena[off..end].copy_from_slice(bytes);
        self.arena_len = end as u32;
        Some((off as u32, bytes.len() as u32))
    }
}

/// A run, as seen by a C host. The cancel token sits outside the run so `latte_cancel` never
/// touches memory another thread may be changing.
pub struct LatteRun {
    cancel: CancelToken,
    connection_json: std::ffi::CString,
    run: std::cell::UnsafeCell<Run>,
}

/// One batch of completions from `latte_submit`, linked into the inbox.
struct InboxBatch {
    next: *mut InboxBatch,
    records: Box<[LatteRecord]>,
    bytes: Box<[u8]>,
}

pub struct LatteInstance {
    busy: AtomicBool,
    inbox: AtomicPtr<InboxBatch>,
    reserved: Mutex<HashMap<u64, Box<[u8]>>>,
    state: std::cell::UnsafeCell<PumpState>,
}

struct PumpState {
    instance: Instance,
    ops: Box<LatteRing>,
    completions: Box<LatteRing>,
    pending: Vec<Op>,
    scratch: Vec<Op>,
    decoded: Vec<Completion>,
    in_flight: usize,
}

thread_local! {
    static LAST_ERROR: RefCell<String> = const { RefCell::new(String::new()) };
}

fn set_error(msg: impl Into<String>) {
    let msg = msg.into();
    LAST_ERROR.with(|e| *e.borrow_mut() = msg);
}

fn guard<T>(fallback: T, f: impl FnOnce() -> T) -> T {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(v) => v,
        Err(panic) => {
            let msg = panic
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_else(|| "panic".to_string());
            set_error(format!("panic in latte core: {msg}"));
            fallback
        }
    }
}

/// Returns `LATTE_ABI_VERSION`. Safe from any thread.
#[no_mangle]
pub extern "C" fn latte_abi_version() -> u32 {
    LATTE_ABI_VERSION
}

/// Reads the clock that op times and pump times use, in nanoseconds since the first read in
/// the process. Safe from any thread.
///
/// Hosts must stamp the start and end of every driver call with this function and with no
/// other clock, so the core and the host share one time base. It reads the CPU counter
/// directly through `quanta` (`cntvct_el0` on aarch64, an invariant TSC on x86_64, else
/// `CLOCK_MONOTONIC`): one instruction and a multiply, with no system call.
#[no_mangle]
pub extern "C" fn latte_clock_ns() -> u64 {
    clock::now_ns()
}

/// Copies the last error of the calling thread into `buf` and returns its full length.
/// Safe from any thread; each thread has its own last error.
///
/// # Safety
/// `buf` must point to `cap` writable bytes, or be null with `cap` 0.
#[no_mangle]
pub unsafe extern "C" fn latte_last_error(buf: *mut u8, cap: usize) -> usize {
    LAST_ERROR.with(|e| {
        let msg = e.borrow();
        if !buf.is_null() && cap > 0 {
            let n = msg.len().min(cap);
            ptr::copy_nonoverlapping(msg.as_ptr(), buf, n);
        }
        msg.len()
    })
}

/// Parses the Latte command line. Commands that need no database run to completion and
/// return null with `*exit_code` set. `schema`, `load` and `run` return a run handle.
/// Call it from the thread that will drive the run.
///
/// # Safety
/// `argv` must point to `argc` valid NUL-terminated strings.
#[no_mangle]
pub unsafe extern "C" fn latte_main(
    argc: c_int,
    argv: *const *const c_char,
    exit_code: *mut c_int,
) -> *mut LatteRun {
    guard(ptr::null_mut(), || {
        let args: Vec<String> = (0..argc.max(0) as usize)
            .map(|i| CStr::from_ptr(*argv.add(i)).to_string_lossy().into_owned())
            .collect();
        match run::start(args) {
            Start::Exit(code) => {
                if !exit_code.is_null() {
                    *exit_code = code;
                }
                ptr::null_mut()
            }
            Start::Driver(run) => {
                let json = serde_json::json!({
                    "backend": run::backend().name(),
                    "client_id": run.client_id(),
                    "connection": serde_json::to_value(run.connection()).unwrap_or_default(),
                    "secrets": run::connection_secrets(run.connection()),
                });
                Box::into_raw(Box::new(LatteRun {
                    cancel: run.cancel_token(),
                    connection_json: std::ffi::CString::new(json.to_string()).unwrap_or_default(),
                    run: std::cell::UnsafeCell::new(*run),
                }))
            }
        }
    })
}

/// Connection settings as JSON: `backend` ("cql" or "alternator"), `client_id`,
/// `connection` and `secrets`. A host that does not serve the backend must stop with an
/// error. Valid until `latte_finish`. Call it from the thread that drives the run.
///
/// # Safety
/// `run` must come from `latte_main`.
#[no_mangle]
pub unsafe extern "C" fn latte_connection_json(run: *const LatteRun) -> *const c_char {
    match run.as_ref() {
        Some(run) => run.connection_json.as_ptr(),
        None => ptr::null(),
    }
}

/// Asks the run to stop. A single atomic store: safe from any thread and from a signal
/// handler. Pumps then release no new op, drain the ops in flight, and end with
/// `LATTE_CANCELLED`. `latte_finish` still writes the report, marked cancelled.
///
/// # Safety
/// `run` must come from `latte_main` and not be finished.
#[no_mangle]
pub unsafe extern "C" fn latte_cancel(run: *const LatteRun) {
    if let Some(run) = run.as_ref() {
        run.cancel.cancel();
    }
}

/// Returns 1 after `latte_cancel`, else 0. Safe from any thread.
///
/// # Safety
/// `run` must come from `latte_main` and not be finished.
#[no_mangle]
pub unsafe extern "C" fn latte_is_cancelled(run: *const LatteRun) -> c_int {
    run.as_ref().map_or(0, |r| r.cancel.is_cancelled() as c_int)
}

/// Stores the effective settings of the host's driver, as a JSON document of `len` bytes.
/// The core writes them into the report. Returns `LATTE_OK`, or `LATTE_ERR_ARG` when the
/// text is not valid JSON. Call it from the thread that drives the run.
///
/// # Safety
/// `run` must come from `latte_main`. `json` must point to `len` readable bytes.
#[no_mangle]
pub unsafe extern "C" fn latte_set_driver_settings(
    run: *mut LatteRun,
    json: *const u8,
    len: usize,
) -> c_int {
    let Some(run) = run.as_ref() else {
        return LATTE_ERR_ARG;
    };
    if json.is_null() {
        return LATTE_ERR_ARG;
    }
    guard(LATTE_ERR_PANIC, || {
        let bytes = std::slice::from_raw_parts(json, len);
        match (*run.run.get()).set_driver_settings(bytes) {
            Ok(()) => LATTE_OK,
            Err(e) => {
                set_error(e.to_string());
                LATTE_ERR_ARG
            }
        }
    })
}

/// Starts the next phase. Returns the number of instances to pump, or 0 when the command
/// is complete, failed or cancelled. Call it from the thread that drives the run.
///
/// # Safety
/// `run` must come from `latte_main`.
#[no_mangle]
pub unsafe extern "C" fn latte_begin_phase(run: *mut LatteRun) -> c_int {
    let Some(run) = run.as_ref() else {
        return LATTE_ERR_ARG;
    };
    guard(LATTE_ERR_PANIC, || {
        (*run.run.get())
            .begin_phase()
            .map_or(0, |info| info.instances as c_int)
    })
}

/// Takes instance `index` of the current phase. Returns null if it was taken already.
/// The caller becomes the owner of the instance. Call it from the thread that drives the run.
///
/// # Safety
/// `run` must come from `latte_main`.
#[no_mangle]
pub unsafe extern "C" fn latte_take_instance(
    run: *mut LatteRun,
    index: c_int,
) -> *mut LatteInstance {
    let Some(run) = run.as_ref() else {
        return ptr::null_mut();
    };
    guard(ptr::null_mut(), || {
        match (*run.run.get()).take_instance(index.max(0) as usize) {
            Some(instance) => {
                instance.set_native(false);
                Box::into_raw(Box::new(LatteInstance {
                    busy: AtomicBool::new(false),
                    inbox: AtomicPtr::new(ptr::null_mut()),
                    reserved: Mutex::new(HashMap::new()),
                    state: std::cell::UnsafeCell::new(PumpState {
                        instance,
                        ops: LatteRing::boxed(),
                        completions: LatteRing::boxed(),
                        pending: Vec::with_capacity(LATTE_RING_RECORDS),
                        scratch: Vec::with_capacity(LATTE_RING_RECORDS),
                        decoded: Vec::with_capacity(LATTE_RING_RECORDS),
                        in_flight: 0,
                    }),
                }))
            }
            None => ptr::null_mut(),
        }
    })
}

/// Op ring of an instance. Only the owner reads it, after its own `latte_pump`.
///
/// # Safety
/// `instance` must come from `latte_take_instance`.
#[no_mangle]
pub unsafe extern "C" fn latte_ops(instance: *mut LatteInstance) -> *mut LatteRing {
    match instance.as_ref() {
        Some(i) => &mut *(*i.state.get()).ops as *mut LatteRing,
        None => ptr::null_mut(),
    }
}

/// Completion ring of an instance. Only the owner writes it, before its next `latte_pump`.
///
/// # Safety
/// `instance` must come from `latte_take_instance`.
#[no_mangle]
pub unsafe extern "C" fn latte_completions(instance: *mut LatteInstance) -> *mut LatteRing {
    match instance.as_ref() {
        Some(i) => &mut *(*i.state.get()).completions as *mut LatteRing,
        None => ptr::null_mut(),
    }
}

/// Hands over `n` completions made on any thread. Their ranges point into the `len` bytes at
/// `bytes`, or into a `latte_reserve` buffer when `buffer` is set. The core copies the batch
/// and publishes it with one atomic operation; the next `latte_pump` applies it.
///
/// # Safety
/// `instance` must come from `latte_take_instance`. `records` must point to `n` records and
/// `bytes` to `len` readable bytes (or be null with `len` 0).
#[no_mangle]
pub unsafe extern "C" fn latte_submit(
    instance: *const LatteInstance,
    records: *const LatteRecord,
    n: usize,
    bytes: *const u8,
    len: usize,
) -> c_int {
    let Some(inst) = instance.as_ref() else {
        return LATTE_ERR_ARG;
    };
    if n == 0 {
        return LATTE_OK;
    }
    if records.is_null() || (bytes.is_null() && len > 0) {
        return LATTE_ERR_ARG;
    }
    guard(LATTE_ERR_PANIC, || {
        let batch = Box::into_raw(Box::new(InboxBatch {
            next: ptr::null_mut(),
            records: std::slice::from_raw_parts(records, n).into(),
            bytes: if len > 0 {
                std::slice::from_raw_parts(bytes, len).into()
            } else {
                Box::default()
            },
        }));
        let mut head = inst.inbox.load(Ordering::Relaxed);
        loop {
            (*batch).next = head;
            match inst.inbox.compare_exchange_weak(
                head,
                batch,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return LATTE_OK,
                Err(current) => head = current,
            }
        }
    })
}

/// Allocates a core-owned buffer of `bytes` bytes for the result of op `op_id` and returns
/// its address, or null above `LATTE_MAX_RESULT_BYTES`. The completion of the op points to
/// it through `buffer`; the core frees it after reading the completion. Safe from any thread.
///
/// # Safety
/// `instance` must come from `latte_take_instance`.
#[no_mangle]
pub unsafe extern "C" fn latte_reserve(
    instance: *const LatteInstance,
    op_id: u64,
    bytes: usize,
) -> *mut u8 {
    let Some(inst) = instance.as_ref() else {
        return ptr::null_mut();
    };
    if bytes > LATTE_MAX_RESULT_BYTES {
        set_error(format!(
            "result of {bytes} bytes exceeds LATTE_MAX_RESULT_BYTES"
        ));
        return ptr::null_mut();
    }
    guard(ptr::null_mut(), || {
        let mut buf = vec![0u8; bytes].into_boxed_slice();
        let addr = buf.as_mut_ptr();
        inst.reserved
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(op_id, buf);
        addr
    })
}

/// Applies the completion ring and the inbox, runs the instance and fills the op ring.
/// Clears the completion ring. Returns `LATTE_OK` (running), `LATTE_DRAINING`,
/// `LATTE_DONE` or `LATTE_CANCELLED`, and the next deadline on the shared clock in
/// `*deadline_ns` (0 for none). Only the owner may call it; an overlapping call returns
/// `LATTE_ERR_BUSY`.
///
/// # Safety
/// `instance` must come from `latte_take_instance`. `deadline_ns` may be null.
#[no_mangle]
pub unsafe extern "C" fn latte_pump(
    instance: *mut LatteInstance,
    now_ns: u64,
    deadline_ns: *mut u64,
) -> c_int {
    let Some(inst) = instance.as_ref() else {
        return LATTE_ERR_ARG;
    };
    if inst.busy.swap(true, Ordering::Acquire) {
        set_error("latte_pump called while another latte_pump runs on the same instance");
        return LATTE_ERR_BUSY;
    }
    let result = guard(LATTE_ERR_PANIC, || pump(inst, now_ns, deadline_ns));
    inst.busy.store(false, Ordering::Release);
    result
}

unsafe fn pump(inst: &LatteInstance, now_ns: u64, deadline_ns: *mut u64) -> c_int {
    let state = &mut *inst.state.get();
    state.decoded.clear();
    let ring_len = (state.completions.len as usize).min(LATTE_RING_RECORDS);
    for rec in &state.completions.records[..ring_len] {
        match decode_record(inst, rec, &state.completions.arena) {
            Ok(c) => state.decoded.push(c),
            Err(e) => {
                set_error(e);
                return LATTE_ERR_DECODE;
            }
        }
    }
    state.completions.clear();
    let mut batch = inst.inbox.swap(ptr::null_mut(), Ordering::Acquire);
    while !batch.is_null() {
        let b = Box::from_raw(batch);
        for rec in b.records.iter() {
            match decode_record(inst, rec, &b.bytes) {
                Ok(c) => state.decoded.push(c),
                Err(e) => {
                    set_error(e);
                    return LATTE_ERR_DECODE;
                }
            }
        }
        batch = b.next;
    }
    state.in_flight = state.in_flight.saturating_sub(state.decoded.len());

    let room = LATTE_RING_RECORDS.saturating_sub(state.in_flight + state.pending.len());
    let status =
        state
            .instance
            .pump_limited(now_ns, state.decoded.drain(..), &mut state.scratch, room);
    state.pending.append(&mut state.scratch);
    state.ops.clear();
    let mut written = 0;
    for op in state.pending.iter() {
        if encode_op(&mut state.ops, op).is_none() {
            break;
        }
        written += 1;
    }
    state.pending.drain(..written);
    state.in_flight += written;
    if matches!(status, PumpStatus::Done | PumpStatus::Cancelled) {
        state.in_flight = 0;
    }
    if !deadline_ns.is_null() {
        *deadline_ns = match status {
            PumpStatus::Running { next_deadline_ns }
            | PumpStatus::Draining { next_deadline_ns } => next_deadline_ns.unwrap_or(0),
            PumpStatus::Done | PumpStatus::Cancelled => 0,
        };
    }
    match status {
        PumpStatus::Running { .. } => LATTE_OK,
        PumpStatus::Draining { .. } => LATTE_DRAINING,
        PumpStatus::Done if state.pending.is_empty() => LATTE_DONE,
        PumpStatus::Done => LATTE_OK,
        PumpStatus::Cancelled => LATTE_CANCELLED,
    }
}

/// Frees an instance after its last pump. Called by the owner.
///
/// # Safety
/// `instance` must come from `latte_take_instance` and not be used afterwards.
#[no_mangle]
pub unsafe extern "C" fn latte_instance_free(instance: *mut LatteInstance) {
    if instance.is_null() {
        return;
    }
    guard((), || {
        let inst = Box::from_raw(instance);
        let mut batch = inst.inbox.swap(ptr::null_mut(), Ordering::Acquire);
        while !batch.is_null() {
            let b = Box::from_raw(batch);
            batch = b.next;
        }
    });
}

/// Ends the current phase. Returns `LATTE_OK`, or 1 when the phase failed or the run was
/// cancelled. Call it from the thread that drives the run.
///
/// # Safety
/// `run` must come from `latte_main`.
#[no_mangle]
pub unsafe extern "C" fn latte_end_phase(run: *mut LatteRun) -> c_int {
    let Some(run) = run.as_ref() else {
        return LATTE_ERR_ARG;
    };
    guard(LATTE_ERR_PANIC, || {
        if (*run.run.get()).end_phase() {
            0
        } else {
            1
        }
    })
}

/// Prints the report, frees the run and returns the process exit code. Call it from the
/// thread that drives the run, after every instance is freed.
///
/// # Safety
/// `run` must come from `latte_main` and not be used afterwards.
#[no_mangle]
pub unsafe extern "C" fn latte_finish(run: *mut LatteRun) -> c_int {
    if run.is_null() {
        return LATTE_ERR_ARG;
    }
    guard(LATTE_ERR_PANIC, || {
        Box::from_raw(run).run.into_inner().finish()
    })
}

/// Writes one op into the op ring. Returns `None` when the ring or its arena is full.
pub fn encode_op(ring: &mut LatteRing, op: &Op) -> Option<()> {
    if ring.len as usize >= LATTE_RING_RECORDS {
        return None;
    }
    let mark = ring.arena_len;
    let mut rec = LatteRecord {
        id: op.id,
        send_at_ns: op.send_at_ns,
        deadline_ns: op.deadline_ns,
        kind: op.kind.code(),
        ..LatteRecord::default()
    };
    let ok = (|| {
        match &op.kind {
            OpKind::Prepare {
                stmt,
                cql,
                page_size,
            } => {
                rec.stmt = *stmt;
                rec.page_size = *page_size;
                (rec.text_off, rec.text_len) = ring.put(cql.as_bytes())?;
            }
            OpKind::Query {
                cql,
                page_size,
                paging_state,
                rows,
            } => {
                rec.page_size = *page_size;
                rec.flags = *rows as u32;
                (rec.text_off, rec.text_len) = ring.put(cql.as_bytes())?;
                if let Some(p) = paging_state {
                    (rec.paging_off, rec.paging_len) = ring.put(p)?;
                }
            }
            OpKind::Execute {
                stmt,
                values,
                paging_state,
                rows,
            } => {
                rec.stmt = *stmt;
                rec.flags = *rows as u32;
                if let Some(p) = paging_state {
                    (rec.paging_off, rec.paging_len) = ring.put(p)?;
                }
                (rec.data_off, rec.data_len) = put_values(ring, &*values.to_serialized().ok()?)?;
            }
            OpKind::Batch { statements } => {
                rec.data_off = ring.arena_len;
                for (stmt, values) in statements {
                    ring.put(&stmt.to_be_bytes())?;
                    ring.put(&(values.element_count() as u16).to_be_bytes())?;
                    put_values(ring, &*values.to_serialized().ok()?)?;
                }
                rec.data_len = ring.arena_len - rec.data_off;
            }
            OpKind::Datacenters => {}
            OpKind::Alternator { op, body } => {
                (rec.text_off, rec.text_len) = ring.put(op.name().as_bytes())?;
                (rec.data_off, rec.data_len) = match body {
                    crate::ops::Json::Request(request) => {
                        ring.put_with(|out| request.write_json(out))?
                    }
                    other => ring.put(other.text().as_bytes())?,
                };
            }
        }
        Some(())
    })();
    if ok.is_none() {
        ring.arena_len = mark;
        return None;
    }
    ring.records[ring.len as usize] = rec;
    ring.len += 1;
    Some(())
}

/// Writes the CQL `[value]` sequence of `values` into the arena, without its count.
fn put_values(
    ring: &mut LatteRing,
    values: &scylla_cql_core::serialize::row::SerializedValues,
) -> Option<(u32, u32)> {
    use scylla_cql_core::frame::types::RawValue;
    let start = ring.arena_len;
    for value in values.iter() {
        match value {
            RawValue::Null => ring.put(&(-1i32).to_be_bytes())?,
            RawValue::Unset => ring.put(&(-2i32).to_be_bytes())?,
            RawValue::Value(bytes) => {
                ring.put(&(bytes.len() as i32).to_be_bytes())?;
                ring.put(bytes)?
            }
        };
    }
    Some((start, ring.arena_len - start))
}

fn range(bytes: &[u8], off: u32, len: u32) -> Result<&[u8], String> {
    let off = off as usize;
    let end = off.checked_add(len as usize).ok_or("bad range")?;
    bytes
        .get(off..end)
        .ok_or_else(|| "range outside its buffer".to_string())
}

/// Decodes one completion. Ranges point into `arena`, or into the reserved buffer of the op
/// when `buffer` is set; the reserved buffer is freed afterwards.
fn decode_record(
    inst: &LatteInstance,
    rec: &LatteRecord,
    arena: &[u8],
) -> Result<Completion, String> {
    let reserved = if rec.buffer.is_null() {
        None
    } else {
        let buf = inst
            .reserved
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&rec.id)
            .ok_or_else(|| format!("op {} has no reserved buffer", rec.id))?;
        if !std::ptr::eq(buf.as_ptr(), rec.buffer) {
            return Err(format!(
                "op {} points to a buffer it did not reserve",
                rec.id
            ));
        }
        Some(buf)
    };
    let bytes = reserved.as_deref().unwrap_or(arena);
    decode_completion(rec, bytes)
}

/// Decodes one completion whose ranges point into `bytes`.
pub fn decode_completion(rec: &LatteRecord, bytes: &[u8]) -> Result<Completion, String> {
    let text = range(bytes, rec.text_off, rec.text_len)?;
    let paging = range(bytes, rec.paging_off, rec.paging_len)?;
    let data = range(bytes, rec.data_off, rec.data_len)?;
    let result = match rec.kind {
        LATTE_STATUS_OK => Ok(decode_response(rec, paging, data)?),
        _ => {
            let class = String::from_utf8_lossy(text);
            let message = String::from_utf8_lossy(data);
            let kind = ClientKind::from_u8(rec.client_kind);
            Err(if rec.server_code == 0 && is_dynamodb_type(&class) {
                DriverError::dynamodb(kind, class, message)
            } else {
                DriverError::new(rec.server_code, kind, class, message)
            })
        }
    };
    Ok(Completion {
        id: rec.id,
        start_ns: rec.start_ns,
        end_ns: rec.end_ns,
        result,
    })
}

/// DynamoDB error types end in `Exception` or are one of a few bare names.
fn is_dynamodb_type(class: &str) -> bool {
    let name = class.rsplit('#').next().unwrap_or(class);
    name.ends_with("Exception")
        || matches!(
            name,
            "RequestLimitExceeded" | "InternalServerError" | "ServiceUnavailable"
        )
}

fn decode_response(rec: &LatteRecord, paging: &[u8], data: &[u8]) -> Result<Response, String> {
    let mut buf = data;
    Ok(match rec.flags {
        LATTE_RESP_PREPARED => Response::Prepared {
            bind_specs: read_specs(&mut buf)?,
        },
        LATTE_RESP_PAGE => {
            let specs = if buf.is_empty() {
                Vec::new()
            } else {
                read_specs(&mut buf)?
            };
            Response::Page {
                rows: Rows {
                    row_count: rec.rows,
                    specs,
                    data: if buf.is_empty() {
                        Vec::new()
                    } else {
                        buf.to_vec()
                    },
                    objects: None,
                },
                paging_state: (!paging.is_empty()).then(|| paging.to_vec()),
            }
        }
        LATTE_RESP_DATACENTERS => {
            let n = read_u16(&mut buf)?;
            let mut names = Vec::with_capacity(n as usize);
            for _ in 0..n {
                names.push(read_string(&mut buf)?);
            }
            Response::Datacenters(names)
        }
        LATTE_RESP_JSON => Response::Json(crate::ops::Json::Text(
            String::from_utf8_lossy(data).into_owned(),
        )),
        _ => Response::Done,
    })
}

fn read_u16(buf: &mut &[u8]) -> Result<u16, String> {
    let (head, rest) = buf.split_at_checked(2).ok_or("truncated u16")?;
    *buf = rest;
    Ok(u16::from_be_bytes([head[0], head[1]]))
}

fn read_string(buf: &mut &[u8]) -> Result<String, String> {
    let len = read_u16(buf)? as usize;
    let (head, rest) = buf.split_at_checked(len).ok_or("truncated string")?;
    *buf = rest;
    String::from_utf8(head.to_vec()).map_err(|e| e.to_string())
}

/// Column specs: `u16` count, then per column its name as a CQL `[string]` and its type as
/// a CQL `[option]`, the same encoding the native protocol uses in result metadata.
fn read_specs(buf: &mut &[u8]) -> Result<Vec<ColumnSpec<'static>>, String> {
    let n = read_u16(buf)?;
    let mut specs = Vec::with_capacity(n as usize);
    for _ in 0..n {
        let name = read_string(buf)?;
        let typ = read_type(buf)?;
        specs.push(ColumnSpec::owned(
            name,
            typ,
            TableSpec::owned(String::new(), String::new()),
        ));
    }
    Ok(specs)
}

fn read_type(buf: &mut &[u8]) -> Result<ColumnType<'static>, String> {
    use NativeType::*;
    let id = read_u16(buf)?;
    let native = |t| Ok(ColumnType::Native(t));
    match id {
        0x0001 => native(Ascii),
        0x0002 => native(BigInt),
        0x0003 => native(Blob),
        0x0004 => native(Boolean),
        0x0005 => native(Counter),
        0x0006 => native(Decimal),
        0x0007 => native(Double),
        0x0008 => native(Float),
        0x0009 => native(Int),
        0x000B => native(Timestamp),
        0x000C => native(Uuid),
        0x000D => native(Text),
        0x000E => native(Varint),
        0x000F => native(Timeuuid),
        0x0010 => native(Inet),
        0x0011 => native(Date),
        0x0012 => native(Time),
        0x0013 => native(SmallInt),
        0x0014 => native(TinyInt),
        0x0015 => native(Duration),
        0x0020 => Ok(ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(Box::new(read_type(buf)?)),
        }),
        0x0021 => {
            let k = read_type(buf)?;
            let v = read_type(buf)?;
            Ok(ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Map(Box::new(k), Box::new(v)),
            })
        }
        0x0022 => Ok(ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Set(Box::new(read_type(buf)?)),
        }),
        0x0030 => {
            let keyspace = read_string(buf)?;
            let name = read_string(buf)?;
            let n = read_u16(buf)?;
            let mut fields = Vec::with_capacity(n as usize);
            for _ in 0..n {
                let field = read_string(buf)?;
                fields.push((field.into(), read_type(buf)?));
            }
            Ok(ColumnType::UserDefinedType {
                frozen: false,
                definition: std::sync::Arc::new(UserDefinedType {
                    name: name.into(),
                    keyspace: keyspace.into(),
                    field_types: fields,
                }),
            })
        }
        0x0031 => {
            let n = read_u16(buf)?;
            let mut types = Vec::with_capacity(n as usize);
            for _ in 0..n {
                types.push(read_type(buf)?);
            }
            Ok(ColumnType::Tuple(types))
        }
        other => Err(format!("unsupported CQL type id {other:#06x}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops::AlternatorOp;
    #[cfg(feature = "cql")]
    use std::ffi::CString;
    #[cfg(feature = "cql")]
    use std::sync::mpsc;

    #[cfg(feature = "cql")]
    /// Starts `latte run` of `script` through the C ABI and returns the run handle.
    unsafe fn start_run(script: &str, cycles: &str) -> *mut LatteRun {
        let args: Vec<CString> = [
            "latte",
            "run",
            script,
            "127.0.0.1:1",
            "-d",
            cycles,
            "-w",
            "0",
            "-q",
        ]
        .iter()
        .map(|a| CString::new(*a).unwrap())
        .collect();
        let argv: Vec<*const c_char> = args.iter().map(|a| a.as_ptr()).collect();
        let mut code = 0;
        let run = latte_main(argv.len() as c_int, argv.as_ptr(), &mut code);
        assert!(!run.is_null(), "latte_main exited with {code}");
        run
    }

    #[cfg(feature = "cql")]
    fn query_script(dir: &std::path::Path) -> String {
        let path = dir.join("query.rn");
        std::fs::write(
            &path,
            "pub async fn run(ctx, i) {\n    ctx.execute(\"SELECT 1\").await?;\n}\n",
        )
        .unwrap();
        path.to_string_lossy().into_owned()
    }

    #[cfg(feature = "cql")]
    fn ok_page(id: u64) -> LatteRecord {
        LatteRecord {
            id,
            kind: LATTE_STATUS_OK,
            flags: LATTE_RESP_PAGE,
            start_ns: latte_clock_ns(),
            end_ns: latte_clock_ns(),
            ..LatteRecord::default()
        }
    }

    #[cfg(feature = "cql")]
    #[test]
    fn an_overlapping_pump_returns_busy() {
        let dir = tempfile::tempdir().unwrap();
        let script = query_script(dir.path());
        unsafe {
            let run = start_run(&script, "10");
            assert!(latte_begin_phase(run) > 0);
            let inst = latte_take_instance(run, 0);
            // Another thread is inside latte_pump on this instance.
            (*inst).busy.store(true, Ordering::Relaxed);
            assert_eq!(latte_pump(inst, 0, ptr::null_mut()), LATTE_ERR_BUSY);
            (*inst).busy.store(false, Ordering::Relaxed);
            assert!(latte_pump(inst, 0, ptr::null_mut()) >= 0);
            latte_cancel(run);
            while latte_pump(inst, 0, ptr::null_mut()) != LATTE_CANCELLED {}
            latte_instance_free(inst);
            latte_end_phase(run);
            latte_finish(run);
        }
    }

    // CQL script; the Alternator build runs DynamoDB ops instead.
    #[cfg(feature = "cql")]
    /// Writer threads complete ops through latte_submit while the owner pumps. Every cycle
    /// must finish exactly once.
    #[test]
    fn writers_on_many_threads_complete_through_the_inbox() {
        const WRITERS: usize = 4;
        const CYCLES: u64 = 20_000;
        let dir = tempfile::tempdir().unwrap();
        let script = query_script(dir.path());
        for _ in 0..3 {
            unsafe {
                let run = start_run(&script, &CYCLES.to_string());
                let mut pumped_cycles = 0u64;
                while latte_begin_phase(run) > 0 {
                    let inst = latte_take_instance(run, 0);
                    let addr = inst as usize;
                    let (senders, receivers): (Vec<_>, Vec<_>) =
                        (0..WRITERS).map(|_| mpsc::channel::<Vec<u64>>()).unzip();
                    std::thread::scope(|s| {
                        for rx in receivers {
                            s.spawn(move || {
                                let inst = addr as *const LatteInstance;
                                for ids in rx {
                                    let records: Vec<_> = ids.into_iter().map(ok_page).collect();
                                    let rc = latte_submit(
                                        inst,
                                        records.as_ptr(),
                                        records.len(),
                                        ptr::null(),
                                        0,
                                    );
                                    assert_eq!(rc, LATTE_OK);
                                }
                            });
                        }
                        let mut next = 0;
                        loop {
                            let rc = latte_pump(inst, 0, ptr::null_mut());
                            assert!(rc >= 0, "pump failed with {rc}");
                            let ring = &*latte_ops(inst);
                            let ids: Vec<u64> = ring.records[..ring.len as usize]
                                .iter()
                                .map(|r| r.id)
                                .collect();
                            pumped_cycles += ids.len() as u64;
                            if !ids.is_empty() {
                                senders[next % WRITERS].send(ids).unwrap();
                                next += 1;
                            }
                            if rc == LATTE_DONE {
                                break;
                            }
                            std::thread::yield_now();
                        }
                        drop(senders);
                    });
                    latte_instance_free(inst);
                    assert_eq!(latte_end_phase(run), 0);
                }
                assert_eq!(latte_finish(run), 0);
                // One cluster-info query, then one query per cycle.
                assert_eq!(pumped_cycles, 1 + CYCLES);
            }
        }
    }

    #[test]
    fn dynamodb_kind_codes_match_the_header_constants() {
        assert_eq!(AlternatorOp::CreateTable.code(), LATTE_OP_DDB_CREATE_TABLE);
        assert_eq!(
            AlternatorOp::DescribeEndpoints.code(),
            LATTE_OP_DDB_DESCRIBE_ENDPOINTS
        );
        assert_eq!(AlternatorOp::Query.code(), LATTE_OP_DDB_QUERY);
    }

    #[test]
    fn error_completion_decodes_into_its_category() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"ReadTimeout");
        bytes.extend_from_slice(b"timed out");
        let rec = LatteRecord {
            id: 7,
            kind: LATTE_STATUS_ERROR,
            server_code: crate::ops::codes::READ_TIMEOUT,
            text_off: 0,
            text_len: 11,
            data_off: 11,
            data_len: 9,
            ..LatteRecord::default()
        };
        let c = decode_completion(&rec, &bytes).unwrap();
        let e = c.result.unwrap_err();
        assert_eq!(e.category, crate::ops::ErrorCategory::ReadTimeout);
        assert_eq!(e.message, "timed out");
    }
}
