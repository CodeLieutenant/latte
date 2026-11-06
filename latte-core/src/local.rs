//! Single-owner cells for state that lives on one instance.
//!
//! An instance has one owner at a time, and the host's handoff between pump calls orders the
//! memory. The state of an instance therefore needs exclusion only against re-entrant use on
//! its own thread, which a plain flag gives without an atomic operation.

use std::cell::{Cell, UnsafeCell};
use std::ops::{Deref, DerefMut};

/// A lock with the interface of `try_lock::TryLock` and no atomic operations.
pub struct TryLock<T> {
    locked: Cell<bool>,
    value: UnsafeCell<T>,
}

// The value moves with the instance that owns it, and is only touched by the owner.
unsafe impl<T: Send> Send for TryLock<T> {}
unsafe impl<T: Send> Sync for TryLock<T> {}

impl<T> TryLock<T> {
    pub const fn new(value: T) -> Self {
        TryLock {
            locked: Cell::new(false),
            value: UnsafeCell::new(value),
        }
    }

    /// Returns a guard, or `None` when a guard of this lock is alive.
    pub fn try_lock(&self) -> Option<Locked<'_, T>> {
        if self.locked.replace(true) {
            return None;
        }
        Some(Locked { lock: self })
    }

    pub fn into_inner(self) -> T {
        self.value.into_inner()
    }
}

impl<T: Default> Default for TryLock<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

pub struct Locked<'a, T> {
    lock: &'a TryLock<T>,
}

impl<T> Deref for Locked<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        unsafe { &*self.lock.value.get() }
    }
}

impl<T> DerefMut for Locked<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.lock.value.get() }
    }
}

impl<T> Drop for Locked<'_, T> {
    fn drop(&mut self) {
        self.lock.locked.set(false);
    }
}

#[cfg(test)]
mod tests {
    use super::TryLock;

    #[test]
    fn a_second_guard_is_refused_while_the_first_lives() {
        let lock = TryLock::new(1);
        let mut first = lock.try_lock().unwrap();
        *first += 1;
        assert!(lock.try_lock().is_none());
        drop(first);
        assert_eq!(*lock.try_lock().unwrap(), 2);
    }
}
