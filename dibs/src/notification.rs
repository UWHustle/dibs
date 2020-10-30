use std::sync::{Condvar, Mutex, WaitTimeoutResult};
use std::time::Duration;

pub struct Notification {
    inner: (Mutex<bool>, Condvar),
}

impl Notification {
    pub fn new() -> Notification {
        Notification {
            inner: (Mutex::new(false), Condvar::new()),
        }
    }

    pub fn notify(&self) {
        let (lock, cvar) = &self.inner;
        let mut notified = lock.lock().unwrap();
        *notified = true;
        cvar.notify_all();
    }

    pub fn wait_timeout(&self, timeout: Duration) -> WaitTimeoutResult {
        let (lock, cvar) = &self.inner;
        cvar.wait_timeout_while(lock.lock().unwrap(), timeout, |notified| *notified)
            .unwrap()
            .1
    }
}
