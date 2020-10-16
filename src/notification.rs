use std::sync::{Condvar, Mutex};

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

    pub fn wait(&self) {
        let (lock, cvar) = &self.inner;
        let mut notified = lock.lock().unwrap();
        while !*notified {
            notified = cvar.wait(notified).unwrap();
        }
    }
}
