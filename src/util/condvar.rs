#![allow(unused)]

use tokio::sync::{Mutex, MutexGuard, Notify};

#[derive(Debug)]
pub struct Condvar {
    notifier: Notify,
}

impl Condvar {
    pub fn new() -> Self {
        return Condvar {
            notifier: Notify::new(),
        };
    }

    pub async fn wait<'a, 'b, T>(
        &self,
        lock: MutexGuard<'a, T>,
        mutex: &'b Mutex<T>,
    ) -> MutexGuard<'b, T> {
        drop(lock);
        self.notifier.notified().await;
        mutex.lock().await
    }

    pub fn notify_waiters<'a, T>(&self, lock: MutexGuard<'a, T>) {
        drop(lock);
        self.notifier.notify_waiters();
    }
}
