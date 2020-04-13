use std::{collections::HashSet, hash::Hash, sync::Mutex, thread, time::Duration};

pub struct Latch<'a, T: Eq + Hash> {
    key: T,
    latches: &'a Mutex<HashSet<T>>,
}

pub fn block_on_latch<'a, T: Copy + Eq + Hash>(
    latches: &'a Mutex<HashSet<T>>,
    key: T,
) -> Latch<'a, T> {
    loop {
        {
            let mut latches = latches.lock().unwrap();
            if !latches.contains(&key) {
                latches.insert(key);
                break;
            }
        }
        thread::sleep(Duration::from_millis(20));
    }

    Latch { key, latches }
}

impl<'a, T: Eq + Hash> Drop for Latch<'a, T> {
    fn drop(&mut self) {
        let mut latches = self.latches.lock().unwrap();
        latches.remove(&self.key);
    }
}
