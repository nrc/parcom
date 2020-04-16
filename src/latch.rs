use std::{fmt, sync::Mutex, thread, time::Duration};

pub struct Latch<'a, const N: usize> {
    key: usize,
    latches: &'a Mutex<Box<[bool; { N }]>>,
}

pub fn block_on_latch<'a, T: Into<usize> + Copy + fmt::Debug, const N: usize>(
    latches: &'a Mutex<Box<[bool; { N }]>>,
    key: T,
) -> Result<Latch<'a, { N }>, String> {
    let mut sleep_count = 100;
    let key = key.into();
    loop {
        if sleep_count == 0 {
            return Err(format!("Timed out waiting for latch {:?}", key));
        }
        {
            let mut latches = latches.lock().unwrap();
            if !latches[key] {
                // eprintln!("Latch {:?}", key);
                latches[key] = true;
                break;
            }
        }
        thread::sleep(Duration::from_millis(20));
        sleep_count -= 1;
    }

    Ok(Latch { key, latches })
}

impl<'a, const N: usize> Drop for Latch<'a, { N }> {
    fn drop(&mut self) {
        // eprintln!("drop latch for {:?}", self.key);
        let mut latches = self.latches.lock().unwrap();
        latches[self.key] = false;
    }
}
