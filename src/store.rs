macro_rules! store {
    ($name: ident, $T: ty, $Id: ty, $size: ident) => {
        struct $name {
            entries: UnsafeCell<Box<[Option<$T>; $size]>>,
            latches: Mutex<Box<[bool; $size]>>,
        }

        #[allow(dead_code)]
        impl $name {
            fn new() -> $name {
                $name {
                    entries: UnsafeCell::new(Box::new([None; $size])),
                    latches: Mutex::new(Box::new([false; $size])),
                }
            }

            fn unlocked(&self) -> bool {
                let latches = self.latches.lock().unwrap();
                for &l in &**latches as &[bool] {
                    if l {
                        return false;
                    }
                }
                true
            }

            // The caller should not hold any latches when calling this function.
            fn get_or_else<'a, F>(
                &'a self,
                id: $Id,
                or_else: F,
            ) -> (latch::Latch<'a, $size>, &mut $T)
            where
                F: FnOnce() -> $T,
            {
                loop {
                    if let Ok(latch) = latch::block_on_latch(&self.latches, id) {
                        let id: usize = id.into();
                        unsafe {
                            let map = self.entries.get().as_mut().unwrap();
                            if let Some(ref mut entry) = map[id] {
                                return (latch, entry);
                            }
                            map[id] = Some(or_else());
                            return (latch, map[id].as_mut().unwrap());
                        }
                    }
                    thread::sleep(Duration::from_millis(50));
                }
            }

            // The caller should not hold any latches when calling this function.
            fn try_get<'a>(
                &'a self,
                id: $Id,
            ) -> Result<Option<(latch::Latch<'a, $size>, &mut $T)>, String> {
                latch::block_on_latch(&self.latches, id).map(|latch| {
                    return unsafe {
                        self.entries.get().as_mut().unwrap()[id.into(): usize]
                            .as_mut()
                            .map(|record| (latch, record))
                    };
                })
            }

            // The caller should not hold any latches when calling this function.
            fn blocking_get<'a>(&'a self, id: $Id) -> Option<(latch::Latch<'a, $size>, &mut $T)> {
                loop {
                    if let Ok(latch) = latch::block_on_latch(&self.latches, id) {
                        return unsafe {
                            self.entries.get().as_mut().unwrap()[id.into(): usize]
                                .as_mut()
                                .map(|record| (latch, record))
                        };
                    }
                    thread::sleep(Duration::from_millis(50));
                }
            }

            fn remove(&self, id: $Id) {
                loop {
                    if let Ok(_latch) = latch::block_on_latch(&self.latches, id) {
                        unsafe {
                            self.entries.get().as_mut().unwrap()[id.into(): usize] = None;
                        }
                        return;
                    }
                    thread::sleep(Duration::from_millis(50));
                }
            }
        }

        unsafe impl Sync for $name {}
    };
}
