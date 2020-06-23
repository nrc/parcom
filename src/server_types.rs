use crate::*;
use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

#[derive(Default, Clone)]
pub struct Record {
    pub values: HashMap<Ts, Value>,
    // commit ts, start ts; ordered by commit ts, descending.
    pub writes: Vec<(Ts, Ts)>,
    pub lock: Option<Lock>,
    pub history: Vec<(Lock, bool)>,
}

impl Record {
    pub fn assert_lock(&self) -> &Lock {
        self.lock.as_ref().unwrap()
    }

    pub fn push_write(&mut self, start_ts: Ts, commit_ts: Ts) {
        self.writes.insert(0, (commit_ts, start_ts));
    }

    pub fn if_locked_by(&mut self, id: TxnId, f: impl FnOnce(&mut Lock)) -> bool {
        match &mut self.lock {
            Some(lock) if lock.id == id => {
                f(lock);
                true
            }
            _ => {
                // Key has been unlocked (and possibly re-locked), which means the transaction
                // has been rolled back.
                false
            }
        }
    }

    pub fn lock(&mut self, lock: Lock) {
        assert!(self.lock.is_none());
        self.lock = Some(lock.clone());
        self.history.push((lock, true));
    }

    pub fn unlock(&mut self) {
        self.history.push((self.lock.take().unwrap(), false));
    }
}

#[derive(Clone, Debug)]
pub struct TxnRecord {
    pub keys: HashMap<Key, LockStatus>,
    pub commit_state: TxnState,
    pub commit_ts: Option<Ts>,
    pub start_ts: Ts,
    pub timeout: Instant,
}

impl TxnRecord {
    pub fn new(start_ts: Ts, timeout: Instant) -> TxnRecord {
        TxnRecord {
            keys: HashMap::new(),
            commit_state: TxnState::Local,
            commit_ts: None,
            start_ts,
            timeout,
        }
    }

    pub fn update_timeout(&mut self, timeout: Instant) {
        if self.timeout < timeout {
            self.timeout = timeout;
        }
    }

    pub fn add_key(&mut self, k: &Key) {
        if !self.keys.contains_key(k) {
            self.keys.insert(*k, LockStatus::Unknown);
        }
    }

    pub fn unlock_keys(
        &mut self,
        f: impl Fn(Key, LockStatus) -> Result<(), String>,
    ) -> Result<(), String> {
        for (&k, s) in &mut self.keys {
            if *s == LockStatus::Unlocked {
                continue;
            }
            f(k, *s)?;
            *s = LockStatus::Unlocked
        }
        Ok(())
    }

    pub fn open(&self) -> bool {
        self.commit_state == TxnState::Local || self.commit_state == TxnState::Consensus
    }
}

#[derive(Debug, Clone)]
pub struct Lock {
    pub id: TxnId,
    pub state: LockState,
    pub kind: LockKind,
}

impl Lock {
    pub fn new(id: TxnId, kind: LockKind) -> Lock {
        Lock {
            id,
            state: LockState::Local,
            kind,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum LockStatus {
    Locked(LockState),
    Unlocked,
    Unknown,
}

impl LockStatus {
    pub fn local() -> LockStatus {
        LockStatus::Locked(LockState::Local)
    }

    pub fn has_consensus(self) -> bool {
        match self {
            LockStatus::Locked(LockState::Consensus) => true,
            _ => false,
        }
    }
}

// Invariant: never change Consensus -> Local.
// Invariant: to change Local -> Consensus the thread must hold the latch for
// the lock's key and the key's transaction record.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum LockState {
    Local,
    Consensus,
}

// Valid state changes: Local -> Consensus -> Finished,
//                      Local -> RolledBack.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum TxnState {
    Local,
    Consensus,
    RollingBack,
    Finished,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum LockKind {
    Read,
    Modify,
}

pub struct Statistics {
    committed_txns: AtomicU64,
    rolled_back_txns: AtomicU64,
    reads: AtomicU64,
    writes: AtomicU64,
    failed_rws: AtomicU64,
}

impl Statistics {
    pub fn new() -> Statistics {
        Statistics {
            committed_txns: AtomicU64::new(0),
            rolled_back_txns: AtomicU64::new(0),
            reads: AtomicU64::new(0),
            writes: AtomicU64::new(0),
            failed_rws: AtomicU64::new(0),
        }
    }

    pub fn committed_txn(&self) {
        self.committed_txns.fetch_add(1, Ordering::SeqCst);
    }

    pub fn rolled_back_txn(&self) {
        self.rolled_back_txns.fetch_add(1, Ordering::SeqCst);
    }

    pub fn read(&self) {
        self.reads.fetch_add(1, Ordering::SeqCst);
    }

    pub fn write(&self) {
        self.writes.fetch_add(1, Ordering::SeqCst);
    }

    pub fn failed_rw(&self) {
        self.failed_rws.fetch_add(1, Ordering::SeqCst);
    }

    pub fn print(&self) {
        println!("\nTransactions:");
        println!("  committed {}", self.committed_txns.load(Ordering::SeqCst));
        println!(
            "  rolled_back {}",
            self.rolled_back_txns.load(Ordering::SeqCst)
        );
        println!("  total {}", TXNS);
        println!("Reads: {}", self.reads.load(Ordering::SeqCst));
        println!("Writes: {}", self.writes.load(Ordering::SeqCst));
        println!("Failed: {}\n", self.failed_rws.load(Ordering::SeqCst));
    }
}
