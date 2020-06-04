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

#[derive(Clone, Debug)]
pub struct TxnRecord {
    // TODO update comment
    // Lock state is none if the lock is not held (mid-rollback, mid-finalisation, or txn is finished)
    // or the state of the lock is unknown.
    // Otherwise LockState::Consensus => key's lock.state == LockState::Consensus.
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
}

#[derive(Debug, Clone)]
pub struct Lock {
    pub id: TxnId,
    pub state: LockState,
    pub kind: LockKind,
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
    pub committed_txns: AtomicU64,
    pub rolled_back_txns: AtomicU64,
    pub reads: AtomicU64,
    pub writes: AtomicU64,
    pub failed_rws: AtomicU64,
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
