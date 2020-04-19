use crate::{messages::MsgRequest, *};
use rand::{self, Rng};
use std::{
    any,
    cell::UnsafeCell,
    collections::HashMap,
    fmt,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

pub struct Server {
    // We use txn records indexed by start TS, rather than storing this info on the primary lock.
    // The two approaches are equivalent.
    txns: TxnStore,
    keys: KvStore,
    transport: transport::TransportSend,
    thread_count: AtomicU64,
    stats: Statistics,
}

impl Server {
    pub fn new(transport: transport::TransportSend) -> Server {
        Server {
            transport,
            txns: TxnStore::new(),
            keys: KvStore::new(),
            thread_count: AtomicU64::new(0),
            stats: Statistics::new(),
        }
    }

    pub fn shutdown(&self) {
        assert!(self.txns.unlocked());
        assert!(self.keys.unlocked());

        for k in 0..MAX_KEY {
            if let Some((_latch, record)) = self.keys.blocking_get(Key(k)) {
                assert!(record.lock.is_none(), "Key({:?}) locked", k)
            }
        }

        self.transport.shutdown();
    }

    fn handle_lock(&self, msg: Box<messages::LockRequest>) {
        // eprintln!("lock {:?} {:?}", msg.key, msg.id);
        {
            let (_latch, txn_record) = self
                .txns
                .get_or_else(msg.id, || TxnRecord::new(msg.start_ts, msg.timeout));
            if txn_record.commit_state == TxnState::RolledBack {
                return;
            }
        }
        {
            // TODO record writes or locks or both?
            if self.txn_contains_key(msg.id, msg.key, msg.start_ts, msg.timeout) {
                // Check we have locked this key.
                let (_latch, record) = self.keys.get_or_else(msg.key, Record::default);
                assert_eq!(record.lock.as_ref().expect("Missing lock").id, msg.id);
            } else {
                // TODO check key as if we were going to read it and perhaps recover.
                // TODO lock before read
                self.read(msg.key, msg.id);
                if let Err(s) = self.aquire_lock_and_set_value(msg.key, msg.id, msg.start_ts, None)
                {
                    self.stats.failed_rws.fetch_add(1, Ordering::SeqCst);
                    self.fail(&*msg, s);
                    return;
                }
                let (_latch, txn_record) = self.txns.blocking_get(msg.id).unwrap();
                assert_eq!(msg.start_ts, txn_record.start_ts);
                txn_record.keys.insert(msg.key, TxnState::Local);
                if txn_record.timeout < msg.timeout {
                    txn_record.timeout = msg.timeout;
                }
            }
        }

        self.ack(&*msg);
        self.async_consensus_write_lock(&msg);
    }

    fn handle_prewrite(&self, msg: Box<messages::PrewriteRequest>) {
        // eprintln!("prewrite {:?} {:?}", msg.id, msg.writes);
        {
            let (_latch, txn_record) = self
                .txns
                .get_or_else(msg.id, || TxnRecord::new(msg.start_ts, msg.timeout));
            if txn_record.commit_state == TxnState::RolledBack {
                return;
            }
        }
        let mut locked = Vec::new();
        for &(k, v) in &msg.writes {
            // eprintln!("prewrite key {:?} {:?}", msg.id, k);
            if self.txn_contains_key(msg.id, k, msg.start_ts, msg.timeout) {
                // Key is already locked, just need to write the value.
                let (_latch, record) = self.keys.get_or_else(k, Record::default);
                assert!(record.lock.as_ref().unwrap().id == msg.id);
                record.values.insert(msg.start_ts, v);
            } else {
                // Lock and set value.
                if let Err(s) = self.aquire_lock_and_set_value(k, msg.id, msg.start_ts, Some(v)) {
                    // eprintln!("Failed to lock {:?} {:?}", k, msg.id);
                    while let Err(_) = self.abort_prewrite(msg.id, &locked) {
                        thread::sleep(Duration::from_millis(50));
                    }
                    self.stats.failed_rws.fetch_add(1, Ordering::SeqCst);
                    self.fail(&*msg, s);
                    return;
                }
                let (_latch, txn_record) = self.txns.blocking_get(msg.id).unwrap();
                txn_record.keys.insert(k, TxnState::Local);
                locked.push(k);
            }
            self.stats.writes.fetch_add(1, Ordering::SeqCst);
        }

        {
            let (_latch, txn_record) = self
                .txns
                .get_or_else(msg.id, || TxnRecord::new(msg.start_ts, msg.timeout));
            txn_record.commit_ts = Some(msg.commit_ts);
            if txn_record.timeout < msg.timeout {
                txn_record.timeout = msg.timeout;
            }
        }

        self.ack(&*msg);
        self.async_consensus_write_prewrite(&msg);
    }

    fn handle_finalise(&self, msg: Box<messages::FinaliseRequest>) {
        // eprintln!("finalise {:?}", msg.id);
        while let Err(_) = self.finalise_txn(&msg) {
            thread::sleep(Duration::from_millis(50));
        }
    }

    fn handle_rollback(&self, msg: Box<messages::RollbackRequest>) {
        // eprintln!("rollback {:?}", msg.id);
        // Retry the rollback since failure must be due to latch deadlock.
        while let Err(_) = self.rollback_txn(&msg) {
            thread::sleep(Duration::from_millis(50));
        }
    }

    fn read(&self, _: Key, _: TxnId) {
        self.stats.reads.fetch_add(1, Ordering::SeqCst);
    }

    // Precondition: lock must not be held by txn id.
    // No latches may be held.
    fn aquire_lock_and_set_value(
        &self,
        key: Key,
        id: TxnId,
        start_ts: Ts,
        value: Option<Value>,
    ) -> Result<(), String> {
        let (_latch, record) = self.keys.get_or_else(key, Record::default);

        if record.lock.is_none() {
            let lock_kind = if let Some(v) = value {
                let exists = record.values.insert(start_ts, v);
                assert!(exists.is_none());
                LockKind::Modify
            } else {
                LockKind::Read
            };
            record.lock = Some(Lock {
                id,
                state: TxnState::Local,
                kind: lock_kind,
            });

            Ok(())
        } else {
            Err(format!(
                "Key locked {:?} by {:?}",
                key,
                record.lock.as_ref().unwrap().id
            ))
        }
    }

    fn abort_prewrite(&self, id: TxnId, locked: &[Key]) -> Result<(), String> {
        // Unlock any keys we previously locked.
        let (_latch, txn_record) = self.txns.blocking_get(id).unwrap();
        for &k in locked {
            txn_record.keys.remove(&k);
            let (_latch, record) = self.keys.try_get(k)?.unwrap();
            if let Some(lock) = record.lock.as_ref() {
                if lock.id == id {
                    record.lock = None;
                }
            }
        }

        Ok(())
    }

    fn wait_for_consensus() {
        // FIXME actually model the replicas
        let wait_time = rand::thread_rng().gen_range(MIN_CONSENSUS_TIME, MAX_CONSENSUS_TIME);
        thread::sleep(Duration::from_millis(wait_time));
    }

    fn update_lock_state(&self, key: Key, id: TxnId) -> Result<(), String> {
        let (_latch, txn_record) = match self.txns.blocking_get(id) {
            Some(tup) => tup,
            None => return Err(format!("Transaction {:?} missing {:?}", id, key)),
        };
        {
            let (_latch, record) = self.keys.blocking_get(key).unwrap();
            match &mut record.lock {
                Some(lock) if lock.id == id => lock.state = TxnState::Consensus,
                _ => {
                    // Key has been unlocked (and possibly re-locked), which means the transaction
                    // has been rolled back.
                    return Ok(());
                }
            }
        }

        match txn_record.keys.get_mut(&key) {
            Some(key) => *key = TxnState::Consensus,
            None => panic!("Expected key {:?}, found none", key),
        }

        Ok(())
    }

    fn async_consensus_write_lock(&self, msg: &messages::LockRequest) {
        Server::wait_for_consensus();

        match self.update_lock_state(msg.key, msg.id) {
            Ok(_) => {
                // eprintln!("consensus lock complete {:?} {:?}", msg.id, msg.key);
                self.respond(msg);
            }
            Err(s) => self.fail(msg, s),
        }
    }

    fn async_consensus_write_prewrite(&self, msg: &messages::PrewriteRequest) {
        // Assumes we do one consensus write for all writes in the transaction.
        Server::wait_for_consensus();

        for &(k, _) in &msg.writes {
            if let Err(s) = self.update_lock_state(k, msg.id) {
                // Leaves keys locked without a txn, but that should get sorted out when we rollback.
                self.fail(msg, s);
                return;
            }
        }
        // eprintln!(
        //     "consensus prewrite complete {:?} {:?}",
        //     msg.id, msg.writes
        // );

        self.respond(msg);
    }

    fn finalise_txn(&self, msg: &messages::FinaliseRequest) -> Result<(), String> {
        {
            let (_latch, txn_record) = match self.txns.blocking_get(msg.id) {
                Some(tup) => tup,
                None => {
                    self.fail(msg, format!("Missing txn record"));
                    return Ok(());
                }
            };
            assert_eq!(
                txn_record.commit_state,
                TxnState::Local,
                "re-finalising? {:?}",
                msg.id
            );
            txn_record.commit_state = TxnState::Consensus;

            // Commit each key write, remove the lock.
            for (&k, &s) in &txn_record.keys {
                assert!(
                    s == TxnState::Consensus,
                    "no consensus for key {:?}, txn {:?}",
                    k,
                    msg.id,
                );
                let (_latch, record) = self.keys.try_get(k)?.unwrap();
                // eprintln!("unlock {:?} {:?}", k, msg.id);
                if let Some(lock) = record.lock.as_ref() {
                    assert!(lock.id == msg.id);
                    let lock_kind = record.lock.as_ref().unwrap().kind;
                    record.lock = None;

                    assert!(
                        record.writes.is_empty()
                            || record.writes[0].0 < txn_record.commit_ts.unwrap()
                    );
                    if lock_kind == LockKind::Modify {
                        record
                            .writes
                            .insert(0, (txn_record.commit_ts.unwrap(), txn_record.start_ts));
                    }
                }
            }
        }

        // Remove the txn record to conclude the transaction.
        self.txns.remove(msg.id);
        self.stats.committed_txns.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn rollback_txn(&self, msg: &messages::RollbackRequest) -> Result<(), String> {
        for &k in &msg.keys {
            if let Some((_latch, record)) = self.keys.try_get(k)? {
                if let Some(lock) = record.lock.as_ref() {
                    if lock.id == msg.id {
                        record.lock = None;
                    }
                }
            }
        }
        // Don't remove because we want a record that the txn is rolled vack
        //self.txns.remove(msg.id);
        // FIXME if we get a rollback without any locks/prewrites, then the unwrap is bogus.
        let (_latch, record) = self.txns.blocking_get(msg.id).unwrap();
        record.commit_state = TxnState::RolledBack;
        self.stats.rolled_back_txns.fetch_add(1, Ordering::SeqCst);
        // TODO ack?
        Ok(())
    }

    fn ack<T: MsgRequest>(&self, msg: &T) {
        let msg = msg.ack().unwrap();
        self.transport.send(Box::new(msg));
    }

    fn fail<T: MsgRequest + fmt::Debug>(&self, msg: &T, _s: String) {
        // println!("Message handling failed {:?}: {}", msg, _s);
        if let Some(msg) = msg.response(false) {
            self.transport.send(Box::new(msg));
        }
    }

    fn respond<T: MsgRequest>(&self, msg: &T) {
        let msg = msg.response(true).unwrap();
        self.transport.send(Box::new(msg));
    }

    fn txn_contains_key(&self, id: TxnId, key: Key, start_ts: Ts, timeout: Instant) -> bool {
        let (_latch, record) = self
            .txns
            .get_or_else(id, || TxnRecord::new(start_ts, timeout));
        record.keys.contains_key(&key)
    }
}

impl transport::Receiver for Server {
    fn recv_msg(self: Arc<Self>, mut msg: Box<dyn any::Any + Send>) -> Result<(), String> {
        macro_rules! handle_msg {
            ($msg: ident, $handler: ident) => {
                match $msg.downcast() {
                    Ok(msg) => {
                        let this = self.clone();
                        thread::spawn(move || {
                            this.thread_count.fetch_add(1, Ordering::SeqCst);
                            this.$handler(msg);
                            this.thread_count.fetch_sub(1, Ordering::SeqCst);
                        });
                        return Ok(());
                    }
                    Err(msg) => $msg = msg,
                };
            };
        }

        handle_msg!(msg, handle_lock);
        handle_msg!(msg, handle_prewrite);
        handle_msg!(msg, handle_finalise);
        handle_msg!(msg, handle_rollback);

        Err(format!("Unknown message type: {:?}", msg.type_id()))
    }

    fn handle_shutdown(self: Arc<Self>) -> Result<(), String> {
        // eprintln!("handle_shutdown");
        while self.thread_count.load(Ordering::SeqCst) > 0 {
            println!(
                "waiting for {} threads...",
                self.thread_count.load(Ordering::SeqCst)
            );
            thread::sleep(Duration::from_millis(100));
        }
        self.stats.print();
        Ok(())
    }
}

store!(TxnStore, TxnRecord, TxnId, TXNS);
store!(KvStore, Record, Key, MAX_KEY);

#[derive(Default, Clone)]
struct Record {
    values: HashMap<Ts, Value>,
    // commit ts, start ts; ordered by commit ts, descending.
    writes: Vec<(Ts, Ts)>,
    lock: Option<Lock>,
}

#[derive(Clone)]
struct TxnRecord {
    keys: HashMap<Key, TxnState>,
    commit_state: TxnState,
    commit_ts: Option<Ts>,
    start_ts: Ts,
    timeout: Instant,
}

impl TxnRecord {
    fn new(start_ts: Ts, timeout: Instant) -> TxnRecord {
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
struct Lock {
    id: TxnId,
    state: TxnState,
    kind: LockKind,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum TxnState {
    Local,
    Consensus,
    RolledBack,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum LockKind {
    Read,
    Modify,
}

struct Statistics {
    committed_txns: AtomicU64,
    rolled_back_txns: AtomicU64,
    reads: AtomicU64,
    writes: AtomicU64,
    failed_rws: AtomicU64,
}

impl Statistics {
    fn new() -> Statistics {
        Statistics {
            committed_txns: AtomicU64::new(0),
            rolled_back_txns: AtomicU64::new(0),
            reads: AtomicU64::new(0),
            writes: AtomicU64::new(0),
            failed_rws: AtomicU64::new(0),
        }
    }

    fn print(&self) {
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
