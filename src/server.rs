use crate::{messages::MsgRequest, *};
use rand::{self, Rng};
use std::{
    any,
    cell::UnsafeCell,
    collections::{HashMap, HashSet},
    fmt,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};

pub struct Server {
    // We use txn records indexed by start TS, rather than storing this info on the primary lock.
    // The two approaches are equivalent.
    txns: UnsafeCell<HashMap<Ts, Box<TxnRecord>>>,
    keys: UnsafeCell<HashMap<Key, Box<Record>>>,
    latches: Mutex<HashSet<Key>>,
    txn_latches: Mutex<HashSet<Ts>>,
    transport: transport::TransportSend,
    thread_count: AtomicU64,
}

impl Server {
    pub fn new(transport: transport::TransportSend) -> Server {
        Server {
            transport,
            txns: UnsafeCell::new(HashMap::new()),
            keys: UnsafeCell::new(HashMap::new()),
            latches: Mutex::new(HashSet::new()),
            txn_latches: Mutex::new(HashSet::new()),
            thread_count: AtomicU64::new(0),
        }
    }

    pub fn shutdown(&self) {
        assert!(self.latches.lock().unwrap().is_empty());
        unsafe {
            println!("shutdown, len {}", self.keys.get().as_mut().unwrap().len());
            self.keys
                .get()
                .as_mut()
                .unwrap()
                .values()
                .for_each(|k| assert!(k.lock.is_none()));
        }

        self.transport.shutdown();
    }

    fn handle_lock(&self, msg: Box<messages::LockRequest>) -> Result<(), String> {
        eprintln!("lock {:?} {:?}", msg.key, msg.start_ts);
        {
            // TODO record writes or locks or both?
            if self.txn_record(msg.start_ts).1.keys.contains_key(&msg.key) {
                // Check we have locked this key.
                let record = self.record(msg.key);
                assert!(record.lock.as_ref().unwrap().start_ts == msg.start_ts);
            } else {
                if let Err(s) = self.aquire_lock_and_set_value(msg.key, msg.start_ts, None) {
                    self.fail(&*msg, s)?;
                    return Ok(());
                }
                let (_latch, txn_record) = self.txn_record(msg.start_ts);
                txn_record.keys.insert(msg.key, TxnState::Local);
            }
        }

        self.ack(&*msg)?;
        self.async_consensus_write_lock(&msg)?;

        Ok(())
    }

    fn handle_prewrite(&self, msg: Box<messages::PrewriteRequest>) -> Result<(), String> {
        eprintln!("prewrite {:?} {:?}", msg.start_ts, msg.writes);
        let mut locked = Vec::new();
        for &(k, v) in &msg.writes {
            eprintln!("prewrite key {:?} {:?}", msg.start_ts, k);
            if self.txn_record(msg.start_ts).1.keys.contains_key(&k) {
                // Key is already locked, just need to write the value.
                let record = self.record(k);
                assert!(record.lock.as_ref().unwrap().start_ts == msg.start_ts);
                record.values.insert(msg.start_ts, v);
            } else {
                // Lock and set value.
                if let Err(s) = self.aquire_lock_and_set_value(k, msg.start_ts, Some(v)) {
                    eprintln!("Failed to lock {:?} {:?}", k, msg.start_ts);
                    // Unlock any keys we previously locked.
                    let (_latch, txn_record) = self.txn_record(msg.start_ts);
                    for &k in &locked {
                        txn_record.keys.remove(&k);
                        let _latch = latch::block_on_latch(&self.latches, k);
                        let record = self.assert_record(k);
                        assert!(record.lock.as_ref().unwrap().start_ts == msg.start_ts);
                        record.lock = None;
                    }
                    self.fail(&*msg, s)?;
                    return Ok(());
                }
                let (_latch, txn_record) = self.txn_record(msg.start_ts);
                txn_record.keys.insert(k, TxnState::Local);
                locked.push(k);
            }
        }

        {
            let (_latch, txn_record) = self.txn_record(msg.start_ts);
            txn_record.commit_ts = Some(msg.commit_ts);
        }

        self.ack(&*msg)?;
        self.async_consensus_write_prewrite(&msg)?;

        Ok(())
    }

    fn handle_finalise(&self, msg: Box<messages::FinaliseRequest>) -> Result<(), String> {
        eprintln!("finalise {:?}", msg.start_ts);
        self.finalise_txn(msg.start_ts)
    }

    fn handle_rollback(&self, msg: Box<messages::RollbackRequest>) -> Result<(), String> {
        eprintln!("rollback {:?}", msg.start_ts);
        for k in msg.keys {
            let _latch = latch::block_on_latch(&self.latches, k);
            let record = self.assert_record(k);
            assert!(record.lock.as_ref().unwrap().start_ts == msg.start_ts);
            record.lock = None;
        }
        self.remove_txn_record(msg.start_ts);
        // TODO ack?
        Ok(())
    }

    // Precondition: lock must not be held by txn start_ts
    fn aquire_lock_and_set_value(
        &self,
        key: Key,
        start_ts: Ts,
        value: Option<Value>,
    ) -> Result<(), String> {
        // TODO should we recover or return to client instead of blocking
        let mut sleeps = 100;
        loop {
            {
                let _latch = latch::block_on_latch(&self.latches, key);
                eprintln!("aquired latch, getting record {:?}", key);
                let record = self.record(key);
                eprintln!("got record {:?}", key);

                if record.lock.is_none() {
                    record.lock = Some(Lock {
                        start_ts,
                        state: TxnState::Local,
                    });

                    if let Some(v) = value {
                        let exists = record.values.insert(start_ts, v);
                        assert!(exists.is_none());
                    }
                    return Ok(());
                }
                if sleeps <= 0 {
                    return Err(format!(
                        "timed out waiting for lock on {:?}, locked by {:?}",
                        key,
                        record.lock.as_ref().unwrap().start_ts
                    ));
                }
            }
            thread::sleep(Duration::from_millis(20));
            sleeps -= 1;
        }
    }

    fn record(&self, key: Key) -> &mut Record {
        unsafe { self.keys.get().as_mut().unwrap().entry(key).or_default() }
    }

    fn assert_record(&self, key: Key) -> &mut Record {
        unsafe {
            self.keys
                .get()
                .as_mut()
                .unwrap()
                .get_mut(&key)
                .expect(&format!("assert_record failed {:?}", key))
        }
    }

    fn txn_record<'a>(&'a self, start_ts: Ts) -> (latch::Latch<'a, Ts>, &mut TxnRecord) {
        let latch = latch::block_on_latch(&self.txn_latches, start_ts);

        unsafe {
            (
                latch,
                self.txns
                    .get()
                    .as_mut()
                    .unwrap()
                    .entry(start_ts)
                    .or_insert_with(|| TxnRecord::new(start_ts)),
            )
        }
    }

    fn assert_txn_record<'a>(&'a self, start_ts: Ts) -> (latch::Latch<'a, Ts>, &mut TxnRecord) {
        let latch = latch::block_on_latch(&self.txn_latches, start_ts);

        unsafe {
            (
                latch,
                self.txns
                    .get()
                    .as_mut()
                    .unwrap()
                    .get_mut(&start_ts)
                    .expect(&format!(
                        "assert_txn_record failed {:?} {:?}",
                        start_ts,
                        self.txns.get().as_ref().unwrap()
                    )),
            )
        }
    }

    fn remove_txn_record(&self, start_ts: Ts) -> Box<TxnRecord> {
        let _latch = latch::block_on_latch(&self.txn_latches, start_ts);
        unsafe { self.txns.get().as_mut().unwrap().remove(&start_ts).unwrap() }
    }

    fn ack<T: MsgRequest>(&self, msg: &T) -> Result<(), String> {
        let msg = msg.ack();
        self.transport
            .send(Box::new(msg))
            .map_err(|e| e.to_string())
    }

    fn fail<T: MsgRequest + fmt::Debug>(&self, msg: &T, s: String) -> Result<(), String> {
        eprintln!("Message handling failed {:?}: {}", msg, s);
        let msg = msg.response(false);
        self.transport
            .send(Box::new(msg))
            .map_err(|e| e.to_string())
    }

    fn respond<T: MsgRequest>(&self, msg: &T) -> Result<(), String> {
        let msg = msg.response(true);
        self.transport
            .send(Box::new(msg))
            .map_err(|e| e.to_string())
    }

    fn wait_for_consensus() {
        // FIXME actually model the replicas
        let wait_time = rand::thread_rng().gen_range(MIN_CONSENSUS_TIME, MAX_CONSENSUS_TIME);
        thread::sleep(Duration::from_millis(wait_time));
    }

    fn update_lock_state(&self, key: Key, start_ts: Ts) {
        {
            let _latch = latch::block_on_latch(&self.latches, key);
            let record = self.assert_record(key);
            match &mut record.lock {
                Some(lock) => lock.state = TxnState::Consensus,
                None => panic!("Expected lock (for {:?}), found none", key),
            }
        }

        let (_latch, txn_record) = self.assert_txn_record(start_ts);
        match txn_record.keys.get_mut(&key) {
            Some(key) => *key = TxnState::Consensus,
            None => panic!("Expected key {:?}, found none", key),
        }
    }

    fn async_consensus_write_lock(&self, msg: &messages::LockRequest) -> Result<(), String> {
        Server::wait_for_consensus();

        self.update_lock_state(msg.key, msg.start_ts);
        eprintln!("consensus lock complete {:?} {:?}", msg.start_ts, msg.key);

        // We could check all keys and set the record lock state.

        self.respond(msg)
    }

    fn async_consensus_write_prewrite(
        &self,
        msg: &messages::PrewriteRequest,
    ) -> Result<(), String> {
        // Assumes we do one consensus write for all writes in the transaction.
        Server::wait_for_consensus();

        for &(k, _) in &msg.writes {
            self.update_lock_state(k, msg.start_ts);
        }
        eprintln!(
            "consensus prewrite complete {:?} {:?}",
            msg.start_ts, msg.writes
        );

        self.respond(msg)
    }

    fn finalise_txn(&self, start_ts: Ts) -> Result<(), String> {
        // TODO rather than remove, we should borrow, write the commit state, do our thing, then remove
        let txn_record = self.remove_txn_record(start_ts);
        assert!(txn_record.commit_state == TxnState::Local);

        // Commit each key write, remove the lock.
        txn_record.keys.iter().for_each(|(&k, &s)| {
            assert!(
                s == TxnState::Consensus,
                "no consensus for key {:?}, start ts {:?}",
                k,
                start_ts
            );
            let _latch = latch::block_on_latch(&self.latches, k);
            let record = self.assert_record(k);
            eprintln!("unlock {:?} {:?}", k, start_ts);
            record.lock = None;
            if !record.writes.is_empty() {
                // FIXME is this always true?
                assert!(record.writes.last().unwrap().0 > txn_record.commit_ts.unwrap());
            }
            // TODO don't write if we only locked for a read
            record
                .writes
                .push((txn_record.commit_ts.unwrap(), start_ts));
        });

        // Remove the txn record to conclude the transaction.

        Ok(())
    }
}

unsafe impl Sync for Server {}

impl transport::Receiver for Server {
    fn recv_msg(self: Arc<Self>, msg: Box<dyn any::Any + Send>) -> Result<(), String> {
        let msg = match msg.downcast() {
            Ok(msg) => {
                let this = self.clone();
                thread::spawn(move || {
                    this.thread_count.fetch_add(1, Ordering::SeqCst);
                    if let Err(s) = this.handle_lock(msg) {
                        // TODO handle error?
                        panic!(s);
                    }
                    this.thread_count.fetch_sub(1, Ordering::SeqCst);
                });
                return Ok(());
            }
            Err(msg) => msg,
        };
        let msg = match msg.downcast() {
            Ok(msg) => {
                let this = self.clone();
                // TODO handle error?
                thread::spawn(move || {
                    this.thread_count.fetch_add(1, Ordering::SeqCst);
                    this.handle_prewrite(msg).unwrap();
                    this.thread_count.fetch_sub(1, Ordering::SeqCst);
                });
                return Ok(());
            }
            Err(msg) => msg,
        };
        let msg = match msg.downcast() {
            Ok(msg) => {
                let this = self.clone();
                // TODO handle error?
                thread::spawn(move || {
                    this.thread_count.fetch_add(1, Ordering::SeqCst);
                    this.handle_finalise(msg).unwrap();
                    this.thread_count.fetch_sub(1, Ordering::SeqCst);
                });
                return Ok(());
            }
            Err(msg) => msg,
        };
        let msg = match msg.downcast() {
            Ok(msg) => {
                let this = self.clone();
                // TODO handle error?
                thread::spawn(move || {
                    this.thread_count.fetch_add(1, Ordering::SeqCst);
                    this.handle_rollback(msg).unwrap();
                    this.thread_count.fetch_sub(1, Ordering::SeqCst);
                });
                return Ok(());
            }
            Err(msg) => msg,
        };
        Err(format!("Unknown message type: {:?}", msg.type_id()))
    }

    fn handle_shutdown(self: Arc<Self>) -> Result<(), String> {
        eprintln!("handle_shutdown");
        while self.thread_count.load(Ordering::SeqCst) > 0 {
            println!(
                "waiting for {} threads...",
                self.thread_count.load(Ordering::SeqCst)
            );
            thread::sleep(Duration::from_millis(100));
        }
        Ok(())
    }
}

struct Cluster {}

#[derive(Debug, Default, Clone)]
struct Record {
    values: HashMap<Ts, Value>,
    // commit ts, start ts; ordered by commit ts, descending.
    writes: Vec<(Ts, Ts)>,
    lock: Option<Lock>,
}

#[derive(Debug, Clone)]
struct TxnRecord {
    keys: HashMap<Key, TxnState>,
    commit_state: TxnState,
    commit_ts: Option<Ts>,
    start_ts: Ts,
}

impl TxnRecord {
    fn new(start_ts: Ts) -> Box<TxnRecord> {
        Box::new(TxnRecord {
            keys: HashMap::new(),
            commit_state: TxnState::Local,
            commit_ts: None,
            start_ts,
        })
    }
}

#[derive(Debug, Clone)]
struct Lock {
    start_ts: Ts,
    state: TxnState,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum TxnState {
    Local,
    Consensus,
    Failed,
}
