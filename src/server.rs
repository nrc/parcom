use crate::{transport::MsgRequest, *};
use rand::{self, Rng};
use std::{
    any,
    cell::UnsafeCell,
    collections::{HashMap, HashSet},
    thread,
    time::Duration,
};

pub struct Server {
    txns: UnsafeCell<HashMap<Ts, TxnRecord>>,
    keys: UnsafeCell<HashMap<Key, Record>>,
    latches: Mutex<HashSet<Key>>,
    transport: transport::TransportSend,
}

struct Latch<'a> {
    key: Key,
    latches: &'a Mutex<HashSet<Key>>,
}

impl<'a> Latch<'a> {
    fn block_on_latch(latches: &'a Mutex<HashSet<Key>>, key: Key) -> Latch<'a> {
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
}

impl<'a> Drop for Latch<'a> {
    fn drop(&mut self) {
        let mut latches = self.latches.lock().unwrap();
        latches.remove(&self.key);
    }
}

impl Server {
    pub fn new(transport: transport::TransportSend) -> Server {
        Server {
            transport,
            txns: UnsafeCell::new(HashMap::new()),
            keys: UnsafeCell::new(HashMap::new()),
            latches: Mutex::new(HashSet::new()),
        }
    }

    pub fn shutdown(&self) {
        self.transport.shutdown();
    }

    // Precondition: lock must not be held by txn start_ts
    fn aquire_lock_and_set_value(&self, key: Key, start_ts: Ts, value: Option<Value>) {
        // TODO can we recover or return to client instead of blocking
        let mut sleeps = 100;
        loop {
            let _latch = Latch::block_on_latch(&self.latches, key);
            let record = self.record(key);

            if record.lock.is_none() {
                record.lock = Some(Lock {
                    start_ts,
                    state: TxnState::Local,
                });

                if let Some(v) = value {
                    let exists = record.values.insert(start_ts, v);
                    assert!(exists.is_none());
                }
                return;
            }
            if sleeps <= 0 {
                panic!("timed out waiting for lock on {:?}", key);
            }
            thread::sleep(Duration::from_millis(20));
            sleeps -= 1;
        }
    }

    fn handle_lock(&self, msg: Box<transport::LockRequest>) -> Result<(), String> {
        {
            // TODO record writes or locks or both?
            let txn_record = self.txn_record(msg.start_ts);
            if txn_record.keys.contains_key(&msg.key) {
                // Check we have locked this key.
                let record = self.record(msg.key);
                assert!(record.lock.as_ref().unwrap().start_ts == msg.start_ts);
            } else {
                self.aquire_lock_and_set_value(msg.key, msg.start_ts, None);

                txn_record.keys.insert(msg.key, TxnState::Local);
            }
        }

        self.ack(&*msg)?;
        self.async_consensus_write_lock(&*msg)?;

        Ok(())
    }

    fn handle_prewrite(&self, msg: Box<transport::PrewriteRequest>) -> Result<(), String> {
        let txn_record = self.txn_record(msg.start_ts);
        for &(k, v) in &msg.writes {
            if txn_record.keys.contains_key(&k) {
                // Key is already locked, just need to write the value.
                let record = self.record(k);
                assert!(record.lock.as_ref().unwrap().start_ts == msg.start_ts);
                record.values.insert(msg.start_ts, v);
            } else {
                // Lock and set value.
                self.aquire_lock_and_set_value(k, msg.start_ts, Some(v));
                txn_record.keys.insert(k, TxnState::Local);
            }

        }

        txn_record.commit_ts = Some(msg.commit_ts);

        self.ack(&*msg)?;
        self.async_consensus_write_prewrite(&msg)?;

        Ok(())
    }

    fn handle_finalise(&self, msg: Box<transport::FinaliseRequest>) -> Result<(), String> {
        self.finalise_txn(msg.start_ts)
    }

    fn record(&self, key: Key) -> &mut Record {
        unsafe { self.keys.get().as_mut().unwrap().entry(key).or_default() }
    }

    fn assert_record(&self, key: Key) -> &mut Record {
        unsafe { self.keys.get().as_mut().unwrap().get_mut(&key).unwrap() }
    }

    fn txn_record(&self, start_ts: Ts) -> &mut TxnRecord {
        unsafe {
            self.txns
                .get()
                .as_mut()
                .unwrap()
                .entry(start_ts)
                .or_insert_with(|| TxnRecord::new(start_ts))
        }
    }

    fn remove_txn_record(&self, start_ts: Ts) -> TxnRecord {
        unsafe {
            self.txns
                .get()
                .as_mut()
                .unwrap()
                .remove(&start_ts)
                .unwrap()
        }
    }

    fn ack<T: MsgRequest>(&self, msg: &T) -> Result<(), String> {
        let msg = msg.ack();
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

    fn update_lock_state(&self, key: Key, txn_record: &mut TxnRecord) {
        let _latch = Latch::block_on_latch(&self.latches, key);
        let record = self.record(key);
        match &mut record.lock {
            Some(lock) => lock.state = TxnState::Consensus,
            None => panic!("Expected lock, found none"),
        }

        match txn_record.keys.get_mut(&key) {
            Some(key) => *key = TxnState::Consensus,
            None => panic!("Expected key, found none"),
        }
    }

    fn async_consensus_write_lock(&self, msg: &transport::LockRequest) -> Result<(), String> {
        Server::wait_for_consensus();

        let txn_record = self.txn_record(msg.start_ts);
        self.update_lock_state(msg.key, txn_record);

        // We could check all keys and set the record lock state.

        self.respond(msg)
    }

    fn async_consensus_write_prewrite(
        &self,
        msg: &transport::PrewriteRequest,
    ) -> Result<(), String> {
        // Assumes we do one consensus write for all writes in the transaction.
        Server::wait_for_consensus();

        let txn_record = self.txn_record(msg.start_ts);
        for &(k, _) in &msg.writes {
            self.update_lock_state(k, txn_record);
        }

        self.respond(msg)
    }

    fn finalise_txn(&self, start_ts: Ts) -> Result<(), String> {
        // TODO rather than remove, we should borrow, write the commit state, do our thing, then remove
        let txn_record = self.remove_txn_record(start_ts);
        assert!(txn_record.commit_state == TxnState::Local);

        // Commit each key write, remove the lock.
        txn_record.keys.iter().for_each(|(&k, &s)| {
            assert!(s == TxnState::Consensus);
            Latch::block_on_latch(&self.latches, k);
            let record = self.assert_record(k);
            record.lock = None;
            if !record.writes.is_empty() {
                // FIXME us thus always true?
                assert!(record.writes.last().unwrap().0 > txn_record.commit_ts.unwrap());
            }
            record.writes.push((txn_record.commit_ts.unwrap(), start_ts));
        });

        // Remove the txn record to conclude the transaction.

        Ok(())
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        // TODO check these on normal shutdown only
        // assert!(self.latches.lock().unwrap().is_empty());
        // unsafe {
        //     eprintln!("len {}", self.keys.get().as_mut().unwrap().len());
        //     self.keys
        //         .get()
        //         .as_mut()
        //         .unwrap()
        //         .values()
        //         .for_each(|k| assert!(k.lock.is_none()));
        // }
    }
}

unsafe impl Sync for Server {}

impl transport::Receiver for Server {
    // TODO spawn onto a thread
    fn recv_msg(&self, msg: Box<dyn any::Any + Send>) -> Result<(), String> {
        let msg = match msg.downcast() {
            Ok(msg) => return self.handle_lock(msg),
            Err(msg) => msg,
        };
        let msg = match msg.downcast() {
            Ok(msg) => return self.handle_prewrite(msg),
            Err(msg) => msg,
        };
        let msg = match msg.downcast() {
            Ok(msg) => return self.handle_finalise(msg),
            Err(msg) => msg,
        };
        Err(format!("Unknown message type: {:?}", msg.type_id()))
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
    fn new(start_ts: Ts) -> TxnRecord {
        TxnRecord {
            keys: HashMap::new(),
            commit_state: TxnState::Local,
            commit_ts: None,
            start_ts,
        }
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
