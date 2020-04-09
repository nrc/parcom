use crate::*;
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

    fn handle_lock(&self, msg: Box<transport::LockRequest>) -> Result<(), String> {
        let _latch = Latch::block_on_latch(&self.latches, msg.key);

        {
            let txn_record = self.txn_record(msg.start_ts);
            txn_record.keys.insert(msg.key, TxnState::Local);

            let record = self.record(msg.key);
            record.lock = Some(Lock {
                start_ts: msg.start_ts,
                state: TxnState::Local,
            });
        }

        self.ack_lock(&msg)?;
        self.async_consensus_write_lock(&msg)?;

        Ok(())
    }

    fn handle_prewrite(&self, msg: Box<transport::PrewriteRequest>) -> Result<(), String> {
        // TODO
        Ok(())
    }

    fn record(&self, key: Key) -> &mut Record {
        unsafe { self.keys.get().as_mut().unwrap().entry(key).or_default() }
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

    fn ack_lock(&self, msg: &transport::LockRequest) -> Result<(), String> {
        let msg = msg.ack();
        self.transport
            .send(Box::new(msg))
            .map_err(|e| e.to_string())
    }

    fn async_consensus_write_lock(&self, msg: &transport::LockRequest) -> Result<(), String> {
        // FIXME actually model the replicas

        let wait_time = rand::thread_rng().gen_range(MIN_CONSENSUS_TIME, MAX_CONSENSUS_TIME);
        thread::sleep(Duration::from_millis(wait_time));

        let record = self.record(msg.key);
        match &mut record.lock {
            Some(lock) => lock.state = TxnState::Consensus,
            None => panic!("Expected lock, found none"),
        }

        let txn_record = self.txn_record(msg.start_ts);
        match txn_record.keys.get_mut(&msg.key) {
            Some(key) => *key = TxnState::Consensus,
            None => panic!("Expected key, found none"),
        }
        // We could check all keys and set the record lock state.

        let msg = msg.response(true);
        self.transport
            .send(Box::new(msg))
            .map_err(|e| e.to_string())
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        assert!(self.latches.lock().unwrap().is_empty());
        unsafe {
            eprintln!("len {}", self.keys.get().as_mut().unwrap().len());
            self.keys
                .get()
                .as_mut()
                .unwrap()
                .values()
                .for_each(|k| assert!(k.lock.is_none()));
        }
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
        Err(format!("Unknown message type: {:?}", msg.type_id()))
    }
}

struct Cluster {}

#[derive(Debug, Default, Clone)]
struct Record {
    values: HashMap<Ts, Value>,
    lock: Option<Lock>,
}

#[derive(Debug, Clone)]
struct TxnRecord {
    keys: HashMap<Key, TxnState>,
    commit_state: TxnState,
    start_ts: Ts,
}

impl TxnRecord {
    fn new(start_ts: Ts) -> TxnRecord {
        TxnRecord {
            keys: HashMap::new(),
            commit_state: TxnState::Local,
            start_ts,
        }
    }
}

#[derive(Debug, Clone)]
struct Lock {
    start_ts: Ts,
    state: TxnState,
}

#[derive(Debug, Clone)]
enum TxnState {
    Local,
    Consensus,
    Failed,
}
