use crate::*;
use std::{any, cell::UnsafeCell, collections::HashMap};

pub struct Server {
    txns: UnsafeCell<HashMap<Ts, TxnRecord>>,
    keys: UnsafeCell<HashMap<Key, Record>>,
    // TODO latches
    transport: transport::TransportSend,
}

impl Server {
    pub fn new(transport: transport::TransportSend) -> Server {
        Server {
            transport,
            txns: UnsafeCell::new(HashMap::new()),
            keys: UnsafeCell::new(HashMap::new()),
        }
    }

    fn handle_lock(&self, msg: Box<transport::LockRequest>) -> Result<(), String> {
        let txn_record = self.txn_record(msg.start_ts);
        let record = self.record(msg.key);

        txn_record.keys.insert(msg.key, TxnState::Local);
        record.lock = Some(Lock {
            start_ts: msg.start_ts,
            state: TxnState::Local,
        });

        self.ack_lock(&msg)?;
        self.async_consensus_write_lock(&msg)?;

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
        self.transport.send(Box::new(msg)).map_err(|e| e.to_string())
    }

    fn async_consensus_write_lock(&self, msg: &transport::LockRequest) -> Result<(), String> {
        // TODO wait here (or actually model the replicas)
        let msg = msg.response(true);
        self.transport.send(Box::new(msg)).map_err(|e| e.to_string())
    }
}

unsafe impl Sync for Server {}

impl transport::Receiver for Server {
    fn recv_msg(&self, msg: Box<dyn any::Any + Send>) -> Result<(), String> {
        let msg = match msg.downcast() {
            Ok(msg) => return self.handle_lock(msg),
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
