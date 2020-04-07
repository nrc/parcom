use crate::*;
use std::{
    any,
    collections::HashMap,
};

pub struct Server {
    txns: HashMap<Ts, TxnRecord>,
    keys: HashMap<Key, Record>,
}

impl Server {
    pub fn new() -> Server {
        Server {
            txns: HashMap::new(),
            keys: HashMap::new(),
        }
    }

    fn handle_lock(&self, msg: Box<transport::LockMsg>) -> Result<(), String> {
        // TODO
        Ok(())
    }
}

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

struct Record {
    values: HashMap<Ts, Value>,
    lock: Option<Lock>,
}

struct TxnRecord {
    keys: HashMap<Key, TxnState>,
    commit_state: TxnState,
    start_ts: Ts,
    timeout: Ts,
}

struct Lock {
    txn: usize,
    state: TxnState,
}

enum TxnState {
    Local,
    Consensus,
    Failed,
}
