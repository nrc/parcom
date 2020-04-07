use crate::*;
use rand::{self, Rng};
use std::{
    collections::HashMap,
    sync::Mutex,
};

pub struct Client {
    txns: Mutex<HashMap<Ts, Txn>>,
    tso: Tso,
    transport: transport::TransportSend,
}

impl Client {
    pub fn new(tso: Tso, transport: transport::TransportSend) -> Client {
        Client {
            txns: Mutex::new(HashMap::new()),
            tso,
            transport,
        }
    }

    pub fn exec_txn(&self) {
        let start_ts = self.tso.ts();
        self.txns.lock().unwrap().insert(start_ts, Txn {});
        for _ in 0..READS_PER_TXN {
            self.exec_lock(start_ts);
        }
        self.exec_prewrite(start_ts);
    }

    fn exec_lock(&self, start_ts: Ts) {
        let key = random_key();
        let msg = transport::LockMsg {
            key,
            start_ts,
            for_update_ts: self.tso.ts(),
        };
        self.transport.send(Box::new(msg)).unwrap();
    }

    fn exec_prewrite(&self, start_ts: Ts) {
        let writes = (0..WRITES_PER_TXN)
            .map(|_| (random_key(), random_value()))
            .collect();
        let msg = transport::PrewriteMsg {
            start_ts,
            commit_ts: self.tso.ts(),
            writes,
        };
        self.transport.send(Box::new(msg)).unwrap();
    }
}

fn random_key() -> Key {
    Key(rand::thread_rng().gen_range(0, MAX_KEY))
}

fn random_value() -> Value {
    Value(rand::random())
}

struct Txn {}
