use crate::*;
use rand::{self, Rng};
use std::{
    any,
    cell::UnsafeCell,
    collections::{HashMap, HashSet},
    sync::Mutex,
};

pub struct Client {
    txns: UnsafeCell<HashMap<Ts, Box<Txn>>>,
    txn_latches: Mutex<HashSet<Ts>>,
    tso: Tso,
    transport: transport::TransportSend,
}

impl Client {
    pub fn new(tso: Tso, transport: transport::TransportSend) -> Client {
        Client {
            txns: UnsafeCell::new(HashMap::new()),
            txn_latches: Mutex::new(HashSet::new()),
            tso,
            transport,
        }
    }

    pub fn shutdown(&self) {
        self.transport.shutdown();
    }

    pub fn exec_txn(&self) -> Result<(), String> {
        let start_ts = self.tso.ts();

        {
            let _latch = latch::block_on_latch(&self.txn_latches, start_ts);
            let txn: &mut Txn = unsafe {
                let txns = self.txns.get().as_mut().unwrap();
                txns.insert(start_ts, Txn::new());
                txns.get_mut(&start_ts).unwrap()
            };

            for _ in 0..READS_PER_TXN {
                let key = self.exec_lock(start_ts)?;
                txn.locks.push(key);
            }
            self.exec_prewrite(start_ts)?;
        }
        // TODO block waiting for acks (currently ignored). Why? Or do it for each request?
        // TODO block waiting for responses.
        Ok(())
    }

    fn exec_lock(&self, start_ts: Ts) -> Result<Key, String> {
        let key = random_key();
        // eprintln!("lock {:?}", key);
        let msg = messages::LockRequest {
            key,
            start_ts,
            for_update_ts: self.tso.ts(),
        };
        self.transport
            .send(Box::new(msg))
            .map_err(|e| e.to_string())
            .map(|_| key)
    }

    fn exec_prewrite(&self, start_ts: Ts) -> Result<(), String> {
        let writes = (0..WRITES_PER_TXN)
            .map(|_| (random_key(), random_value()))
            .collect();
        // eprintln!("write {:?}", writes);
        let msg = messages::PrewriteRequest {
            start_ts,
            commit_ts: self.tso.ts(),
            writes,
        };
        self.transport
            .send(Box::new(msg))
            .map_err(|e| e.to_string())
    }

    fn handle_prewrite_response(&self, msg: Box<messages::PrewriteResponse>) -> Result<(), String> {
        // TODO retry if no success
        assert!(msg.success);
        self.assert_txn(msg.start_ts).1.prewrite = true;
        self.check_responses_and_commit(msg.start_ts)?;
        Ok(())
    }

    fn handle_lock_response(&self, msg: Box<messages::LockResponse>) -> Result<(), String> {
        // TODO retry if no success
        assert!(msg.success);
        self.assert_txn(msg.start_ts)
            .1
            .locks
            .remove_item(&msg.key)
            .unwrap();
        self.check_responses_and_commit(msg.start_ts)?;
        Ok(())
    }

    fn check_responses_and_commit(&self, start_ts: Ts) -> Result<(), String> {
        if !self.assert_txn(start_ts).1.complete() {
            return Ok(());
        }

        let msg = messages::FinaliseRequest { start_ts };
        self.transport
            .send(Box::new(msg))
            .map_err(|e| e.to_string())
    }

    fn assert_txn<'a>(&'a self, start_ts: Ts) -> (latch::Latch<'a, Ts>, &mut Txn) {
        let latch = latch::block_on_latch(&self.txn_latches, start_ts);
        let txn = unsafe {
            self.txns
                .get()
                .as_mut()
                .unwrap()
                .get_mut(&start_ts)
                .unwrap()
        };
        (latch, txn)
    }
}

unsafe impl Sync for Client {}

impl transport::Receiver for Client {
    fn recv_msg(self: Arc<Self>, msg: Box<dyn any::Any + Send>) -> Result<(), String> {
        // We don't need to spawn a thread here because none of these operations can block.

        // Ignore ACKs for now.
        let msg = match msg.downcast::<messages::LockAck>() {
            Ok(_) => return Ok(()),
            Err(msg) => msg,
        };
        let msg = match msg.downcast::<messages::PrewriteAck>() {
            Ok(_) => return Ok(()),
            Err(msg) => msg,
        };
        let msg = match msg.downcast() {
            Ok(msg) => return self.handle_lock_response(msg),
            Err(msg) => msg,
        };
        let msg = match msg.downcast() {
            Ok(msg) => return self.handle_prewrite_response(msg),
            Err(msg) => msg,
        };
        Err(format!("Unknown message type: {:?}", msg.type_id()))
    }

    fn handle_shutdown(self: Arc<Self>) -> Result<(), String> {
        Ok(())
    }
}

fn random_key() -> Key {
    Key(rand::thread_rng().gen_range(0, MAX_KEY))
}

fn random_value() -> Value {
    Value(rand::random())
}

struct Txn {
    locks: Vec<Key>,
    prewrite: bool,
}

impl Txn {
    fn new() -> Box<Txn> {
        Box::new(Txn {
            locks: Vec::new(),
            prewrite: false,
        })
    }

    fn complete(&self) -> bool {
        self.prewrite && self.locks.is_empty()
    }
}
