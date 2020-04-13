use crate::*;
use rand::{self, Rng};
use std::{any, collections::HashMap, sync::Mutex};

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

    pub fn shutdown(&self) {
        self.transport.shutdown();
    }

    pub fn exec_txn(&self) -> Result<(), String> {
        let start_ts = self.tso.ts();
        self.txns.lock().unwrap().insert(start_ts, Txn {});
        for _ in 0..READS_PER_TXN {
            self.exec_lock(start_ts)?;
        }
        self.exec_prewrite(start_ts)
        // TODO block waiting for acks (currently ignored). Why? Or do it for each request?
        // TODO block waiting for responses.
    }

    fn exec_lock(&self, start_ts: Ts) -> Result<(), String> {
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
        // TODO add to list of responses
        // TODO retry if no success
        assert!(msg.success);
        self.check_responses_and_commit(msg.start_ts)?;
        Ok(())
    }

    fn check_responses_and_commit(&self, start_ts: Ts) -> Result<(), String> {
        // TODO count responses, only send if all ticked off.
        let msg = messages::FinaliseRequest { start_ts };
        self.transport
            .send(Box::new(msg))
            .map_err(|e| e.to_string())
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
        // TODO record the response
        let msg = match msg.downcast::<messages::LockResponse>() {
            Ok(_) => return Ok(()),
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

struct Txn {}
