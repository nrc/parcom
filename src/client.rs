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
        let msg = transport::LockRequest {
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
        let msg = transport::PrewriteRequest {
            start_ts,
            commit_ts: self.tso.ts(),
            writes,
        };
        self.transport
            .send(Box::new(msg))
            .map_err(|e| e.to_string())
    }

    fn handle_prewrite_response(&self, msg: Box<transport::PrewriteResponse>) -> Result<(), String> {
        // TODO add to list of responses
        // TODO retry if no success
        assert!(msg.success);
        self.check_responses_and_commit(msg.start_ts)?;
        Ok(())
    }

    fn check_responses_and_commit(&self, start_ts: Ts) -> Result<(), String> {
        // TODO count responses, only send if all ticked off.
        let msg = transport::FinaliseRequest {
            start_ts,
        };
        self.transport
            .send(Box::new(msg))
            .map_err(|e| e.to_string())
    }
}

unsafe impl Sync for Client {}

impl transport::Receiver for Client {
    fn recv_msg(&self, msg: Box<dyn any::Any + Send>) -> Result<(), String> {
        // Ignore ACKs
        let msg = match msg.downcast::<transport::LockAck>() {
            Ok(_) => return Ok(()),
            Err(msg) => msg,
        };
        let msg = match msg.downcast::<transport::PrewriteAck>() {
            Ok(_) => return Ok(()),
            Err(msg) => msg,
        };
        // TODO record the response
        let msg = match msg.downcast::<transport::LockResponse>() {
            Ok(_) => return Ok(()),
            Err(msg) => msg,
        };
        let msg = match msg.downcast::<transport::PrewriteResponse>() {
            Ok(msg) => return self.handle_prewrite_response(msg),
            Err(msg) => msg,
        };
        Err(format!("Unknown message type: {:?}", msg.type_id()))
    }
}

fn random_key() -> Key {
    Key(rand::thread_rng().gen_range(0, MAX_KEY))
}

fn random_value() -> Value {
    Value(rand::random())
}

struct Txn {}
