// TODO
//
// recovery
//   timeouts
// consensus write after failure
// retries
// for_update_ts and partial retries
// non-locking reads (and the reading part of locking reads)
// failures
// multi-threaded client
// multiple clients and servers
// deadlock?
// add a User, check invariants?

#![allow(incomplete_features)]
#![feature(const_generics)]
#![feature(const_in_array_repeat_expressions)]
#![feature(never_type)]
#![feature(type_ascription)]
#![feature(vec_remove_item)]

use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

#[macro_use]
mod store;

mod client;
mod latch;
mod messages;
mod server;
mod transport;

const READS_PER_TXN: usize = 10;
const WRITES_PER_TXN: usize = 10;
const TXNS: usize = 2;
const MAX_KEY: usize = 1000;
const MIN_CONSENSUS_TIME: u64 = 10;
const MAX_CONSENSUS_TIME: u64 = 100;

#[derive(Debug, Clone)]
pub struct Tso {
    next_ts: Arc<Mutex<usize>>,
    next_id: Arc<Mutex<usize>>,
}

impl Tso {
    fn new() -> Tso {
        Tso {
            next_ts: Arc::new(Mutex::new(0)),
            next_id: Arc::new(Mutex::new(0)),
        }
    }

    fn ts(&self) -> Ts {
        let mut next = self.next_ts.lock().unwrap();
        let result = Ts(*next);
        *next += 1;
        result
    }

    fn id(&self) -> TxnId {
        let mut next = self.next_id.lock().unwrap();
        let result = TxnId(*next);
        *next += 1;
        result
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct Key(pub usize);
#[derive(Debug, Copy, Clone, Default)]
pub struct Value(usize);
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct Ts(usize);
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct TxnId(usize);

impl Into<usize> for Key {
    fn into(self) -> usize {
        self.0
    }
}

impl Into<usize> for TxnId {
    fn into(self) -> usize {
        self.0
    }
}

pub fn start() {
    let tso = Tso::new();
    let (send1, mut recv1, handle1) = transport::new();
    let (send2, mut recv2, handle2) = transport::new();
    let server = Arc::new(server::Server::new(send1));
    let client = Arc::new(client::Client::new(tso.clone(), send2));
    recv1.set_handler(client.clone());
    recv2.set_handler(server.clone());
    let j1 = recv1.listen();
    let j2 = recv2.listen();

    for _ in 0..TXNS {
        client.exec_txn();
    }

    println!("Begin shutdown");
    client.shutdown();
    handle2.join().unwrap();
    j2.join().unwrap();
    server.shutdown();
    j1.join().unwrap();
    handle1.join().unwrap();
}
