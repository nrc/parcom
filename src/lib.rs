// TODO
//
// record state changes in Txn
// timeouts
// commit and tidy up
// recovery
// non-locking reads
// multiple clients and servers
// deadlock
// consensus write after failure
// for_update_ts

#![feature(never_type)]

use std::sync::{Arc, Mutex};

mod client;
mod messages;
mod server;
mod transport;

const READS_PER_TXN: usize = 1;
const WRITES_PER_TXN: usize = 1;
// TODO multiple txns are failing
const TXNS: usize = 1;
const MAX_KEY: u64 = 1000;
const MIN_CONSENSUS_TIME: u64 = 10;
const MAX_CONSENSUS_TIME: u64 = 100;

#[derive(Debug, Clone)]
pub struct Tso {
    next: Arc<Mutex<u64>>,
}

impl Tso {
    fn new() -> Tso {
        Tso {
            next: Arc::new(Mutex::new(0)),
        }
    }

    fn ts(&self) -> Ts {
        let mut next = self.next.lock().unwrap();
        let result = Ts(*next);
        *next += 1;
        result
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct Key(u64);
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct Ts(u64);
#[derive(Debug, Copy, Clone, Default)]
pub struct Value(u64);

pub fn start() {
    let tso = Tso::new();
    let (send1, mut recv1) = transport::new();
    let (send2, mut recv2) = transport::new();
    let server = Arc::new(server::Server::new(send1));
    let client = Arc::new(client::Client::new(tso.clone(), send2));
    recv1.set_handler(client.clone());
    recv2.set_handler(server.clone());
    let j1 = recv1.listen();
    let j2 = recv2.listen();

    for _ in 0..TXNS {
        if let Err(_) = client.exec_txn() {
            return;
        }
    }

    client.shutdown();
    j2.join().unwrap();
    server.shutdown();
    j1.join().unwrap();
}
