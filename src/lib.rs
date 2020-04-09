// TODO
//
// record state changes in Txn
// timeouts
// commit and tidy up
// recovery
// non-locking reads
// multiple clients and servers
// deadlock
//
// Constraints
//
// first key locked must be the primary key.
// in TiKV we'd store the txn record as part of the primary lock.
// all reads are locking reads
// point access only
//
// Questions
//
// What if a lock message is lost? Is it checked at prewrite?

use std::sync::{Arc, Mutex};

mod client;
mod server;
mod transport;

const READS_PER_TXN: usize = 10;
const WRITES_PER_TXN: usize = 10;
const TXNS: usize = 100;
const MAX_KEY: u64 = 1000;
const MIN_CONSENSUS_TIME: u64 = 100;
const MAX_CONSENSUS_TIME: u64 = 1000;

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
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
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

    j2.join().unwrap();
    j1.join().unwrap();
}
