// TODO
//
// record state changes in Txn
// timeouts
// commit and tidy up
// server replies
// recovery
// reads
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

use rand::{self, Rng};
use std::{
    any,
    collections::HashMap,
    sync::{mpsc, Arc, Mutex},
    thread,
};

const READS_PER_TXN: usize = 10;
const WRITES_PER_TXN: usize = 10;
const TXNS: usize = 100;
const MAX_KEY: u64 = 1000;

struct Client {
    txns: Mutex<HashMap<Ts, Txn>>,
    tso: Tso,
    transport: TransportSend,
}

impl Client {
    fn new(tso: Tso, transport: TransportSend) -> Client {
        Client {
            txns: Mutex::new(HashMap::new()),
            tso,
            transport,
        }
    }

    fn exec_txn(&self) {
        let start_ts = self.tso.ts();
        self.txns.lock().unwrap().insert(start_ts, Txn {});
        for _ in 0..READS_PER_TXN {
            self.exec_lock(start_ts);
        }
        self.exec_prewrite(start_ts);
    }

    fn exec_lock(&self, start_ts: Ts) {
        let key = random_key();
        let msg = LockMsg {
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
        let msg = PrewriteMsg {
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

#[derive(Debug, Clone)]
struct Tso {
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

#[derive(Debug, Clone)]
struct TransportSend {
    channel: mpsc::Sender<Box<dyn any::Any + Send>>,
}

struct TransportRecv<T: Receiver> {
    channel: mpsc::Receiver<Box<dyn any::Any + Send>>,
    recv: Arc<T>,
}

impl TransportSend {
    fn new<T: Receiver>(t: Arc<T>) -> (TransportSend, TransportRecv<T>) {
        let (send, recv) = mpsc::channel();
        (
            TransportSend { channel: send },
            TransportRecv {
                channel: recv,
                recv: t,
            },
        )
    }

    fn send(&self, msg: Box<dyn any::Any + Send>) -> Result<(), String> {
        self.channel
            .send(msg)
            .map_err(|e| format!("Sending message failed: {}", e))
    }
}

impl<T: Receiver + 'static> TransportRecv<T> {
    fn listen(self) {
        thread::spawn(move || loop {
            if let Err(_) = self
                .channel
                .recv()
                .map_err(|e| e.to_string())
                .and_then(|msg| self.recv.recv_msg(msg))
            {
                return;
            }
        });
    }
}

trait Receiver: Send + Sync {
    fn recv_msg(&self, msg: Box<dyn any::Any + Send>) -> Result<(), String>;
}

struct Server {
    txns: HashMap<Ts, TxnRecord>,
    keys: HashMap<Key, Record>,
}

impl Server {
    fn new() -> Server {
        Server {
            txns: HashMap::new(),
            keys: HashMap::new(),
        }
    }

    fn handle_lock(&self, msg: Box<LockMsg>) -> Result<(), String> {
        // TODO
        Ok(())
    }
}

impl Receiver for Server {
    fn recv_msg(&self, msg: Box<dyn any::Any + Send>) -> Result<(), String> {
        let msg = match msg.downcast() {
            Ok(msg) => return self.handle_lock(msg),
            Err(msg) => msg,
        };
        Err(format!("Unknown message type: {:?}", msg.type_id()))
    }
}

struct Cluster {}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
struct Key(u64);
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
struct Ts(u64);
#[derive(Debug, Copy, Clone)]
struct Value(u64);

#[derive(Debug, Clone)]
struct LockMsg {
    key: Key,
    start_ts: Ts,
    for_update_ts: Ts,
}

#[derive(Debug, Clone)]
struct PrewriteMsg {
    start_ts: Ts,
    commit_ts: Ts,
    writes: Vec<(Key, Value)>,
}

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

pub fn start() {
    let tso = Tso::new();
    let server = Arc::new(Server::new());
    let (send, recv) = TransportSend::new(server);
    let client = Client::new(tso.clone(), send);
    recv.listen();

    for _ in 0..TXNS {
        client.exec_txn();
    }
}
