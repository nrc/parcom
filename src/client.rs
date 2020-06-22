use crate::messages::MsgRequest;
use crate::*;
use rand::{self, Rng};
use std::{
    any,
    cell::UnsafeCell,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
    time::Instant,
};

// The real client is TiDB.
pub struct Client {
    txns: TxnStore,
    tso: Tso,
    transport: transport::TransportSend,
    pending: AtomicUsize,
}

store!(TxnStore, Txn, TxnId, TXNS);

impl Client {
    pub fn new(tso: Tso, transport: transport::TransportSend) -> Client {
        Client {
            txns: TxnStore::new(),
            tso,
            transport,
            pending: AtomicUsize::new(0),
        }
    }

    pub fn shutdown(&self) {
        self.wait_for_pending(|| {
            println!(
                "shutdown timed out {} transactions pending",
                self.pending.load(Ordering::SeqCst)
            );
            for id in 0..TXNS {
                let txn = self.txns.get_unsafe(TxnId(id));
                println!(
                    "{}: {}",
                    id,
                    txn.map(|t| t.status()).unwrap_or("no txn".to_owned())
                );
            }
        });

        self.transport.shutdown();
    }

    // Completion of this function indicates that the transaction was sent, not
    // that it succeeded.
    pub fn exec_txn(&self) {
        let start_ts = self.tso.ts();
        let id = self.tso.id();

        let (_latch, txn) = self.txns.get_or_else(id, Txn::new);

        for _ in 0..READS_PER_TXN {
            let key = self.exec_lock(id, start_ts);
            txn.locks.push((key, false));
        }

        // TODO block waiting for acks (currently ignored). Retry if no ack.
        let keys = self.exec_prewrite(id, start_ts);
        txn.prewrite = Some((keys, false));
        self.pending.fetch_add(1, Ordering::SeqCst);
        // TODO block waiting for prewrite ack (currently ignored). Retry if no ack.
    }

    fn exec_lock(&self, id: TxnId, start_ts: Ts) -> Key {
        let key = random_key();
        let now = Instant::now();

        self.send(messages::LockRequest::new(
            key,
            id,
            start_ts,
            now,
            now + Duration::from_millis(500),
            self.tso.ts(),
        ));

        key
    }

    fn exec_prewrite(&self, id: TxnId, start_ts: Ts) -> Vec<Key> {
        let writes = random_writes();
        let keys = writes.iter().map(|&(k, _)| k).collect();
        let now = Instant::now();

        self.send(messages::PrewriteRequest::new(
            id,
            start_ts,
            self.tso.ts(),
            now,
            now + Duration::from_millis(500),
            writes,
        ));

        keys
    }

    fn handle_prewrite_response(&self, msg: Box<messages::PrewriteResponse>) {
        // TODO retry if no success
        if !msg.success {
            return self.rollback(msg.id);
        }

        self.with_txn(msg.id, |_, txn| {
            txn.prewrite.as_mut().unwrap().1 = true;
        });

        self.check_responses_and_commit(msg.id);
    }

    fn handle_lock_response(&self, msg: Box<messages::LockResponse>) {
        // TODO retry if no success
        if !msg.success {
            return self.rollback(msg.id);
        }

        self.with_txn(msg.id, |_, txn| {
            if txn.rolled_back {
                return;
            }
            for &mut (k, ref mut b) in &mut txn.locks {
                if k == msg.key {
                    *b = true;
                }
            }
        });

        self.check_responses_and_commit(msg.id);
    }

    fn check_responses_and_commit(&self, id: TxnId) {
        self.with_txn(id, |this, txn| {
            if !txn.complete() || txn.rolled_back || txn.committed {
                return;
            }
            // At this point we can communicate success to the user.
            txn.committed = true;

            this.send(messages::FinaliseRequest::new(id));
            this.pending.fetch_sub(1, Ordering::SeqCst);
        });
    }

    fn rollback(&self, id: TxnId) {
        self.with_txn(id, |this, txn| {
            if txn.rolled_back {
                return;
            }
            assert!(!txn.complete() && !txn.committed);
            txn.rolled_back = true;

            this.send(messages::RollbackRequest::new(id, txn.keys()));
            this.pending.fetch_sub(1, Ordering::SeqCst);
        });
    }

    fn with_txn(&self, id: TxnId, f: impl FnOnce(&Client, &mut Txn)) {
        let (_latch, txn) = self.txns.blocking_get(id).unwrap();
        f(self, txn);
    }

    fn wait_for_pending(&self, failure: impl Fn()) {
        let mut sleep_count = 100;
        while self.pending.load(Ordering::SeqCst) > 0 {
            if sleep_count == 0 {
                failure();
                break;
            }

            sleep_count -= 1;
            thread::sleep(Duration::from_millis(100));
        }
    }

    fn send(&self, msg: impl MsgRequest + Send + 'static) {
        self.transport.send(Box::new(msg))
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
        let msg = match msg.downcast::<messages::UnitAck>() {
            Ok(_) => return Ok(()),
            Err(msg) => msg,
        };
        let msg = match msg.downcast() {
            Ok(msg) => {
                self.handle_lock_response(msg);
                return Ok(());
            }
            Err(msg) => msg,
        };
        let msg = match msg.downcast() {
            Ok(msg) => {
                self.handle_prewrite_response(msg);
                return Ok(());
            }
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

fn random_writes() -> Vec<(Key, Value)> {
    (0..WRITES_PER_TXN)
        .map(|_| (random_key(), random_value()))
        .collect()
}

struct Txn {
    locks: Vec<(Key, bool)>,
    prewrite: Option<(Vec<Key>, bool)>,
    rolled_back: bool,
    committed: bool,
}

impl Txn {
    fn new() -> Txn {
        Txn {
            locks: Vec::new(),
            prewrite: None,
            rolled_back: false,
            committed: false,
        }
    }

    fn complete(&self) -> bool {
        match &self.prewrite {
            Some((_, true)) => {}
            _ => return false,
        }

        self.locks.iter().all(|(_, b)| *b)
    }

    fn status(&self) -> String {
        if self.committed {
            return "committed".to_owned();
        }
        if self.rolled_back {
            return "rolled back".to_owned();
        }
        if self.complete() {
            return "complete".to_owned();
        }

        let prewrite = match &self.prewrite {
            Some((_, true)) => "prewrite complete",
            None => "No prewrite",
            _ => "Unfinished prewrite",
        };

        if self.locks.iter().all(|(_, b)| *b) {
            format!("all locks complete, {}", prewrite)
        } else {
            format!("not all locks complete, {}", prewrite)
        }
    }

    fn keys(&self) -> Vec<Key> {
        self.locks
            .iter()
            .map(|&(k, _)| k)
            .chain(
                self.prewrite
                    .iter()
                    .flat_map(|&(ref vks, _)| vks.iter().cloned()),
            )
            .collect()
    }
}
