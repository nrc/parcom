use crate::{messages::MsgRequest, *};
use rand::{self, Rng};
use std::{
    any,
    cell::UnsafeCell,
    collections::HashMap,
    fmt,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};

pub struct Server {
    // We use txn records indexed by start TS, rather than storing this info on the primary lock.
    // The two approaches are equivalent.
    txns: TxnStore,
    keys: KvStore,
    transport: transport::TransportSend,
    thread_count: AtomicU64,
}

impl Server {
    pub fn new(transport: transport::TransportSend) -> Server {
        Server {
            transport,
            txns: TxnStore::new(),
            keys: KvStore::new(),
            thread_count: AtomicU64::new(0),
        }
    }

    pub fn shutdown(&self) {
        assert!(self.txns.latches.lock().unwrap().is_empty());
        assert!(self.keys.latches.lock().unwrap().is_empty());
        unsafe {
            println!(
                "shutdown, len {}",
                self.keys.entries.get().as_mut().unwrap().len()
            );
            self.keys
                .entries
                .get()
                .as_mut()
                .unwrap()
                .iter()
                .for_each(|k| assert!(k.is_none() || k.as_ref().unwrap().lock.is_none()));
        }

        self.transport.shutdown();
    }

    fn handle_lock(&self, msg: Box<messages::LockRequest>) {
        // eprintln!("lock {:?} {:?}", msg.key, msg.id);
        {
            // TODO record writes or locks or both?
            if self.txn_contains_key(msg.id, msg.key, msg.start_ts) {
                // Check we have locked this key.
                let (_latch, record) = self.keys.get_or_else(msg.key, Record::default);
                assert_eq!(record.lock.as_ref().expect("Missing lock").id, msg.id);
            } else {
                if let Err(s) = self.aquire_lock_and_set_value(msg.key, msg.id, msg.start_ts, None)
                {
                    self.fail(&*msg, s);
                    return;
                }
                let (_latch, txn_record) = self
                    .txns
                    .get_or_else(msg.id, || TxnRecord::new(msg.start_ts));
                assert_eq!(msg.start_ts, txn_record.start_ts);
                txn_record.keys.insert(msg.key, TxnState::Local);
            }
        }

        self.ack(&*msg);
        self.async_consensus_write_lock(&msg);
    }

    fn handle_prewrite(&self, msg: Box<messages::PrewriteRequest>) {
        // eprintln!("prewrite {:?} {:?}", msg.id, msg.writes);
        let mut locked = Vec::new();
        for &(k, v) in &msg.writes {
            // eprintln!("prewrite key {:?} {:?}", msg.id, k);
            if self.txn_contains_key(msg.id, k, msg.start_ts) {
                // Key is already locked, just need to write the value.
                let (_latch, record) = self.keys.get_or_else(k, Record::default);
                assert!(record.lock.as_ref().unwrap().id == msg.id);
                record.values.insert(msg.start_ts, v);
            } else {
                // Lock and set value.
                if let Err(s) = self.aquire_lock_and_set_value(k, msg.id, msg.start_ts, Some(v)) {
                    // eprintln!("Failed to lock {:?} {:?}", k, msg.id);
                    while let Err(_) = self.abort_prewrite(msg.id, &locked) {
                        thread::sleep(Duration::from_millis(50));
                    }
                    self.fail(&*msg, s);
                    return;
                }
                let (_latch, txn_record) = self
                    .txns
                    .get_or_else(msg.id, || TxnRecord::new(msg.start_ts));
                txn_record.keys.insert(k, TxnState::Local);
                locked.push(k);
            }
        }

        {
            let (_latch, txn_record) = self
                .txns
                .get_or_else(msg.id, || TxnRecord::new(msg.start_ts));
            txn_record.commit_ts = Some(msg.commit_ts);
        }

        self.ack(&*msg);
        self.async_consensus_write_prewrite(&msg);
    }

    fn handle_finalise(&self, msg: Box<messages::FinaliseRequest>) {
        // eprintln!("finalise {:?}", msg.id);
        while let Err(_) = self.finalise_txn(&msg) {
            thread::sleep(Duration::from_millis(50));
        }
    }

    fn handle_rollback(&self, msg: Box<messages::RollbackRequest>) {
        // eprintln!("rollback {:?}", msg.id);
        // Retry the rollback since failure must be due to latch deadlock.
        while let Err(_) = self.rollback_txn(&msg) {
            thread::sleep(Duration::from_millis(50));
        }
    }

    // Precondition: lock must not be held by txn id.
    // No latches may be held.
    fn aquire_lock_and_set_value(
        &self,
        key: Key,
        id: TxnId,
        start_ts: Ts,
        value: Option<Value>,
    ) -> Result<(), String> {
        let (_latch, record) = self.keys.get_or_else(key, Record::default);

        if record.lock.is_none() {
            let lock_kind = if let Some(v) = value {
                let exists = record.values.insert(start_ts, v);
                assert!(exists.is_none());
                LockKind::Modify
            } else {
                LockKind::Read
            };
            record.lock = Some(Lock {
                id,
                state: TxnState::Local,
                kind: lock_kind,
            });

            Ok(())
        } else {
            Err(format!(
                "Key locked {:?} by {:?}",
                key,
                record.lock.as_ref().unwrap().id
            ))
        }
    }

    fn abort_prewrite(&self, id: TxnId, locked: &[Key]) -> Result<(), String> {
        // Unlock any keys we previously locked.
        let (_latch, txn_record) = self.txns.blocking_get(id).unwrap();
        for &k in locked {
            txn_record.keys.remove(&k);
            let (_latch, record) = self.keys.try_get(k)?.unwrap();
            if let Some(lock) = record.lock.as_ref() {
                if lock.id == id {
                    record.lock = None;
                }
            }
        }

        Ok(())
    }

    fn wait_for_consensus() {
        // FIXME actually model the replicas
        let wait_time = rand::thread_rng().gen_range(MIN_CONSENSUS_TIME, MAX_CONSENSUS_TIME);
        thread::sleep(Duration::from_millis(wait_time));
    }

    fn update_lock_state(&self, key: Key, id: TxnId) -> Result<(), String> {
        let (_latch, txn_record) = match self.txns.blocking_get(id) {
            Some(tup) => tup,
            None => return Err(format!("Transaction {:?} missing {:?}", id, key)),
        };
        {
            let (_latch, record) = self.keys.blocking_get(key).unwrap();
            match &mut record.lock {
                Some(lock) if lock.id == id => lock.state = TxnState::Consensus,
                _ => {
                    // Key has been unlocked (and possibly re-locked), which means the transaction
                    // has been rolled back.
                    return Ok(());
                }
            }
        }

        match txn_record.keys.get_mut(&key) {
            Some(key) => *key = TxnState::Consensus,
            None => panic!("Expected key {:?}, found none", key),
        }

        Ok(())
    }

    fn async_consensus_write_lock(&self, msg: &messages::LockRequest) {
        Server::wait_for_consensus();

        match self.update_lock_state(msg.key, msg.id) {
            Ok(_) => {
                // eprintln!("consensus lock complete {:?} {:?}", msg.id, msg.key);

                // We could check all keys and set the record lock state.

                self.respond(msg);
            }
            Err(s) => self.fail(msg, s),
        }
    }

    fn async_consensus_write_prewrite(&self, msg: &messages::PrewriteRequest) {
        // Assumes we do one consensus write for all writes in the transaction.
        Server::wait_for_consensus();

        for &(k, _) in &msg.writes {
            if let Err(s) = self.update_lock_state(k, msg.id) {
                // Leaves locks messed up, but that should get sorted out when we rollback.
                self.fail(msg, s);
                return;
            }
        }
        // eprintln!(
        //     "consensus prewrite complete {:?} {:?}",
        //     msg.id, msg.writes
        // );

        self.respond(msg);
    }

    fn finalise_txn(&self, msg: &messages::FinaliseRequest) -> Result<(), String> {
        {
            let (_latch, txn_record) = match self.txns.blocking_get(msg.id) {
                Some(tup) => tup,
                None => {
                    self.fail(msg, format!("Missing txn record"));
                    return Ok(());
                }
            };
            assert_eq!(
                txn_record.commit_state,
                TxnState::Local,
                "re-finalising? {:?}",
                msg.id
            );
            txn_record.commit_state = TxnState::Consensus;

            // Commit each key write, remove the lock.
            for (&k, &s) in &txn_record.keys {
                assert!(
                    s == TxnState::Consensus,
                    "no consensus for key {:?}, txn {:?}",
                    k,
                    msg.id,
                );
                let (_latch, record) = self.keys.try_get(k)?.unwrap();
                // eprintln!("unlock {:?} {:?}", k, msg.id);
                if let Some(lock) = record.lock.as_ref() {
                    assert!(lock.id == msg.id);
                    let lock_kind = record.lock.as_ref().unwrap().kind;
                    record.lock = None;

                    assert!(
                        record.writes.is_empty()
                            || record.writes[0].0 < txn_record.commit_ts.unwrap()
                    );
                    if lock_kind == LockKind::Modify {
                        record
                            .writes
                            .insert(0, (txn_record.commit_ts.unwrap(), txn_record.start_ts));
                    }
                }
            }
        }

        // Remove the txn record to conclude the transaction.
        self.txns.remove(msg.id);
        Ok(())
    }

    fn rollback_txn(&self, msg: &messages::RollbackRequest) -> Result<(), String> {
        for &k in &msg.keys {
            if let Some((_latch, record)) = self.keys.try_get(k)? {
                if let Some(lock) = record.lock.as_ref() {
                    if lock.id == msg.id {
                        record.lock = None;
                    }
                }
            }
        }
        self.txns.remove(msg.id);
        // TODO ack?
        Ok(())
    }

    fn ack<T: MsgRequest>(&self, msg: &T) {
        let msg = msg.ack();
        self.transport.send(Box::new(msg));
    }

    fn fail<T: MsgRequest + fmt::Debug>(&self, msg: &T, s: String) {
        println!("Message handling failed {:?}: {}", msg, s);
        let msg = msg.response(false);
        self.transport.send(Box::new(msg));
    }

    fn respond<T: MsgRequest>(&self, msg: &T) {
        let msg = msg.response(true);
        self.transport.send(Box::new(msg));
    }

    fn txn_contains_key(&self, id: TxnId, key: Key, start_ts: Ts) -> bool {
        let (_latch, record) = self.txns.get_or_else(id, || TxnRecord::new(start_ts));
        record.keys.contains_key(&key)
    }
}

impl transport::Receiver for Server {
    fn recv_msg(self: Arc<Self>, mut msg: Box<dyn any::Any + Send>) -> Result<(), String> {
        macro_rules! handle_msg {
            ($msg: ident, $handler: ident) => {
                match $msg.downcast() {
                    Ok(msg) => {
                        let this = self.clone();
                        thread::spawn(move || {
                            this.thread_count.fetch_add(1, Ordering::SeqCst);
                            this.$handler(msg);
                            this.thread_count.fetch_sub(1, Ordering::SeqCst);
                        });
                        return Ok(());
                    }
                    Err(msg) => $msg = msg,
                };
            };
        }

        handle_msg!(msg, handle_lock);
        handle_msg!(msg, handle_prewrite);
        handle_msg!(msg, handle_finalise);
        handle_msg!(msg, handle_rollback);

        Err(format!("Unknown message type: {:?}", msg.type_id()))
    }

    fn handle_shutdown(self: Arc<Self>) -> Result<(), String> {
        // eprintln!("handle_shutdown");
        while self.thread_count.load(Ordering::SeqCst) > 0 {
            println!(
                "waiting for {} threads...",
                self.thread_count.load(Ordering::SeqCst)
            );
            thread::sleep(Duration::from_millis(100));
        }
        Ok(())
    }
}

macro_rules! store {
    ($name: ident, $T: ty, $Id: ty, $size: ident) => {
        struct $name {
            entries: UnsafeCell<Box<[Option<$T>; $size]>>,
            latches: Mutex<Box<[bool; $size]>>,
        }

        #[allow(dead_code)]
        impl $name {
            fn new() -> $name {
                $name {
                    entries: UnsafeCell::new(Box::new([None; $size])),
                    latches: Mutex::new(Box::new([false; $size])),
                }
            }

            // The caller should not hold any latches when calling this function.
            fn get_or_else<'a, F>(
                &'a self,
                id: $Id,
                or_else: F,
            ) -> (latch::Latch<'a, $size>, &mut $T)
            where
                F: FnOnce() -> $T,
            {
                loop {
                    if let Ok(latch) = latch::block_on_latch(&self.latches, id) {
                        let id: usize = id.into();
                        unsafe {
                            let map = self.entries.get().as_mut().unwrap();
                            if let Some(ref mut entry) = map[id] {
                                return (latch, entry);
                            }
                            map[id] = Some(or_else());
                            return (latch, map[id].as_mut().unwrap());
                        }
                    }
                    thread::sleep(Duration::from_millis(50));
                }
            }

            // The caller should not hold any latches when calling this function.
            fn try_get<'a>(
                &'a self,
                id: $Id,
            ) -> Result<Option<(latch::Latch<'a, $size>, &mut $T)>, String> {
                latch::block_on_latch(&self.latches, id).map(|latch| {
                    return unsafe {
                        self.entries.get().as_mut().unwrap()[id.into(): usize]
                            .as_mut()
                            .map(|record| (latch, record))
                    };
                })
            }

            // The caller should not hold any latches when calling this function.
            fn blocking_get<'a>(&'a self, id: $Id) -> Option<(latch::Latch<'a, $size>, &mut $T)> {
                loop {
                    if let Ok(latch) = latch::block_on_latch(&self.latches, id) {
                        return unsafe {
                            self.entries.get().as_mut().unwrap()[id.into(): usize]
                                .as_mut()
                                .map(|record| (latch, record))
                        };
                    }
                    thread::sleep(Duration::from_millis(50));
                }
            }

            fn remove(&self, id: $Id) {
                loop {
                    if let Ok(_latch) = latch::block_on_latch(&self.latches, id) {
                        unsafe {
                            self.entries.get().as_mut().unwrap()[id.into(): usize] = None;
                        }
                        return;
                    }
                    thread::sleep(Duration::from_millis(50));
                }
            }
        }

        unsafe impl Sync for $name {}
    };
}

store!(TxnStore, TxnRecord, TxnId, TXNS);
store!(KvStore, Record, Key, MAX_KEY);

#[derive(Default, Clone)]
struct Record {
    values: HashMap<Ts, Value>,
    // commit ts, start ts; ordered by commit ts, descending.
    writes: Vec<(Ts, Ts)>,
    lock: Option<Lock>,
}

#[derive(Clone)]
struct TxnRecord {
    keys: HashMap<Key, TxnState>,
    commit_state: TxnState,
    commit_ts: Option<Ts>,
    start_ts: Ts,
}

impl TxnRecord {
    fn new(start_ts: Ts) -> TxnRecord {
        TxnRecord {
            keys: HashMap::new(),
            commit_state: TxnState::Local,
            commit_ts: None,
            start_ts,
        }
    }
}

#[derive(Debug, Clone)]
struct Lock {
    id: TxnId,
    state: TxnState,
    kind: LockKind,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum TxnState {
    Local,
    Consensus,
    // Failed,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum LockKind {
    Read,
    Modify,
}
