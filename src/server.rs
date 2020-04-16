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
    txns: UnsafeCell<Box<[Option<TxnRecord>; TXNS]>>,
    keys: UnsafeCell<Box<[Option<Record>; MAX_KEY]>>,
    latches: Mutex<Box<[bool; MAX_KEY]>>,
    txn_latches: Mutex<Box<[bool; TXNS]>>,
    transport: transport::TransportSend,
    thread_count: AtomicU64,
}

impl Server {
    pub fn new(transport: transport::TransportSend) -> Server {
        Server {
            transport,
            txns: UnsafeCell::new(Box::new([None; TXNS])),
            keys: UnsafeCell::new(Box::new([None; MAX_KEY])),
            latches: Mutex::new(Box::new([false; MAX_KEY])),
            txn_latches: Mutex::new(Box::new([false; TXNS])),
            thread_count: AtomicU64::new(0),
        }
    }

    pub fn shutdown(&self) {
        assert!(self.latches.lock().unwrap().is_empty());
        unsafe {
            println!("shutdown, len {}", self.keys.get().as_mut().unwrap().len());
            self.keys
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
                let record = self.record(msg.key);
                assert_eq!(record.lock.as_ref().expect("Missing lock").id, msg.id);
            } else {
                if let Err(s) = self.aquire_lock_and_set_value(msg.key, msg.id, msg.start_ts, None)
                {
                    self.fail(&*msg, s);
                    return;
                }
                let (_latch, txn_record) = self.txn_record(msg.id, msg.start_ts);
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
                let record = self.record(k);
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
                let (_latch, txn_record) = self.txn_record(msg.id, msg.start_ts);
                txn_record.keys.insert(k, TxnState::Local);
                locked.push(k);
            }
        }

        {
            let (_latch, txn_record) = self.txn_record(msg.id, msg.start_ts);
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
        loop {
            if let Ok(_latch) = latch::block_on_latch(&self.latches, key) {
                // eprintln!("aquired latch, getting record {:?}", key);
                let record = self.record(key);
                // eprintln!("got record {:?}", key);

                return if record.lock.is_none() {
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
                };
            }
            thread::sleep(Duration::from_millis(50));
        }
    }

    fn record(&self, key: Key) -> &mut Record {
        let map = unsafe { self.keys.get().as_mut().unwrap() };
        let key: usize = key.into();
        match map[key] {
            Some(ref mut r) => r,
            None => {
                map[key] = Some(Record::default());
                map[key].as_mut().unwrap()
            }
        }
    }

    fn assert_record(&self, key: Key) -> &mut Record {
        self.get_record(key)
            .expect(&format!("assert_record failed {:?}", key))
    }

    fn get_record(&self, key: Key) -> Option<&mut Record> {
        unsafe { self.keys.get().as_mut().unwrap()[key.into(): usize].as_mut() }
    }

    // The caller should not hold any latches when calling this function.
    fn txn_record<'a>(
        &'a self,
        id: TxnId,
        start_ts: Ts,
    ) -> (latch::Latch<'a, { TXNS }>, &mut TxnRecord) {
        loop {
            if let Ok(latch) = latch::block_on_latch(&self.txn_latches, id) {
                let id: usize = id.into();
                unsafe {
                    let map = self.txns.get().as_mut().unwrap();
                    if let Some(ref mut txn_record) = map[id] {
                        return (latch, txn_record);
                    }
                    map[id] = Some(TxnRecord::new(start_ts));
                    return (latch, map[id].as_mut().unwrap());
                }
            }
            thread::sleep(Duration::from_millis(50));
        }
    }

    fn txn_contains_key(&self, id: TxnId, key: Key, start_ts: Ts) -> bool {
        let (_latch, record) = self.txn_record(id, start_ts);
        record.keys.contains_key(&key)
    }

    // The caller should not hold any latches when calling this function.
    fn get_txn_record<'a>(
        &'a self,
        id: TxnId,
    ) -> Option<(latch::Latch<'a, { TXNS }>, &mut TxnRecord)> {
        loop {
            if let Ok(latch) = latch::block_on_latch(&self.txn_latches, id) {
                return unsafe {
                    self.txns.get().as_mut().unwrap()[id.into(): usize]
                        .as_mut()
                        .map(|record| (latch, record))
                };
            }
            thread::sleep(Duration::from_millis(50));
        }
    }

    fn remove_txn_record(&self, id: TxnId) {
        loop {
            if let Ok(_latch) = latch::block_on_latch(&self.txn_latches, id) {
                unsafe {
                    self.txns.get().as_mut().unwrap()[id.into(): usize] = None;
                }
                return;
            }
            thread::sleep(Duration::from_millis(50));
        }
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

    fn abort_prewrite(&self, id: TxnId, locked: &[Key]) -> Result<(), String> {
        // Unlock any keys we previously locked.
        let (_latch, txn_record) = self.get_txn_record(id).unwrap();
        for &k in locked {
            txn_record.keys.remove(&k);
            let _latch = latch::block_on_latch(&self.latches, k)?;
            let record = self.assert_record(k);
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
        let (_latch, txn_record) = match self.get_txn_record(id) {
            Some(tup) => tup,
            None => return Err(format!("Transaction {:?} missing {:?}", id, key)),
        };
        {
            let _latch = loop {
                if let Ok(latch) = latch::block_on_latch(&self.latches, key) {
                    break latch;
                }
                thread::sleep(Duration::from_millis(50));
            };

            let record = self.assert_record(key);
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
            let (_latch, txn_record) = match self.get_txn_record(msg.id) {
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
                let _latch = latch::block_on_latch(&self.latches, k)?;
                let record = self.assert_record(k);
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
        self.remove_txn_record(msg.id);
        Ok(())
    }

    fn rollback_txn(&self, msg: &messages::RollbackRequest) -> Result<(), String> {
        for &k in &msg.keys {
            let _latch = latch::block_on_latch(&self.latches, k)?;
            if let Some(record) = self.get_record(k) {
                if let Some(lock) = record.lock.as_ref() {
                    if lock.id == msg.id {
                        record.lock = None;
                    }
                }
            }
        }
        self.remove_txn_record(msg.id);
        // TODO ack?
        Ok(())
    }
}

unsafe impl Sync for Server {}

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
