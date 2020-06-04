use crate::{messages::MsgRequest, server_types::*, *};
use rand::{self, Rng};
use std::{
    any,
    cell::UnsafeCell,
    fmt, mem,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

// The real Server is TiKV.
pub struct Server {
    // We use txn records indexed by start TS, rather than storing this info on the primary lock.
    // The two approaches are equivalent.
    txns: TxnStore,
    keys: KvStore,
    transport: transport::TransportSend,
    thread_count: AtomicU64,
    stats: Statistics,
}

impl Server {
    pub fn new(transport: transport::TransportSend) -> Server {
        Server {
            transport,
            txns: TxnStore::new(),
            keys: KvStore::new(),
            thread_count: AtomicU64::new(0),
            stats: Statistics::new(),
        }
    }

    pub fn shutdown(&self) {
        assert!(self.txns.unlocked());
        assert!(self.keys.unlocked());

        // Check for any keys which are still locked. A key locked suggests an
        // error in the protocol because it means a transaction is still in progress.
        for k in 0..MAX_KEY {
            if let Some((_latch, record)) = self.keys.blocking_get(Key(k)) {
                if record.lock.is_some() {
                    println!("ERROR key({:?}) locked {:?}", k, record.history);
                    let (_latch, txn) = self
                        .txns
                        .blocking_get(record.lock.as_ref().unwrap().id)
                        .unwrap();
                    println!("  {:?}", txn);
                }
            }
        }

        self.transport.shutdown();
    }

    fn handle_lock(&self, msg: Box<messages::LockRequest>) {
        // eprintln!("lock {:?} {:?}", msg.key, msg.id);
        {
            let (_latch, txn_record) = self
                .txns
                .get_or_else(msg.id, || TxnRecord::new(msg.start_ts, msg.timeout));
            if txn_record.commit_state != TxnState::Local {
                // eprintln!("non-local commit state in lock {:?} {:?}", txn_record.commit_state, msg.id);
                self.fail(&*msg, "Transaction concluded".to_owned());
                return;
            }

            // TODO record writes or locks or both?
            if txn_record.keys.contains_key(&msg.key) {
                // Check we have locked this key.
                let (_latch, record) = self.keys.get_or_else(msg.key, Record::default);
                assert_eq!(
                    record
                        .lock
                        .as_ref()
                        .expect(&format!("Missing lock for {:?} {:?}", msg.key, msg.id))
                        .id,
                    msg.id
                );
            } else {
                if let Err(s) = self.aquire_lock_and_set_value(
                    msg.key,
                    msg.id,
                    txn_record,
                    msg.timeout,
                    None,
                    false,
                ) {
                    self.stats.failed_rws.fetch_add(1, Ordering::SeqCst);
                    self.fail(&*msg, s);
                    // println!("locking read failed {:?} {:?}", msg.key, msg.id);
                    return;
                }
                assert_eq!(msg.start_ts, txn_record.start_ts);
                txn_record.keys.insert(msg.key, LockStatus::local());
                if txn_record.timeout < msg.timeout {
                    txn_record.timeout = msg.timeout;
                }
                self.read(msg.key, msg.id);
            }
        }

        self.ack(&*msg);
        self.async_consensus_write_lock(&msg);
    }

    fn handle_prewrite(&self, msg: Box<messages::PrewriteRequest>) {
        // eprintln!("prewrite {:?} {:?}", msg.id, msg.writes);
        {
            let (_latch, txn_record) = self
                .txns
                .get_or_else(msg.id, || TxnRecord::new(msg.start_ts, msg.timeout));
            if txn_record.commit_state != TxnState::Local {
                // eprintln!("non-local commit state {:?} {:?}", txn_record.commit_state, msg.id);
                self.fail(&*msg, "Transaction concluded".to_owned());
                return;
            }

            let mut locked = Vec::new();
            for &(k, v) in &msg.writes {
                // eprintln!("prewrite key {:?} {:?}", msg.id, k);
                if txn_record.keys.contains_key(&k) {
                    // Key is already locked, just need to write the value.
                    let (_latch, record) = self.keys.get_or_else(k, Record::default);
                    assert_eq!(record.lock.as_ref().unwrap().id, msg.id);
                    record.values.insert(msg.start_ts, v);
                } else {
                    // Lock and set value.
                    // eprintln!("pre-aquire_lock_and_set_value {:?}", msg.id);
                    if let Err(s) = self.aquire_lock_and_set_value(
                        k,
                        msg.id,
                        txn_record,
                        msg.timeout,
                        Some(v),
                        false,
                    ) {
                        // eprintln!("Failed to lock {:?} {:?}", k, msg.id);
                        while let Err(_) = self.abort_prewrite(txn_record, msg.id, &locked) {
                            thread::sleep(Duration::from_millis(50));
                        }
                        // eprintln!("prewrite aborted {:?}", msg.id);
                        self.stats.failed_rws.fetch_add(1, Ordering::SeqCst);
                        self.fail(&*msg, s);
                        return;
                    }
                    txn_record.keys.insert(k, LockStatus::local());
                    locked.push(k);
                }
                self.stats.writes.fetch_add(1, Ordering::SeqCst);
            }

            txn_record.commit_ts = Some(msg.commit_ts);
            if txn_record.timeout < msg.timeout {
                txn_record.timeout = msg.timeout;
            }
        }

        // eprintln!("prewrite success {:?}", msg.id);
        self.ack(&*msg);
        self.async_consensus_write_prewrite(&msg);
    }

    fn handle_finalise(&self, msg: Box<messages::FinaliseRequest>) {
        //eprintln!("h finalise {:?}", msg.id);
        // TODO should we break out of this after a while?
        while let Err(_) = self.finalise_txn(&msg) {
            thread::sleep(Duration::from_millis(50));
        }
    }

    fn handle_rollback(&self, msg: Box<messages::RollbackRequest>) {
        //eprintln!("h rollback {:?}", msg.id);
        // Retry the rollback since failure must be due to latch deadlock.
        while let Err(_) = self.rollback_txn(&msg) {
            thread::sleep(Duration::from_millis(50));
        }
    }

    fn read(&self, _: Key, _: TxnId) {
        self.stats.reads.fetch_add(1, Ordering::SeqCst);
    }

    // Precondition: lock must not be held by txn id.
    // Caller should hold the latch for id, but no latches for keys.
    fn aquire_lock_and_set_value(
        &self,
        key: Key,
        id: TxnId,
        txn_record: &mut TxnRecord,
        time: Instant,
        value: Option<Value>,
        is_retry: bool,
    ) -> Result<(), String> {
        let (latch, record) = self.keys.get_or_else(key, Record::default);

        if let Some(lock) = &record.lock {
            let lock_id = lock.id;

            if is_retry {
                return Err(format!("Key locked {:?} by {:?}", key, lock_id,));
            }

            mem::drop(latch);
            {
                // TODO could be moved inside resolve_lock
                if let Some(txn) = self.txns.try_try_get(lock_id) {
                    let (_txn_latch, lock_txn) = txn.expect("No txn record?");
                    let result = self.resolve_lock(lock_id, lock_txn, time);
                    // eprintln!("post-resolve lock {:?} {:?} {:?}", id, lock_id, result);
                    result?;
                } else {
                    // TODO retry
                    return Err("Transaction locked".to_owned());
                }
            }

            // retry
            self.aquire_lock_and_set_value(key, id, txn_record, time, value, true)
        } else {
            let lock_kind = if let Some(v) = value {
                let exists = record.values.insert(txn_record.start_ts, v);
                assert!(exists.is_none());
                LockKind::Modify
            } else {
                LockKind::Read
            };
            let lock = Lock {
                id,
                state: LockState::Local,
                kind: lock_kind,
            };
            record.lock = Some(lock.clone());
            record.history.push((lock, true));
            Ok(())
        }
    }

    // Postcondition: if returns Ok, then all keys in key's txn are unlocked (including key).
    fn resolve_lock(
        &self,
        txn_id: TxnId,
        txn_record: &mut TxnRecord,
        time: Instant,
    ) -> Result<(), String> {
        if time < txn_record.timeout {
            // FIXME return lock time to client so it can resolve and retry.
            return Err("lock still alive".to_owned());
        }

        // Once we get here, if any other thread attempts to set any lock's state to consensus, there
        // would be a problem. We prevent this by forcing consensus write recording to hold the latch
        // for the transaction (which we also hold in this function), which prevents that write being
        // recorded until after this function completes.

        // First check the status of the transaction in case there was an incomplete finalisation or
        // rollback, in those cases continue it.
        match txn_record.commit_state {
            TxnState::Consensus => return self.finalise(txn_id, txn_record),
            TxnState::RollingBack => {
                // eprintln!("Rollback due to resolve_lock {:?}", txn_id);
                return self.rollback(txn_id, txn_record);
            }
            TxnState::Local => {}
            // Unreachable because key must not be locked in this case.
            TxnState::Finished => unreachable!(),
        }

        // Now we must check each key.
        for &s in txn_record.keys.values() {
            if !s.has_consensus() {
                // Reaching consensus timed out, rollback the whole txn.
                // eprintln!("Consensus failure {:?}", txn_id);
                txn_record.commit_state = TxnState::RollingBack;
                return self.rollback(txn_id, txn_record);
            }
        }
        // If we finished the above loop, then every lock has consensus but the txn
        // was never finalised.
        txn_record.commit_state = TxnState::Consensus;
        self.finalise(txn_id, txn_record)
    }

    fn abort_prewrite(
        &self,
        txn_record: &mut TxnRecord,
        id: TxnId,
        locked: &[Key],
    ) -> Result<(), String> {
        // Unlock any keys we previously locked.
        for &k in locked {
            let (_latch, record) = self.keys.try_get(k)?.unwrap();
            txn_record.keys.remove(&k);
            if let Some(lock) = record.lock.as_ref() {
                if lock.id == id {
                    record.history.push((record.lock.take().unwrap(), false));
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

    // TODO never returns Err
    fn update_lock_state(&self, key: Key, id: TxnId) -> Result<(), String> {
        let (_latch, txn_record) = self
            .txns
            .blocking_get(id)
            .expect(&format!("Transaction {:?} missing {:?}", id, key));
        if txn_record.commit_state != TxnState::Local {
            return Ok(());
        }

        {
            let (_latch, record) = self.keys.blocking_get(key).unwrap();
            match &mut record.lock {
                Some(lock) if lock.id == id => lock.state = LockState::Consensus,
                _ => {
                    // Key has been unlocked (and possibly re-locked), which means the transaction
                    // has been rolled back.
                    return Ok(());
                }
            }
        }

        match txn_record.keys.get_mut(&key) {
            Some(key) => *key = LockStatus::Locked(LockState::Consensus),
            None => panic!("Expected key {:?}, found none", key),
        }

        // println!("got consensus {:?} {:?}", key, id);
        Ok(())
    }

    fn async_consensus_write_lock(&self, msg: &messages::LockRequest) {
        Server::wait_for_consensus();

        match self.update_lock_state(msg.key, msg.id) {
            Ok(_) => {
                // eprintln!("consensus lock complete {:?} {:?}", msg.id, msg.key);
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
                eprintln!("update lock failed {:?} {}", msg.id, s);
                // Leaves keys locked without a txn, but that should get sorted out when we rollback.
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

    fn finalise(&self, txn_id: TxnId, txn_record: &mut TxnRecord) -> Result<(), String> {
        assert_eq!(txn_record.commit_state, TxnState::Consensus);
        // Commit each key write, remove the lock.
        for (&k, s) in &mut txn_record.keys {
            if *s == LockStatus::Unlocked {
                continue;
            }
            let (_latch, record) = self.keys.try_get(k)?.unwrap();
            assert!(
                s.has_consensus(),
                "no consensus for key {:?}, txn {:?}",
                k,
                txn_id,
            );
            // eprintln!("unlock {:?} {:?}", k, txn_id);
            let lock = record.lock.as_ref().unwrap();
            assert!(lock.id == txn_id);
            let lock_kind = record.lock.as_ref().unwrap().kind;
            record.lock = None;

            assert!(record.writes.is_empty() || record.writes[0].0 < txn_record.commit_ts.unwrap());
            if lock_kind == LockKind::Modify {
                record
                    .writes
                    .insert(0, (txn_record.commit_ts.unwrap(), txn_record.start_ts));
            }
            *s = LockStatus::Unlocked;
        }

        txn_record.commit_state = TxnState::Finished;
        // eprintln!("committed {:?}", txn_id);
        self.stats.committed_txns.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn finalise_txn(&self, msg: &messages::FinaliseRequest) -> Result<(), String> {
        let (_latch, txn_record) = self
            .txns
            .blocking_get(msg.id)
            .expect(&format!("Missing txn record {:?}", msg.id));
        if txn_record.commit_state == TxnState::RollingBack
            || txn_record.commit_state == TxnState::Finished
        {
            return Ok(());
        }
        txn_record.commit_state = TxnState::Consensus;
        self.finalise(msg.id, txn_record)
    }

    fn rollback(&self, txn_id: TxnId, txn_record: &mut TxnRecord) -> Result<(), String> {
        assert_eq!(txn_record.commit_state, TxnState::RollingBack);
        for (&k, s) in &mut txn_record.keys {
            // println!("rolling back {:?}, {:?}", msg.id, k);
            if *s == LockStatus::Unlocked {
                continue;
            }
            let (_latch, record) = match self.keys.try_get(k)? {
                Some(tup) => tup,
                None if *s == LockStatus::Unknown => {
                    *s = LockStatus::Unlocked;
                    continue;
                }
                None => unreachable!("Missing key record for {:?}({:?}) from {:?}", k, s, txn_id),
            };
            if let Some(lock) = record.lock.as_ref() {
                if lock.id == txn_id {
                    record.history.push((record.lock.take().unwrap(), false));
                }
            }
            *s = LockStatus::Unlocked;
        }

        txn_record.commit_state = TxnState::Finished;
        // eprintln!("rollback {:?}", txn_id);
        self.stats.rolled_back_txns.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn rollback_txn(&self, msg: &messages::RollbackRequest) -> Result<(), String> {
        // FIXME if we get a rollback without previously receiving any locks/prewrites, then the unwrap is bogus.
        let (_latch, txn_record) = self.txns.blocking_get(msg.id).unwrap();
        if txn_record.commit_state == TxnState::Finished {
            return Ok(());
        }
        for k in &msg.keys {
            if !txn_record.keys.contains_key(k) {
                txn_record.keys.insert(*k, LockStatus::Unknown);
            }
        }

        txn_record.commit_state = TxnState::RollingBack;
        // eprintln!("rollback request {:?}", msg.id);
        self.rollback(msg.id, txn_record)
        // TODO ack?
    }

    fn ack<T: MsgRequest>(&self, msg: &T) {
        let msg = msg.ack().unwrap();
        self.transport.send(Box::new(msg));
    }

    fn fail<T: MsgRequest + fmt::Debug>(&self, msg: &T, _s: String) {
        // println!("Message handling failed {:?}: {}", msg, _s);
        if let Some(msg) = msg.response(false) {
            self.transport.send(Box::new(msg));
        }
    }

    fn respond<T: MsgRequest>(&self, msg: &T) {
        let msg = msg.response(true).unwrap();
        self.transport.send(Box::new(msg));
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
        self.stats.print();
        Ok(())
    }
}

// Invariant: never hold a key latch when attempting to get a txn latch.
store!(TxnStore, TxnRecord, TxnId, TXNS);
store!(KvStore, Record, Key, MAX_KEY);
