use crate::{
    messages::{MsgRequest, TxnRequest},
    server_types::*,
    *,
};
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
            self.with_key_record(Key(k), |this, record| {
                if record.lock.is_some() {
                    println!("ERROR key({:?}) locked {:?}", k, record.history);
                    this.with_txn_record(record.assert_lock().id, |_, txn| println!("  {:?}", txn));
                }
            });
        }

        self.transport.shutdown();
    }

    fn handle_lock(&self, msg: Box<messages::LockRequest>) {
        // eprintln!("lock {:?} {:?}", msg.key, msg.id);
        self.with_txn_record_for_msg(&*msg, |this, txn_record| {
            if txn_record.commit_state != TxnState::Local {
                // eprintln!("non-local commit state in lock {:?} {:?}", txn_record.commit_state, msg.id);
                this.fail(&*msg, "Transaction concluded".to_owned());
                return;
            }

            if txn_record.keys.contains_key(&msg.key) {
                // Check we have locked this key.
                this.with_key_record_or_default(msg.key, |_, record| {
                    assert_eq!(record.assert_lock().id, msg.id);
                });
            } else {
                if let Err(s) = this.aquire_lock_and_set_value(
                    msg.key,
                    msg.id,
                    txn_record,
                    msg.current_time,
                    None,
                    false,
                ) {
                    // println!("locking read failed {:?} {:?}", msg.key, msg.id);
                    this.stats.failed_rw();
                    this.force_rollback(msg.id, txn_record);
                    this.fail(&*msg, s);
                    return;
                }
                assert_eq!(msg.start_ts, txn_record.start_ts);
                txn_record.keys.insert(msg.key, LockStatus::local());
                txn_record.update_timeout(msg.timeout);
            }
            this.read(msg.key, msg.id);
        });

        // TODO ack can return the read value.
        self.ack(&*msg);
        // TODO maybe retry?
        if let Err(s) = self.async_consensus_write_lock(&msg) {
            self.fail(&*msg, s);
        }
    }

    fn handle_prewrite(&self, msg: Box<messages::PrewriteRequest>) {
        // eprintln!("prewrite {:?} {:?}", msg.id, msg.writes);
        self.with_txn_record_for_msg(&*msg, |this, txn_record| {
            if txn_record.commit_state != TxnState::Local {
                // eprintln!("non-local commit state {:?} {:?}", txn_record.commit_state, msg.id);
                this.fail(&*msg, "Transaction concluded".to_owned());
                return;
            }

            for &(k, v) in &msg.writes {
                // eprintln!("prewrite key {:?} {:?}", msg.id, k);
                if txn_record.keys.contains_key(&k) {
                    // Key is already locked, just need to write the value.
                    this.with_key_record_or_default(k, |_, record| {
                        assert_eq!(record.assert_lock().id, msg.id);
                        record.values.insert(msg.start_ts, v);
                    });
                } else {
                    // Lock and set value.
                    // eprintln!("pre-aquire_lock_and_set_value {:?}", msg.id);
                    if let Err(s) = this.aquire_lock_and_set_value(
                        k,
                        msg.id,
                        txn_record,
                        msg.current_time,
                        Some(v),
                        false,
                    ) {
                        // eprintln!("Failed to lock {:?} {:?}", k, msg.id);
                        this.force_rollback(msg.id, txn_record);
                        this.stats.failed_rw();
                        this.fail(&*msg, s);
                        return;
                    }
                    txn_record.keys.insert(k, LockStatus::local());
                }
                this.stats.write();
            }

            txn_record.commit_ts = Some(msg.commit_ts);
            txn_record.update_timeout(msg.timeout);
        });

        // eprintln!("prewrite success {:?}", msg.id);
        self.ack(&*msg);
        if let Err(s) = self.async_consensus_write_prewrite(&msg) {
            self.fail(&*msg, s);
        }
    }

    fn handle_finalise(&self, msg: Box<messages::FinaliseRequest>) {
        //eprintln!("h finalise {:?}", msg.id);
        while let Err(_) = self.finalise_txn(&msg) {
            thread::sleep(Duration::from_millis(50));
        }
        self.ack(&*msg);
    }

    fn handle_rollback(&self, msg: Box<messages::RollbackRequest>) {
        //eprintln!("h rollback {:?}", msg.id);
        // Retry the rollback since failure must be due to latch deadlock.
        while let Err(_) = self.rollback_txn(&msg) {
            thread::sleep(Duration::from_millis(50));
        }
        self.ack(&*msg);
    }

    fn read(&self, _: Key, _: TxnId) {
        // TODO do we need consensus here.
        self.stats.read();
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
            if is_retry {
                return Err(format!("Key locked {:?} by {:?}", key, lock.id,));
            }

            mem::drop(latch);
            self.resolve_lock(lock.id, time)?;

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
            record.lock(Lock::new(id, lock_kind));
            Ok(())
        }
    }

    // Postcondition: if returns Ok, then all keys in key's txn are unlocked (including key).
    fn resolve_lock(&self, txn_id: TxnId, time: Instant) -> Result<(), String> {
        let (_txn_latch, txn_record) = if let Some(txn) = self.txns.try_try_get(txn_id) {
            txn.expect("No txn record?")
        } else {
            // TODO retry
            return Err("Transaction locked".to_owned());
        };

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

        // Now we must check each key. Note that we have latched the transaction record, so we don't
        // to worry about a lock changing state.
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

    fn wait_for_consensus() -> Result<(), String> {
        // FIXME actually model the replicas
        let wait_time = rand::thread_rng().gen_range(MIN_CONSENSUS_TIME, MAX_CONSENSUS_TIME);
        thread::sleep(Duration::from_millis(wait_time));
        if network_failure() {
            // Could be due to network failure or a conflict in Raft.
            Err("Consensus failure".to_owned())
        } else {
            Ok(())
        }
    }

    fn update_lock_state(&self, key: Key, id: TxnId) {
        self.with_txn_record(id, |this, txn_record| {
            if txn_record.commit_state != TxnState::Local {
                return;
            }

            this.with_key_record(key, |_, record| {
                record.if_locked_by(id, |lock| lock.state = LockState::Consensus);
            });

            let key = txn_record.keys.get_mut(&key).unwrap();
            *key = LockStatus::Locked(LockState::Consensus);
        });
    }

    fn async_consensus_write_lock(&self, msg: &messages::LockRequest) -> Result<(), String> {
        Server::wait_for_consensus()?;

        self.update_lock_state(msg.key, msg.id);
        // eprintln!("consensus lock complete {:?} {:?}", msg.id, msg.key);
        self.respond(msg);
        Ok(())
    }

    fn async_consensus_write_prewrite(
        &self,
        msg: &messages::PrewriteRequest,
    ) -> Result<(), String> {
        // Assumes we do one consensus write for all writes in the transaction.
        Server::wait_for_consensus()?;

        for &(k, _) in &msg.writes {
            self.update_lock_state(k, msg.id);
        }

        // eprintln!(
        //     "consensus prewrite complete {:?} {:?}",
        //     msg.id, msg.writes
        // );

        self.respond(msg);
        Ok(())
    }

    fn finalise(&self, txn_id: TxnId, txn_record: &mut TxnRecord) -> Result<(), String> {
        assert_eq!(txn_record.commit_state, TxnState::Consensus);
        let commit_ts = txn_record.commit_ts.unwrap();
        let start_ts = txn_record.start_ts;

        // Commit each key write, remove the lock.
        txn_record.unlock_keys(|k, s| {
            self.try_with_key_record(k, |_, record| {
                assert!(
                    s.has_consensus(),
                    "no consensus for key {:?}, txn {:?}",
                    k,
                    txn_id,
                );

                let record = record.unwrap();
                let lock = record.assert_lock();
                assert!(lock.id == txn_id);

                let lock_kind = lock.kind;
                record.unlock();

                assert!(record.writes.is_empty() || record.writes[0].0 < commit_ts);
                if lock_kind == LockKind::Modify {
                    record.push_write(start_ts, commit_ts);
                }
            })
        })?;

        txn_record.commit_state = TxnState::Finished;
        // eprintln!("committed {:?}", txn_id);
        self.stats.committed_txn();
        Ok(())
    }

    fn finalise_txn(&self, msg: &messages::FinaliseRequest) -> Result<(), String> {
        self.with_txn_record(msg.id, |this, txn_record| {
            if !txn_record.open() {
                return Ok(());
            }
            txn_record.commit_state = TxnState::Consensus;
            this.finalise(msg.id, txn_record)
        })
    }

    fn force_rollback(&self, id: TxnId, txn_record: &mut TxnRecord) {
        txn_record.commit_state = TxnState::RollingBack;
        while let Err(_) = self.rollback(id, txn_record) {
            thread::sleep(Duration::from_millis(50));
        }
    }

    fn rollback(&self, txn_id: TxnId, txn_record: &mut TxnRecord) -> Result<(), String> {
        assert_eq!(txn_record.commit_state, TxnState::RollingBack);

        txn_record.unlock_keys(|k, s| {
            // println!("rolling back {:?}, {:?}", msg.id, k);

            self.try_with_key_record(k, |_, record| match record {
                Some(r) => {
                    if r.if_locked_by(txn_id, |_| {}) {
                        r.unlock();
                    }
                }
                None => assert_eq!(
                    s,
                    LockStatus::Unknown,
                    "Missing key record for {:?}({:?}) from {:?}",
                    k,
                    s,
                    txn_id
                ),
            })
        })?;

        txn_record.commit_state = TxnState::Finished;
        // eprintln!("rollback {:?}", txn_id);
        self.stats.rolled_back_txn();
        Ok(())
    }

    fn rollback_txn(&self, msg: &messages::RollbackRequest) -> Result<(), String> {
        // FIXME if we get a rollback without previously receiving any locks/prewrites, then the unwrap is bogus.
        self.with_txn_record(msg.id, |this, txn_record| {
            if txn_record.commit_state == TxnState::Finished {
                return Ok(());
            }

            msg.keys.iter().for_each(|k| txn_record.add_key(k));

            // eprintln!("rollback request {:?}", msg.id);
            txn_record.commit_state = TxnState::RollingBack;
            this.rollback(msg.id, txn_record)
        })
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

    fn with_txn_record<T>(&self, id: TxnId, f: impl FnOnce(&Server, &mut TxnRecord) -> T) -> T {
        let (_latch, txn) = self.txns.blocking_get(id).unwrap();
        f(self, txn)
    }

    fn with_key_record(&self, id: Key, f: impl FnOnce(&Server, &mut Record)) {
        if let Some((_latch, record)) = self.keys.blocking_get(id) {
            f(self, record);
        }
    }

    fn with_txn_record_for_msg(
        &self,
        msg: &impl TxnRequest,
        f: impl FnOnce(&Server, &mut TxnRecord),
    ) {
        let (_latch, txn) = self
            .txns
            .get_or_else(msg.id(), || TxnRecord::new(msg.start_ts(), msg.timeout()));
        f(self, txn);
    }

    fn with_key_record_or_default(&self, id: Key, f: impl FnOnce(&Server, &mut Record)) {
        let (_latch, record) = self.keys.get_or_else(id, Record::default);
        f(self, record);
    }

    fn try_with_key_record(
        &self,
        id: Key,
        f: impl FnOnce(&Server, Option<&mut Record>),
    ) -> Result<(), String> {
        let tup = self.keys.try_get(id)?;
        match tup {
            Some((_latch, r)) => f(self, Some(r)),
            None => f(self, None),
        }
        Ok(())
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
