# A 'model' for parallel commit in TiKV

## Questions

* Interaction with large transactions.
* Interaction with optimistic transactions.
* check txn status message?
* heartbeat messages?

## Protocol design

Parallel commit builds on top of pipelined pessimistic locking.

### Phase 1: reads and writes

The client (TiDB) sends `lock` messages to the server (TiKV) for each `SELECT ... FOR UPDATE`. Each lock message has a `for_update_ts`.

Modifications are collected in a buffer and sent as a `prewrite` message. No reads are permitted after prewrite.

The client gets a start TS for the whole transaction which is included with every message. The client gets a commit ts just before sending the `prewrite` message.

For both messages, the server checks validity and writes locally. It then sends an `ack` back to the client. For a read, the read value is returned with the ack. Then the server writes the lock and/or modifications to the RaftStore (a 'consensus write'). After the consensus write completes, the server sends a `response` to the client.

For every lock and modification, the server writes a lock to the key's lock CF; each lock stores a reference to the transaction's primary key. For modifications, we also store a value in the default CF.

For the primary key's lock, we store a list of keys in the transaction and their status (whether the key is locked locally, across all nodes, or unlocked), plus an overall status for the transaction. When the prewrite message is received, we store the commit ts.

TODO multiple regions.

### Phase 2: finalisation

When the client has `response`s (not `ack`s) for every message in a transaction, it sends a single `finalise` message to the server. The client considers the transaction complete when it sends the `finalise` message, it does not need to wait for a response.

When the server receives a `finalise` message, it *commits* the transaction. Committing is guaranteed to succeed.

Possible optimisation: the server could finalise the transaction when the prewrite completes (see the resolve lock section).

Writes are added to each key which is written, with the commit ts from the primary key. The server unlocks all keys, primary key last. 

### Reads

On read, if a key is locked, then we must look up the primary key and it's lock which holds the transaction record. We wait for the lock's ttl to expire (based on the read's `for_update_ts`), if it has expired then the server resolves the lock. As long as there is no error in resolution, the read result can be returned to the client.

### Resolve lock

Resolve lock determines a transaction's commit status. If the transaction has been rolled back or committed, there is nothing to do. Otherwise, if every consensus write has succeeded, the transaction is committed. Otherwise, the transaction is considered to have timed out and is rolled back.

First we check the txn's state as recorded in the primary key. If it is written to Raft, then we can finalise (something must have failed in finalisation or finalisation was never received). (I think this is only an optimisation, we could skip this step and go straight to the next).

Otherwise, we check each lock, if all locks have state consensus write, then we run finalisation. If any lock is only written locally or has failed, then we must rollback the transaction.

### Rollback

For each modification in a transaction, add a rollback write to the key and remove the lock. For each read, remove the lock. The primary lock should be removed last.

TODO partial rollback with for_update_ts


## The model

We run a client (representing TiDB) and a server (representing TiKV) on different threads. We abstract away as many details as possible and check more invariants, panicking where possible. The implementation inside the client and server is different to the real implementation but the interface and core concepts are the same. Errors are injected randomly in various places (WIP).

### Assumptions

* first key locked must be the primary key.
* all reads are locking reads
* point access only

### Differences to TiKV

* Clock times are stored outside of timestamps which are equivalent to the logical part of TiDB timestamps. All time comes from the client (we abstract away PD).
* Latching is simpler.
* We avoid deadlock due to latching by latching order, rather than by detecting deadlock.
* Transactions are identified by id rather than by start ts.
* We have a transaction record (`TxnRecord`) rather than storing extra data in the primary lock
* The data storage model (CFs, etc.) is abstracted (`Record`).
* We model Raft reads and writes as a random sleep with a random chance of error.
* We have one thread per request, rather than use a thread pool or scheduler or any async code.
