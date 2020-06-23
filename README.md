# A 'model' for parallel commit in TiKV

## TODO

* Use futures instead of threads
* Fix resolve lock
* Implement other transactions models

## Questions

* Interaction with large transactions.
* Interaction with optimistic transactions.
* check txn status message?
* heartbeat messages?


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


## Disadvantages

Reads can block waiting for locks. We should be sure reads can't fail (other than for network issues, etc.).


## Single node parallel commit and 1pc

If a transaction takes place on a single region (and I think this extends to multiple regions on the same host; what about reads on a different node?), then we don't need to do a two-phase commit (with separate prewrite and commit phases). Instead the prewrite message can do both and we do a one-phase commit.

In the single node case of parallel commit, there is effectively 1pc because the keys are considered committed as soon as all the keys are locked, we should return success to the client at the same time as an explicit 1pc. Furthermore, if there are multiple regions on the same node, we'll still get roughly the same performance as 1pc.
