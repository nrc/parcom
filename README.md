# A 'model' for parallel commit in TiKV

## Assumptions

* first key locked must be the primary key.
* in TiKV we'd store the txn record as part of the primary lock.
* all reads are locking reads
* point access only

## Questions

What if a lock message is lost? Is it checked at prewrite?

## Protocol

Parallel commit builds on top of pipelined pessimistic locking.

### Phase 1: reads and writes

The client (TiDB) sends `lock` messages to the server (TiKV) for each `SELECT ... FOR UPDATE`. Each lock message has a `for_update_ts` (for what?)

Modifications are collected and sent as a `prewrite` message.

Timestamps are acquired in the normal way. The client gets a start TS for the whole transaction which is included with every message. The client must get a timestamp just before sending the `prewrite` message for the commit TS (included in the message).

For both messages, the server checks validity and writes locally. It then sends an `ack` back to the client (is this necessary?). Then it writes to the RaftStore (a 'consensus write'). After the consensus write completes, the server sends a `response` to the client.

For every lock and modification, the server writes a lock to the key's lock CF, each lock stores the transaction's primary key. For modifications, we also store a value in the default CF.

For the primary key's lock, we store a list of keys in the transaction and their status (TODO which is what exactly?), plus an overall status for the transaction. When the prewrite message is received, we store the commit ts.

### Phase 2: finalisation

When the client has `response`s (not `ack`s) for every message in a transaction, it sends a single `finalise` message to the server.

When the server receives a `finalise` message, it *commits* the transaction.

Possible optimisation: the server could track the transaction and finalise it when complete. The issue would be if a `lock` message from the client were lost, but this could be verified in the `prewrite` message.

### Phase 3: commit

Writes are added to each key which is written, with the commit ts from the primary key. The server unlocks all keys, primary key last (is this necessary?). 

### Reads

On read, if a key is locked, then we must look up the primary key and it's lock which holds the transaction record. We wait for the lock's ttl to expire (can we do this? Or do we need to return to the client?), if it expires then we run recovery. If it is unlocked, then the read should be woken up.

### Phase 4: recovery

If the txn's state is written to Raft, then we can delete all remaining locks and then the primary key and lock (something must have failed in finalisation). (Do we need to maintain overall state, seems to only be an optimisation).

Otherwise, we must look up each lock, if all locks have state consensus write, then we run finalisation. If any lock is only written locally or has failed, then we must rollback the transaction.

### Phase 5: rollback

For each modification, we add a rollback write to the key and remove the lock. For each read, we remove the lock. We should remove the primary lock last.
