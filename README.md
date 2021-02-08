# pg_tm_aux
Extension to create a logical replication slot in the past. It is useful to implement continuous logical streaming from the highly available cluster on physical replication. When primary node of a cluster is failovered, we need to start logical streaming from new node.

We cannot start logical replication from LSN different from LSN of a slot. And cannot create a slot on LSN in the past, particularly before or right after promotion.

This leads to massive waste of network bandwidth in our installations, due to necessity of initial table sync.

This extension implements Yandex Data Transfer auxiliary functions to create slot in the past.

## Usage
```
SELECT * from  pg_create_logical_replication_slot_lsn('dtt3gjq2tfmocenb6vru', 'wal2json', false, pg_lsn('1/20030948'));
SELECT * from pg_logical_slot_peek_changes('dtt3gjq2tfmocenb6vru', null, null);
```
