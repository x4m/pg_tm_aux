# pg_tm_aux (Not needed since Postgres 17, use logical slots failover)
Extension to create a logical replication slot in the past. It is useful to implement continuous logical streaming from the highly available cluster on physical replication. When primary node of a cluster is failovered, we need to start logical streaming from new node.

We cannot start logical replication from LSN different from LSN of a slot. And cannot create a slot on LSN in the past, particularly before or right after promotion.

This leads to massive waste of network bandwidth in our installations, due to necessity of initial table sync.

This extension implements Yandex Data Transfer auxiliary functions to create slot in the past.

## Usage
```
SELECT * from  pg_create_logical_replication_slot_lsn('dtt3gjq2tfmocenb6vru', 'wal2json', false, pg_lsn('1/20030948'));
SELECT * from pg_logical_slot_peek_changes('dtt3gjq2tfmocenb6vru', null, null);
```
## Limitations

In certain cases pg_tm_aux cannot create slot:
1. WAL for LSN is not accesible anymore on the new primary server
2. Catalog snapshot cannot be built for LSN

Logical replication may be slightly ahead of physical replication that is acknowledged by synchronous\quorum replics. In this case, logical stream might have some transactions that are not committed and will not survive Primary node failover. To protect from this pg_tm_aux do not allow to create a slot on current timeline. For more informatin please refer to [FOSDEM talk Caveats of replication](https://archive.fosdem.org/2021/schedule/event/postgresql_caveats_of_replication/).
