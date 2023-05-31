# ch-monitoring

ClickHouse monitoring tool.

It provides monitoring for:
- Backup:
  - Validity
  - Age
  - Count
  - Restoration failures
- Presence of orphaned S3 backups
- Core dumps
- Old chunks on Distributed tables
- Presence of geobase
- Aliveness of ClickHouse Keeper
- Count of errors in logs
- Ping-ability of ClickHouse
- Replication lag between replicas
- Re-setup state
- Read-only replicas
- System metrics
- TLS certificate validity

Each monitoring check outputs in following format:
```
<status code>;<message>
```
Where `<status code>` is one of
- `0` - OK
- `1` - WARN
- `2` - CRIT
