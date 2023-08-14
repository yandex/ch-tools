# keeper-monitoring

ClickHouse Keeper / ZooKeeper monitoring tool.

It provides monitoring for:
- Aliveness of Keeper
- Average latency
- Minimum latency
- Maximum latency
- Request queue size
- Open file descriptors
- Version of Keeper
- Presence of snapshots
- Presence of `NullPointerException` in logs for 24 hours

Each monitoring check outputs in following format:
```
<status code>;<message>
```
Where `<status code>` is one of
- `0` - OK
- `1` - WARN
- `2` - CRIT

