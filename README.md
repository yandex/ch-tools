# ch-tools

**ch-tools** is a set of tools for administration and diagnostics of [ClickHouse](https://clickhouse.com/) DBMS.

## Tools

**ch-tools** consists of following components:
- [chadmin](./src/chtools/chadmin/README.md) - ClickHouse administration tool
- [ch-monitoring](./src/chtools/monrun_checks/README.md) - ClickHouse monitoring tool
- [keeper-monitoring](./src/chtools/monrun_checks_keeper/README.md) - ClickHouse Keeper / ZooKeeper monitoring tool
- [ch-s3-credentials](./src/chtools/s3_credentials/README.md) - ClickHouse S3 credentials management tool

All of these tools must be run on the same host as ClickHouse server is running.

## Installation

TBD

## Testing

```bash
cd tests
make venv
source venv/bin/activate
make test
```

If you don't have access to dbaas infra build cache: replace `prebuild_cmd` in `configuration.py`

```python
'prebuild_cmd': [
    'mkdir -p images/minio/bin',
    'wget -N https://dl.min.io/server/minio/release/linux-amd64/minio -O bin/minio',
    'wget -N https://dl.min.io/client/mc/release/linux-amd64/mc -O bin/mc',
],
```
