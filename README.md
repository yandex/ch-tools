# ch-tools

**ch-tools** is a set of tools for administration and diagnostics of [ClickHouse](https://clickhouse.com/) DBMS.

## Tools

**ch-tools** consist of following components:
- [chadmin](./src/chtools/chadmin/README.md) - ClickHouse administration tool
- [ch-monitoring](./src/chtools/monrun_checks/README.md) - ClickHouse monitoring tool
- [keeper-monitoring](./src/chtools/monrun_checks_keeper/README.md) - ClickHouse Keeper / ZooKeeper monitoring tool
- [ch-s3-credentials](./src/chtools/s3_credentials/README.md) - ClickHouse S3 credentials management tool

All of these tools must be run on the same host as ClickHouse server is running.

## Local development (using venv)

```bash
python3 -m venv venv
source venv/bin/activate
pip install .[test]
flit install
flit build --no-use-vcs

# lint
black .
isort .

# run tests
pytest

CLICKHOUSE_VERSION="1.2.3.4" make test-integration-prepare
make test-integration
```
