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

```sh
python3 -m venv venv
source venv/bin/activate
pip install .[test]
flit build --no-use-vcs
flit install

# lint
make lint

# unit tests
pytest

# integration tests (rebuild docker images using a .whl file)
cd tests; behave

# integration tests (do not rebuild docker images)
# useful when you didn't change source code
cd tests; behave -D skip_setup

# integration tests (supply a custom ClickHouse version to test against)
cd tests; CLICKHOUSE_VERSION="1.2.3.4" behave

# If you want to have containers running on failure, supply a flag:
# behave -D no_stop_on_fail
```

Please note: base images for tests are pulled from [chtools Dockerhub](https://hub.docker.com/u/chtools).
If you want to build base images locally, run

```sh
docker buildx bake -f tests/bake.hcl
```

If you want to build base images for multiple versions of ClickHouse, run:

```sh
CLICKHOUSE_VERSIONS='1.2.3.4, 5.6.7.8, latest' docker buildx bake -f tests/bake.hcl
```
