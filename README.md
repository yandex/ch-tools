[![license](https://img.shields.io/github/license/yandex/ch-tools)](https://github.com/yandex/ch-tools/blob/main/LICENSE)
[![tests status](https://img.shields.io/github/actions/workflow/status/yandex/ch-tools/.github%2Fworkflows%2Fworkflow.yml?event=push&label=tests&logo=github)](https://github.com/yandex/ch-tools/actions/workflows/workflow.yml?query=event%3Apush)
[![chat](https://img.shields.io/badge/telegram-chat-blue)](https://t.me/+O4gURpLnQ604OTE6)

# clickhouse-tools

**clickhouse-tools** is a set of tools for administration and diagnostics of [ClickHouse](https://clickhouse.com/) DBMS.

## Tools

**clickhouse-tools** consist of following components:
- [chadmin](./ch_tools/chadmin/README.md) - ClickHouse administration tool
- [ch-monitoring](./ch_tools/monrun_checks/README.md) - ClickHouse monitoring tool
- [keeper-monitoring](./ch_tools/monrun_checks_keeper/README.md) - ClickHouse Keeper / ZooKeeper monitoring tool
- [ch-s3-credentials](./ch_tools/s3_credentials/README.md) - ClickHouse S3 credentials management tool

All of these tools must be run on the same host as ClickHouse server is running.

## Local development (using poetry)

Requirements: 
* GNU Make version > 3.81

```sh
sudo make install-poetry
# or to install in user's home directory
make POETRY_HOME=~/opt/poetry install-poetry

# lint
make lint

# unit tests
make test-unit
make test-unit PYTEST_ARGS="-k test_name"

# integration tests (rebuild docker images using a .whl file)
make test-integration
make test-integration BEHAVE_ARGS="-i feature_name"

# integration tests (supply a custom ClickHouse version to test against)
CLICKHOUSE_VERSION="1.2.3.4" make test-integration
# If you want to have containers running on failure, supply a flag:
# BEHAVE_ARGS="-D no_stop_on_fail"

# For building deb packages
make prepare-build-deb
make build-deb-package
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
