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

All of these tools must be run on the same host as ClickHouse server is running.


## Installation

In order to get an up-to-date version of ch-tools, run `make build`. It will produce
Python whl package that can be installed using `pip install` or `uv tool install`.

<details>
<summary>Example</summary>

```
$ make build
Building python packages...
Building source distribution...
Building wheel from source distribution...
Successfully built dist/clickhouse_tools-2.866.67727059.tar.gz
Successfully built dist/clickhouse_tools-2.866.67727059-py3-none-any.whl
```

```
$ uv tool install dist/*whl
Resolved 37 packages in 716ms
Prepared 10 packages in 1.36s
Installed 37 packages in 97ms
 + boto3==1.35.99
 + botocore==1.35.99
 + certifi==2026.2.25
 + cffi==2.0.0
 + charset-normalizer==3.4.7
 + click==8.1.8
 + clickhouse-tools==2.866.67727059 (from file:///Users/alex-burmak/workspace/ch-tools/dist/clickhouse_tools-2.866.67727059-py3-none-any.whl)
 + cloup==3.0.9
 + cryptography==46.0.6
 + deepdiff==9.0.0
 + dnspython==2.8.0
 + file-read-backwards==3.2.0
 + humanfriendly==10.0
 + idna==3.11
 + jinja2==3.1.6
 + jmespath==1.1.0
 + kazoo==2.11.0
 + loguru==0.7.3
 + lxml==6.0.2
 + markupsafe==3.0.3
 + orderly-set==5.5.0
 + packaging==26.0
 + psutil==7.2.2
 + pycparser==3.0
 + pygments==2.20.0
 + pyopenssl==26.0.0
 + python-dateutil==2.9.0.post0
 + pyyaml==6.0.3
 + requests==2.33.1
 + s3transfer==0.10.4
 + six==1.17.0
 + tabulate==0.10.0
 + tenacity==9.1.4
 + termcolor==3.3.0
 + tqdm==4.67.3
 + urllib3==2.6.3
 + xmltodict==1.0.4
Installed 3 executables: ch-monitoring, chadmin, keeper-monitoring
```

</details>


## Local development

Requirements: 
* GNU Make version > 3.81
* [uv](https://docs.astral.sh/uv)
* Docker

```sh
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
make build-deb-package
```
