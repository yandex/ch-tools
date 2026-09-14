# Local deletion stress test

Requires Docker and the project's `.venv`. Run from the repository root; excluded from CI.

```sh
bash tests/stress/run_zookeeper_delete.sh --leaves 100000
bash tests/stress/run_zookeeper_delete.sh --leaves 25000000 --timeout 7200
```

Creates direct children in temporary Keeper, reports deletion time and test-process
peak RSS (including seeding), verifies root absence,
then removes the container and test data. Failure exits nonzero.

Keeper defaults: 4 CPUs, 16 GiB RAM, no swap; client needs additional RAM.
Override `STRESS_KEEPER_MEMORY` / `STRESS_KEEPER_IMAGE` as needed.
Uses test configuration with `force_sync=false`.
Requires GNU `timeout`; `STRESS_WALL_TIMEOUT` caps the whole run (default: 3h).

For an existing disposable server, use `zookeeper_delete.py --hosts host:port`
with `PYTHONPATH=.`; failed runs leave their generated root for manual cleanup.
