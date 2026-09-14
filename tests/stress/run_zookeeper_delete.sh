#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
keeper_image=${STRESS_KEEPER_IMAGE:-clickhouse/clickhouse-keeper:26.3.12.3}
keeper_memory=${STRESS_KEEPER_MEMORY:-16g}
container_id=

cleanup() {
    status=$?
    trap - EXIT
    if [[ -n "$container_id" ]]; then
        if (( status != 0 )); then
            docker logs --tail 60 "$container_id" >&2 || true
        fi
        docker rm -fv "$container_id" >/dev/null || true
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

container_id=$(docker run -d --hostname zookeeper01 \
    --memory "$keeper_memory" --memory-swap "$keeper_memory" --cpus 4 \
    --publish 127.0.0.1::2181 \
    --mount "type=bind,src=$repo_dir/tests/images/zookeeper/config/config.xml,dst=/etc/clickhouse-keeper/stress.xml,readonly" \
    --entrypoint clickhouse-keeper "$keeper_image" \
    --config-file=/etc/clickhouse-keeper/stress.xml)
echo "Disposable Keeper: $container_id (memory limit: $keeper_memory)"
keeper_address=$(docker port "$container_id" 2181/tcp)

cd "$repo_dir"
PYTHONPATH="$repo_dir${PYTHONPATH:+:$PYTHONPATH}" \
    timeout -k 5s "${STRESS_WALL_TIMEOUT:-3h}" \
    .venv/bin/python tests/stress/zookeeper_delete.py --hosts "$keeper_address" "$@"
