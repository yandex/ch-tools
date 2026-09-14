"""Create N direct children on a test server, delete their root, verify absence."""

import argparse
import resource
import sys
from time import monotonic
from uuid import uuid4

from kazoo.client import KazooClient

from ch_tools.chadmin.internal.zookeeper import delete_recursive
from ch_tools.common import logging


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hosts", required=True)
    parser.add_argument("--leaves", type=int, default=100_000)
    parser.add_argument(
        "--timeout", type=float, default=600, help="Soft deletion deadline, seconds"
    )
    args = parser.parse_args()
    if args.leaves < 1 or args.timeout <= 0:
        parser.error("leaves and timeout must be positive")

    logging.configure({"handlers": {}}, "zookeeper-stress")
    root = f"/chadmin-delete-stress-{uuid4()}"
    print(f"Test root: {root}", flush=True)
    client = KazooClient(hosts=args.hosts)
    try:
        client.start(timeout=10)
        started = monotonic()
        client.create(root)
        for start in range(0, args.leaves, 500):
            end = min(start + 500, args.leaves)
            transaction = client.transaction()
            for leaf in range(start, end):
                transaction.create(f"{root}/leaf-{leaf}")
            results = transaction.commit()
            assert len(results) == end - start and all(
                isinstance(result, str) for result in results
            ), f"Seeding failed: {results}"
            if end % 100_000 == 0 or end == args.leaves:
                print(f"Created {end}/{args.leaves} children", flush=True)
        print(f"Seed seconds: {monotonic() - started:.2f}", flush=True)

        started = monotonic()
        delete_recursive(client, [root], delete_timeout=args.timeout)
        assert client.exists(root) is None, f"Root survived: {root}"
        print(f"Delete seconds: {monotonic() - started:.2f}", flush=True)
    finally:
        client.stop()
        client.close()
        peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        peak_bytes = peak_rss if sys.platform == "darwin" else peak_rss * 1024
        print(
            f"Test process peak RSS bytes (including seeding): {peak_bytes}", flush=True
        )


if __name__ == "__main__":
    main()
