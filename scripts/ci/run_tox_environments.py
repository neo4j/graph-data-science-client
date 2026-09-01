import argparse
import logging
import os
import subprocess
from pathlib import Path

logging.basicConfig(level=logging.INFO)

# Rough steady-state cost of one environment's containers (two capped Neo4j JVMs, a
# session, mocks) plus boot transients; all environments of a partition run at once.
PARALLEL_MEMORY_THRESHOLD_GIB = 16
PARALLEL_CPU_THRESHOLD = 4


def available_memory_gib() -> float | None:
    """Best-effort memory budget of this agent: the cgroup limit of the container we run
    in when set, else the host's available memory; None when it cannot be determined."""
    try:
        raw = Path("/sys/fs/cgroup/memory.max").read_text().strip()
        if raw != "max":
            return int(raw) / 2**30
    except OSError:
        pass
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) / 2**20  # kB
    except OSError:
        pass
    return None


def can_run_parallel() -> bool:
    memory_gib = available_memory_gib()
    cpus = os.cpu_count() or 0
    if memory_gib is None:
        logging.warning(f"Could not determine the available memory; deciding by cpu count alone ({cpus})")
        return cpus >= PARALLEL_CPU_THRESHOLD
    if memory_gib < PARALLEL_MEMORY_THRESHOLD_GIB or cpus < PARALLEL_CPU_THRESHOLD:
        logging.info(
            f"Environment too small for parallel environments ({memory_gib:.1f} GiB, {cpus} cpus); running them one at a time"
        )
        return False
    return True


def range_partition(total_environments: int, n_partitions: int, partition_index: int) -> tuple[int, int]:
    """Return the (start, end) indices for a partition (0-based, end-exclusive).

    Inspired by numpy.array_split: distributes the remainder evenly across the first partitions.
    """
    partition_size = total_environments // n_partitions
    remainder = total_environments % n_partitions

    start = partition_size * partition_index
    start += partition_index if partition_index < remainder else remainder

    end = start + partition_size
    end += 1 if partition_index < remainder else 0
    end = min(end, total_environments)

    return start, end


def get_partition_environments(n_partitions: int, partition_index: int) -> list[str]:
    """Return the list of environments for a partition (0-based)."""
    available_environments = subprocess.getoutput("uvx -q tox -l -q | sort").splitlines()
    start, end = range_partition(len(available_environments), n_partitions, partition_index)
    partition_environments = available_environments[start:end]
    print(f"Running partition {partition_index} with {len(partition_environments)} environments")
    return partition_environments


def main() -> None:
    parser = argparse.ArgumentParser(description="Run tox environments for a specific partition")
    parser.add_argument("num_partitions", type=int, help="Total number of partitions")
    parser.add_argument("partition_index", type=int, help="Index of the partition to run (0-based)")
    args = parser.parse_args()

    environments_to_run = ", ".join(get_partition_environments(args.num_partitions, args.partition_index))

    logging.info(f"Running environments: {environments_to_run}")

    # Each environment spins up its own containers, so the environments of a partition
    # are safe to run concurrently — when the agent can afford them. TOX_SEQUENTIAL=1
    # opts out explicitly.
    if os.environ.get("TOX_SEQUENTIAL") == "1" or not can_run_parallel():
        tox_command = f'uvx tox run -e "{environments_to_run}"'
    else:
        # Read by the integration tests to pick collision-free host ports (see
        # tests/integration/services.py).
        tox_command = f'TOX_RUNNING_PARALLEL=1 uvx tox run-parallel --parallel all -e "{environments_to_run}"'

    if os.system(tox_command) != 0:
        raise Exception("Failed to run tox environments")


if __name__ == "__main__":
    main()
