import argparse
import os
import subprocess

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
    available_environments =  subprocess.getoutput("uvx tox -l -q | sort").splitlines()
    start, end = range_partition(len(available_environments), n_partitions, partition_index)
    partition_environments = available_environments[start:end]
    print(f"Running partition {partition_index} with {len(partition_environments)} environments")
    return partition_environments

parser = argparse.ArgumentParser(description="Run tox environments for a specific partition")
parser.add_argument("num_partitions", type=int, help="Total number of partitions")
parser.add_argument("partition_index", type=int, help="Index of the partition to run (0-based)")
args = parser.parse_args()

environments_to_run = ", ".join(get_partition_environments(args.num_partitions, args.partition_index))

subprocess.run(["uvx", "tox", "run", "-e", environments_to_run], check=True)