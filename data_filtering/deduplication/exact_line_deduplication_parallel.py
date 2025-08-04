from typing import List
from pathlib import Path
import hashlib
import os
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed

# Map & Reduce pattern:

def local_hashes_counter(path: str | os.PathLike) -> Counter[str]:
    local_counter = Counter()
    with open(path, "rb") as f:
            for line in f:
                h = hashlib.sha256(line.rstrip(b'\r\n')).hexdigest()
                local_counter[h] += 1
    return local_counter

def write_uniques(path: str | os.PathLike,
                  output_dir:str | os.PathLike,
                  global_counter: Counter[str]):

    output_path = Path(output_dir) / Path(path).name
    with open(output_path, "wb") as f_output, open(path, "rb") as f_input:
            for line in f_input:
                h = hashlib.sha256(line.rstrip(b'\r\n')).hexdigest()
                if global_counter[h] == 1:
                    f_output.write(line)

def exact_line_dedup_parallel(list_paths: List[str] | list[os.PathLike],
                              output_directory: str | os.PathLike,
                              num_workers: int = None):
    num_workers = num_workers or os.cpu_count() or 1

    global_counter = Counter()

    with ProcessPoolExecutor(max_workers=num_workers)  as exe:
        futures = [exe.submit(local_hashes_counter, path)
                   for path in list_paths]
        for fut in as_completed(futures):
            global_counter.update(fut.result())

    Path(output_directory).mkdir(parents=True, exist_ok=True)

    with ProcessPoolExecutor(max_workers=num_workers) as exe:
        futures = [
            exe.submit(write_uniques, path, output_directory, global_counter)
            for path in list_paths
        ]
        for _ in as_completed(futures):
            pass