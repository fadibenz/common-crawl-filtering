from typing import List
import sqlite3
from tempfile import TemporaryDirectory
from pathlib import Path
import hashlib
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from data_filtering.deduplication.utils import setup_db_connection

def local_hashes_counter(path: str | os.PathLike,
                         db_path: str | os.PathLike):
    conn = setup_db_connection(db_path)
    cur = conn.cursor()

    try:
        with open(path, "rb") as f:
            rows = [(
                hashlib.sha256(line.rstrip(b"\r\n")).hexdigest(), 1)
                for line in f
            ]

        cur.executemany(
            """
                  INSERT INTO hash_cnt(hash, cnt) VALUES(?, ?)
                  ON CONFLICT(hash) DO UPDATE SET cnt=cnt+1
                  """,
            rows
        )
    finally:
        conn.close()


def write_uniques(path: str | os.PathLike,
                  output_dir: str | os.PathLike,
                  db_path: str | os.PathLike):
    conn = setup_db_connection(db_path, read_only=True)
    cur = conn.cursor()

    output_path = Path(output_dir) / Path(path).name

    try:
        with open(output_path, "wb") as f_output, open(path, "rb") as f_input:
            for line in f_input:
                h = hashlib.sha256(line.rstrip(b'\r\n')).hexdigest()

                result = cur.execute("SELECT cnt from hash_cnt WHERE hash=?", (h,)).fetchone()
                if result and result[0] == 1:
                    f_output.write(line)
    finally:
        conn.close()


def exact_line_dedup_parallel(list_paths: List[str] | list[os.PathLike],
                              output_directory: str | os.PathLike,
                              num_workers: int = None):
    num_workers = num_workers or os.cpu_count() or 1
    with TemporaryDirectory(prefix="dedup_") as tmp_root:
        db_path = Path(tmp_root) / "freqs.db"
        Path(tmp_root).mkdir(exist_ok=True, parents=True)

        conn = setup_db_connection(db_path)
        try:
            conn.execute("CREATE TABLE IF NOT EXISTS hash_cnt(hash TEXT PRIMARY KEY, cnt INTEGER)")
            conn.execute("DELETE FROM hash_cnt")
            conn.execute("PRAGMA wal_checkpoint(FULL)")
        finally:
            conn.close()

        with ProcessPoolExecutor(max_workers=num_workers) as exe:
            futures = [exe.submit(local_hashes_counter, path, db_path)
                       for path in list_paths]
            for fut in as_completed(futures):
                try:
                    fut.result()
                except Exception as e:
                    print(f"Worker for hash counting failed: {e!r}")

        Path(output_directory).mkdir(parents=True, exist_ok=True)

        with ProcessPoolExecutor(max_workers=num_workers) as exe:
            futures = [
                exe.submit(write_uniques, path, output_directory, db_path)
                for path in list_paths
            ]
            for fut in as_completed(futures):
                try:
                    fut.result()
                except Exception as e:
                    print(f"Worker for writing uniques failed: {e!r}")