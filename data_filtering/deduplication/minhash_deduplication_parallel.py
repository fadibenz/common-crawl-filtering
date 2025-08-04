import itertools
import json
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Set, List, Tuple
import os
from pathlib import Path
from data_filtering.deduplication.utils import setup_db_connection, build_clusters, get_ngrams, compute_minhash_signature, compute_jaccard
from tempfile import TemporaryDirectory

def lsh_candidates_sqlite(db_path,
                   num_bands:int
                   )-> Set[Tuple[str, str]]:

    conn = setup_db_connection(db_path)
    cur = conn.execute("SELECT path, signature_list FROM signature")
    candidate_pairs_set = set()

    try:
        for doc, signature in cur:
            signature = json.loads(signature)
            band_size = len(signature) // num_bands

            for i in range(0, len(signature), band_size):
                band = tuple(signature[i: i + band_size])
                conn.execute("INSERT INTO bands(band, doc) VALUES(?, ?)", (json.dumps(band), doc))
        conn.commit()
        conn.close()

        conn = setup_db_connection(db_path, read_only=True)

        cur = conn.execute("""
            SELECT band, GROUP_CONCAT(doc)
            FROM bands
            GROUP BY band
            HAVING COUNT(*) > 1
        """)
        for _, group_str in cur:
            docs = group_str.split(",")
            candidate_pairs_set.update(itertools.combinations(docs, 2))

    finally:
        conn.close()

    return candidate_pairs_set


def generate_signature_sqlite(path: str | os.PathLike,
                              num_hashes: int,
                              num_grams: int,
                              db_path: str | os.PathLike):

    conn = setup_db_connection(db_path)
    try:
        with open(path, "r", encoding="utf-8") as f:
            text = f.read()
        ngram_set = get_ngrams(text, num_grams)
        signature = compute_minhash_signature(ngram_set, num_hashes)

        conn.execute(
            """
            INSERT INTO signature(path, signature_list) VALUES(?, ?)
            """,
            (str(path), json.dumps(signature))
        )
    finally:
        conn.close()

def confirm_pair(pair: Tuple[str, str], num_grams: int, threshold: float) -> Tuple[bool, Tuple[str, str]]:
    try:
        score = compute_jaccard(pair, num_grams)
        return score >= threshold, pair
    except Exception as e:
        print(f"Failed pair {pair}: {e}")
        return False, pair

def copy_to_output(path: str, output_dir: str):
    try:
        output_path = Path(output_dir) / Path(path).name
        with open(path, "r", encoding="utf-8") as f_in, open(output_path, "w", encoding="utf-8") as f_out:
            f_out.write(f_in.read())
    except Exception as e:
        print(f"Failed to copy {path}: {e}")


def minhash_deduplication_parallel(list_paths: List[str] | list[os.PathLike],
                          num_hashes: int,
                          num_bands: int,
                          num_grams: int,
                          jaccard_threshold: float,
                          output_directory: str | os.PathLike,
                          num_workers: int = None):

    num_workers = num_workers or os.cpu_count() or 1


    with TemporaryDirectory(prefix="dedup_") as tmp_root:

        db_path = Path(tmp_root) / "signatures.db"
        conn = setup_db_connection(db_path)
        try:
            conn.execute("CREATE TABLE IF NOT EXISTS signature(path TEXT PRIMARY KEY, signature_list TEXT )")
            conn.execute("CREATE TABLE IF NOT EXISTS bands(band TEXT , doc TEXT )")
        finally:
            conn.close()

        with ProcessPoolExecutor(max_workers=num_workers) as exe:

            futures = [
                exe.submit(generate_signature_sqlite, path, num_hashes, num_grams, db_path)
                for path in list_paths
            ]

            for fut in as_completed(futures):
                try:
                    fut.result()
                except Exception as e:
                    print(f"Worker for signature calculation failed: {e!r}")

        candidate_pairs_set = lsh_candidates_sqlite(db_path, num_bands)


    confirmed_pairs = set()
    with ProcessPoolExecutor(max_workers=num_workers) as exe:
        futures = [
            exe.submit(confirm_pair, pair, num_grams, jaccard_threshold)
            for pair in candidate_pairs_set
        ]

        for fut in as_completed(futures):
            try:
                ok, pair = fut.result()
                if ok: confirmed_pairs.add(pair)
            except Exception as e:
                print(f"Worker for confirming pairs failed: {e!r}")


    duplicate_random = build_clusters(confirmed_pairs)

    all_paths = set(str(p) for p in list_paths)
    clustered_paths = set().union(*confirmed_pairs)
    non_duplicates = all_paths - clustered_paths

    paths_to_write = list(non_duplicates) + duplicate_random

    output_dir = Path(output_directory)
    output_dir.mkdir(parents=True, exist_ok=True)

    with ProcessPoolExecutor(max_workers=num_workers) as exe:
        futures = [
            exe.submit(copy_to_output, path, output_directory)
            for path in paths_to_write
        ]
        try:
            for f in as_completed(futures):
                f.result()
        except Exception as e:
            print(f"Worker for copying files to output failed: {e!r}")
