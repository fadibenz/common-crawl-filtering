import itertools, json, logging, gzip, os

from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Set, List, Tuple
from pathlib import Path
from data_filtering.deduplication.utils import setup_db_connection, build_clusters, get_ngrams, compute_minhash_signature, compute_jaccard
from tempfile import TemporaryDirectory

def lsh_candidates_sqlite(db_path,
                   num_bands:int
                   )-> Set[Tuple[str, str]]:

    conn = setup_db_connection(db_path)
    candidate_pairs_set = set()

    try:
        batch_size = 10000
        cur = conn.cursor()
        cur.execute("SELECT path, signature_list FROM signature")
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            bands_to_insert = []
            for doc, signature in rows:
                signature = json.loads(signature)
                band_size = len(signature) // num_bands
                for i in range(0, len(signature), band_size):
                    band = tuple(signature[i: i + band_size])
                    bands_to_insert.append((json.dumps(band), doc))

            conn.executemany("INSERT INTO bands(band, doc) VALUES(?, ?)", bands_to_insert)
            conn.commit()

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
                              num_grams: int):

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        text = f.read()
    ngram_set = get_ngrams(text, num_grams)
    signature = compute_minhash_signature(ngram_set, num_hashes)
    return str(path), json.dumps(signature)


def confirm_pair(pair: Tuple[str, str], num_grams: int, threshold: float) -> Tuple[bool, Tuple[str, str]]:
    try:
        score = compute_jaccard(pair, num_grams)
        return score >= threshold, pair
    except Exception as e:
        logging.warning(f"Failed pair {pair}: {e}")
        return False, pair


def insert_signatures(db_path, signatures_to_insert):
    conn = setup_db_connection(db_path)
    try:
        conn.executemany(
            """
            INSERT INTO signature(path, signature_list) VALUES(?, ?)
            """,
            signatures_to_insert
        )
    finally:
        conn.close()

def minhash_deduplication_parallel(list_paths: List[str] | list[os.PathLike],
                          num_hashes: int,
                          num_bands: int,
                          num_grams: int,
                          jaccard_threshold: float,
                          output_directory: str | os.PathLike,
                          num_workers: int = None):

    num_workers = num_workers or os.cpu_count() or 1
    batch_size = 1000

    with TemporaryDirectory(prefix="dedup_") as tmp_root:

        db_path = Path(tmp_root) / "signatures.db"
        conn = setup_db_connection(db_path)

        try:
            conn.execute("CREATE TABLE IF NOT EXISTS signature(path TEXT PRIMARY KEY, signature_list TEXT )")
            conn.execute("CREATE TABLE IF NOT EXISTS bands(band TEXT , doc TEXT )")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_band ON bands(band)")
            conn.execute("PRAGMA wal_checkpoint(FULL)")

        finally:
            conn.close()

        with ProcessPoolExecutor(max_workers=num_workers) as exe:

            futures = [
                exe.submit(generate_signature_sqlite, path, num_hashes, num_grams)
                for path in list_paths
            ]
            signatures_to_insert = []

            for fut in as_completed(futures):
                try:
                    signatures_to_insert.append((fut.result()))

                    if len(signatures_to_insert) >= batch_size:
                        insert_signatures(db_path, signatures_to_insert)
                        signatures_to_insert = []

                except Exception as e:
                    logging.error(f"Worker for signature calculation failed: {e!r}")

        if len(signatures_to_insert) > 0:
            insert_signatures(db_path, signatures_to_insert)
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
                logging.error(f"Worker for confirming pairs failed: {e!r}")


    duplicate_random = build_clusters(confirmed_pairs) # Use DFS to build clusters

    all_paths = set(str(p) for p in list_paths)
    clustered_paths = set().union(*confirmed_pairs)
    non_duplicates = all_paths - clustered_paths

    paths_to_write = list(non_duplicates) + duplicate_random

    output_path = Path(output_directory) / "pre_processed_training.txt.gz"
    Path(output_directory).mkdir(parents=True, exist_ok=True)

    logging.info(f"Successfully finished fuzzy deduplication, retained {len(paths_to_write)} files")
    logging.info(f"Writing retained files into {output_path}")

    with gzip.open(output_path, "wt", encoding="utf-8") as f_out:
        try:
            for path in paths_to_write:
                with open(path, "r", encoding="utf-8", errors="ignore") as f_in:
                    f_out.write(f_in.read().strip() + "\n")
        except Exception as e:
            logging.warning(f"Failed to copy file {path}: {e}")
