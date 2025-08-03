import logging
from typing import List
import requests
import gzip
import random
from io import BytesIO
from fastwarc.warc import ArchiveIterator, WarcRecordType



BASE = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-18"

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler()]
    )

def list_wet_paths(max_files: int=5000,
                   is_wet: bool =False,) -> List[str]:
    paths_url = "wet.paths.gz" if is_wet else "warc.paths.gz"
    idx_url = f"{BASE}/{paths_url}"
    response = requests.get(idx_url)
    with gzip.open(BytesIO(response.content), "rt", encoding="utf-8") as f:
        all_paths = f.read().splitlines()
    random.shuffle(all_paths)
    sampled_paths = all_paths[:max_files]
    return [f"https://data.commoncrawl.org/{p}"
                   for p in sampled_paths]


def stream_one_file(file_url: str
                    ) -> WarcRecordType.response:
    with requests.get(file_url, stream = True) as r:
        with gzip.GzipFile(fileobj=r.raw) as gz:
            for record in ArchiveIterator(gz, record_types=WarcRecordType.response):
                yield record