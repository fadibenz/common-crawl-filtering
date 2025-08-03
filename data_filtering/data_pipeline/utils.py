import random
from typing import List
import requests
import gzip
from io import BytesIO
BASE = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-18"

def list_file_paths(max_files: int = 5000,
                    is_wet: bool = False, ) -> List[str]:
    paths_url = "wet.paths.gz" if is_wet else "warc.paths.gz"
    idx_url = f"{BASE}/{paths_url}"
    response = requests.get(idx_url)

    with gzip.open(BytesIO(response.content), "rt", encoding="utf-8") as f:
        all_paths = f.read().splitlines()
    random.shuffle(all_paths)
    sampled_paths = all_paths[:max_files]

    return [f"https://data.commoncrawl.org/{p}"
            for p in sampled_paths]
