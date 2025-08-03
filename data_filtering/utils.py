import logging
import requests
import gzip
from fastwarc.warc import ArchiveIterator, WarcRecordType

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler()]
    )


def stream_one_file(file_url: str
                    ) -> WarcRecordType.response:
    with requests.get(file_url, stream = True) as r:
        with gzip.GzipFile(fileobj=r.raw) as gz:
            for record in ArchiveIterator(gz, record_types=WarcRecordType.response):
                yield record