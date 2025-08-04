# 2025-07-29
- Setup repository, dependencies (uv as usual), and unit tests.
- Inspected raw samples of Common Crawl (CC), both the WARC and WET versions.
- Implemented HTML2Text conversion and extraction using resiliparse. 
- Implemented language identification using fasttext pretrained model.
- Implemented masking of Personal Identifiable Information using regex patterns.
- Implemented harmful content filtering using fasttext.
- Implemented Gopher Quality Rules.

# 2025-07-30
- Added more Gopher rules following the paper
- Tested filters on random WARC files, and experimented with different thresholds.
- Added different observations to writeup.
- Implemented exact line deduplication using hashing. 
- Implemented fuzzy deduplication using MinHash and LSH. 

# 2025-07-31
- Started scrapping for high-quality articles to use for fastText quality classifier.
- Added script to construct data for fastText classifier.
- Finished writeup sections on deduplication.

# 2025-08-03:
- Implemented Stage 1 for the pre-processing pipeline:
  - Download Common-Crawl WARC/WET shards. 
  - Extracts clean text, and keeps only English pages that pass quality, NSFW and hate-speech thresholds. 
  - Implemented async download with aiohttp → temporary .gz file. 
  - Per-file worker in ProcessPoolExecutor, parses WARC file and applies different filters.
  - Writes each document in each shard as a different file to fit inside the deduplication script.

# 2025-08-04:
- Started implementing stage 2 of the pre-processing pipeline.
- Implemented parallel exact line deduplication with ProcessPoolExecutor and sqlite3 extension to allow for a scalable 
solution.
- Implemented parallel minhash fuzzy deduplication, using the same tricks as before and with robust integration of sqlite3.
