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