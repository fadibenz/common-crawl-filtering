# Filtering Language Modeling Data

## Filtering Common Crawl (CC):

### Design Philosophy

The goal of this pipeline is to isolate high-quality, English, non-redundant, and safe documents from raw Common Crawl, preserving only what’s worth training on. Each filter is modular, well-logged, and reversible, enabling future iterations and ablations.


### Inspecting CC:

I downloaded sample files as WARC and WET to get an idea of what a random 
sample would contain.

1. The WARC file contains URLs, metadata, HTTP request details and the 
raw HTML content.
2. The WET file contains only the extracted text parts. However, it's every single text in the page
not just the main content, looking at the WET file all the headers, footers, and buttons should have been 
filtered by the extractor.
3. Looking at more than 25 WET records, it is clear that "high quality" webpages 
are very rare, none of the ones I saw is something I thought LMs were or should be trained on.
it felt like complete gibberish.

Looking at the WET files, I think I can do a better job in HTML to text conversion.

### HTML to text conversion:

To experiment with HTML-to-text conversion, I started from the raw `.warc.gz` files instead of the WET format. 

This allows full control over the parsing and filtering steps. 
I used `fastwarc` to iterate over the `WarcRecordType.response` records, 
and `resiliparse` to detect encoding and extract readable text from HTML.
Full script in `data_filtering/filtering_utilities/extract_text.py`.

At this stage, I only extract text content and write it out to a `.txt` file, 
adding a special delimiter `<|endofdoc|>` after each webpage to explicitly mark document boundaries. 
This is important for downstream training (e.g., language modeling or chunked retrieval) 
and is common practice in datasets like The Pile and C4. 

> Even without filtering, it's clear that a large portion of these documents are boilerplate-heavy, repetitive, or empty. 
Many contain large blocks of whitespace or newline padding. 

The structure I’m writing to looks like this:

```
Some site content...

<|endofdoc|>

Another site...

<|endofdoc|>
```

This preserves boundaries and leaves all options open for filtering and preprocessing in later stages. 
For now, I’m keeping everything including whitespace and adding only minimal structure. 

The full quality pipeline
(deduplication, language ID, harmful content detection, compression stats, length filtering, etc.) 
will come after this raw extraction phase.

> More Inspection:
> + The current extraction still extracts all content, including the headers and footers and so forth; I think I will experiment with more flags inside the `extract_text` function.
> + The extraction is not perfect; I noticed a lot of fragmented HTML tags, mainly those with non-standard names. ![img.png](writeup_assets/html_conversion_tags_failure.png)
> + There are a lot of languages in the full file, will be interesting to do some language identification.

### Language Identification

After the raw extraction, I switched to JSONL output to preserve document structure along with metadata, instead of 
separating documents with special tokens.  
Each record now contains:

```json
{
  "text": "...",
  "lang": "en",
  "confidence": 0.9984,
  "url": "http://some.site/page"
}
```
For language ID, I used fastText’s lid.176.bin model via fasttext.
The classifier outputs the most likely language along with a confidence score.
I exposed --lang, --confidence, and --filter_lang as CLI flags so I could choose to either keep all documents or only a subset.

As mentioned above, a lot of pages have trailing or leading newlines, large indentations, or blank padding across dozens of lines.

I added a `normalize_whitespace()` utility that collapses all excessive spacing and strips trailing/leading junk.

> Results:
> + Inspecting some random samples shows that the model did a very descent job in language identification; I only noticed one mistake in which a text in Spanish was considered 
> French.
> + One important observation was that it sometimes allocated very low confidence to text that was obviously english, specially in cases where it was math stuff.
> + Also, some text with obvious low quality content was given low confidence, suggesting that setting a good threshold would be the first quality filter we might use.
> + In high stakes scenarios, I believe doing ensembelling or deriving the threshold for a ROC curve would be necessary. 
> + I will stick with 0.8 threshold for now. Filtering for english in one WARC sample, only 15.8% (4401 out of 2784) were kept
> as english documents. Looking at random samples showed no signs of false positives.

#### Important note:

I went on to change the extraction function to include the `main_content=True` flag, this lead to cleaner extractions, and it also increased 
the number of english documents kept to 25.25% (6747 out of 27,824), I will stick with it preliminary.

### Personal identifiable information (PII) Masking

After language filtering and normalization, I added a data-cleaning step to detect and mask personally identifiable information (PII),
such as emails, phone numbers, and IP addresses. 

I implemented regex-based masking for three key types of PII:

- **Emails**: `[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}`
- **Phone numbers**: Patterns such as `(283) 182 3829` or `+1-800-555-1234`, this was tricky as there are a lot of variations, I focused on american numbers for now.
- **IPv4 addresses**

Each PII match is replaced with a descriptive placeholder, e.g., `|||EMAIL_ADDRESS|||`, to maintain formatting without leaking sensitive data.

A major design decision was to **track the number of substitutions** per document. 
This count is stored alongside the text in the output `.jsonl`, like so:

```json
{
  "text": "Please contact us at |||EMAIL_ADDRESS||| or visit our office.",
  "lang": "en",
  "confidence": 0.9841,
  "url": "http://example.com",
  "pii_counts": {
    "email": 1,
    "phone_numbers": 0,
    "ip_address": 0,
    "pii_total": 1
  }
}
```

To ensure correctness and performance, I compile all regexes ahead of time and use `re.subn()` 
to get both the masked text and the number of matches. 


PII masking is executed after language identification and whitespace normalization, ensuring the cleaner text is passed downstream. 
For now, this regex-based approach offers a reliable baseline. In the future, I may extend the system to mask names and addresses using NER models like spaCy or Stanza, 
especially if I observe leakage in real examples.

> + Early results show that while most documents have zero or one PII element, a small but non-negligible number (mostly from contact forms or review sections) 
> contain multiple.
> + There were some false positives, especially with phone numbers were some long numbers were mistaken for phone numbers.

### Harmful content: 

Similar to before, I used two fasttext models to detect for NSFW content and 
hate speech content. 

This is important because looking at the raw text it included many 
documents with very explicit content. 

Looking at some classified samples, there were no false positives 
for NSFW or toxic content, at least for this WARC sample, but 
there was a non-negligible number of false negatives, which is very harmful.

Fortunately, it's very rare to find documents that were false negatives for both toxicity and NSFW.
Still, to allow for robust filtering, I set a very high threshold of 0.95 for both non-NSFW and non-toxic 
classes. 

Other quality filters will filter out this content even further. 

> Applying this filter lead to 23.98% of documents being kept down from 25.25%

### Quality Rules: 

I started by implementing simple quality filters taken from the Gopher 
paper, they include several criteria based on document length, word length, symbol-to-word ratios, 
and the presence of certain English stop words.

Specifically, I removed documents that:

+ Contain less than 50 or more than 100,000 words.  
+ Have a mean word length outside the range of 3 to 10 characters.  
+ Have more than 30% of lines ending with an ellipsis (“...”).
+ Contain less than 80% of words with at least one alphabetic character.

> Applying this filter lead to 9.86%% of documents being kept, down from 23.98%.

Great work so far — this is shaping up to be a rigorous and thoughtful writeup. Let’s enhance it with a clean, high-signal section for **Deduplication**, maintaining your technical tone and clarity. I’ll also tie it smoothly into your existing structure.

---

### Deduplication
To avoid duplicate content (very common in CC),
I aggressively filtered to avoid model overfitting or memorizing redundant patterns. 
I implemented two levels of deduplication:

#### 1. Exact Line Deduplication

First, I removed exact duplicate lines across all documents. This is a low-cost but effective baseline.

**Approach**:

* Each line is SHA256 hashed and counted.
* Lines appearing more than once across the entire dataset are discarded.
* Only documents containing **unique lines** are retained in the output directory.

This step removes low-effort spam, template-heavy pages, or copy-pasted boilerplate content that escapes HTML filtering.


#### 2. Fuzzy Document Deduplication (MinHash + LSH)

For near-duplicate detection, I implemented a full MinHash + Locality-Sensitive Hashing (LSH) pipeline.

**Pipeline Overview**:

1. **Normalize & Tokenize**:
   * Lowercase, remove accents and punctuation, collapse whitespace.
   * Convert to word-based n-grams.
2. **MinHash Signature**:
   * For each document, compute a signature using multiple MurmurHash functions (via `mmh3`) over the n-grams.
3. **Banding (LSH)**:
   * Split signature into bands and group documents by identical band chunks.
   * Pairs within a band are considered **candidates**.
4. **Jaccard Filtering**:
   * Compute true Jaccard similarity on n-gram sets.
   * Confirm pairs with similarity ≥ `threshold` (e.g., 0.85).
5. **Clustering & Filtering**:
   * Build connected clusters of near-duplicate documents (Using DFS).
   * From each cluster, retain a random representative and discard the rest.

**Design Choices**:
* Used 100 hash functions, 20 bands (5 hashes per band), and 5-grams.
* Jaccard similarity threshold of 0.85 offered good precision-recall balance.

**Output**:
Only a single document from each near-duplicate group is retained, ensuring reduced redundancy while preserving diversity.

> **Note on my understanding:**
>
>We want to know if documents are very similar,
> to do so, we construct n-grams from each document (we can control granularity), 
> An intuitive way to see if they are similar is to calculate the jaccard index between the n-gram sets. 
> The thing is we would need to compare each document to all other documents, $O(n^2)$ complexity, not very god. 
> A smart way to do this is to calculate a MinHash for each n-gram set, we set a seed for determinism,  
> hash each n-gram in the document and take the smallest hash.
> Now this says nothing, 
> two documents can be not similar at all except one word that coincides to be the smallest one, 
> to make this more robust, we use k seeds and collect the min hashes.
> The more similarities we have, more MinHashes match.
> We still haven’t solved the complexity problem, since we would still need to compare each document to all the others. 
> To mitigate this, we use banding, we only compare documents that fall in the same band, the more similar the documents, 
> the higher the probability of two adjacent MinHash's being similar (hence the local sensitivity).
> Now finally, we proceed to compare the documents that pass all these *filters* by calculating the true Jaccard index.