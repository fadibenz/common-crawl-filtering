# Filtering Language Modeling Data

## Filtering Common Crawl (CC):

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

