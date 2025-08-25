# End-to-End LLM Data Curation and Training

This repository contains the complete codebase for the project detailed in my blog post: [A Recipe for Filtering Language Modeling Data.](https://fadibenz.vercel.app/blog/data-pipeline)


It provides a robust, scalable pipeline for processing thousands of WET raw web data from Common Crawl into a high-quality dataset, 
and includes a from-scratch transformer implementation to train a language model on the curated data.

The core philosophy of this project is to demonstrate a rigorous, 
methodology-driven approach to data curation and model training, 
inspired by the engineering challenges covered in Stanford's CS336 course.

The pipeline covers:

* **Text extraction, language identification, and PII masking**
* **Harmful content detection and quality filtering**
* **Exact and fuzzy deduplication**
* **Efficient large-scale execution across CPU, RAM, and I/O bottlenecks**
* **Final tokenization into memory-mappable binary datasets**
* **Validation via GPT-2 small training run**

---

## 🔹 Pipeline Overview

The pipeline is implemented as **three stages**, each addressing a different system bottleneck:

### **Stage 1: Asynchronous Download & Filtering**

* **Goal**: Download WET files and apply CPU-bound filters (language ID, harmful content detection, quality heuristics).
* **Challenge**: I/O-bound (hundreds of GB downloads) + CPU-bound (FastText-based classifiers).
* **Solution**:
  * `aiohttp` async layer handles hundreds of concurrent downloads with disk spill + semaphores to cap peak storage.
  * `ProcessPoolExecutor` CPU worker pool loads ML models **once** per worker to avoid memory blowups.
  * Hand-off mechanism: async layer downloads → worker filters → results returned as compact manifests.
* **Outcome**: Processed **4000 WET files on a small local machine** with modest bandwidth.

---

### **Stage 2: Deduplication at Scale**

* **Goal**: Perform both exact-line and fuzzy document deduplication across millions of files.
* **Challenge**: Deduplication is global — naive in-memory approaches require hundreds of GB of RAM.
* **Solution**: Central **SQLite database** as shared state:

  * **Exact-line dedup**: Workers hash lines (SHA256), write to `hash_cnt` table with conflict-resolution counters (WAL-mode for safe concurrent writes).
  * **Fuzzy dedup**: Workers compute MinHash signatures and store them in SQLite; LSH banding queries find candidate pairs.
* **Engineering Highlight**: File-based database as synchronization primitive → **minimal RAM footprint, high robustness**.

---

### **Stage 3: Final Tokenization**

* **Goal**: Convert cleaned, deduplicated corpus into a compact binary format.
* **Challenge**: Tokenization is CPU-heavy but embarrassingly parallel.
* **Solution**:

  * Parent streams text line-by-line (tiny RAM footprint).
  * Workers pre-load GPT-2 tokenizer, tokenize batches, and return lists of token IDs.
  * Parent writes raw `uint16` IDs sequentially into a binary file.
* **Outcome**: Single mmap-able file, enabling ultra-fast data access during training.

---

## 🔹 Filtering Components

Independent filtering modules (used in Stage 1) include:

* **Text Extraction** → `run_extract_text_from_html_bytes`
* **Language Identification** → `run_identify_language`
* **PII Masking** → `run_mask_emails`, `run_mask_phone_numbers`, `run_mask_ips`
* **Harmful Content Filters** → `run_classify_nsfw`, `run_classify_toxic_speech`
* **Quality Filters** → `run_gopher_quality_filter`, `run_classify_quality`
* **Deduplication** → `run_exact_line_deduplication`, `run_minhash_deduplication`

Each module is testable in isolation:

```bash
uv run pytest -k test_extract_text_from_html_bytes
```

---

## 🔹 Training Validation

To validate the effectiveness of the curated dataset:

* **Model**: GPT-2 small (124M parameters)
* **Training setup**: DDP, AdamW, cosine annealing schedule
* **Run**: 200,000 steps on 2× T4 GPUs (Kaggle)
* **Result**: Final perplexity **3.2** on a 20k sample of C4 validation
* **Context**: Comparable models trained on unfiltered web data plateau at perplexity **>10**
  → Demonstrating **dramatic gains** from systematic filtering.

---


## 🔹 Project Structure
The repository is organized into two main Python modules:

- [`./data_filtering`](./data_filtering): Contains the complete, multi-stage data processing pipeline. This includes scripts for downloading, quality filtering, PII masking, and large-scale exact and fuzzy deduplication.
  - [`./data_filtering/data_pipeline`](./data_filtering/data_pipeline): Contains scripts to run the three stages of the pipeline 
    - stage_1: Asynchronous download and initial CPU-bound filtering. 
    - stage_2: Scalable deduplication using SQLite as a synchronization primitive. 
    - stage_3: Final tokenization into a memory-mappable binary format.
  - [`./data_filtering/deduplication`](./data_filtering/deduplication): Contains all the utilities for deduplication job.
  - [`./data_filtering/filtering_tokenization_scripts`](./data_filtering/filtering_tokenization_scripts): Contains scripts to test filtering, prepare data for validation and for training classifier.
  - [`./data_filtering/filtering_utilities`](./data_filtering/filtering_utilities): Contains different filtering primitives: text extraction, language identifiation, quality filtering, etc.
  - [`./data_filtering/notebooks`](./data_filtering/notebooks): Contains experimental notebooks for the different utilities.

-[`./transformer_training`](./transformer_training): A self-contained implementation of a GPT-style language model, based on CS336: Assignment 4, 2025. 
  It includes scripts for distributed training on the processed data.

## 🔹 Citation
If you find this work useful in your own research or projects, please consider citing the accompanying blog post:

```bibtex

@misc{benz2025datapipeline,
  author       = {Fadi Benzaima},
  title        = {Building a Scalable Data Filtering Pipeline for Language Modeling},
  year         = {2025},
  howpublished = {\url{https://fadibenz.vercel.app/blog/data-pipeline}}
}
```

## 🔹 Acknowledgements

This project was heavily inspired by Stanford's CS336: Language Modellig form scratch. 
Many of the tests for the transformer implementation are adapted from the course's public assignment repositories.
