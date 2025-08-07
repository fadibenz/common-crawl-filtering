import asyncio
import concurrent.futures
import logging
import os
import random
from pathlib import Path
from argparse import Namespace
import aiofiles
import aiohttp
from typing import List
from tqdm import tqdm
from data_filtering.data_pipeline.stage_1.config import parse_args
from data_filtering.data_pipeline.stage_1.processing_one_file import filter_one_file, init_models
from data_filtering.utils import setup_logging
from data_filtering.data_pipeline.utils import list_file_paths

async def process_one_file_async(session: aiohttp.ClientSession,
                                 url: str,
                                 args: Namespace,
                                 loop,
                                 process_pool):
    output_dir = Path(args.STAGE1_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = None

    try:
        async with aiofiles.tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as tmp_file:
            tmp_path = tmp_file.name
            async with session.get(url) as resp:
                resp.raise_for_status()
                while True:
                    chunk = await resp.content.read(args.CHUNK_MB * 1024 * 1024)
                    if not chunk:
                        break
                    await tmp_file.write(chunk)

        # Offload the CPU-bound task to the process pool
        manifest = await loop.run_in_executor(
            process_pool,
            filter_one_file,
            tmp_path,
            args,
            output_dir,
        )

    except Exception as e:
        logging.error(f"Failed processing {url}: {e}")
        return None

    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)

    manifest_path = output_dir / (Path(url).stem + ".manifest")
    async with aiofiles.open(manifest_path, "w") as f:
        await f.write("\n".join(manifest))

    return manifest_path


async def main_orchestrator(urls: List[str],
                            args: Namespace):
    conn = aiohttp.TCPConnector(limit=args.concurrency, limit_per_host=args.concurrency)
    sem = asyncio.Semaphore(args.max_concurrent_downloads)
    process_pool = concurrent.futures.ProcessPoolExecutor(
        max_workers=args.num_workers,
        initializer=init_models,
        initargs=(args.lang_model, args.nsfw_model, args.hatespeech_model, args.quality_model)
    )

    loop = asyncio.get_running_loop()
    async def task_with_semaphore(session, url):
        async with sem:
            return await process_one_file_async(session, url, args, loop, process_pool)

    try:
        async with aiohttp.ClientSession(connector=conn) as session:
            tasks = [
                asyncio.create_task(task_with_semaphore(session, u))
                for u in urls
            ]
            manifests = []

            for future in tqdm(
                    asyncio.as_completed(tasks),
                    total=len(tasks),
                    desc="Processing files",
            ):
                manifest_path = await future
                if manifest_path is not None:
                    manifests.append(manifest_path)
            logging.info(f"All files processed. Total manifests created: {len(manifests)}")

    finally:
        logging.info("Shutting down process pool.")
        process_pool.shutdown()

if __name__ == "__main__":
    setup_logging()
    args = parse_args()
    random.seed(args.seed)

    logging.info("Starting Stage 1 pre-processing...")
    logging.info(f"Args: {vars(args)}")

    urls = list_file_paths(args.num_urls, args.use_wet)
    asyncio.run(main_orchestrator(urls, args))