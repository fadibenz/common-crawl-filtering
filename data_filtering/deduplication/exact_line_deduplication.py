from typing import List
from pathlib import Path
import hashlib
import os

def exact_line_deduplication(list_paths: List[str] | list[os.PathLike],
                             output_directory: str | os.PathLike ):

    hashed_lines: dict[str, int] = {}

    for path in list_paths:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                h = hashlib.sha256(line.encode("utf-8")).hexdigest()
                hashed_lines[h] = hashed_lines.get(h, 0) + 1

    output_dir = Path(output_directory)
    output_dir.mkdir(parents=True, exist_ok=True)

    for path in list_paths:
        path_name = Path(path).name
        output_path = output_dir / path_name

        with open(output_path, "w", encoding="utf-8") as f_output:
            with open(path, "r", encoding="utf-8") as f_input:
                for line in f_input:
                    line = line.strip()
                    h = hashlib.sha256(line.encode("utf-8")).hexdigest()
                    if hashed_lines[h] == 1:
                        f_output.write(line + "\n")