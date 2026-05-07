#!/usr/bin/env python3
"""
Shuffle downloaded TikTok files by renaming them with random numeric prefixes.
Old USB-TV players sort files alphabetically, so a numeric prefix controls play order.

Usage:
    python shuffle_tiktoks.py
    python shuffle_tiktoks.py downloads/tiktok
"""

import random
import re
import sys
from pathlib import Path

DEFAULT_DIR = Path("downloads") / "tiktok"
PREFIX_RE = re.compile(r"^\d{3,}_")


def main() -> int:
    target = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_DIR
    if not target.is_dir():
        print(f"Not a directory: {target}", file=sys.stderr)
        return 1

    files = [p for p in target.iterdir() if p.is_file() and p.suffix.lower() == ".mp4"]
    if not files:
        print(f"No .mp4 files in {target}")
        return 0

    random.shuffle(files)
    width = max(3, len(str(len(files))))

    for i, path in enumerate(files, start=1):
        clean = PREFIX_RE.sub("", path.name)
        new_name = f"{i:0{width}d}_{clean}"
        path.rename(path.with_name(new_name))

    print(f"Shuffled {len(files)} files in {target}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
