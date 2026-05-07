#!/usr/bin/env python3
"""
Prepare JPEG images for old USB-TV playback.
Re-saves images as baseline JPEG (sRGB) and renames them to img_001.jpg, img_002.jpg, ...

Usage:
    python prepare_images.py
    python prepare_images.py images
"""

import shutil
import sys
from pathlib import Path

from PIL import Image

DEFAULT_DIR = Path("images")
EXTS = {".jpg", ".jpeg", ".png", ".webp", ".bmp"}
MAX_WIDTH = 1920
MAX_HEIGHT = 1080


def main() -> int:
    target = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_DIR
    if not target.is_dir():
        print(f"Not a directory: {target}", file=sys.stderr)
        return 1

    files = sorted(p for p in target.iterdir() if p.is_file() and p.suffix.lower() in EXTS)
    if not files:
        print(f"No images found in {target}")
        return 0

    out_dir = target / "tv_ready"
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir()

    width = max(3, len(str(len(files))))
    for i, path in enumerate(files, start=1):
        with Image.open(path) as img:
            if img.mode != "RGB":
                img = img.convert("RGB")
            img.thumbnail((MAX_WIDTH, MAX_HEIGHT))
            new_name = f"img_{i:0{width}d}.jpg"
            img.save(
                out_dir / new_name,
                format="JPEG",
                quality=90,
                progressive=False,
                optimize=True,
            )
        print(f"  {path.name}  ->  {new_name}")

    print(f"\nWrote {len(files)} TV-ready images to {out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
