#!/usr/bin/env python3
"""
TikTok Downloader
Download TikTok videos (or audio only) by URL using yt-dlp.

Usage:
    python tiktok_downloader.py <url> [<url> ...] [options]

Examples:
    # Download a single TikTok video:
    python tiktok_downloader.py "https://www.tiktok.com/@user/video/1234567890"

    # Download multiple videos:
    python tiktok_downloader.py "<url1>" "<url2>" "<url3>"

    # Audio only (MP3):
    python tiktok_downloader.py "<url>" --audio

    # Download an entire user's profile:
    python tiktok_downloader.py "https://www.tiktok.com/@username" --profile

    # Custom output directory:
    python tiktok_downloader.py "<url>" -o ~/Videos/tiktok
"""

import argparse
import sys
from pathlib import Path

try:
    from yt_dlp import YoutubeDL
    from yt_dlp.utils import DownloadError
except ImportError:
    print("yt-dlp is not installed. Install with: pip install yt-dlp", file=sys.stderr)
    sys.exit(1)


DEFAULT_OUTPUT = Path("downloads") / "tiktok"


def build_options(
    output_dir: Path,
    audio_only: bool,
    audio_format: str,
    audio_quality: str,
    profile: bool,
    limit: int | None,
) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)

    outtmpl = str(output_dir / "%(uploader)s - %(id)s - %(title).80s.%(ext)s")

    opts: dict = {
        "outtmpl": outtmpl,
        "ignoreerrors": profile,
        "restrictfilenames": False,
        "windowsfilenames": True,
        "concurrent_fragment_downloads": 4,
        "retries": 10,
        "fragment_retries": 10,
        "continuedl": True,
        "writeinfojson": False,
        "noplaylist": not profile,
    }

    if limit is not None and profile:
        opts["playlistend"] = limit

    if audio_only:
        opts["format"] = "bestaudio/best"
        opts["postprocessors"] = [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": audio_format,
                "preferredquality": audio_quality,
            },
            {"key": "FFmpegMetadata"},
        ]
    else:
        opts["format"] = "best"
        opts["merge_output_format"] = "mp4"

    return opts


def download(urls: list[str], opts: dict) -> int:
    failures = 0
    with YoutubeDL(opts) as ydl:
        for url in urls:
            try:
                ydl.download([url])
            except DownloadError as e:
                print(f"Failed to download {url}: {e}", file=sys.stderr)
                failures += 1
    return failures


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download TikTok videos or audio by URL.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("urls", nargs="+", help="One or more TikTok video or profile URLs.")
    parser.add_argument(
        "-a", "--audio",
        action="store_true",
        help="Download audio only instead of video.",
    )
    parser.add_argument(
        "-o", "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output directory (default: {DEFAULT_OUTPUT}).",
    )
    parser.add_argument(
        "--audio-format",
        default="mp3",
        choices=["mp3", "m4a", "opus", "flac", "wav", "aac"],
        help="Audio codec when --audio is set (default: mp3).",
    )
    parser.add_argument(
        "--audio-quality",
        default="0",
        help="Audio quality (0=best, 9=worst for VBR; or kbps like '192'). Default: 0.",
    )
    parser.add_argument(
        "--profile",
        action="store_true",
        help="Treat URLs as user profiles and download all their videos.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="When using --profile, cap the number of videos downloaded (newest first).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    opts = build_options(
        output_dir=args.output,
        audio_only=args.audio,
        audio_format=args.audio_format,
        audio_quality=args.audio_quality,
        profile=args.profile,
        limit=args.limit,
    )
    mode = "audio" if args.audio else "video"
    scope = "profile" if args.profile else "single"
    print(f"Mode: {mode} | Scope: {scope} | Output: {args.output.resolve()}")
    failures = download(args.urls, opts)
    if failures:
        print(f"Done with {failures} failure(s).", file=sys.stderr)
        return 1
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
