#!/usr/bin/env python3
"""
YouTube Downloader
Download videos or audio-only (music) from YouTube using yt-dlp.

Usage:
    python youtube_downloader.py <url> [options]

Examples:
    # Download a video at best quality (default):
    python youtube_downloader.py "https://www.youtube.com/watch?v=dQw4w9WgXcQ"

    # Download audio only (music) as MP3:
    python youtube_downloader.py "https://www.youtube.com/watch?v=dQw4w9WgXcQ" --audio

    # Download a whole playlist as MP3s:
    python youtube_downloader.py "https://www.youtube.com/playlist?list=PL..." --audio

    # Pick a max video resolution (e.g. 1080p):
    python youtube_downloader.py "<url>" --max-height 1080

    # Choose output directory:
    python youtube_downloader.py "<url>" -o ~/Music --audio
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


DEFAULT_OUTPUT = Path("downloads") / "youtube"


def build_options(
    output_dir: Path,
    audio_only: bool,
    audio_format: str,
    audio_quality: str,
    max_height: int | None,
    playlist: bool,
) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)

    if playlist:
        outtmpl = str(output_dir / "%(playlist_title|Playlist)s" / "%(playlist_index)03d - %(title)s.%(ext)s")
    else:
        outtmpl = str(output_dir / "%(title)s.%(ext)s")

    opts: dict = {
        "outtmpl": outtmpl,
        "noplaylist": not playlist,
        "ignoreerrors": playlist,
        "restrictfilenames": False,
        "windowsfilenames": True,
        "concurrent_fragment_downloads": 4,
        "retries": 10,
        "fragment_retries": 10,
        "continuedl": True,
    }

    if audio_only:
        opts["format"] = "bestaudio/best"
        opts["postprocessors"] = [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": audio_format,
                "preferredquality": audio_quality,
            },
            {"key": "FFmpegMetadata"},
            {"key": "EmbedThumbnail", "already_have_thumbnail": False},
        ]
        opts["writethumbnail"] = True
    else:
        if max_height:
            opts["format"] = (
                f"bestvideo[height<={max_height}][ext=mp4]+bestaudio[ext=m4a]/"
                f"bestvideo[height<={max_height}]+bestaudio/"
                f"best[height<={max_height}]/best"
            )
        else:
            opts["format"] = "bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio/best"
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
        description="Download videos or audio-only (music) from YouTube.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("urls", nargs="+", help="One or more YouTube URLs (video or playlist).")
    parser.add_argument(
        "-a", "--audio",
        action="store_true",
        help="Download audio only (music) instead of video.",
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
        choices=["mp3", "m4a", "opus", "flac", "wav", "aac", "vorbis"],
        help="Audio codec when --audio is set (default: mp3).",
    )
    parser.add_argument(
        "--audio-quality",
        default="0",
        help="Audio quality (0=best, 9=worst for VBR; or kbps like '192'). Default: 0.",
    )
    parser.add_argument(
        "--max-height",
        type=int,
        default=None,
        help="Cap video resolution height (e.g. 720, 1080). Default: best available.",
    )
    parser.add_argument(
        "--playlist",
        action="store_true",
        help="Treat the URL as a playlist and download all items.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    opts = build_options(
        output_dir=args.output,
        audio_only=args.audio,
        audio_format=args.audio_format,
        audio_quality=args.audio_quality,
        max_height=args.max_height,
        playlist=args.playlist,
    )
    mode = "audio" if args.audio else "video"
    print(f"Mode: {mode} | Output: {args.output.resolve()}")
    failures = download(args.urls, opts)
    if failures:
        print(f"Done with {failures} failure(s).", file=sys.stderr)
        return 1
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
