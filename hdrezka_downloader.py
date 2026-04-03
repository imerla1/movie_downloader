#!/usr/bin/env python3
"""
HDRezka TV Series/Movie Downloader
Downloads TV series and movies from HDRezka using the HdRezkaApi library.

Usage:
    python hdrezka_downloader.py <url> [options]

Examples:
    # Download entire series with default translator
    python hdrezka_downloader.py "https://hdrezka.sh/series/drama/40535-vlast-v-nochnom-gorode-kniga-tretya-vospitanie-kenana-2021.html"

    # Download specific season
    python hdrezka_downloader.py "https://hdrezka.sh/series/..." --season 1

    # Download specific episode
    python hdrezka_downloader.py "https://hdrezka.sh/series/..." --season 1 --episode 5

    # Download with specific quality
    python hdrezka_downloader.py "https://hdrezka.sh/series/..." --quality 1080p

    # List available translators and seasons/episodes
    python hdrezka_downloader.py "https://hdrezka.sh/series/..." --info

    # Download with specific translator
    python hdrezka_downloader.py "https://hdrezka.sh/series/..." --translator "TVShows"
    python hdrezka_downloader.py "https://hdrezka.sh/series/..." --translator 232
"""

import argparse
import os
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Optional, List, Tuple
from urllib.parse import urlparse

import requests
from tqdm import tqdm

try:
    from HdRezkaApi import HdRezkaApi
except ImportError:
    print("Error: HdRezkaApi not installed. Install it with:")
    print("  pip install HdRezkaApi")
    sys.exit(1)


def sanitize_filename(name: str) -> str:
    """Remove invalid characters from filename."""
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        name = name.replace(char, '')
    name = re.sub(r'\s+', ' ', name)
    name = name.strip(' .')
    return name


def format_size(size_bytes: int) -> str:
    """Format bytes to human readable size."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} PB"


@dataclass
class DownloadTask:
    """Represents a single download task."""
    season: int
    episode: int
    stream_url: str
    output_path: Path


class HdRezkaDownloader:
    """Downloader for HDRezka website."""

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
        "sec-ch-ua": '"Chromium";v="143", "Not A(Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
    }

    def __init__(self, output_dir: str = "downloads", quality: str = "1080p", parallel: int = 1):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.quality = quality
        self.parallel = parallel
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        self.print_lock = Lock()

    def get_content_info(self, url: str, translator_priority: list = None) -> Optional[HdRezkaApi]:
        """Initialize HdRezkaApi and get content information."""
        try:
            kwargs = {
                'headers': self.HEADERS,
            }
            if translator_priority:
                kwargs['translators_priority'] = translator_priority
            rezka = HdRezkaApi(url, **kwargs)
            if not rezka.ok:
                print(f"Error: {rezka.exception}")
                return None
            return rezka
        except Exception as e:
            print(f"Error initializing API: {e}")
            return None

    def get_translator_id(self, rezka: HdRezkaApi, translator: str) -> Optional[str]:
        """Get translator ID from name or return as-is if already an ID."""
        if translator is None:
            # Return first available translator
            if rezka.translators:
                return str(list(rezka.translators.keys())[0])
            return None

        # Check if it's already an ID
        if translator.isdigit() and int(translator) in rezka.translators:
            return translator

        # Search by name
        for tid, info in rezka.translators.items():
            if info.get('name', '').lower() == translator.lower():
                return str(tid)
            if translator.lower() in info.get('name', '').lower():
                return str(tid)

        return translator  # Return as-is, let the API handle it

    def show_info(self, rezka: HdRezkaApi):
        """Print comprehensive content information."""
        print("\n" + "=" * 60)
        print(f"Title: {rezka.name}")
        if hasattr(rezka, 'origName') and rezka.origName:
            print(f"Original: {rezka.origName}")
        print(f"Type: {rezka.type}")
        if hasattr(rezka, 'releaseYear') and rezka.releaseYear:
            print(f"Year: {rezka.releaseYear}")
        if hasattr(rezka, 'rating') and rezka.rating:
            print(f"Rating: {rezka.rating.value} ({rezka.rating.votes} votes)")
        print("=" * 60)

        # Show translators with their available seasons/episodes
        print("\nAvailable translators and content:")
        print("-" * 60)

        series_info = None
        try:
            series_info = rezka.seriesInfo
        except (ValueError, AttributeError):
            pass

        if series_info:
            for tid, info in series_info.items():
                name = info.get('translator_name', 'Unknown')
                premium = " [Premium]" if info.get('premium') else ""
                seasons = info.get('seasons', {})
                episodes = info.get('episodes', {})

                print(f"\n  [{tid}] {name}{premium}")

                if seasons:
                    for season_num in sorted(seasons.keys()):
                        ep_count = len(episodes.get(season_num, {}))
                        print(f"      Season {season_num}: {ep_count} episodes")
        else:
            # Movie or no series info
            print("\n  Translators:")
            for tid, info in rezka.translators.items():
                name = info.get('name', 'Unknown')
                premium = " [Premium]" if info.get('premium') else ""
                print(f"    [{tid}] {name}{premium}")

        print("-" * 60)

    def get_seasons_episodes(self, rezka: HdRezkaApi, translator_id: str) -> dict:
        """Get available seasons and episodes for a specific translator."""
        try:
            series_info = rezka.seriesInfo
        except (ValueError, AttributeError):
            return {}

        if not series_info:
            return {}

        tid = int(translator_id) if translator_id.isdigit() else translator_id

        if tid in series_info:
            return series_info[tid].get('episodes', {})

        # Try string key
        if str(tid) in series_info:
            return series_info[str(tid)].get('episodes', {})

        return {}

    def get_stream_url(self, rezka: HdRezkaApi, season: Optional[str] = None,
                       episode: Optional[str] = None, translator: Optional[str] = None) -> Optional[str]:
        """Get the stream URL for content."""
        try:
            type_str = str(rezka.type).lower()
            is_series = 'series' in type_str or 'tv' in type_str
            if is_series and season and episode:
                stream = rezka.getStream(season, episode, translation=translator)
            else:
                stream = rezka.getStream(translation=translator)

            # Get available qualities
            if hasattr(stream, 'videos') and stream.videos:
                available = list(stream.videos.keys())
                print(f"  Available qualities: {available}")

            # Try to get the requested quality
            url = stream(self.quality)

            # url might be a dict with quality keys
            if isinstance(url, dict):
                if self.quality in url:
                    url = url[self.quality]
                else:
                    # Get highest available quality
                    url = list(url.values())[0] if url else None

            # URL might be a list
            if isinstance(url, list):
                url = url[0] if url else None

            return url

        except Exception as e:
            print(f"  Error getting stream: {e}")
            return None

    def download_with_ffmpeg(self, url: str, output_path: Path, quiet: bool = False) -> bool:
        """Download HLS stream using ffmpeg."""
        try:
            result = subprocess.run(['ffmpeg', '-version'], capture_output=True)
            if result.returncode != 0:
                with self.print_lock:
                    print("ffmpeg not found. Please install ffmpeg.")
                return False
        except FileNotFoundError:
            with self.print_lock:
                print("ffmpeg not found. Please install ffmpeg.")
            return False

        if not quiet:
            with self.print_lock:
                print(f"  Downloading: {output_path.name}")

        cmd = [
            'ffmpeg',
            '-hide_banner',
            '-loglevel', 'error',
            '-i', url,
            '-c', 'copy',
            '-bsf:a', 'aac_adtstoasc',
            '-y',
            str(output_path)
        ]

        if not quiet:
            cmd.insert(4, '-stats')

        try:
            process = subprocess.run(cmd, capture_output=quiet)
            return process.returncode == 0
        except Exception as e:
            with self.print_lock:
                print(f"  Download error: {e}")
            return False

    def download_direct(self, url: str, output_path: Path, quiet: bool = False,
                        progress_callback=None) -> bool:
        """Download direct MP4 file."""
        try:
            # Create a new session for thread safety
            session = requests.Session()
            session.headers.update(self.HEADERS)

            response = session.get(url, stream=True, timeout=120)
            response.raise_for_status()

            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0

            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=65536):  # 64KB chunks
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if progress_callback:
                            progress_callback(downloaded, total_size)
                        elif not quiet:
                            # Simple progress for non-parallel
                            percent = (downloaded / total_size * 100) if total_size else 0
                            print(f"\r  {output_path.name}: {percent:.1f}% ({format_size(downloaded)}/{format_size(total_size)})", end='', flush=True)

            if not quiet and not progress_callback:
                print()  # New line after progress

            return True

        except Exception as e:
            with self.print_lock:
                print(f"\n  Download error for {output_path.name}: {e}")
            if output_path.exists():
                output_path.unlink()
            return False

    def download(self, url: str, output_path: Path, quiet: bool = False) -> bool:
        """Download video from URL (auto-detect method)."""
        if output_path.exists():
            if not quiet:
                with self.print_lock:
                    print(f"  Already exists: {output_path.name}")
            return True

        if '.m3u8' in url or 'hls' in url.lower():
            return self.download_with_ffmpeg(url, output_path, quiet)
        else:
            return self.download_direct(url, output_path, quiet)

    def _download_task(self, task: DownloadTask, task_num: int, total: int,
                       progress_dict: dict) -> Tuple[bool, int, int]:
        """Download a single task (for parallel execution)."""
        ep_key = f"S{task.season:02d}E{task.episode:02d}"

        def update_progress(downloaded, total_size):
            percent = (downloaded / total_size * 100) if total_size else 0
            progress_dict[ep_key] = f"{percent:.0f}%"

        progress_dict[ep_key] = "0%"

        if '.m3u8' in task.stream_url or 'hls' in task.stream_url.lower():
            success = self.download_with_ffmpeg(task.stream_url, task.output_path, quiet=True)
        else:
            success = self.download_direct(task.stream_url, task.output_path, quiet=True,
                                          progress_callback=update_progress)

        if success:
            progress_dict[ep_key] = "DONE"
        else:
            progress_dict[ep_key] = "FAIL"

        return success, task.season, task.episode

    def download_movie(self, rezka: HdRezkaApi, translator: Optional[str] = None):
        """Download a movie."""
        translator_id = self.get_translator_id(rezka, translator)
        translator_name = rezka.translators.get(int(translator_id), {}).get('name', translator_id) if translator_id else 'default'

        print(f"\nDownloading movie: {rezka.name}")
        print(f"Translator: {translator_name}")
        print(f"Quality: {self.quality}")

        stream_url = self.get_stream_url(rezka, translator=translator_id)
        if not stream_url:
            print("Could not get stream URL")
            return False

        filename = sanitize_filename(f"{rezka.name}_{translator_name}_{self.quality}.mp4")
        output_path = self.output_dir / filename

        success = self.download(stream_url, output_path)

        if success:
            print(f"\nDownload complete: {output_path}")
        return success

    def download_series(self, rezka: HdRezkaApi,
                       season_filter: Optional[int] = None,
                       episode_filter: Optional[int] = None,
                       translator: Optional[str] = None):
        """Download TV series episodes."""
        translator_id = self.get_translator_id(rezka, translator)
        tid_int = int(translator_id) if translator_id and translator_id.isdigit() else None

        if tid_int and tid_int in rezka.translators:
            translator_name = rezka.translators[tid_int].get('name', translator_id)
        else:
            translator_name = translator_id or 'default'

        print(f"\nDownloading series: {rezka.name}")
        print(f"Translator: {translator_name} (ID: {translator_id})")
        print(f"Quality: {self.quality}")

        # Create series directory
        series_name = sanitize_filename(rezka.name)
        series_dir = self.output_dir / series_name
        series_dir.mkdir(parents=True, exist_ok=True)

        # Get available episodes for this translator
        episodes_info = self.get_seasons_episodes(rezka, translator_id)

        if not episodes_info:
            print("No episode information found. Attempting probe download...")
            episodes_info = self._probe_episodes(rezka, translator_id)

        if not episodes_info:
            print("No episodes found!")
            return

        # Determine what to download
        if season_filter and episode_filter:
            # Single episode
            to_download = {season_filter: [episode_filter]}
        elif season_filter:
            # Specific season
            if season_filter in episodes_info:
                to_download = {season_filter: list(episodes_info[season_filter].keys())}
            else:
                print(f"Season {season_filter} not available for translator {translator_name}")
                print(f"Available seasons: {list(episodes_info.keys())}")
                return
        else:
            # All seasons
            to_download = {s: list(eps.keys()) for s, eps in episodes_info.items()}

        # Count total
        total = sum(len(eps) for eps in to_download.values())
        print(f"\nEpisodes to download: {total}")
        print(f"Parallel downloads: {self.parallel}")

        # Prepare download tasks
        print("\nPreparing download URLs...")
        tasks: List[DownloadTask] = []

        for season_num in sorted(to_download.keys()):
            season_dir = series_dir / f"Season_{season_num:02d}"
            season_dir.mkdir(parents=True, exist_ok=True)

            for episode_num in sorted(to_download[season_num]):
                filename = f"S{season_num:02d}E{episode_num:02d}_{self.quality}.mp4"
                output_path = season_dir / filename

                # Skip if already exists
                if output_path.exists():
                    print(f"  Skipping S{season_num:02d}E{episode_num:02d} (already exists)")
                    continue

                stream_url = self.get_stream_url(
                    rezka, str(season_num), str(episode_num), translator_id
                )

                if not stream_url:
                    print(f"  Could not get stream URL for S{season_num:02d}E{episode_num:02d}")
                    continue

                tasks.append(DownloadTask(
                    season=season_num,
                    episode=episode_num,
                    stream_url=stream_url,
                    output_path=output_path
                ))

        if not tasks:
            print("\nNo episodes to download (all already exist or failed to get URLs)")
            return

        print(f"\nStarting download of {len(tasks)} episodes...")

        # Download
        downloaded = 0
        failed = 0

        if self.parallel > 1:
            # Parallel download with progress display
            progress_dict = {}

            def display_progress():
                """Display progress for all active downloads."""
                while True:
                    if not progress_dict:
                        time.sleep(0.5)
                        continue

                    # Build status line
                    active = [f"{k}:{v}" for k, v in sorted(progress_dict.items())
                             if v not in ("DONE", "FAIL")]
                    done = sum(1 for v in progress_dict.values() if v == "DONE")
                    fail = sum(1 for v in progress_dict.values() if v == "FAIL")

                    status = f"\rProgress: {done} done, {fail} failed | Active: {' | '.join(active[:5])}"
                    if len(active) > 5:
                        status += f" (+{len(active)-5} more)"
                    print(status + " " * 20, end='', flush=True)

                    # Check if all done
                    if done + fail >= len(tasks):
                        print()  # New line
                        break

                    time.sleep(1)

            # Start progress display thread
            import threading
            progress_thread = threading.Thread(target=display_progress, daemon=True)
            progress_thread.start()

            with ThreadPoolExecutor(max_workers=self.parallel) as executor:
                futures = {
                    executor.submit(self._download_task, task, i + 1, len(tasks), progress_dict): task
                    for i, task in enumerate(tasks)
                }

                for future in as_completed(futures):
                    success, season, episode = future.result()
                    if success:
                        downloaded += 1
                    else:
                        failed += 1

            progress_thread.join(timeout=2)
        else:
            # Sequential download
            for i, task in enumerate(tasks):
                print(f"\n[{i + 1}/{len(tasks)}] S{task.season:02d}E{task.episode:02d}")

                if self.download(task.stream_url, task.output_path):
                    downloaded += 1
                else:
                    failed += 1

                time.sleep(1)  # Rate limiting

        print(f"\n{'=' * 50}")
        print(f"Download complete!")
        print(f"  Downloaded: {downloaded}")
        print(f"  Failed: {failed}")
        print(f"  Skipped: {total - len(tasks)}")
        print(f"  Output: {series_dir}")

    def _probe_episodes(self, rezka: HdRezkaApi, translator_id: str,
                       max_seasons: int = 10, max_episodes: int = 30) -> dict:
        """Probe for available episodes by trying to fetch them."""
        print("Probing for episodes...")
        available = {}

        for season in range(1, max_seasons + 1):
            season_eps = {}
            consecutive_failures = 0

            for episode in range(1, max_episodes + 1):
                try:
                    stream_url = self.get_stream_url(
                        rezka, str(season), str(episode), translator_id
                    )
                    if stream_url:
                        season_eps[episode] = f"Episode {episode}"
                        consecutive_failures = 0
                        print(f"  Found: S{season}E{episode}")
                    else:
                        consecutive_failures += 1
                except Exception:
                    consecutive_failures += 1

                if consecutive_failures >= 3:
                    break

            if season_eps:
                available[season] = season_eps
            elif season > 1:
                break

        return available


def main():
    parser = argparse.ArgumentParser(
        description="Download movies and TV series from HDRezka",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s "https://hdrezka.sh/series/..." --info
  %(prog)s "https://hdrezka.sh/series/..."
  %(prog)s "https://hdrezka.sh/series/..." --season 1
  %(prog)s "https://hdrezka.sh/series/..." -s 1 -e 5
  %(prog)s "https://hdrezka.sh/series/..." -t TVShows -q 720p
  %(prog)s "https://hdrezka.sh/series/..." -t 232 -p 4  # 4 parallel downloads
  %(prog)s "https://hdrezka.sh/films/..." --translator 232
        """
    )

    parser.add_argument("url", help="URL of the movie/series page on HDRezka")
    parser.add_argument("-s", "--season", type=int, help="Download specific season")
    parser.add_argument("-e", "--episode", type=int, help="Download specific episode (requires --season)")
    parser.add_argument("-q", "--quality", default="1080p",
                       help="Video quality (360p, 480p, 720p, 1080p, 1080p Ultra). Default: 1080p")
    parser.add_argument("-o", "--output", default="downloads",
                       help="Output directory (default: downloads)")
    parser.add_argument("-t", "--translator", help="Translator/voice-over name or ID")
    parser.add_argument("-p", "--parallel", type=int, default=1,
                       help="Number of parallel downloads (default: 1, recommended: 3-5)")
    parser.add_argument("--info", action="store_true",
                       help="Show content information and exit")

    args = parser.parse_args()

    if args.episode and not args.season:
        parser.error("--episode requires --season")

    # Initialize
    downloader = HdRezkaDownloader(output_dir=args.output, quality=args.quality, parallel=args.parallel)

    print(f"Fetching: {args.url}")

    # Get translator priority if specified
    translator_priority = None
    if args.translator and args.translator.isdigit():
        translator_priority = [args.translator]

    rezka = downloader.get_content_info(args.url, translator_priority)
    if not rezka:
        sys.exit(1)

    # Info mode
    if args.info:
        downloader.show_info(rezka)
        sys.exit(0)

    # Show basic info
    print(f"\nTitle: {rezka.name}")
    print(f"Type: {rezka.type}")

    # Download
    try:
        # Check if it's a TV series (handle different type formats)
        type_str = str(rezka.type).lower()
        is_series = 'series' in type_str or 'tv' in type_str

        if is_series:
            downloader.download_series(
                rezka,
                season_filter=args.season,
                episode_filter=args.episode,
                translator=args.translator
            )
        else:
            downloader.download_movie(rezka, translator=args.translator)

    except KeyboardInterrupt:
        print("\n\nDownload interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
