#!/usr/bin/env python3
"""
GE.Movie TV Series Downloader v2
Enhanced version with direct CDN URL construction based on observed patterns.

Video URL Pattern discovered from HAR:
https://01-cdn.videodb.cloud/serials/{MM_YY}/{Title}_{Year}_SE{Season}/{Title}_{Year}_SE{Season}_EP{Episode}_{LANG}.mp4?hash={hash}&expires={timestamp}

Usage:
    python ge_movie_downloader_v2.py <series_url> [options]
"""

import argparse
import hashlib
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


@dataclass
class Episode:
    """Represents a single episode."""
    season: int
    episode: int
    title: str
    video_url: Optional[str] = None


@dataclass
class Series:
    """Represents a TV series."""
    id: str
    slug: str
    title_en: str
    title_ge: str
    year: str
    tmdb_id: Optional[str] = None


class GEMovieDownloaderV2:
    """Enhanced downloader for ge.movie website."""

    BASE_URL = "https://ge.movie"
    EMBED_URLS = [
        "https://embed.kinoflix.stream",
        "https://embed.kinoflix.live",
        "https://embed.kinoflix.co",
    ]
    CDN_BASES = [
        "https://01-cdn.videodb.cloud",
        "https://02-cdn.videodb.cloud",
    ]

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "sec-ch-ua": '"Brave";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-gpc": "1",
    }

    VIDEO_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "identity;q=1, *;q=0",
        "Referer": "https://embed.kinoflix.live/",
        "Origin": "https://embed.kinoflix.live",
        "sec-ch-ua": '"Brave";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "video",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "cross-site",
        "sec-gpc": "1",
    }

    def __init__(self, output_dir: str = "downloads", verbose: bool = False):
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.verbose = verbose

    def log(self, message: str):
        """Print verbose messages."""
        if self.verbose:
            print(f"[DEBUG] {message}")

    def parse_series_url(self, url: str) -> tuple[str, str]:
        """Extract series ID and slug from URL."""
        match = re.search(r'/serial/(\d+)/([^/?]+)', url)
        if not match:
            raise ValueError(f"Invalid series URL: {url}")
        return match.group(1), match.group(2)

    def fetch_series_page(self, series_id: str, slug: str, season: int = 1, episode: int = 1) -> str:
        """Fetch the series page HTML."""
        url = f"{self.BASE_URL}/serial/{series_id}/{slug}?season={season}&episode={episode}"
        self.log(f"Fetching: {url}")
        response = self.session.get(url)
        response.raise_for_status()
        return response.text

    def parse_series_info(self, html: str, series_id: str, slug: str) -> Series:
        """Parse series information from HTML."""
        soup = BeautifulSoup(html, 'html.parser')

        # Extract title from h1
        title_tag = soup.find('h1')
        title_ge = ""
        title_en = slug.replace('-', ' ').title()
        year = "Unknown"

        if title_tag:
            # Get all text content from h1
            full_text = title_tag.get_text(separator=' ', strip=True)
            self.log(f"H1 full text: {full_text}")

            # Parse Georgian title and year: "პინგვინი (2024)"
            year_match = re.search(r'\((\d{4})\)', full_text)
            if year_match:
                year = year_match.group(1)

            # Try to get Georgian title (before the year)
            ge_match = re.match(r'^([^\(]+)', full_text)
            if ge_match:
                title_ge = ge_match.group(1).strip()

            # Try to get English title (after year, before /)
            en_match = re.search(r'\(\d{4}\)\s*([^/]+)', full_text)
            if en_match:
                title_en = en_match.group(1).strip()

        # Extract TMDB ID from embed iframe - try multiple patterns
        iframe = soup.find('iframe', id='serial_embed')
        tmdb_id = None
        embed_url = None

        if iframe and iframe.get('src'):
            embed_url = iframe['src']
            self.log(f"Embed iframe src: {embed_url}")
            tmdb_match = re.search(r'id=(\d+)', embed_url)
            if tmdb_match:
                tmdb_id = tmdb_match.group(1)

        # Fallback: search for TMDB ID in script tags or other places
        if not tmdb_id:
            # Look for tmdb_id or similar in the page
            tmdb_patterns = [
                r'tmdb_id["\']?\s*[=:]\s*["\']?(\d+)',
                r'id["\']?\s*[=:]\s*["\']?(\d+)',
                r'/serial/\d+/[^?]+\?.*?id=(\d+)',
            ]
            for pattern in tmdb_patterns:
                match = re.search(pattern, html)
                if match:
                    tmdb_id = match.group(1)
                    self.log(f"Found TMDB ID via pattern: {tmdb_id}")
                    break

        # Store the discovered embed base URL for later use
        if embed_url:
            embed_base_match = re.match(r'(https?://[^/]+)', embed_url)
            if embed_base_match:
                discovered_base = embed_base_match.group(1)
                if discovered_base not in self.EMBED_URLS:
                    self.EMBED_URLS.insert(0, discovered_base)
                    self.log(f"Added discovered embed base: {discovered_base}")

        return Series(
            id=series_id,
            slug=slug,
            title_en=title_en,
            title_ge=title_ge,
            year=year,
            tmdb_id=tmdb_id
        )

    def fetch_embed_page(self, tmdb_id: str, slug: str, season: int, episode: int) -> Optional[str]:
        """Fetch the embed player page, trying multiple embed domains."""
        for embed_base in self.EMBED_URLS:
            url = f"{embed_base}/splayer.php?type=serial&id={tmdb_id}&name={slug}&season={season}&episode={episode}&r_d=on&v=2.5.6"
            self.log(f"Trying embed URL: {url}")

            try:
                headers = self.HEADERS.copy()
                headers["Referer"] = f"{self.BASE_URL}/"
                response = self.session.get(url, headers=headers, timeout=10)
                if response.status_code == 200:
                    return response.text
            except Exception as e:
                self.log(f"Failed to fetch from {embed_base}: {e}")
                continue

        return None

    def parse_playlist(self, embed_html: str) -> dict:
        """
        Parse the Playerjs playlist from embed page.
        Returns: {(season, episode): {language: url, ...}, ...}
        """
        episodes = {}

        # Find all episode entries with file and id
        # Order in JSON is: "file":"...", "id":"1-1"
        episode_pattern = re.compile(
            r'"file"\s*:\s*"([^"]+)"[^}]*"id"\s*:\s*"(\d+)-(\d+)"',
            re.DOTALL
        )

        for match in episode_pattern.finditer(embed_html):
            file_str = match.group(1)
            season = int(match.group(2))
            episode = int(match.group(3))

            # Parse the file string to extract URLs by language
            # Format: [HD]{ქართულად}url_geo;{ინგლისურად}url_eng;,[SD]...
            urls = {}

            # Extract Georgian URLs (GEO)
            geo_match = re.search(r'\{ქართულად\}(https?://[^;{]+)', file_str)
            if geo_match:
                urls['GEO'] = geo_match.group(1)

            # Extract English URLs (ENG)
            eng_match = re.search(r'\{ინგლისურად\}(https?://[^;{]+)', file_str)
            if eng_match:
                urls['ENG'] = eng_match.group(1)

            # Extract Russian URLs (RUS) if present
            rus_match = re.search(r'\{რუსულად\}(https?://[^;{]+)', file_str)
            if rus_match:
                urls['RUS'] = rus_match.group(1)

            if urls:
                episodes[(season, episode)] = urls
                self.log(f"Found S{season}E{episode}: {list(urls.keys())}")

        return episodes

    def extract_video_urls(self, embed_html: str) -> list[dict]:
        """
        Extract all video URLs from embed page.
        Returns list of {url, quality, language} dicts.
        """
        videos = []

        # Parse the playlist structure
        playlist = self.parse_playlist(embed_html)

        # Convert to list format
        for (season, episode), urls in playlist.items():
            for lang, url in urls.items():
                videos.append({
                    'url': url,
                    'quality': 'HD',
                    'language': lang,
                    'season': season,
                    'episode': episode
                })

        # Fallback: Direct videodb.cloud URLs if playlist parsing failed
        if not videos:
            videodb_matches = re.findall(
                r'(https?://[\w.-]*videodb\.cloud/[^"\'<>\s;]+\.mp4[^"\'<>\s;]*)',
                embed_html
            )
            for url in videodb_matches:
                # Try to extract language from URL
                lang_match = re.search(r'_([A-Z]{3})\.mp4', url)
                lang = lang_match.group(1) if lang_match else 'unknown'
                # Try to extract episode from URL
                ep_match = re.search(r'_EP(\d+)_', url)
                ep = int(ep_match.group(1)) if ep_match else 0
                se_match = re.search(r'_SE(\d+)', url)
                se = int(se_match.group(1)) if se_match else 1

                videos.append({
                    'url': url,
                    'quality': 'unknown',
                    'language': lang,
                    'season': se,
                    'episode': ep
                })

        # Deduplicate
        seen = set()
        unique_videos = []
        for v in videos:
            if v['url'] not in seen:
                seen.add(v['url'])
                unique_videos.append(v)

        return unique_videos

    def get_video_url_for_episode(
        self,
        tmdb_id: str,
        slug: str,
        season: int,
        episode: int,
        preferred_lang: str = "GEO"
    ) -> Optional[str]:
        """Get the direct video URL for a specific episode."""
        embed_html = self.fetch_embed_page(tmdb_id, slug, season, episode)
        if not embed_html:
            return None

        videos = self.extract_video_urls(embed_html)
        if not videos:
            self.log("No videos found in embed page")
            # Save embed HTML for debugging
            if self.verbose:
                debug_file = self.output_dir / f"debug_embed_s{season}e{episode}.html"
                debug_file.write_text(embed_html)
                self.log(f"Saved embed HTML to {debug_file}")
            return None

        self.log(f"Found {len(videos)} video entries total")

        # Filter by season and episode
        episode_videos = [v for v in videos if v.get('season') == season and v.get('episode') == episode]

        if not episode_videos:
            self.log(f"No videos found for S{season}E{episode}")
            return None

        self.log(f"Found {len(episode_videos)} video(s) for S{season}E{episode}")
        for v in episode_videos:
            self.log(f"  - {v['language']}: {v['url'][:80]}...")

        # Prefer the requested language
        for video in episode_videos:
            if video['language'].upper() == preferred_lang.upper():
                return video['url']

        # Fall back to first available language for this episode
        return episode_videos[0]['url'] if episode_videos else None

    def probe_episodes(
        self,
        series_id: str,
        slug: str,
        tmdb_id: str,
        max_seasons: int = 5,
        max_episodes: int = 25
    ) -> dict[int, list[int]]:
        """Probe for available episodes by fetching embed page once."""
        available = {}
        print("Fetching episode list...")

        # The embed page contains ALL episodes, so we only need to fetch once
        embed_html = self.fetch_embed_page(tmdb_id, slug, 1, 1)
        if not embed_html:
            print("  Failed to fetch embed page")
            return available

        # Parse the playlist to get all episodes
        playlist = self.parse_playlist(embed_html)

        if not playlist:
            print("  No episodes found in playlist")
            return available

        # Organize by season
        for (season, episode), urls in playlist.items():
            if season not in available:
                available[season] = []
            available[season].append(episode)

        # Sort episodes within each season
        for season in available:
            available[season] = sorted(available[season])

        # Print summary
        for season in sorted(available.keys()):
            eps = available[season]
            print(f"  Season {season}: {len(eps)} episodes (EP {eps[0]}-{eps[-1]})")

        return available

    def verify_video_url(self, url: str) -> bool:
        """Verify that a video URL is accessible."""
        try:
            response = self.session.head(url, headers=self.VIDEO_HEADERS, timeout=10)
            return response.status_code in [200, 206]
        except Exception:
            return False

    def download_video(self, url: str, output_path: Path, chunk_size: int = 1024 * 1024) -> bool:
        """Download a video file with progress bar."""
        try:
            # Get file size with range header
            headers = self.VIDEO_HEADERS.copy()
            headers["Range"] = "bytes=0-"

            response = self.session.get(url, headers=headers, stream=True, timeout=30)

            # Handle both 200 and 206 responses
            if response.status_code not in [200, 206]:
                print(f"  HTTP Error: {response.status_code}")
                return False

            # Get file size from content-range or content-length
            content_range = response.headers.get('content-range', '')
            if content_range:
                file_size = int(content_range.split('/')[-1])
            else:
                file_size = int(response.headers.get('content-length', 0))

            # Check if already downloaded
            if output_path.exists():
                existing_size = output_path.stat().st_size
                if existing_size == file_size:
                    print(f"  Already complete: {output_path.name}")
                    return True
                elif existing_size > 0:
                    print(f"  Resuming from {existing_size / (1024*1024):.1f} MB...")
                    # Resume download
                    return self._resume_download(url, output_path, existing_size, file_size)

            # Fresh download
            with open(output_path, 'wb') as f:
                with tqdm(total=file_size, unit='B', unit_scale=True, desc=output_path.name[:40]) as pbar:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))

            return True

        except Exception as e:
            print(f"  Download error: {e}")
            return False

    def _resume_download(self, url: str, output_path: Path, start_byte: int, total_size: int) -> bool:
        """Resume a partial download."""
        try:
            headers = self.VIDEO_HEADERS.copy()
            headers["Range"] = f"bytes={start_byte}-"

            response = self.session.get(url, headers=headers, stream=True, timeout=30)

            if response.status_code not in [200, 206]:
                return False

            with open(output_path, 'ab') as f:
                with tqdm(total=total_size, initial=start_byte, unit='B', unit_scale=True,
                         desc=output_path.name[:40]) as pbar:
                    for chunk in response.iter_content(chunk_size=1024*1024):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))

            return True

        except Exception as e:
            print(f"  Resume error: {e}")
            return False

    def download_series(
        self,
        url: str,
        season_filter: Optional[int] = None,
        episode_filter: Optional[int] = None,
        language: str = "GEO"
    ):
        """Download a TV series."""
        print("=" * 60)
        print("GE.Movie Series Downloader v2")
        print("=" * 60)

        # Parse URL
        series_id, slug = self.parse_series_url(url)
        print(f"\nSeries ID: {series_id}")
        print(f"Slug: {slug}")

        # Fetch and parse series info
        print("\nFetching series information...")
        html = self.fetch_series_page(series_id, slug)
        series = self.parse_series_info(html, series_id, slug)

        print(f"\nTitle (GE): {series.title_ge}")
        print(f"Title (EN): {series.title_en}")
        print(f"Year: {series.year}")
        print(f"TMDB ID: {series.tmdb_id}")

        if not series.tmdb_id:
            print("\nError: Could not determine TMDB ID")
            return False

        # Create output directory
        safe_title = re.sub(r'[<>:"/\\|?*]', '_', series.title_en)
        series_dir = self.output_dir / f"{safe_title}_{series.year}"
        series_dir.mkdir(parents=True, exist_ok=True)

        # Determine episodes to download
        if season_filter and episode_filter:
            episodes_to_download = {season_filter: [episode_filter]}
        else:
            episodes_to_download = self.probe_episodes(series_id, slug, series.tmdb_id)

            if season_filter:
                if season_filter in episodes_to_download:
                    episodes_to_download = {season_filter: episodes_to_download[season_filter]}
                else:
                    print(f"\nSeason {season_filter} not found!")
                    return False

        if not episodes_to_download:
            print("\nNo episodes found to download!")
            return False

        # Summary
        total_episodes = sum(len(eps) for eps in episodes_to_download.values())
        print(f"\nReady to download {total_episodes} episode(s)")
        for s, eps in sorted(episodes_to_download.items()):
            print(f"  Season {s}: Episodes {eps[0]}-{eps[-1]} ({len(eps)} eps)")

        print(f"\nOutput directory: {series_dir}")
        print(f"Preferred language: {language}")
        print("-" * 60)

        # Download loop
        downloaded = 0
        failed = 0
        current = 0

        for season_num in sorted(episodes_to_download.keys()):
            season_dir = series_dir / f"Season_{season_num:02d}"
            season_dir.mkdir(exist_ok=True)

            for episode_num in episodes_to_download[season_num]:
                current += 1
                print(f"\n[{current}/{total_episodes}] S{season_num:02d}E{episode_num:02d}")

                # Get video URL
                video_url = self.get_video_url_for_episode(
                    series.tmdb_id, slug, season_num, episode_num, language
                )

                if not video_url:
                    print("  ✗ Could not find video URL")
                    failed += 1
                    continue

                self.log(f"Video URL: {video_url}")

                # Generate filename
                filename = f"{safe_title}_S{season_num:02d}E{episode_num:02d}_{language}.mp4"
                output_path = season_dir / filename

                # Download
                if self.download_video(video_url, output_path):
                    print(f"  ✓ Downloaded successfully")
                    downloaded += 1
                else:
                    print(f"  ✗ Download failed")
                    failed += 1

                time.sleep(0.5)

        # Final summary
        print("\n" + "=" * 60)
        print("Download Complete!")
        print(f"  ✓ Successful: {downloaded}")
        print(f"  ✗ Failed: {failed}")
        print(f"  📁 Saved to: {series_dir}")
        print("=" * 60)

        return failed == 0


def main():
    parser = argparse.ArgumentParser(
        description="Download TV series from ge.movie (v2)",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument("url", help="Series URL from ge.movie")
    parser.add_argument("-s", "--season", type=int, help="Download specific season")
    parser.add_argument("-e", "--episode", type=int, help="Download specific episode (requires -s)")
    parser.add_argument("-o", "--output", default="downloads", help="Output directory")
    parser.add_argument("-l", "--language", default="GEO",
                       choices=["GEO", "ENG", "RUS"], help="Preferred language")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")

    args = parser.parse_args()

    if args.episode and not args.season:
        parser.error("--episode requires --season")

    downloader = GEMovieDownloaderV2(output_dir=args.output, verbose=args.verbose)

    try:
        success = downloader.download_series(
            url=args.url,
            season_filter=args.season,
            episode_filter=args.episode,
            language=args.language
        )
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\nFatal error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
