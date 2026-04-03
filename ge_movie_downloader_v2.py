#!/usr/bin/env python3
"""
GE.Movie Downloader v2
Enhanced version for downloading movies and TV series from ge.movie.

Usage:
    python ge_movie_downloader_v2.py <url> [options]

Examples:
    # Movies:
    python ge_movie_downloader_v2.py "https://ge.movie/movie/49614/sinners"
    python ge_movie_downloader_v2.py "https://ge.movie/movie/49614/sinners" -l GEO

    # TV Series:
    python ge_movie_downloader_v2.py "https://ge.movie/serial/49495/the-penguin"
    python ge_movie_downloader_v2.py "https://ge.movie/serial/49495/the-penguin" -s 1 -e 3
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

    def parse_url(self, url: str) -> tuple[str, str, str]:
        """Extract content type, ID and slug from URL.
        Returns: (content_type, id, slug) where content_type is 'movie' or 'serial'
        """
        match = re.search(r'/(movie|serial)/(\d+)/([^/?]+)', url)
        if not match:
            raise ValueError(f"Invalid URL: {url}")
        return match.group(1), match.group(2), match.group(3)

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

    def _parse_file_string(self, file_str: str, preferred_quality: str = "HD") -> dict:
        """Parse a file string with quality blocks into {language: url} dict.

        Format: [SD]{რუსულად}url;{ქართულად}url;,[HD]{რუსულად}url;{ქართულად}url;
        """
        lang_map = {
            'GEO': 'ქართულად',
            'ENG': 'ინგლისურად',
            'RUS': 'რუსულად',
        }

        # Try to parse quality blocks first
        quality_blocks = {}
        for block_match in re.finditer(r'\[(\w+)\]((?:(?!\[).)+)', file_str):
            quality = block_match.group(1)
            block = block_match.group(2)
            quality_blocks[quality] = {}
            for lang_code, lang_name in lang_map.items():
                lang_pattern = re.compile(r'\{' + re.escape(lang_name) + r'\}(https?://[^;{\s"\']+)')
                url_match = lang_pattern.search(block)
                if url_match:
                    quality_blocks[quality][lang_code] = url_match.group(1)

        if quality_blocks:
            # Return URLs from preferred quality, fallback to other
            for quality in [preferred_quality] + [q for q in quality_blocks if q != preferred_quality]:
                if quality in quality_blocks and quality_blocks[quality]:
                    return quality_blocks[quality]

        # Fallback: no quality blocks, just extract language URLs directly
        urls = {}
        for lang_code, lang_name in lang_map.items():
            lang_pattern = re.compile(r'\{' + re.escape(lang_name) + r'\}(https?://[^;{\s"\']+)')
            url_match = lang_pattern.search(file_str)
            if url_match:
                urls[lang_code] = url_match.group(1)
        return urls

    def parse_playlist(self, embed_html: str, preferred_quality: str = "HD") -> dict:
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

            urls = self._parse_file_string(file_str, preferred_quality)

            if urls:
                episodes[(season, episode)] = urls
                self.log(f"Found S{season}E{episode}: {list(urls.keys())}")

        return episodes

    def extract_video_urls(self, embed_html: str, preferred_quality: str = "HD") -> list[dict]:
        """
        Extract all video URLs from embed page.
        Returns list of {url, quality, language} dicts.
        """
        videos = []

        # Parse the playlist structure
        playlist = self.parse_playlist(embed_html, preferred_quality)

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
        preferred_lang: str = "GEO",
        preferred_quality: str = "HD"
    ) -> Optional[str]:
        """Get the direct video URL for a specific episode."""
        embed_html = self.fetch_embed_page(tmdb_id, slug, season, episode)
        if not embed_html:
            return None

        videos = self.extract_video_urls(embed_html, preferred_quality)
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

    def fetch_movie_embed_page(self, iframe_src: str, tmdb_id: str, slug: str) -> Optional[str]:
        """Fetch the embed player page for a movie.
        First tries the actual iframe src URL, then falls back to constructed URLs.
        """
        headers = self.HEADERS.copy()
        headers["Referer"] = f"{self.BASE_URL}/"

        # Try the actual iframe src first (most reliable)
        if iframe_src:
            self.log(f"Trying actual iframe src: {iframe_src}")
            try:
                response = self.session.get(iframe_src, headers=headers, timeout=15)
                if response.status_code == 200 and len(response.text) > 200:
                    return response.text
            except Exception as e:
                self.log(f"Failed with iframe src: {e}")

        # Fallback: try constructed URLs
        for embed_base in self.EMBED_URLS:
            for php_file in ["movie_player_4.php", "splayer.php", "player.php"]:
                url = f"{embed_base}/{php_file}?type=movie&id={tmdb_id}&name={slug}&r_d=on&v=2.5.6"
                self.log(f"Trying movie embed URL: {url}")
                try:
                    response = self.session.get(url, headers=headers, timeout=10)
                    if response.status_code == 200 and len(response.text) > 200:
                        return response.text
                except Exception as e:
                    self.log(f"Failed: {e}")
                    continue
        return None

    def extract_movie_video_url(self, embed_html: str, preferred_lang: str = "GEO", preferred_quality: str = "HD") -> Optional[str]:
        """Extract movie video URL from embed page with language and quality preference."""
        # Extract the full "file" string
        file_match = re.search(r'"file"\s*:\s*"([^"]+)"', embed_html)
        if not file_match:
            if re.search(r'"file"\s*:\s*\[\s*\]', embed_html):
                print("  Note: Video file list is empty - movie may not be available yet")
            return None

        file_str = file_match.group(1)
        self.log(f"File string: {file_str[:200]}...")

        urls = self._parse_file_string(file_str, preferred_quality)

        if urls:
            if preferred_lang in urls:
                print(f"  Found: {preferred_quality} / {preferred_lang}")
                return urls[preferred_lang]
            # Fallback to any available language
            for lang in ['GEO', 'ENG', 'RUS']:
                if lang in urls:
                    print(f"  Note: {preferred_lang} not available, using {lang}")
                    return urls[lang]

        # Fallback: any URL from file string
        any_url = re.search(r'(https?://[^;{\s"\']+\.mp4[^;{\s"\']*)', file_str)
        if any_url:
            return any_url.group(1)

        return None

    def download_movie(self, url: str, language: str = "GEO", quality: str = "HD"):
        """Download a movie."""
        print("=" * 60)
        print("GE.Movie Downloader v2")
        print("=" * 60)

        # Parse URL
        _, movie_id, slug = self.parse_url(url)
        print(f"\nMovie ID: {movie_id}")
        print(f"Slug: {slug}")

        # Fetch movie page
        print("\nFetching movie information...")
        page_url = f"{self.BASE_URL}/movie/{movie_id}/{slug}"
        self.log(f"Fetching: {page_url}")
        response = self.session.get(page_url)
        response.raise_for_status()
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')

        # Extract title
        title_tag = soup.find('h1')
        title_text = title_tag.get_text(separator=' ', strip=True) if title_tag else slug
        year_match = re.search(r'\((\d{4})\)', title_text)
        year = year_match.group(1) if year_match else "Unknown"
        title_ge = re.match(r'^([^\(]+)', title_text)
        title_ge = title_ge.group(1).strip() if title_ge else title_text
        title_en = slug.replace('-', ' ').title()

        print(f"\nTitle (GE): {title_ge}")
        print(f"Title (EN): {title_en}")
        print(f"Year: {year}")

        # Find the embed iframe - try multiple ID patterns
        iframe = (
            soup.find('iframe', id='emplayer') or
            soup.find('iframe', id='movie_embed') or
            soup.find('iframe', id='serial_embed') or
            soup.find('iframe', src=re.compile(r'player|splayer|embed')) or
            soup.find('iframe')
        )

        tmdb_id = None
        iframe_src = None

        if iframe and iframe.get('src'):
            iframe_src = iframe['src']
            self.log(f"Embed iframe src: {iframe_src}")

            # Make sure iframe src is absolute
            if iframe_src.startswith('//'):
                iframe_src = 'https:' + iframe_src
            elif iframe_src.startswith('/'):
                iframe_src = f"{self.BASE_URL}{iframe_src}"

            tmdb_match = re.search(r'id=(\d+)', iframe_src)
            if tmdb_match:
                tmdb_id = tmdb_match.group(1)

            # Add discovered embed base
            embed_base_match = re.match(r'(https?://[^/]+)', iframe_src)
            if embed_base_match:
                discovered_base = embed_base_match.group(1)
                if discovered_base not in self.EMBED_URLS:
                    self.EMBED_URLS.insert(0, discovered_base)

        if not tmdb_id:
            for pattern in [r'tmdb_id["\']?\s*[=:]\s*["\']?(\d+)', r'"id"\s*:\s*(\d+)']:
                match = re.search(pattern, html)
                if match:
                    tmdb_id = match.group(1)
                    break

        if not tmdb_id:
            print("\nError: Could not determine TMDB ID")
            return False

        print(f"TMDB ID: {tmdb_id}")

        # Fetch embed page using actual iframe src
        print(f"\nSearching for {language} video...")
        embed_html = self.fetch_movie_embed_page(iframe_src, tmdb_id, slug)
        if not embed_html:
            print("Error: Could not fetch embed page")
            return False

        if self.verbose:
            debug_file = self.output_dir / "debug_movie_embed.html"
            debug_file.write_text(embed_html)
            self.log(f"Saved embed HTML to {debug_file}")

        video_url = self.extract_movie_video_url(embed_html, language, quality)
        if not video_url:
            print("Error: Could not find video URL")
            return False

        self.log(f"Video URL: {video_url}")

        # Generate filename and download
        safe_title = re.sub(r'[<>:"/\\|?*]', '_', title_en)
        filename = f"{safe_title}_{year}_{language}_{quality}.mp4"
        output_path = self.output_dir / filename

        print(f"\nDownloading: {filename}")
        print(f"Output: {output_path}")
        print("-" * 60)

        if self.download_video(video_url, output_path):
            print(f"\n{'=' * 60}")
            print("Download Complete!")
            print(f"  Saved to: {output_path}")
            print("=" * 60)
            return True
        else:
            print("\nDownload failed!")
            return False

    def download_series(
        self,
        url: str,
        season_filter: Optional[int] = None,
        episode_filter: Optional[int] = None,
        language: str = "GEO",
        quality: str = "HD"
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
        print(f"Preferred quality: {quality}")
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
                    series.tmdb_id, slug, season_num, episode_num, language, quality
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
        description="Download movies and TV series from ge.movie (v2)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Movies:
  %(prog)s "https://ge.movie/movie/49614/sinners"
  %(prog)s "https://ge.movie/movie/49614/sinners" -l GEO

  # TV Series:
  %(prog)s "https://ge.movie/serial/49495/the-penguin"
  %(prog)s "https://ge.movie/serial/49495/the-penguin" -s 1 -e 3
        """
    )

    parser.add_argument("url", help="URL of the movie or series page on ge.movie")
    parser.add_argument("-s", "--season", type=int, help="Download specific season (series only)")
    parser.add_argument("-e", "--episode", type=int, help="Download specific episode (requires -s)")
    parser.add_argument("-o", "--output", default="downloads", help="Output directory")
    parser.add_argument("-l", "--language", default="GEO",
                       choices=["GEO", "ENG", "RUS"], help="Preferred language")
    parser.add_argument("-q", "--quality", default="HD",
                       choices=["HD", "SD"], help="Preferred quality (default: HD)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")

    args = parser.parse_args()

    if args.episode and not args.season:
        parser.error("--episode requires --season")

    downloader = GEMovieDownloaderV2(output_dir=args.output, verbose=args.verbose)

    try:
        content_type, _, _ = downloader.parse_url(args.url)

        if content_type == "movie":
            if args.season or args.episode:
                parser.error("--season and --episode are not applicable for movies")
            success = downloader.download_movie(
                url=args.url,
                language=args.language,
                quality=args.quality
            )
        else:
            success = downloader.download_series(
                url=args.url,
                season_filter=args.season,
                episode_filter=args.episode,
                language=args.language,
                quality=args.quality
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
