#!/usr/bin/env python3
"""
GE.Movie Downloader
Downloads movies and TV series from ge.movie website.

Usage:
    python ge_movie_downloader.py <url> [--season N] [--episode N] [--output DIR]

Examples:
    # Movies:
    python ge_movie_downloader.py "https://ge.movie/movie/49227/dune-part-two"

    # TV Series:
    python ge_movie_downloader.py "https://ge.movie/serial/49495/the-penguin"
    python ge_movie_downloader.py "https://ge.movie/serial/49495/the-penguin" --season 1
    python ge_movie_downloader.py "https://ge.movie/serial/49495/the-penguin" --season 1 --episode 3
"""

import argparse
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse

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
    seasons: dict = None  # {season_num: [Episode, ...]}

    def __post_init__(self):
        if self.seasons is None:
            self.seasons = {}


class GEMovieDownloader:
    """Downloader for ge.movie website."""

    BASE_URL = "https://ge.movie"
    EMBED_URL = "https://embed.kinoflix.live"
    CDN_BASE = "https://01-cdn.videodb.cloud"

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",  # Removed 'br' (brotli) as requests doesn't handle it by default
        "sec-ch-ua": '"Brave";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-fetch-user": "?1",
        "sec-gpc": "1",
    }

    VIDEO_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "identity;q=1, *;q=0",
        "Referer": "https://embed.kinoflix.live/",
        "sec-ch-ua": '"Brave";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "video",
        "sec-fetch-mode": "no-cors",
        "sec-fetch-site": "cross-site",
        "sec-gpc": "1",
    }

    def __init__(self, output_dir: str = "downloads"):
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def parse_url(self, url: str) -> tuple[str, str, str]:
        """Extract content type, ID and slug from URL.
        Returns: (content_type, id, slug) where content_type is 'movie' or 'serial'
        """
        # URL format: https://ge.movie/movie/{id}/{slug} or https://ge.movie/serial/{id}/{slug}
        match = re.search(r'/(movie|serial)/(\d+)/([^/?]+)', url)
        if not match:
            raise ValueError(f"Invalid URL: {url}")
        return match.group(1), match.group(2), match.group(3)

    def parse_series_url(self, url: str) -> tuple[str, str]:
        """Extract series ID and slug from URL (legacy method)."""
        content_type, content_id, slug = self.parse_url(url)
        return content_id, slug

    def fetch_page(self, content_type: str, content_id: str, slug: str, season: int = 1, episode: int = 1) -> str:
        """Fetch the page HTML for movie or series."""
        if content_type == "movie":
            url = f"{self.BASE_URL}/movie/{content_id}/{slug}"
        else:
            url = f"{self.BASE_URL}/serial/{content_id}/{slug}?season={season}&episode={episode}"
        response = self.session.get(url)
        response.raise_for_status()
        return response.text

    def fetch_series_page(self, series_id: str, slug: str, season: int = 1, episode: int = 1) -> str:
        """Fetch the series page HTML (legacy method)."""
        return self.fetch_page("serial", series_id, slug, season, episode)

    def parse_series_info(self, html: str, series_id: str, slug: str) -> Series:
        """Parse series information from HTML."""
        soup = BeautifulSoup(html, 'html.parser')

        # Extract title
        title_tag = soup.find('h1')
        title_text = title_tag.get_text(strip=True) if title_tag else slug

        # Try to extract Georgian and English titles
        title_ge = title_text.split('(')[0].strip() if '(' in title_text else title_text
        title_en = slug.replace('-', ' ').title()

        # Extract year
        year_match = re.search(r'\((\d{4})\)', title_text)
        year = year_match.group(1) if year_match else "Unknown"

        # Try to extract TMDB ID from embed iframe
        iframe = soup.find('iframe', id='serial_embed')
        tmdb_id = None
        if iframe and iframe.get('src'):
            tmdb_match = re.search(r'id=(\d+)', iframe['src'])
            if tmdb_match:
                tmdb_id = tmdb_match.group(1)

        return Series(
            id=series_id,
            slug=slug,
            title_en=title_en,
            title_ge=title_ge,
            year=year,
            tmdb_id=tmdb_id
        )

    def fetch_embed_page(self, tmdb_id: str, slug: str, season: int, episode: int) -> str:
        """Fetch the embed player page for series."""
        url = f"{self.EMBED_URL}/splayer.php?type=serial&id={tmdb_id}&name={slug}&season={season}&episode={episode}&r_d=on&v=2.5.6"
        headers = self.HEADERS.copy()
        headers["Referer"] = f"{self.BASE_URL}/"
        response = self.session.get(url, headers=headers)
        response.raise_for_status()
        return response.text

    def fetch_movie_embed_page(self, tmdb_id: str, slug: str) -> str:
        """Fetch the embed player page for movie."""
        url = f"{self.EMBED_URL}/splayer.php?type=movie&id={tmdb_id}&name={slug}&r_d=on&v=2.5.6"
        headers = self.HEADERS.copy()
        headers["Referer"] = f"{self.BASE_URL}/"
        response = self.session.get(url, headers=headers)
        response.raise_for_status()
        return response.text

    def extract_video_url(self, embed_html: str) -> Optional[str]:
        """Extract the video URL from embed page."""
        # Look for video URL patterns in the embed page
        # Pattern 1: Direct MP4 URL
        mp4_match = re.search(r'(https?://[^"\']+\.mp4[^"\']*)', embed_html)
        if mp4_match:
            return mp4_match.group(1)

        # Pattern 2: videodb.cloud URL
        videodb_match = re.search(r'(https?://[\w.-]*videodb\.cloud[^"\']+)', embed_html)
        if videodb_match:
            return videodb_match.group(1)

        # Pattern 3: Look in JavaScript sources array
        sources_match = re.search(r'sources\s*[=:]\s*\[([^\]]+)\]', embed_html)
        if sources_match:
            src_match = re.search(r'["\']?src["\']?\s*:\s*["\']([^"\']+)["\']', sources_match.group(1))
            if src_match:
                return src_match.group(1)

        # Pattern 4: file parameter
        file_match = re.search(r'file\s*[=:]\s*["\']([^"\']+\.mp4[^"\']*)["\']', embed_html)
        if file_match:
            return file_match.group(1)

        return None

    def get_available_episodes(self, series_id: str, slug: str, tmdb_id: str) -> dict[int, list[int]]:
        """
        Discover available seasons and episodes.
        Returns: {season_num: [episode_nums]}
        """
        available = {}

        # Try to find episode list from the page
        html = self.fetch_series_page(series_id, slug)
        soup = BeautifulSoup(html, 'html.parser')

        # Look for episode selectors/lists in the HTML
        # This may vary based on the site structure
        episode_links = soup.find_all('a', href=re.compile(r'season=\d+&episode=\d+'))

        for link in episode_links:
            href = link.get('href', '')
            match = re.search(r'season=(\d+)&episode=(\d+)', href)
            if match:
                season = int(match.group(1))
                episode = int(match.group(2))
                if season not in available:
                    available[season] = []
                if episode not in available[season]:
                    available[season].append(episode)

        # If no episodes found in HTML, try probing
        if not available:
            available = self._probe_episodes(series_id, slug, tmdb_id)

        # Sort episodes
        for season in available:
            available[season] = sorted(available[season])

        return available

    def _probe_episodes(self, series_id: str, slug: str, tmdb_id: str, max_seasons: int = 10, max_episodes: int = 30) -> dict[int, list[int]]:
        """Probe for available episodes by trying to fetch them."""
        available = {}

        print("Probing for available episodes...")

        for season in range(1, max_seasons + 1):
            season_episodes = []
            consecutive_failures = 0

            for episode in range(1, max_episodes + 1):
                try:
                    embed_html = self.fetch_embed_page(tmdb_id, slug, season, episode)
                    video_url = self.extract_video_url(embed_html)

                    if video_url:
                        season_episodes.append(episode)
                        consecutive_failures = 0
                        print(f"  Found: Season {season}, Episode {episode}")
                    else:
                        consecutive_failures += 1
                except Exception:
                    consecutive_failures += 1

                # Stop if we've had 3 consecutive failures
                if consecutive_failures >= 3:
                    break

                time.sleep(0.5)  # Be nice to the server

            if season_episodes:
                available[season] = season_episodes
            elif season > 1:
                # No episodes in this season, probably no more seasons
                break

        return available

    def get_video_url_for_episode(self, tmdb_id: str, slug: str, season: int, episode: int) -> Optional[str]:
        """Get the direct video URL for a specific episode."""
        try:
            embed_html = self.fetch_embed_page(tmdb_id, slug, season, episode)
            return self.extract_video_url(embed_html)
        except Exception as e:
            print(f"Error fetching video URL for S{season:02d}E{episode:02d}: {e}")
            return None

    def download_video(self, url: str, output_path: Path, chunk_size: int = 8192) -> bool:
        """Download a video file with progress bar."""
        try:
            # Get file size
            response = self.session.head(url, headers=self.VIDEO_HEADERS, allow_redirects=True)
            file_size = int(response.headers.get('content-length', 0))

            # Check if file already exists and is complete
            if output_path.exists() and output_path.stat().st_size == file_size:
                print(f"  Already downloaded: {output_path.name}")
                return True

            # Download with progress bar
            response = self.session.get(url, headers=self.VIDEO_HEADERS, stream=True)
            response.raise_for_status()

            with open(output_path, 'wb') as f:
                with tqdm(total=file_size, unit='B', unit_scale=True, desc=output_path.name) as pbar:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))

            return True

        except Exception as e:
            print(f"  Download failed: {e}")
            if output_path.exists():
                output_path.unlink()  # Remove partial file
            return False

    def download_series(
        self,
        url: str,
        season_filter: Optional[int] = None,
        episode_filter: Optional[int] = None,
        language: str = "GEO"
    ):
        """Download a TV series or specific episodes."""

        # Parse URL
        series_id, slug = self.parse_series_url(url)
        print(f"Series ID: {series_id}, Slug: {slug}")

        # Fetch series info
        html = self.fetch_series_page(series_id, slug)
        series = self.parse_series_info(html, series_id, slug)
        print(f"Title: {series.title_ge} / {series.title_en} ({series.year})")
        print(f"TMDB ID: {series.tmdb_id}")

        if not series.tmdb_id:
            print("Error: Could not find TMDB ID")
            return

        # Create output directory
        series_dir = self.output_dir / f"{series.title_en.replace(' ', '_')}_{series.year}"
        series_dir.mkdir(parents=True, exist_ok=True)

        # Get available episodes
        if season_filter and episode_filter:
            # Single episode
            episodes_to_download = {season_filter: [episode_filter]}
        elif season_filter:
            # Specific season
            all_episodes = self.get_available_episodes(series_id, slug, series.tmdb_id)
            if season_filter in all_episodes:
                episodes_to_download = {season_filter: all_episodes[season_filter]}
            else:
                print(f"Season {season_filter} not found")
                return
        else:
            # All seasons
            episodes_to_download = self.get_available_episodes(series_id, slug, series.tmdb_id)

        if not episodes_to_download:
            print("No episodes found!")
            return

        # Count total episodes
        total_episodes = sum(len(eps) for eps in episodes_to_download.values())
        print(f"\nFound {total_episodes} episode(s) to download")

        # Download each episode
        downloaded = 0
        failed = 0

        for season_num in sorted(episodes_to_download.keys()):
            season_dir = series_dir / f"Season_{season_num:02d}"
            season_dir.mkdir(parents=True, exist_ok=True)

            for episode_num in episodes_to_download[season_num]:
                print(f"\n[{downloaded + failed + 1}/{total_episodes}] Season {season_num}, Episode {episode_num}")

                # Get video URL
                video_url = self.get_video_url_for_episode(
                    series.tmdb_id, slug, season_num, episode_num
                )

                if not video_url:
                    print("  Could not find video URL")
                    failed += 1
                    continue

                # Generate filename
                filename = f"{series.title_en.replace(' ', '_')}_S{season_num:02d}E{episode_num:02d}_{language}.mp4"
                output_path = season_dir / filename

                # Download
                if self.download_video(video_url, output_path):
                    downloaded += 1
                else:
                    failed += 1

                # Small delay between downloads
                time.sleep(1)

        print(f"\n{'='*50}")
        print(f"Download complete!")
        print(f"  Downloaded: {downloaded}")
        print(f"  Failed: {failed}")
        print(f"  Output directory: {series_dir}")

    def extract_movie_video_url(self, embed_html: str, preferred_lang: str = "GEO") -> Optional[str]:
        """Extract movie video URL from embed page with language preference."""
        # Pattern for language-labeled URLs in movie embeds
        # Format: {ქართულად}url_geo or similar
        lang_patterns = {
            'GEO': r'\{ქართულად\}(https?://[^;{\s"\']+)',
            'ENG': r'\{ინგლისურად\}(https?://[^;{\s"\']+)',
            'RUS': r'\{რუსულად\}(https?://[^;{\s"\']+)',
        }

        # Try preferred language first
        if preferred_lang in lang_patterns:
            match = re.search(lang_patterns[preferred_lang], embed_html)
            if match:
                return match.group(1)

        # Try other languages as fallback
        for lang, pattern in lang_patterns.items():
            if lang != preferred_lang:
                match = re.search(pattern, embed_html)
                if match:
                    print(f"  Note: {preferred_lang} not available, using {lang}")
                    return match.group(1)

        # Fallback to basic extraction
        return self.extract_video_url(embed_html)

    def download_movie(self, url: str, language: str = "GEO"):
        """Download a movie."""
        # Parse URL
        _, movie_id, slug = self.parse_url(url)
        print(f"Movie ID: {movie_id}, Slug: {slug}")

        # Fetch movie page
        html = self.fetch_page("movie", movie_id, slug)
        soup = BeautifulSoup(html, 'html.parser')

        # Extract title
        title_tag = soup.find('h1')
        title_text = title_tag.get_text(strip=True) if title_tag else slug

        # Try to extract year
        year_match = re.search(r'\((\d{4})\)', title_text)
        year = year_match.group(1) if year_match else "Unknown"
        title_clean = title_text.split('(')[0].strip() if '(' in title_text else title_text
        title_en = slug.replace('-', ' ').title()

        print(f"Title: {title_clean} / {title_en} ({year})")

        # Extract TMDB ID from embed iframe - try multiple patterns
        iframe = soup.find('iframe', id='emplayer')  # Common movie iframe ID
        if not iframe:
            iframe = soup.find('iframe', id='movie_embed')
        if not iframe:
            # Try alternative: any iframe with player.php or splayer.php
            iframe = soup.find('iframe', src=re.compile(r'player\.php'))
        if not iframe:
            iframe = soup.find('iframe', src=re.compile(r'splayer\.php'))
        if not iframe:
            # Try finding any iframe
            iframe = soup.find('iframe')

        tmdb_id = None
        embed_base = None
        embed_type = "splayer"  # Default embed type

        if iframe and iframe.get('src'):
            iframe_src = iframe['src']
            print(f"Found iframe: {iframe_src[:100]}...")
            tmdb_match = re.search(r'id=(\d+)', iframe_src)
            if tmdb_match:
                tmdb_id = tmdb_match.group(1)
            # Extract embed base URL
            base_match = re.match(r'(https?://[^/]+)', iframe_src)
            if base_match:
                embed_base = base_match.group(1)
            # Detect embed type (player.php vs splayer.php)
            if 'player.php' in iframe_src and 'splayer.php' not in iframe_src:
                embed_type = "player"

        # Fallback: search for TMDB ID in the HTML
        if not tmdb_id:
            # Look in script tags or data attributes
            tmdb_patterns = [
                r'tmdb_id["\']?\s*[=:]\s*["\']?(\d+)',
                r'"id"\s*:\s*(\d+)',
                r'tmdb["\']?\s*[=:]\s*["\']?(\d+)',
            ]
            for pattern in tmdb_patterns:
                match = re.search(pattern, html)
                if match:
                    tmdb_id = match.group(1)
                    break

        if not tmdb_id:
            print("Error: Could not find TMDB ID")
            return

        print(f"TMDB ID: {tmdb_id}")
        print(f"Embed type: {embed_type}")

        # Try multiple embed URLs
        embed_bases = []
        if embed_base:
            embed_bases.append(embed_base)
        embed_bases.extend([
            "https://embed.filmix.bond",
            "https://embed.kinoflix.stream",
            "https://embed.kinoflix.live",
            "https://embed.kinoflix.co",
        ])

        video_url = None
        for base in embed_bases:
            try:
                # Use the detected embed type (player.php or splayer.php)
                php_file = "player.php" if embed_type == "player" else "splayer.php"
                embed_url = f"{base}/{php_file}?type=movie&id={tmdb_id}&name={slug}&r_d=on&v=2.5.6"
                print(f"  Trying: {embed_url[:80]}...")
                headers = self.HEADERS.copy()
                headers["Referer"] = f"{self.BASE_URL}/"
                response = self.session.get(embed_url, headers=headers, timeout=10)
                if response.status_code == 200:
                    embed_html = response.text
                    video_url = self.extract_movie_video_url(embed_html, language)
                    if video_url:
                        print(f"  Found video URL!")
                        break
            except Exception as e:
                print(f"  Failed with {base}: {e}")
                continue

        if not video_url:
            print("Error: Could not find video URL")
            return

        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Generate filename
        filename = f"{title_en.replace(' ', '_')}_{year}_{language}.mp4"
        output_path = self.output_dir / filename

        print(f"\nDownloading: {filename}")

        # Download
        if self.download_video(video_url, output_path):
            print(f"\n{'='*50}")
            print(f"Download complete!")
            print(f"  Output: {output_path}")
        else:
            print(f"\nDownload failed!")


def main():
    parser = argparse.ArgumentParser(
        description="Download movies and TV series from ge.movie",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Movies:
  %(prog)s "https://ge.movie/movie/49227/dune-part-two"

  # TV Series:
  %(prog)s "https://ge.movie/serial/49495/the-penguin"
  %(prog)s "https://ge.movie/serial/49495/the-penguin" --season 1
  %(prog)s "https://ge.movie/serial/49495/the-penguin" -s 1 -e 3
  %(prog)s "https://ge.movie/serial/49495/the-penguin" --output ./my_downloads
        """
    )

    parser.add_argument("url", help="URL of the movie or series page on ge.movie")
    parser.add_argument("-s", "--season", type=int, help="Download specific season only (series only)")
    parser.add_argument("-e", "--episode", type=int, help="Download specific episode only (requires --season)")
    parser.add_argument("-o", "--output", default="downloads", help="Output directory (default: downloads)")
    parser.add_argument("-l", "--language", default="GEO", choices=["GEO", "ENG", "RUS"],
                       help="Preferred language (default: GEO)")

    args = parser.parse_args()

    if args.episode and not args.season:
        parser.error("--episode requires --season")

    downloader = GEMovieDownloader(output_dir=args.output)

    try:
        # Auto-detect movie vs series from URL
        content_type, _, _ = downloader.parse_url(args.url)

        if content_type == "movie":
            if args.season or args.episode:
                parser.error("--season and --episode are not applicable for movies")
            downloader.download_movie(
                url=args.url,
                language=args.language
            )
        else:
            downloader.download_series(
                url=args.url,
                season_filter=args.season,
                episode_filter=args.episode,
                language=args.language
            )
    except KeyboardInterrupt:
        print("\n\nDownload interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
