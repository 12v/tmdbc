import requests
import json
import time
from pathlib import Path
from typing import Optional, Dict, Any
import os

TMDB_API_KEY = os.environ.get("TMDB_API_TOKEN")
TMDB_BASE_URL = "https://api.themoviedb.org/3"
STATE_FILE = Path("cache_state.txt")
MAX_RUNTIME_SECS = 10 * 60
RATE_LIMIT = 50  # requests per second
RATE_LIMIT_WINDOW = 1.0  # seconds

# Track requests for rate limiting
request_times = []


def load_state() -> str:
    """Load last processed slug from state file."""
    if STATE_FILE.exists():
        try:
            slug = STATE_FILE.read_text().strip()
            if slug:
                print(f"Resuming after {slug}...")
            return slug
        except IOError:
            pass
    return ""


def save_state(slug: str):
    """Save last processed slug for next run."""
    STATE_FILE.write_text(slug)


def rate_limit_wait():
    """Respect rate limit of 50 requests per second."""
    global request_times
    current_time = time.time()

    # Remove old timestamps outside the window
    request_times = [t for t in request_times if current_time - t < RATE_LIMIT_WINDOW]

    if len(request_times) >= RATE_LIMIT:
        # Wait until the oldest request is outside the window
        sleep_time = RATE_LIMIT_WINDOW - (current_time - request_times[0])
        if sleep_time > 0:
            time.sleep(sleep_time)

    request_times.append(time.time())


def handle_rate_limit_error():
    """Exponential backoff for 429 errors."""
    wait_time = 60  # Start with 60 seconds
    for _ in range(3):
        print(f"Rate limited. Waiting {wait_time} seconds before retry...")
        time.sleep(wait_time)
        wait_time *= 2
        return True
    return False


def fetch_tmdb_data(movie_id: int) -> Optional[Dict[str, Any]]:
    """Fetch movie data from TMDB API."""
    rate_limit_wait()

    url = f"{TMDB_BASE_URL}/movie/{movie_id}"
    params = {"api_key": TMDB_API_KEY}

    try:
        response = requests.get(url, params=params, timeout=10)

        if response.status_code == 429:
            if handle_rate_limit_error():
                # Retry after backoff
                return fetch_tmdb_data(movie_id)
            return None

        if response.status_code == 404:
            return None

        response.raise_for_status()
        return response.json()

    except requests.RequestException as e:
        print(f"Error fetching movie {movie_id}: {e}")
        return None


def filter_movie_data(movie_data: Dict[str, Any]) -> Dict[str, Any]:
    """Extract only the fields we want to cache."""
    return {
        "id": movie_data.get("id"),
        "title": movie_data.get("title"),
        "original_title": movie_data.get("original_title"),
        "release_date": movie_data.get("release_date"),
        "status": movie_data.get("status"),
        "runtime": movie_data.get("runtime"),
        "original_language": movie_data.get("original_language"),
        "spoken_languages": movie_data.get("spoken_languages"),
        "origin_country": movie_data.get("origin_country"),
        "genres": movie_data.get("genres"),
    }


def save_movie_cache(movie_data: Dict[str, Any]):
    """Save movie data to cache directory structure (docs/XX/YYYY.json)."""
    filtered_data = filter_movie_data(movie_data)
    movie_id = str(filtered_data["id"])
    dir_prefix = movie_id[:2].zfill(2)
    cache_dir = Path("docs") / dir_prefix
    cache_dir.mkdir(parents=True, exist_ok=True)

    cache_file = cache_dir / f"{movie_id}.json"
    cache_file.write_text(json.dumps(filtered_data, indent=2))


def fetch_lbc_batch(last_slug: str = "", batch_size: int = 10) -> list[tuple[str, int]]:
    """Fetch a batch of TMDB IDs from lbc repository after last_slug."""
    url = "https://api.github.com/repos/12v/lbc/git/trees/main?recursive=1"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        # Extract all .txt files from docs/ with their slugs
        file_paths = []
        for item in data.get("tree", []):
            if item["type"] == "blob" and item["path"].startswith("docs/") and item["path"].endswith(".txt"):
                # Extract slug from path (e.g., "docs/4d/parasite-2019.txt" -> "4d/parasite-2019")
                slug = item["path"][5:-4]  # Remove "docs/" and ".txt"
                file_paths.append(slug)

        # Sort and find starting point
        file_paths.sort()
        if last_slug:
            try:
                start_idx = file_paths.index(last_slug) + 1
            except ValueError:
                start_idx = 0
        else:
            start_idx = 0

        batch_slugs = file_paths[start_idx : start_idx + batch_size]

        if not batch_slugs:
            return []

        print(f"Fetching batch ({len(batch_slugs)} files after {last_slug or 'start'})...")
        batch = []

        for slug in batch_slugs:
            try:
                file_response = requests.get(
                    f"https://raw.githubusercontent.com/12v/lbc/main/docs/{slug}.txt",
                    timeout=10
                )
                if file_response.status_code == 200:
                    try:
                        tmdb_id = int(file_response.text.strip())
                        batch.append((slug, tmdb_id))
                    except ValueError:
                        pass
            except requests.RequestException:
                pass
            time.sleep(0.1)  # Small delay between requests

        return batch

    except requests.RequestException as e:
        print(f"Error fetching lbc batch: {e}")
        return []


def main():
    if not TMDB_API_KEY:
        print("Error: TMDB_API_TOKEN environment variable not set")
        return

    start_time = time.time()
    last_slug = load_state()
    batch_size = 10

    print("Processing lbc mappings in batches of 10...")

    while True:
        if time.time() - start_time > MAX_RUNTIME_SECS:
            print(f"\nTimeout reached. Saving state: {last_slug}")
            save_state(last_slug)
            return

        # Fetch 10 (slug, tmdb_id) pairs from lbc
        batch = fetch_lbc_batch(last_slug, batch_size)
        if not batch:
            print("All movies processed!")
            save_state("")
            return

        # Fetch TMDB data for all 10 IDs
        print(f"  Looking up {len(batch)} movies on TMDB...")
        batch_count = 0
        for slug, movie_id in batch:
            movie_data = fetch_tmdb_data(movie_id)
            if movie_data:
                save_movie_cache(movie_data)
                batch_count += 1
            last_slug = slug

        print(f"  Saved {batch_count}/{len(batch)} movies.")


if __name__ == "__main__":
    main()
