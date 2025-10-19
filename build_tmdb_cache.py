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


def load_state() -> Dict[str, Any]:
    """Load state from previous run."""
    if STATE_FILE.exists():
        try:
            data = json.loads(STATE_FILE.read_text().strip())
            print(f"Resuming from ID {data.get('last_id', 0)}, cached: {data.get('cached_count', 0)}")
            return data
        except (ValueError, IOError):
            pass
    return {"last_id": 0, "cached_count": 0, "start_time": time.time()}


def save_state(state: Dict[str, Any]):
    """Save state for next run."""
    STATE_FILE.write_text(json.dumps(state))


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


def fetch_lbc_mapping_ids() -> list[int]:
    """Fetch all TMDB IDs from lbc repository."""
    print("Fetching lbc repository file listing...")
    url = "https://api.github.com/repos/12v/lbc/git/trees/main?recursive=1"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        # Extract all .txt files from docs/
        tmdb_ids = []
        for item in data.get("tree", []):
            if item["type"] == "blob" and item["path"].startswith("docs/") and item["path"].endswith(".txt"):
                # Fetch the file content to get the TMDB ID
                file_response = requests.get(
                    f"https://raw.githubusercontent.com/12v/lbc/main/{item['path']}",
                    timeout=10
                )
                if file_response.status_code == 200:
                    try:
                        tmdb_id = int(file_response.text.strip())
                        tmdb_ids.append(tmdb_id)
                    except ValueError:
                        pass
                time.sleep(0.1)  # Small delay between requests

        return sorted(set(tmdb_ids))  # Remove duplicates and sort

    except requests.RequestException as e:
        print(f"Error fetching lbc mappings: {e}")
        return []


def main():
    if not TMDB_API_KEY:
        print("Error: TMDB_API_TOKEN environment variable not set")
        return

    start_time = time.time()
    state = load_state()
    last_id = state.get("last_id", 0)
    cached_count = state.get("cached_count", 0)

    print(f"Fetching TMDB IDs from lbc repository...")
    all_ids = fetch_lbc_mapping_ids()
    print(f"Found {len(all_ids)} unique TMDB IDs")

    # Filter to IDs we haven't cached yet (continuing from last_id)
    remaining_ids = [id for id in all_ids if id > last_id]

    if not remaining_ids:
        print("All IDs already cached!")
        return

    print(f"Processing {len(remaining_ids)} new IDs...")

    for idx, movie_id in enumerate(remaining_ids):
        if time.time() - start_time > MAX_RUNTIME_SECS:
            print(f"\nTimeout reached. Saving state after {cached_count} new movies cached.")
            state["last_id"] = movie_id
            state["cached_count"] += cached_count
            save_state(state)
            return

        movie_data = fetch_tmdb_data(movie_id)
        if movie_data:
            save_movie_cache(movie_data)
            cached_count += 1

            if (idx + 1) % 10 == 0:
                elapsed = time.time() - start_time
                rate = (idx + 1) / elapsed if elapsed > 0 else 0
                eta_remaining = (len(remaining_ids) - idx - 1) / rate if rate > 0 else 0
                print(f"Cached {idx + 1}/{len(remaining_ids)} ({cached_count} total) - ETA: {int(eta_remaining)}s")
        else:
            print(f"Failed to cache movie {movie_id}")

    # All done
    state["last_id"] = 0  # Reset for next full run
    state["cached_count"] = 0
    save_state(state)

    elapsed = time.time() - start_time
    print(f"\nCompleted! Cached {cached_count} movies in {int(elapsed)}s")


if __name__ == "__main__":
    main()
