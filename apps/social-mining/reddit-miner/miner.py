#!/usr/bin/env python3
"""
Reddit Keyword Velocity Miner — Module 3: Social Mining
Uses asyncpraw to monitor subreddits and detect keyword velocity spikes.
"""

import asyncio
import json
import logging
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import asyncpraw
import asyncpraw.models
import redis.asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("reddit-miner")

# ─── Config ───────────────────────────────────────────────────────────────────
REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
REDDIT_CLIENT_ID: str = os.getenv("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET: str = os.getenv("REDDIT_CLIENT_SECRET", "")
REDDIT_USER_AGENT: str = os.getenv(
    "REDDIT_USER_AGENT", "signal-aggregator-miner/1.0 (by /u/signal_bot)"
)
REDDIT_USERNAME: Optional[str] = os.getenv("REDDIT_USERNAME", None)
REDDIT_PASSWORD: Optional[str] = os.getenv("REDDIT_PASSWORD", None)

KEYWORD_SPIKE_THRESHOLD: float = float(os.getenv("KEYWORD_SPIKE_THRESHOLD", "300"))
ANALYSIS_WINDOW_MINUTES: int = int(os.getenv("ANALYSIS_WINDOW_MINUTES", "120"))
TICK_INTERVAL_SECONDS: int = int(os.getenv("TICK_INTERVAL_SECONDS", "120"))
KEYWORD_HISTORY_TTL: int = 4 * 3600  # 4 hours in seconds
TOP_POSTS_LIMIT: int = int(os.getenv("TOP_POSTS_LIMIT", "5"))

SUBREDDITS: list[str] = [
    "wallstreetbets",
    "CryptoMoonShots",
    "Bitcoin",
    "ethereum",
    "StockMarket",
    "Superstonk",
    "SatoshiStreetBets",
    "altcoin",
    "CryptoCurrency",
    "investing",
]

BASE_DIR = Path(__file__).parent
KEYWORDS_PATH = BASE_DIR / "keywords.json"

# ─── In-memory post store ─────────────────────────────────────────────────────
# post_id -> {"title": str, "body": str, "subreddit": str, "url": str, "created_utc": float}
POST_STORE: dict[str, dict] = {}
POST_STORE_LOCK = asyncio.Lock()


# ─── Keyword matching ──────────────────────────────────────────────────────────

def build_keyword_patterns(keywords_dict: dict[str, list[str]]) -> dict[str, re.Pattern]:
    """Compile regex patterns for each keyword category."""
    all_keywords: list[str] = []
    for category_words in keywords_dict.values():
        all_keywords.extend(category_words)

    # Deduplicate preserving order
    seen: set[str] = set()
    unique_keywords: list[str] = []
    for kw in all_keywords:
        if kw.lower() not in seen:
            seen.add(kw.lower())
            unique_keywords.append(kw)

    patterns: dict[str, re.Pattern] = {}
    for kw in unique_keywords:
        # Word boundary match, case-insensitive
        escaped = re.escape(kw)
        try:
            patterns[kw] = re.compile(rf"\b{escaped}\b", re.IGNORECASE)
        except re.error:
            # Some keywords like "$BTC" need special handling
            patterns[kw] = re.compile(re.escape(kw), re.IGNORECASE)
    return patterns


def find_keywords_in_text(text: str, patterns: dict[str, re.Pattern]) -> list[str]:
    """Return list of matched keywords in text."""
    found = []
    for kw, pattern in patterns.items():
        if pattern.search(text):
            found.append(kw)
    return found


# ─── Post collection ──────────────────────────────────────────────────────────

async def ingest_post(
    post: asyncpraw.models.Submission,
    subreddit_name: str,
    patterns: dict[str, re.Pattern],
) -> None:
    """Store a post in memory if it has keyword matches."""
    title = getattr(post, "title", "") or ""
    body = getattr(post, "selftext", "") or ""
    full_text = f"{title} {body}"
    url = getattr(post, "url", "") or f"https://reddit.com{getattr(post, 'permalink', '')}"
    created_utc = getattr(post, "created_utc", time.time())

    matched = find_keywords_in_text(full_text, patterns)
    if not matched:
        return

    post_id = getattr(post, "id", str(time.time()))

    async with POST_STORE_LOCK:
        POST_STORE[post_id] = {
            "title": title[:500],
            "subreddit": subreddit_name,
            "url": url,
            "created_utc": created_utc,
            "keywords": matched,
        }

    log.debug(f"New post: r/{subreddit_name} - {title[:50]}")


async def stream_subreddit(
    reddit: asyncpraw.Reddit,
    subreddit_name: str,
    patterns: dict[str, re.Pattern],
) -> None:
    """Continuously stream new posts from a subreddit."""
    while True:
        try:
            subreddit = await reddit.subreddit(subreddit_name)
            log.info(f"🔗 r/{subreddit_name} stream shuru hoise")

            async for post in subreddit.stream.submissions(skip_existing=True, pause_after=5):
                if post is None:
                    await asyncio.sleep(1)
                    continue
                await ingest_post(post, subreddit_name, patterns)

        except asyncpraw.exceptions.APIException as e:
            wait_s = 60
            log.warning(f"Reddit API rate limit, {wait_s}s wait korchi: {e}")
            await asyncio.sleep(wait_s)
        except asyncio.CancelledError:
            log.info(f"r/{subreddit_name} stream stop korchi")
            raise
        except Exception as e:
            log.error(f"r/{subreddit_name} stream error: {e}. 30s pore retry korchi...")
            await asyncio.sleep(30)


async def fetch_hot_posts(
    reddit: asyncpraw.Reddit,
    subreddit_name: str,
    patterns: dict[str, re.Pattern],
    limit: int = 50,
) -> None:
    """Fetch hot posts (non-streaming) for initial backfill."""
    try:
        subreddit = await reddit.subreddit(subreddit_name)
        async for post in subreddit.hot(limit=limit):
            await ingest_post(post, subreddit_name, patterns)
        log.info(f"r/{subreddit_name}: Hot posts backfill complete")
    except Exception as e:
        log.warning(f"r/{subreddit_name} hot fetch error: {e}")


# ─── Keyword velocity analysis ────────────────────────────────────────────────

def count_keywords_in_window(
    window_start_utc: float,
    window_end_utc: float,
    patterns: dict[str, re.Pattern],
) -> dict[str, dict]:
    """
    Count keyword occurrences across all posts within [window_start, window_end].
    Returns {keyword: {"count": int, "post_urls": list[str], "subreddits": set}}
    """
    keyword_data: dict[str, dict] = defaultdict(
        lambda: {"count": 0, "post_urls": [], "subreddits": set()}
    )

    snapshot = dict(POST_STORE)  # thread-safe snapshot
    for post_id, post in snapshot.items():
        created = post["created_utc"]
        if not (window_start_utc <= created <= window_end_utc):
            continue
        for kw in post.get("keywords", []):
            keyword_data[kw]["count"] += 1
            if len(keyword_data[kw]["post_urls"]) < TOP_POSTS_LIMIT:
                keyword_data[kw]["post_urls"].append(post["url"])
            keyword_data[kw]["subreddits"].add(post["subreddit"])

    return keyword_data


async def purge_old_posts(window_minutes: int) -> None:
    """Remove posts older than the analysis window to keep memory bounded."""
    cutoff = time.time() - (window_minutes * 60 * 2)  # keep 2x window for comparison
    async with POST_STORE_LOCK:
        to_delete = [
            pid for pid, p in POST_STORE.items()
            if p["created_utc"] < cutoff
        ]
        for pid in to_delete:
            del POST_STORE[pid]
    if to_delete:
        log.debug(f"Purged {len(to_delete)} purana posts memory theke")


async def push_spike_to_redis(
    redis: aioredis.Redis,
    keyword: str,
    subreddit: str,
    current_count: int,
    previous_count: int,
    spike_percent: float,
    top_post_urls: list[str],
    category: str,
) -> None:
    """Push keyword spike event to Redis stream:reddit."""
    now_iso = datetime.now(timezone.utc).isoformat()
    event = {
        "subreddit": subreddit,
        "keyword": keyword,
        "category": category,
        "current_count": str(current_count),
        "previous_count": str(previous_count),
        "spike_percent": f"{spike_percent:.2f}",
        "timestamp": now_iso,
        "top_post_urls": json.dumps(top_post_urls),
    }
    await redis.xadd("stream:reddit", event, maxlen=10_000, approximate=True)

    # Also update keyword history sorted set
    hist_key = f"keywords:history:{keyword}"
    pipe = redis.pipeline()
    pipe.zadd(hist_key, {f"{now_iso}:{current_count}": time.time()})
    pipe.expire(hist_key, KEYWORD_HISTORY_TTL)
    # Trim to last 4 hours worth of entries
    cutoff_score = time.time() - KEYWORD_HISTORY_TTL
    pipe.zremrangebyscore(hist_key, "-inf", cutoff_score)
    await pipe.execute()


def build_keyword_category_map(keywords_dict: dict[str, list[str]]) -> dict[str, str]:
    """Map each keyword back to its category."""
    mapping: dict[str, str] = {}
    for category, words in keywords_dict.items():
        for w in words:
            mapping[w.lower()] = category
    return mapping


async def run_velocity_analysis_tick(
    redis: aioredis.Redis,
    patterns: dict[str, re.Pattern],
    keywords_dict: dict[str, list[str]],
    window_minutes: int,
    spike_threshold: float,
) -> None:
    """
    Velocity analysis tick:
    - Current window: [now - window_minutes, now]
    - Previous window: [now - 2*window_minutes, now - window_minutes]
    - Calculate spike percent per keyword, emit alerts
    """
    now = time.time()
    w = window_minutes * 60

    current_data = count_keywords_in_window(now - w, now, patterns)
    previous_data = count_keywords_in_window(now - 2 * w, now - w, patterns)

    category_map = build_keyword_category_map(keywords_dict)
    spikes_found = 0

    for keyword, curr_info in current_data.items():
        current_count = curr_info["count"]
        prev_info = previous_data.get(keyword, {"count": 0, "post_urls": [], "subreddits": set()})
        previous_count = prev_info["count"]

        spike_percent = ((current_count - previous_count) / max(previous_count, 1)) * 100

        if spike_percent >= spike_threshold and current_count >= 3:
            # Determine dominant subreddit
            subreddits_involved = curr_info["subreddits"]
            dominant_sub = (
                max(subreddits_involved, key=lambda s: s)
                if subreddits_involved
                else "unknown"
            )
            category = category_map.get(keyword.lower(), "unknown")
            top_urls = curr_info["post_urls"][:TOP_POSTS_LIMIT]

            log.info(
                f"🚨 Keyword spike detect hoise! '{keyword}' - {spike_percent:.0f}% badhe gese "
                f"(prev={previous_count}, curr={current_count}) in r/{dominant_sub}"
            )

            await push_spike_to_redis(
                redis=redis,
                keyword=keyword,
                subreddit=dominant_sub,
                current_count=current_count,
                previous_count=previous_count,
                spike_percent=spike_percent,
                top_post_urls=top_urls,
                category=category,
            )
            spikes_found += 1

    if spikes_found == 0:
        log.debug(f"Tick complete. Kono spike detect hoy ni. Posts in memory: {len(POST_STORE)}")
    else:
        log.info(f"Tick complete. {spikes_found}ta keyword spike emit hoise")


# ─── Continuous tick loop ─────────────────────────────────────────────────────

async def velocity_tick_loop(
    redis: aioredis.Redis,
    patterns: dict[str, re.Pattern],
    keywords_dict: dict[str, list[str]],
    window_minutes: int,
    spike_threshold: float,
    tick_interval: int,
) -> None:
    """Run velocity analysis every tick_interval seconds indefinitely."""
    while True:
        try:
            await run_velocity_analysis_tick(
                redis=redis,
                patterns=patterns,
                keywords_dict=keywords_dict,
                window_minutes=window_minutes,
                spike_threshold=spike_threshold,
            )
            await purge_old_posts(window_minutes)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error(f"Velocity tick error: {e}")
        await asyncio.sleep(tick_interval)


# ─── Reddit client factory ────────────────────────────────────────────────────

def create_reddit_client() -> asyncpraw.Reddit:
    """Create an asyncpraw Reddit instance. Supports read-only or authenticated modes."""
    if REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET:
        kwargs: dict = {
            "client_id": REDDIT_CLIENT_ID,
            "client_secret": REDDIT_CLIENT_SECRET,
            "user_agent": REDDIT_USER_AGENT,
        }
        if REDDIT_USERNAME and REDDIT_PASSWORD:
            kwargs["username"] = REDDIT_USERNAME
            kwargs["password"] = REDDIT_PASSWORD
        log.info("Reddit authenticated mode (client credentials found)")
        return asyncpraw.Reddit(**kwargs)
    else:
        log.warning(
            "Reddit API credentials not set — using read-only mode with rate limits. "
            "Set REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET for better performance."
        )
        return asyncpraw.Reddit(
            client_id="public_read",
            client_secret="",
            user_agent=REDDIT_USER_AGENT,
        )


# ─── Main ─────────────────────────────────────────────────────────────────────

async def main() -> None:
    # Load keyword config
    keywords_dict: dict[str, list[str]] = {}
    with open(KEYWORDS_PATH, "r") as f:
        keywords_dict = json.load(f)

    patterns = build_keyword_patterns(keywords_dict)

    total_kw = sum(len(v) for v in keywords_dict.values())
    log.info(
        f"🚀 Reddit miner shuru holo, {len(SUBREDDITS)}ta subreddit monitor korchi"
    )
    log.info(
        f"   Keywords: {total_kw} | Patterns: {len(patterns)} | "
        f"Window: {ANALYSIS_WINDOW_MINUTES}min | Threshold: {KEYWORD_SPIKE_THRESHOLD}%"
    )

    redis = await aioredis.from_url(REDIS_URL, decode_responses=True)
    reddit = create_reddit_client()

    try:
        # Backfill hot posts from all subreddits
        log.info("Hot posts backfill shuru korchi...")
        backfill_tasks = [
            fetch_hot_posts(reddit, sub, patterns, limit=100)
            for sub in SUBREDDITS
        ]
        await asyncio.gather(*backfill_tasks, return_exceptions=True)
        log.info(f"Backfill complete. {len(POST_STORE)} posts loaded")

        # Start streaming tasks for each subreddit
        stream_tasks = [
            asyncio.create_task(
                stream_subreddit(reddit, sub, patterns),
                name=f"stream-{sub}",
            )
            for sub in SUBREDDITS
        ]

        # Start velocity analysis tick loop
        tick_task = asyncio.create_task(
            velocity_tick_loop(
                redis=redis,
                patterns=patterns,
                keywords_dict=keywords_dict,
                window_minutes=ANALYSIS_WINDOW_MINUTES,
                spike_threshold=KEYWORD_SPIKE_THRESHOLD,
                tick_interval=TICK_INTERVAL_SECONDS,
            ),
            name="velocity-tick",
        )

        all_tasks = stream_tasks + [tick_task]

        try:
            await asyncio.gather(*all_tasks)
        except asyncio.CancelledError:
            log.info("Main task cancelled, shutdown korchi...")
            for t in all_tasks:
                t.cancel()
            await asyncio.gather(*all_tasks, return_exceptions=True)

    except KeyboardInterrupt:
        log.info("Keyboard interrupt received, Reddit miner stop korchi...")
    finally:
        await reddit.close()
        await redis.aclose()
        log.info("Reddit client o Redis connection close hoise")


if __name__ == "__main__":
    asyncio.run(main())
