#!/usr/bin/env python3
"""
Twitter/X Headless Scraper — Module 3: Social Mining
Uses Playwright + Nitter public instances, zero authentication required.
"""

import asyncio
import json
import logging
import os
import random
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from itertools import cycle
from pathlib import Path
from typing import Optional

import redis.asyncio as aioredis
from dotenv import load_dotenv
from playwright.async_api import (
    async_playwright,
    Browser,
    BrowserContext,
    Page,
    PlaywrightContextManager,
    TimeoutError as PlaywrightTimeoutError,
)

load_dotenv()

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("twitter-scraper")

# ─── Config ───────────────────────────────────────────────────────────────────
REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
SOCIAL_WINDOW_MINUTES: int = int(os.getenv("SOCIAL_WINDOW_MINUTES", "120"))
PROXY_LIST_PATH: Optional[str] = os.getenv("PROXY_LIST_PATH", None)
SWEEP_SLEEP_SECONDS: int = int(os.getenv("SWEEP_SLEEP_SECONDS", "300"))
MAX_CONCURRENT_PAGES: int = int(os.getenv("MAX_CONCURRENT_PAGES", "5"))

NITTER_INSTANCES: list[str] = [
    "https://nitter.net",
    "https://nitter.privacydev.net",
    "https://nitter.poast.org",
    "https://nitter.1d4.us",
    "https://nitter.kavin.rocks",
    "https://nitter.fdn.fr",
    "https://nitter.it",
]

BASE_DIR = Path(__file__).parent
WATCHLIST_PATH = BASE_DIR / "watchlist.json"
USER_AGENTS_PATH = BASE_DIR / "user-agents.json"

# ─── Regex patterns ───────────────────────────────────────────────────────────
TICKER_RE = re.compile(r"\$([A-Z]{1,6})\b")
HASHTAG_RE = re.compile(r"#(\w+)")
KEY_TERMS_RE = re.compile(
    r"\b(moon|pump|dump|bullish|bearish|rug|scam|liquidat|whale|launchpad|airdrop|"
    r"presale|ICO|IDO|NFT|DeFi|staking|yield|APY|TVL|partnership|listing|delist|"
    r"SEC|ETF|halving|merger|acquisition|earnings|inflation|CPI|FOMC|Fed|rate|"
    r"bankruptcy|hack|exploit|bridge|flash loan|governance|snapshot|vote)\b",
    re.IGNORECASE,
)

# Accept-Language pool for header rotation
ACCEPT_LANGUAGES = [
    "en-US,en;q=0.9",
    "en-GB,en;q=0.9,en-US;q=0.8",
    "en-US,en;q=0.9,es;q=0.8",
    "en-US,en;q=0.8,fr;q=0.6",
    "en-CA,en;q=0.9,fr-CA;q=0.8",
    "en-AU,en;q=0.9",
    "en-US,en;q=0.9,de;q=0.7",
    "en-US,en;q=0.9,zh-CN;q=0.7",
]

ACCEPT_ENCODINGS = [
    "gzip, deflate, br",
    "gzip, deflate, br, zstd",
    "gzip, deflate",
    "br, gzip, deflate",
]

CACHE_CONTROLS = [
    "no-cache",
    "max-age=0",
    "no-cache, no-store",
    "no-store",
]

REFERERS = [
    "https://www.google.com/",
    "https://duckduckgo.com/",
    "https://www.bing.com/",
    "https://search.yahoo.com/",
    "",  # no referer
]


# ─── Utility helpers ──────────────────────────────────────────────────────────

def load_json(path: Path) -> list | dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_proxies(path: Optional[str]) -> list[dict]:
    """Load proxies from a newline-delimited file. Format: protocol://host:port or host:port:user:pass"""
    if not path or not os.path.exists(path):
        return []
    proxies = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(":")
            if len(parts) == 2:
                proxies.append({"server": line})
            elif len(parts) == 4:
                proxies.append({
                    "server": f"{parts[0]}:{parts[1]}",
                    "username": parts[2],
                    "password": parts[3],
                })
            else:
                proxies.append({"server": line})
    log.info(f"Loaded {len(proxies)} proxies from {path}")
    return proxies


def extract_keywords(text: str) -> list[str]:
    """Extract $tickers, #hashtags, and key financial/crypto terms from text."""
    tickers = [f"${t}" for t in TICKER_RE.findall(text)]
    hashtags = [f"#{h}" for h in HASHTAG_RE.findall(text)]
    key_terms = list({m.lower() for m in KEY_TERMS_RE.findall(text)})
    return list(set(tickers + hashtags + key_terms))


def parse_nitter_timestamp(ts_str: str) -> Optional[datetime]:
    """Parse Nitter's various timestamp formats to UTC datetime."""
    formats = [
        "%b %d, %Y · %I:%M %p %Z",
        "%d %b %Y · %H:%M %Z",
        "%b %d, %Y · %H:%M %p UTC",
        "%Y-%m-%d %H:%M:%S",
    ]
    ts_str = ts_str.strip()
    for fmt in formats:
        try:
            dt = datetime.strptime(ts_str, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue

    # Try title attribute datetime (ISO-ish)
    try:
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc)
    except Exception:
        pass

    return None


def is_within_window(dt: Optional[datetime], window_minutes: int) -> bool:
    if dt is None:
        return False
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
    return dt >= cutoff


def calculate_engagement_score(likes: int, retweets: int, replies: int = 0) -> float:
    """Weighted engagement score."""
    return round(likes * 1.0 + retweets * 2.0 + replies * 1.5, 2)


def random_delay(min_s: float = 2.0, max_s: float = 8.0) -> float:
    return random.uniform(min_s, max_s)


# ─── Playwright helpers ───────────────────────────────────────────────────────

async def build_browser_context(
    browser: Browser,
    user_agents: list[str],
    proxy: Optional[dict] = None,
) -> BrowserContext:
    """Create a new browser context with randomized fingerprint."""
    ua = random.choice(user_agents)
    width = random.randint(1280, 1920)
    height = random.randint(720, 1080)

    ctx_options: dict = {
        "user_agent": ua,
        "viewport": {"width": width, "height": height},
        "locale": random.choice(["en-US", "en-GB", "en-CA"]),
        "timezone_id": random.choice([
            "America/New_York", "America/Chicago", "America/Los_Angeles",
            "Europe/London", "Europe/Berlin",
        ]),
        "extra_http_headers": {
            "Accept-Language": random.choice(ACCEPT_LANGUAGES),
            "Accept-Encoding": random.choice(ACCEPT_ENCODINGS),
            "Cache-Control": random.choice(CACHE_CONTROLS),
            "Referer": random.choice(REFERERS),
            "DNT": random.choice(["1", "0"]),
            "Upgrade-Insecure-Requests": "1",
        },
        "java_script_enabled": True,
        "ignore_https_errors": True,
    }

    if proxy:
        ctx_options["proxy"] = proxy

    context = await browser.new_context(**ctx_options)

    # Override navigator properties to reduce detection
    await context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
        Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
        window.chrome = { runtime: {} };
    """)

    return context


async def human_scroll(page: Page, num_scrolls: int = 3) -> None:
    """Simulate human-like scrolling behavior."""
    for _ in range(num_scrolls):
        scroll_amount = random.randint(300, 800)
        await page.mouse.wheel(0, scroll_amount)
        await asyncio.sleep(random.uniform(0.3, 1.2))


async def human_mouse_move(page: Page) -> None:
    """Simulate random mouse movement."""
    vp = page.viewport_size or {"width": 1280, "height": 720}
    for _ in range(random.randint(2, 5)):
        x = random.randint(50, vp["width"] - 50)
        y = random.randint(50, vp["height"] - 50)
        await page.mouse.move(x, y)
        await asyncio.sleep(random.uniform(0.1, 0.4))


# ─── Nitter scraping ──────────────────────────────────────────────────────────

async def scrape_nitter_profile(
    page: Page,
    username: str,
    instance: str,
    window_minutes: int,
) -> list[dict]:
    """Scrape tweets from a Nitter instance profile page."""
    url = f"{instance}/{username}"
    tweets: list[dict] = []

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=20_000)
        await asyncio.sleep(random.uniform(1.5, 3.0))
        await human_mouse_move(page)
        await human_scroll(page, num_scrolls=random.randint(2, 5))

        # Check if profile exists (Nitter returns error page for non-existent users)
        title = await page.title()
        if "error" in title.lower() or "not found" in title.lower() or "404" in title.lower():
            log.warning(f"{username}: Profile not found on {instance}")
            return []

        # Extract tweet elements — Nitter structure: div.timeline-item > div.tweet-content
        tweet_items = await page.query_selector_all(".timeline-item")

        for item in tweet_items:
            try:
                # Skip pinned/promoted
                classes = await item.get_attribute("class") or ""
                if "pinned" in classes:
                    continue

                # Get tweet content text
                content_el = await item.query_selector(".tweet-content")
                if not content_el:
                    continue
                content = (await content_el.inner_text()).strip()
                if not content:
                    continue

                # Get timestamp — Nitter renders <span class="tweet-date"><a title="...">
                date_el = await item.query_selector(".tweet-date a")
                ts_str = ""
                if date_el:
                    ts_str = (await date_el.get_attribute("title")) or ""
                    if not ts_str:
                        ts_str = (await date_el.inner_text()).strip()
                dt = parse_nitter_timestamp(ts_str)

                if not is_within_window(dt, window_minutes):
                    continue

                # Get tweet URL for deduplication
                link_el = await item.query_selector(".tweet-date a")
                href = ""
                if link_el:
                    href = (await link_el.get_attribute("href")) or ""
                tweet_url = f"https://twitter.com{href}" if href.startswith("/") else href

                # Engagement metrics
                likes = 0
                retweets = 0
                replies = 0

                for stat_el in await item.query_selector_all(".icon-container"):
                    parent = await stat_el.query_selector_all("span")
                    for span in parent:
                        label = (await span.get_attribute("class")) or ""
                        txt = (await span.inner_text()).strip().replace(",", "")
                        try:
                            val = int(txt) if txt.isdigit() else 0
                        except Exception:
                            val = 0
                        if "icon-heart" in label:
                            likes = val
                        elif "icon-retweet" in label:
                            retweets = val
                        elif "icon-comment" in label:
                            replies = val

                # Try alternative engagement selectors
                if likes == 0 and retweets == 0:
                    stat_els = await item.query_selector_all(".tweet-stat")
                    for se in stat_els:
                        txt = (await se.inner_text()).strip().replace(",", "").replace("\n", " ")
                        nums = re.findall(r"\d+", txt)
                        if nums and "like" in txt.lower():
                            likes = int(nums[0])
                        elif nums and "retweet" in txt.lower():
                            retweets = int(nums[0])
                        elif nums and "repl" in txt.lower():
                            replies = int(nums[0])

                keywords = extract_keywords(content)
                engagement = calculate_engagement_score(likes, retweets, replies)

                tweets.append({
                    "platform": "twitter",
                    "author": username,
                    "content": content,
                    "url": tweet_url,
                    "timestamp": dt.isoformat() if dt else datetime.now(timezone.utc).isoformat(),
                    "engagement_score": engagement,
                    "keywords": keywords,
                    "likes": likes,
                    "retweets": retweets,
                    "replies": replies,
                    "scraped_from": instance,
                })

            except Exception as e:
                log.debug(f"Tweet parse error for {username}: {e}")
                continue

    except PlaywrightTimeoutError:
        log.warning(f"{username}: Timeout on {instance}")
    except Exception as e:
        log.warning(f"{username}: Scrape error on {instance}: {e}")

    return tweets


async def scrape_user_with_fallback(
    browser: Browser,
    username: str,
    user_agents: list[str],
    nitter_instances: list[str],
    proxy_cycle: cycle,
    proxy_health: dict,
    window_minutes: int,
    retries: int = 3,
) -> list[dict]:
    """
    Try each Nitter instance with exponential backoff. Returns all tweets found.
    """
    backoff_delays = [2, 4, 8]

    for instance in nitter_instances:
        for attempt in range(retries):
            # Proxy selection
            proxy: Optional[dict] = None
            try:
                candidate_proxy = next(proxy_cycle)
                if candidate_proxy and proxy_health.get(str(candidate_proxy), True):
                    proxy = candidate_proxy
            except StopIteration:
                pass

            context: Optional[BrowserContext] = None
            page: Optional[Page] = None

            try:
                context = await build_browser_context(browser, user_agents, proxy)
                page = await context.new_page()

                tweets = await scrape_nitter_profile(page, username, instance, window_minutes)

                if tweets:
                    log.info(f"✅ {username} er tweet pawa gelo: {len(tweets)}ta [instance={instance}]")
                    return tweets

                # Empty result might mean instance is down
                log.debug(f"{username}: 0 tweets from {instance}, attempt {attempt+1}")

            except Exception as e:
                delay = backoff_delays[min(attempt, len(backoff_delays) - 1)]
                if proxy:
                    proxy_health[str(proxy)] = False
                    log.warning(f"Proxy fail, direct connection use korchi: {proxy}")
                log.warning(f"{username}: Attempt {attempt+1} failed on {instance}: {e}. Waiting {delay}s...")
                await asyncio.sleep(delay)

            finally:
                if page:
                    try:
                        await page.close()
                    except Exception:
                        pass
                if context:
                    try:
                        await context.close()
                    except Exception:
                        pass

        log.warning(f"Nitter down, fallback instance try korchi: {instance} gave up")

    log.error(f"{username} scrape fail hoise, next e jachi")
    return []


# ─── Redis helpers ─────────────────────────────────────────────────────────────

async def push_to_redis_stream(
    redis: aioredis.Redis,
    tweet: dict,
) -> None:
    """Push a tweet dict to the Redis stream stream:social."""
    fields = {k: (json.dumps(v) if isinstance(v, list) else str(v)) for k, v in tweet.items()}
    await redis.xadd("stream:social", fields, maxlen=50_000, approximate=True)


async def is_seen(redis: aioredis.Redis, tweet_url: str) -> bool:
    """Check if a tweet URL has already been seen (deduplication)."""
    return bool(await redis.sismember("seen:tweets", tweet_url))


async def mark_seen(redis: aioredis.Redis, tweet_url: str) -> None:
    """Mark a tweet URL as seen. TTL: 48 hours."""
    pipe = redis.pipeline()
    pipe.sadd("seen:tweets", tweet_url)
    pipe.expire("seen:tweets", 172_800)  # 48h
    await pipe.execute()


# ─── Main sweep ───────────────────────────────────────────────────────────────

async def run_sweep(
    browser: Browser,
    watchlist: list[str],
    user_agents: list[str],
    nitter_instances: list[str],
    proxies: list[dict],
    redis: aioredis.Redis,
    window_minutes: int,
) -> None:
    """Run one full sweep of all usernames with max concurrency control."""
    proxy_cycle_list: list[Optional[dict]] = proxies if proxies else [None]
    proxy_cycle_iter: cycle = cycle(proxy_cycle_list)
    proxy_health: dict = {}

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_PAGES)
    total_new = 0

    async def process_user(username: str) -> None:
        nonlocal total_new
        async with semaphore:
            await asyncio.sleep(random_delay(1.5, 4.0))
            tweets = await scrape_user_with_fallback(
                browser=browser,
                username=username,
                user_agents=user_agents,
                nitter_instances=nitter_instances,
                proxy_cycle=proxy_cycle_iter,
                proxy_health=proxy_health,
                window_minutes=window_minutes,
            )

            new_count = 0
            for tweet in tweets:
                url = tweet.get("url", "")
                if url and await is_seen(redis, url):
                    continue
                await push_to_redis_stream(redis, tweet)
                if url:
                    await mark_seen(redis, url)
                new_count += 1

            total_new += new_count
            if new_count > 0:
                log.info(f"📤 {username}: {new_count} naya tweet Redis e push hoise")

    tasks = [process_user(u) for u in watchlist]
    await asyncio.gather(*tasks, return_exceptions=True)

    log.info(f"✅ Sweep complete. Mোট {total_new}ta naya tweet stream:social e gelo")


async def main() -> None:
    """Entry point — load config, connect Redis, launch Playwright, loop forever."""
    watchlist: list[str] = load_json(WATCHLIST_PATH)  # type: ignore
    user_agents: list[str] = load_json(USER_AGENTS_PATH)  # type: ignore
    proxies: list[dict] = load_proxies(PROXY_LIST_PATH)

    log.info(f"🚀 Twitter scraper shuru holo, {len(watchlist)} janke monitor korchi")
    log.info(f"   Nitter instances: {len(NITTER_INSTANCES)}, Proxies: {len(proxies)}")

    redis = await aioredis.from_url(REDIS_URL, decode_responses=True)

    sweep_count = 0
    async with async_playwright() as pw:
        browser: Browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                "--disable-infobars",
                "--window-size=1920,1080",
                "--disable-extensions",
                "--disable-gpu",
                "--no-first-run",
                "--no-default-browser-check",
                "--ignore-certificate-errors",
            ],
        )

        try:
            while True:
                sweep_count += 1
                log.info(f"═══ Sweep #{sweep_count} shuru hoise ═══")
                start = time.monotonic()

                await run_sweep(
                    browser=browser,
                    watchlist=watchlist,
                    user_agents=user_agents,
                    nitter_instances=NITTER_INSTANCES,
                    proxies=proxies,
                    redis=redis,
                    window_minutes=SOCIAL_WINDOW_MINUTES,
                )

                elapsed = time.monotonic() - start
                log.info(
                    f"Sweep #{sweep_count} শেষ হলো {elapsed:.1f}s e. "
                    f"Next sweep {SWEEP_SLEEP_SECONDS}s pore"
                )
                await asyncio.sleep(SWEEP_SLEEP_SECONDS)

        except asyncio.CancelledError:
            log.info("Scraper gracefully stop korchi...")
        except KeyboardInterrupt:
            log.info("Keyboard interrupt, scraper stop korchi...")
        finally:
            await browser.close()
            await redis.aclose()
            log.info("Browser o Redis connection close hoise")


if __name__ == "__main__":
    asyncio.run(main())
