"""
CSA SEC EDGAR Poller - Module 2
Async poller for SEC EDGAR insider filings (Form 4, 13D, 13G).
De-duplicates by accession number and publishes to Redis stream.
"""

import asyncio
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import aiohttp
import redis.asyncio as redis
from dotenv import load_dotenv

from edgar_parser import (
    EDGAR_RSS_FEEDS,
    Form4Filing,
    Schedule13DFiling,
    Schedule13GFiling,
    SecFiling,
    enrich_13d_filing,
    enrich_13g_filing,
    enrich_form4_filing,
    fetch_and_parse_feed,
)

# ─────────────────────────────────────────────────────────────────────────────
# Environment
# ─────────────────────────────────────────────────────────────────────────────

load_dotenv()

REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379")
SEC_POLL_INTERVAL_MS: int = int(os.getenv("SEC_POLL_INTERVAL_MS", "500"))  # 500ms
REDIS_STREAM_KEY: str = "stream:sec"
REDIS_SEEN_KEY: str = "seen:sec"
REDIS_STREAM_MAXLEN: int = 200_000
SEC_MAX_RPS: int = int(os.getenv("SEC_MAX_RPS", "10"))  # Max 10 req/s per SEC policy
ENRICH_FORM4: bool = os.getenv("ENRICH_FORM4", "true").lower() == "true"
ENRICH_13D_13G: bool = os.getenv("ENRICH_13D_13G", "false").lower() == "true"

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("sec-poller")


# ─────────────────────────────────────────────────────────────────────────────
# Redis Clients
# ─────────────────────────────────────────────────────────────────────────────

class SecRedisClient:
    """
    Redis client for SEC poller.
    - Manages the `seen:sec` Set for deduplication
    - Publishes to `stream:sec` Stream
    """

    def __init__(self, url: str) -> None:
        self._url = url
        self._client: Optional[redis.Redis] = None

    async def connect(self) -> None:
        self._client = await redis.from_url(
            self._url,
            encoding="utf-8",
            decode_responses=True,
            retry_on_timeout=True,
            socket_keepalive=True,
        )
        await self._client.ping()
        logger.info("[REDIS] Redis connection established for SEC poller")

    async def is_seen(self, accession_number: str) -> bool:
        """Check if accession number already processed."""
        if self._client is None:
            raise RuntimeError("Redis not connected")
        return bool(await self._client.sismember(REDIS_SEEN_KEY, accession_number))  # type: ignore[misc]

    async def mark_seen(self, accession_number: str) -> None:
        """Add accession number to seen set."""
        if self._client is None:
            raise RuntimeError("Redis not connected")
        await self._client.sadd(REDIS_SEEN_KEY, accession_number)  # type: ignore[misc]
        # Cap the seen set size to avoid unbounded growth (keep last ~100k)
        seen_count = await self._client.scard(REDIS_SEEN_KEY)  # type: ignore[misc]
        if seen_count > 150_000:
            logger.warning(
                f"[REDIS] seen:sec set boro hoise ({seen_count}), "
                "manual cleanup er jonno log korchi"
            )

    async def publish_filing(self, entry_data: dict[str, str]) -> str:
        """Publish a filing to the stream."""
        if self._client is None:
            raise RuntimeError("Redis not connected")
        stream_id = await self._client.xadd(  # type: ignore[misc]
            REDIS_STREAM_KEY,
            entry_data,
            maxlen=REDIS_STREAM_MAXLEN,
            approximate=True,
        )
        return stream_id or "unknown"

    async def get_stream_length(self) -> int:
        if self._client is None:
            return 0
        return await self._client.xlen(REDIS_STREAM_KEY)  # type: ignore[misc]

    async def get_seen_count(self) -> int:
        if self._client is None:
            return 0
        return await self._client.scard(REDIS_SEEN_KEY)  # type: ignore[misc]

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()


# ─────────────────────────────────────────────────────────────────────────────
# Entry Builders (Filing → Redis Fields)
# ─────────────────────────────────────────────────────────────────────────────

def _build_form4_entry(filing: Form4Filing) -> dict[str, str]:
    """Convert Form4Filing to Redis stream entry dict."""
    # Determine human-readable transaction description
    txn_desc = ""
    code = filing.transaction_type
    if code == "P":
        txn_desc = f"Insider BUY: {filing.shares_transacted:,.0f} shares @ ${filing.price_per_share:.2f}"
    elif code == "S":
        txn_desc = f"Insider SELL: {filing.shares_transacted:,.0f} shares @ ${filing.price_per_share:.2f}"
    elif code == "A":
        txn_desc = f"Award/Grant: {filing.shares_transacted:,.0f} shares"
    else:
        txn_desc = f"{filing.transaction_code_label}: {filing.shares_transacted:,.0f} shares"

    total_value = filing.shares_transacted * filing.price_per_share

    return {
        "id": str(uuid.uuid4()),
        "accession_number": filing.accession_number,
        "form_type": filing.form_type,
        "company_name": filing.company_name,
        "ticker": filing.ticker,
        "cik": filing.cik,
        "filed_at": filing.filed_at,
        "url": filing.url,
        "insider_name": filing.insider_name,
        "insider_title": filing.insider_title,
        "transaction_type": filing.transaction_type,
        "transaction_code_label": filing.transaction_code_label,
        "transaction_date": filing.transaction_date,
        "shares_delta": f"{filing.shares_transacted:.4f}",
        "price_per_share": f"{filing.price_per_share:.4f}",
        "shares_owned_after": f"{filing.shares_owned_after:.4f}",
        "total_value_usd": f"{total_value:.2f}",
        "security_type": filing.security_type,
        "transaction_description": txn_desc,
        "feed_source": filing.feed_source,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def _build_13d_entry(filing: Schedule13DFiling) -> dict[str, str]:
    """Convert Schedule13DFiling to Redis stream entry dict."""
    return {
        "id": str(uuid.uuid4()),
        "accession_number": filing.accession_number,
        "form_type": filing.form_type,
        "company_name": filing.company_name,
        "ticker": filing.ticker,
        "cik": filing.cik,
        "filed_at": filing.filed_at,
        "url": filing.url,
        "filer_name": filing.filer_name,
        "percent_owned": f"{filing.percent_owned:.4f}",
        "aggregate_amount": f"{filing.aggregate_amount:.2f}",
        "purpose_of_transaction": filing.purpose_of_transaction,
        "subject_company": filing.subject_company,
        "feed_source": filing.feed_source,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def _build_13g_entry(filing: Schedule13GFiling) -> dict[str, str]:
    """Convert Schedule13GFiling to Redis stream entry dict."""
    return {
        "id": str(uuid.uuid4()),
        "accession_number": filing.accession_number,
        "form_type": filing.form_type,
        "company_name": filing.company_name,
        "ticker": filing.ticker,
        "cik": filing.cik,
        "filed_at": filing.filed_at,
        "url": filing.url,
        "filer_name": filing.filer_name,
        "percent_owned": f"{filing.percent_owned:.4f}",
        "aggregate_amount": f"{filing.aggregate_amount:.2f}",
        "is_amendment": "1" if filing.is_amendment else "0",
        "subject_company": filing.subject_company,
        "feed_source": filing.feed_source,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def build_redis_entry(filing: SecFiling) -> dict[str, str]:
    """Dispatch to the correct builder based on filing type."""
    if isinstance(filing, Form4Filing):
        return _build_form4_entry(filing)
    elif isinstance(filing, Schedule13DFiling):
        return _build_13d_entry(filing)
    elif isinstance(filing, Schedule13GFiling):
        return _build_13g_entry(filing)
    else:
        # Generic fallback
        return {
            "id": str(uuid.uuid4()),
            "accession_number": filing.accession_number,
            "form_type": filing.form_type,
            "company_name": filing.company_name,
            "ticker": filing.ticker,
            "cik": filing.cik,
            "filed_at": filing.filed_at,
            "url": filing.url,
            "feed_source": filing.feed_source,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }


# ─────────────────────────────────────────────────────────────────────────────
# SEC Rate Limiter (asyncio.Semaphore-based)
# ─────────────────────────────────────────────────────────────────────────────

class SecRateLimiter:
    """
    Token-bucket rate limiter for SEC EDGAR requests.
    SEC policy: max 10 requests/second.
    """

    def __init__(self, max_rps: int = 10) -> None:
        self._semaphore = asyncio.Semaphore(max_rps)
        self._max_rps = max_rps
        self._window_start = time.monotonic()
        self._request_count = 0

    async def __aenter__(self) -> "SecRateLimiter":
        await self._semaphore.acquire()
        return self

    async def __aexit__(self, *args: Any) -> None:
        # Release after 1 second to enforce per-second limit
        asyncio.get_event_loop().call_later(1.0, self._semaphore.release)
        self._request_count += 1

    @property
    def request_count(self) -> int:
        return self._request_count


# ─────────────────────────────────────────────────────────────────────────────
# Main SEC Poller
# ─────────────────────────────────────────────────────────────────────────────

class SecEdgarPoller:
    """
    Polls 3 SEC EDGAR RSS feeds concurrently every 500ms.
    De-duplicates filings by accession number via Redis Set.
    Optionally enriches Form 4 filings with transaction details.
    Publishes to Redis stream `stream:sec`.
    """

    def __init__(self) -> None:
        self._redis = SecRedisClient(REDIS_URL)
        self._rate_limiter = SecRateLimiter(SEC_MAX_RPS)
        self._session: Optional[aiohttp.ClientSession] = None
        self._is_running = False
        self._total_published = 0
        self._total_skipped = 0
        self._total_errors = 0
        self._cycle_count = 0

    async def _initialize(self) -> None:
        logger.info("╔══════════════════════════════════════════════════════════╗")
        logger.info("║        CSA SEC EDGAR Poller v1.0.0 - Module 2           ║")
        logger.info("╚══════════════════════════════════════════════════════════╝")

        await self._redis.connect()

        # Build aiohttp session with SEC-compliant headers
        connector = aiohttp.TCPConnector(
            limit=20,
            limit_per_host=5,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
        )
        self._session = aiohttp.ClientSession(
            connector=connector,
            headers={
                "User-Agent": "CSA-SignalAggregator/1.0 research@cryptosignal.example.com",
                "Accept-Encoding": "gzip, deflate",
            },
            timeout=aiohttp.ClientTimeout(total=30),
        )

        seen_count = await self._redis.get_seen_count()
        stream_len = await self._redis.get_stream_length()

        logger.info(
            f"[INIT] SEC poller chalu holo, 3ta feed monitor korchi. "
            f"Already seen: {seen_count} filings. Stream length: {stream_len}"
        )
        logger.info(f"[INIT] Poll interval: {SEC_POLL_INTERVAL_MS}ms, Max RPS: {SEC_MAX_RPS}")
        logger.info(f"[INIT] Form 4 enrichment: {ENRICH_FORM4}, 13D/13G enrichment: {ENRICH_13D_13G}")

    async def _fetch_feed_with_rate_limit(
        self,
        feed_name: str,
        feed_url: str,
    ) -> list[SecFiling]:
        """Fetch a single feed respecting the rate limiter."""
        async with self._rate_limiter:
            return await fetch_and_parse_feed(self._session, feed_name, feed_url)

    async def _enrich_filing(self, filing: SecFiling) -> SecFiling:
        """Optionally enrich a filing with detail data."""
        if isinstance(filing, Form4Filing) and ENRICH_FORM4:
            async with self._rate_limiter:
                return await enrich_form4_filing(filing, self._session)
        elif isinstance(filing, Schedule13DFiling) and ENRICH_13D_13G:
            async with self._rate_limiter:
                return await enrich_13d_filing(filing, self._session)
        elif isinstance(filing, Schedule13GFiling) and ENRICH_13D_13G:
            async with self._rate_limiter:
                return await enrich_13g_filing(filing, self._session)
        return filing

    async def _process_filing(self, filing: SecFiling) -> bool:
        """
        Process a single filing: check dedup, enrich, publish.
        Returns True if published, False if skipped.
        """
        # Check deduplication
        if await self._redis.is_seen(filing.accession_number):
            logger.debug(
                f"[DEDUP] Already dekha filing skip korchi: {filing.accession_number}"
            )
            self._total_skipped += 1
            return False

        # Mark as seen BEFORE enrichment to prevent race condition
        await self._redis.mark_seen(filing.accession_number)

        # Enrich with detail data
        try:
            filing = await self._enrich_filing(filing)
        except Exception as e:
            logger.warning(f"[ENRICH] Enrichment fail for {filing.accession_number}: {e}")
            # Continue with basic data

        # Log based on filing type
        self._log_filing(filing)

        # Build and publish Redis entry
        try:
            entry_data = build_redis_entry(filing)
            stream_id = await self._redis.publish_filing(entry_data)
            self._total_published += 1
            logger.debug(f"[REDIS] Filing published: {stream_id} ({filing.accession_number})")
            return True
        except Exception as e:
            logger.error(f"[REDIS] Publish fail for {filing.accession_number}: {e}")
            self._total_errors += 1
            return False

    def _log_filing(self, filing: SecFiling) -> None:
        """Emit appropriate Banglish log based on filing type and content."""
        if isinstance(filing, Form4Filing):
            code = filing.transaction_type
            if code == "P":
                logger.info(
                    f"[SEC] Naya SEC filing! Form 4: {filing.company_name} "
                    f"({filing.ticker}) insider buy korche | "
                    f"Insider: {filing.insider_name} | "
                    f"Shares: {filing.shares_transacted:,.0f} @ ${filing.price_per_share:.2f}"
                )
            elif code == "S":
                logger.warning(
                    f"[SEC] Naya SEC filing! Form 4: {filing.company_name} "
                    f"({filing.ticker}) insider sell korche | "
                    f"Insider: {filing.insider_name} | "
                    f"Shares: {filing.shares_transacted:,.0f} @ ${filing.price_per_share:.2f}"
                )
            else:
                logger.info(
                    f"[SEC] Form 4 filing: {filing.company_name} ({filing.ticker}) | "
                    f"{filing.transaction_code_label} | "
                    f"Accession: {filing.accession_number}"
                )

        elif isinstance(filing, Schedule13DFiling):
            logger.warning(
                f"[SEC] 13D filing! {filing.company_name} major stake change | "
                f"Filer: {filing.filer_name or 'Unknown'} | "
                f"Ownership: {filing.percent_owned:.1f}% | "
                f"Accession: {filing.accession_number}"
            )

        elif isinstance(filing, Schedule13GFiling):
            logger.info(
                f"[SEC] 13G filing {'(amendment) ' if filing.is_amendment else ''}"
                f"{filing.company_name} | "
                f"Filer: {filing.filer_name or 'Unknown'} | "
                f"Ownership: {filing.percent_owned:.1f}% | "
                f"Accession: {filing.accession_number}"
            )
        else:
            logger.info(
                f"[SEC] Filing: {filing.form_type} - {filing.company_name} | "
                f"Accession: {filing.accession_number}"
            )

    async def _run_one_cycle(self) -> None:
        """
        Run one poll cycle:
        1. Fetch all 3 feeds concurrently
        2. Process (dedup + enrich + publish) each filing
        """
        self._cycle_count += 1

        # Fetch all 3 feeds concurrently
        feed_tasks = [
            self._fetch_feed_with_rate_limit(name, url)
            for name, url in EDGAR_RSS_FEEDS.items()
        ]

        results = await asyncio.gather(*feed_tasks, return_exceptions=True)

        all_filings: list[SecFiling] = []
        for i, (feed_name, _) in enumerate(EDGAR_RSS_FEEDS.items()):
            result = results[i]
            if isinstance(result, Exception):
                logger.error(f"[FETCH] {feed_name} feed fetch fail: {result}")
                self._total_errors += 1
            elif isinstance(result, list):
                all_filings.extend(result)

        if not all_filings:
            logger.debug(f"[CYCLE #{self._cycle_count}] No filings fetched this cycle")
            return

        # Process all filings — use gather but limit concurrency
        # Process in batches of 5 to avoid hammering SEC with enrichment requests
        batch_size = 5
        published_this_cycle = 0

        for i in range(0, len(all_filings), batch_size):
            batch = all_filings[i:i + batch_size]
            process_tasks = [self._process_filing(f) for f in batch]
            batch_results = await asyncio.gather(*process_tasks, return_exceptions=True)

            for result in batch_results:
                if isinstance(result, Exception):
                    logger.error(f"[PROCESS] Filing process error: {result}")
                    self._total_errors += 1
                elif result is True:
                    published_this_cycle += 1

        if published_this_cycle > 0:
            logger.info(
                f"[CYCLE #{self._cycle_count}] {published_this_cycle} naya filing publish hoise | "
                f"Total published: {self._total_published} | "
                f"Skipped (dedup): {self._total_skipped}"
            )

        # Periodic stats log (every 100 cycles ≈ 50 seconds)
        if self._cycle_count % 100 == 0:
            seen_count = await self._redis.get_seen_count()
            stream_len = await self._redis.get_stream_length()
            logger.info(
                f"[STATS] Cycle {self._cycle_count} | "
                f"Published: {self._total_published} | "
                f"Skipped: {self._total_skipped} | "
                f"Errors: {self._total_errors} | "
                f"Seen set: {seen_count} | "
                f"Stream length: {stream_len} | "
                f"Rate limiter requests: {self._rate_limiter.request_count}"
            )

    async def start(self) -> None:
        """Main entry — initialize and run the polling loop."""
        await self._initialize()
        self._is_running = True

        try:
            while self._is_running:
                cycle_start = time.monotonic()

                try:
                    await self._run_one_cycle()
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(
                        f"[ERROR] Poll cycle e unhandled error: {e}", exc_info=True
                    )
                    self._total_errors += 1
                    await asyncio.sleep(5)

                elapsed_ms = (time.monotonic() - cycle_start) * 1000
                wait_ms = max(0.0, SEC_POLL_INTERVAL_MS - elapsed_ms)

                if wait_ms > 0:
                    await asyncio.sleep(wait_ms / 1000)

        except asyncio.CancelledError:
            logger.info("[SHUTDOWN] SEC poller gracefully shutting down...")
        finally:
            await self._cleanup()

    async def _cleanup(self) -> None:
        """Release all resources on shutdown."""
        if self._session and not self._session.closed:
            await self._session.close()
        await self._redis.close()
        logger.info(
            f"[SHUTDOWN] SEC poller thama gelo. "
            f"Total published: {self._total_published}, "
            f"Skipped: {self._total_skipped}, "
            f"Errors: {self._total_errors}"
        )

    def stop(self) -> None:
        self._is_running = False


# ─────────────────────────────────────────────────────────────────────────────
# Bootstrap
# ─────────────────────────────────────────────────────────────────────────────

async def main() -> None:
    poller = SecEdgarPoller()

    loop = asyncio.get_event_loop()

    def _signal_handler() -> None:
        logger.info("[SIGNAL] Shutdown signal received, poller bondho korchi")
        poller.stop()

    import signal
    loop.add_signal_handler(signal.SIGTERM, _signal_handler)
    loop.add_signal_handler(signal.SIGINT, _signal_handler)

    await poller.start()


if __name__ == "__main__":
    asyncio.run(main())
