#!/usr/bin/env python3
"""
Audio Alert Worker — Module 3: Social Mining
Subscribes to Redis stream:signals and speaks high-impact alerts via TTS.
"""

import asyncio
import json
import logging
import os
import platform
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import redis.asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("audio-worker")

# ─── Config ───────────────────────────────────────────────────────────────────
REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
AUDIO_ALERT_THRESHOLD: float = float(os.getenv("AUDIO_ALERT_THRESHOLD", "7.0"))
STREAM_KEY: str = os.getenv("SIGNAL_STREAM_KEY", "stream:signals")
LAST_ID_REDIS_KEY: str = "audio_worker:last_id"
BLOCK_TIMEOUT_MS: int = int(os.getenv("BLOCK_TIMEOUT_MS", "5000"))
TTS_RATE: int = int(os.getenv("TTS_RATE", "165"))  # words per minute
TTS_VOLUME: float = float(os.getenv("TTS_VOLUME", "0.9"))

# TTS engine singleton and lock
_TTS_ENGINE = None
_TTS_LOCK = threading.Lock()
_TTS_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="tts-worker")


# ─── TTS Engine setup ─────────────────────────────────────────────────────────

def detect_platform() -> str:
    """Detect the current OS platform."""
    system = platform.system().lower()
    if system == "darwin":
        return "macos"
    elif system == "linux":
        return "linux"
    elif system == "windows":
        return "windows"
    return "unknown"


def init_tts_engine():
    """
    Initialize and return a pyttsx3 TTS engine configured for the current platform.
    Returns None if TTS is unavailable.
    """
    global _TTS_ENGINE
    if _TTS_ENGINE is not None:
        return _TTS_ENGINE

    current_platform = detect_platform()
    log.info(f"Platform detect hoise: {current_platform}")

    try:
        import pyttsx3

        if current_platform == "macos":
            engine = pyttsx3.init(driverName="nsss")
            log.info("macOS nsss TTS driver initialize hoise")
        elif current_platform == "linux":
            try:
                engine = pyttsx3.init(driverName="espeak")
                log.info("Linux espeak TTS driver initialize hoise")
            except Exception:
                engine = pyttsx3.init()
                log.info("Linux default TTS driver initialize hoise (espeak fallback)")
        elif current_platform == "windows":
            engine = pyttsx3.init(driverName="sapi5")
            log.info("Windows SAPI5 TTS driver initialize hoise")
        else:
            engine = pyttsx3.init()
            log.info("Unknown platform, default TTS driver use korchi")

        # Configure engine properties
        engine.setProperty("rate", TTS_RATE)
        engine.setProperty("volume", TTS_VOLUME)

        # Try to set a clear voice
        voices = engine.getProperty("voices")
        if voices:
            # Prefer English voices
            english_voice = None
            for v in voices:
                v_id = (v.id or "").lower()
                v_name = (v.name or "").lower()
                if "english" in v_name or "en_" in v_id or "en-us" in v_id or "en-gb" in v_id:
                    english_voice = v
                    break
            if english_voice:
                engine.setProperty("voice", english_voice.id)
                log.info(f"TTS voice set: {english_voice.name}")
            else:
                engine.setProperty("voice", voices[0].id)
                log.info(f"TTS voice set (first available): {voices[0].name}")

        _TTS_ENGINE = engine
        return engine

    except ImportError:
        log.error("pyttsx3 install hoy ni! Audio alerts disabled.")
        return None
    except Exception as e:
        log.error(f"TTS engine initialize fail hoise: {e}. Audio alerts disabled.")
        return None


def _speak_sync(text: str) -> None:
    """
    Synchronous TTS speech function — runs in thread pool.
    Uses the global TTS engine with a lock to prevent concurrent speech.
    """
    engine = init_tts_engine()
    if engine is None:
        log.warning(f"TTS unavailable, alert text: {text}")
        return

    with _TTS_LOCK:
        try:
            engine.say(text)
            engine.runAndWait()
        except RuntimeError as e:
            # runAndWait can fail if called from wrong thread context
            log.warning(f"TTS runAndWait error (platform issue): {e}")
            try:
                engine.stop()
            except Exception:
                pass
        except Exception as e:
            log.error(f"TTS speak error: {e}")


async def speak_async(text: str, loop: asyncio.AbstractEventLoop) -> None:
    """
    Async wrapper — submits TTS to thread pool so it doesn't block the event loop.
    """
    await loop.run_in_executor(_TTS_EXECUTOR, _speak_sync, text)


# ─── Signal parsing ───────────────────────────────────────────────────────────

def parse_signal_event(fields: dict) -> Optional[dict]:
    """
    Parse a raw Redis stream field dict into a structured signal event.
    Expected fields: Impact_Rating, Data_Source, Directional_Bias, Raw_Intel_Summary, etc.
    """
    try:
        raw_rating = fields.get("Impact_Rating") or fields.get("impact_rating") or "0"
        rating = float(str(raw_rating).strip())

        source = (
            fields.get("Data_Source")
            or fields.get("data_source")
            or fields.get("platform")
            or "Unknown"
        )

        bias = (
            fields.get("Directional_Bias")
            or fields.get("directional_bias")
            or fields.get("bias")
            or "Neutral"
        )

        summary = (
            fields.get("Raw_Intel_Summary")
            or fields.get("raw_intel_summary")
            or fields.get("content")
            or fields.get("summary")
            or ""
        )

        ticker = (
            fields.get("ticker")
            or fields.get("Ticker")
            or fields.get("asset")
            or ""
        )

        return {
            "rating": rating,
            "source": str(source),
            "bias": str(bias),
            "summary": str(summary)[:200],
            "ticker": str(ticker),
        }
    except (ValueError, TypeError, KeyError) as e:
        log.warning(f"Signal parse error: {e} | fields={fields}")
        return None


def build_alert_text(signal: dict) -> str:
    """
    Build a clear, spoken alert text from a signal event.
    """
    rating_int = int(round(signal["rating"]))
    source = signal["source"].replace("_", " ").replace("-", " ")
    bias = signal["bias"]
    summary = signal["summary"].strip()
    ticker = signal["ticker"]

    parts = [f"ALPHA ALERT!"]

    if ticker:
        parts.append(f"{ticker} signal detected.")

    parts.append(f"{source} signal.")
    parts.append(f"Impact rating {rating_int} out of 10.")

    if bias and bias.lower() not in ("", "neutral", "unknown"):
        parts.append(f"Direction: {bias}.")

    if summary:
        # Sanitize for TTS: remove URLs, special chars
        clean_summary = " ".join(
            w for w in summary.split()
            if not w.startswith("http") and not w.startswith("www")
        )
        # Limit length for TTS
        if len(clean_summary) > 150:
            clean_summary = clean_summary[:147] + "..."
        if clean_summary:
            parts.append(clean_summary)

    return " ".join(parts)


# ─── Redis stream consumer ────────────────────────────────────────────────────

async def get_last_id(redis: aioredis.Redis) -> str:
    """Retrieve the last-processed stream ID from Redis for persistence."""
    stored = await redis.get(LAST_ID_REDIS_KEY)
    if stored:
        log.info(f"Resuming from stream ID: {stored}")
        return stored
    return "0-0"  # Start from beginning if no checkpoint


async def save_last_id(redis: aioredis.Redis, stream_id: str) -> None:
    """Persist the last-processed stream ID."""
    await redis.set(LAST_ID_REDIS_KEY, stream_id, ex=86400 * 7)  # 7-day TTL


async def consume_signals(
    redis: aioredis.Redis,
    loop: asyncio.AbstractEventLoop,
) -> None:
    """
    Main consumer loop: XREAD with BLOCK from stream:signals.
    Speaks alerts for signals above the impact threshold.
    """
    last_id = await get_last_id(redis)
    alerts_spoken = 0
    messages_processed = 0

    log.info(
        f"Audio worker shuru holo, Redis e listen korchi "
        f"[stream={STREAM_KEY}, threshold={AUDIO_ALERT_THRESHOLD}]"
    )

    while True:
        try:
            # Block up to BLOCK_TIMEOUT_MS waiting for new messages
            results = await redis.xread(
                streams={STREAM_KEY: last_id},
                count=10,
                block=BLOCK_TIMEOUT_MS,
            )

            if not results:
                # Timeout — no new messages, continue loop
                continue

            for stream_name, messages in results:
                for msg_id, fields in messages:
                    messages_processed += 1
                    last_id = msg_id

                    signal = parse_signal_event(fields)
                    if signal is None:
                        continue

                    rating = signal["rating"]
                    summary = signal["summary"]

                    if rating >= AUDIO_ALERT_THRESHOLD:
                        alert_text = build_alert_text(signal)
                        log.info(
                            f"🔊 HIGH IMPACT SIGNAL! Rating: {rating:.1f}, "
                            f"TTS diye bolchi: {summary[:50]}..."
                        )

                        # Non-blocking TTS in thread pool
                        asyncio.ensure_future(speak_async(alert_text, loop))
                        alerts_spoken += 1

                        log.info(f"Audio complete hoise: {summary[:30]}")

                    else:
                        log.debug(
                            f"Signal rating {rating:.1f} < threshold {AUDIO_ALERT_THRESHOLD}, "
                            f"skip korchi: {summary[:40]}"
                        )

            # Persist checkpoint after processing batch
            await save_last_id(redis, last_id)

            if messages_processed % 100 == 0 and messages_processed > 0:
                log.info(
                    f"📊 Status: {messages_processed} signals processed, "
                    f"{alerts_spoken} audio alerts spoken"
                )

        except asyncio.CancelledError:
            log.info("Audio worker cancelled, gracefully stop korchi...")
            await save_last_id(redis, last_id)
            raise
        except aioredis.RedisError as e:
            log.error(f"Redis error: {e}. 5s pore retry korchi...")
            await asyncio.sleep(5)
        except Exception as e:
            log.error(f"Unexpected error in consumer loop: {e}")
            await asyncio.sleep(2)


# ─── Main ─────────────────────────────────────────────────────────────────────

async def main() -> None:
    # Initialize TTS engine eagerly (in thread) before entering main loop
    current_platform = detect_platform()
    log.info(f"🎙️  Audio worker platform: {current_platform}")

    loop = asyncio.get_running_loop()

    # Init TTS in thread pool to avoid blocking the event loop
    tts_engine = await loop.run_in_executor(_TTS_EXECUTOR, init_tts_engine)
    if tts_engine is None:
        log.warning(
            "TTS engine initialize hote pareni. "
            "Audio alerts disabled — only log output will work."
        )
    else:
        log.info("TTS engine ready, audio alerts active")

    redis = await aioredis.from_url(REDIS_URL, decode_responses=True)

    try:
        await consume_signals(redis, loop)
    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        log.info("Keyboard interrupt, audio worker stop korchi...")
    finally:
        _TTS_EXECUTOR.shutdown(wait=False)
        await redis.aclose()
        log.info("Audio worker cleanly shutdown hoise")


if __name__ == "__main__":
    asyncio.run(main())
