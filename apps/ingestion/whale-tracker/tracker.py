"""
CSA Whale Tracker - Module 2
Async Ethereum on-chain whale movement detector for USDT/USDC transfers.
Monitors known CEX hot wallet inflows and large transfer events.
"""

import asyncio
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

import redis.asyncio as redis
from dotenv import load_dotenv
from web3 import AsyncWeb3
from web3.exceptions import ContractLogicError
from web3.middleware import ExtraDataToPOAMiddleware
from web3.types import LogReceipt

# ─────────────────────────────────────────────────────────────────────────────
# Environment & Configuration
# ─────────────────────────────────────────────────────────────────────────────

load_dotenv()

ETH_RPC_URL: str = os.getenv("ETH_RPC_URL", "https://eth.llamarpc.com")
ETH_RPC_FALLBACK_URL: str = os.getenv("ETH_RPC_FALLBACK_URL", "https://rpc.ankr.com/eth")
REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379")
WHALE_THRESHOLD_USD: float = float(os.getenv("WHALE_THRESHOLD_USD", "500000"))  # $500k default
POLL_INTERVAL_SECS: float = float(os.getenv("POLL_INTERVAL_SECS", "12"))  # 1 ETH block
REDIS_STREAM_KEY: str = "stream:whale"
REDIS_STREAM_MAXLEN: int = 500_000
KNOWN_WALLETS_FILE: Path = Path(__file__).parent / "known-wallets.json"

# Token contract addresses (checksummed)
USDT_CONTRACT: str = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
USDC_CONTRACT: str = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
USDT_DECIMALS: int = 6
USDC_DECIMALS: int = 6

# Transfer event ABI (ERC-20)
TRANSFER_ABI: list[dict] = [
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "from", "type": "address"},
            {"indexed": True, "name": "to", "type": "address"},
            {"indexed": False, "name": "value", "type": "uint256"},
        ],
        "name": "Transfer",
        "type": "event",
    }
]

# Transfer event topic (keccak256 of "Transfer(address,address,uint256)")
TRANSFER_TOPIC: str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# ─────────────────────────────────────────────────────────────────────────────
# Logging Setup
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("whale-tracker")


# ─────────────────────────────────────────────────────────────────────────────
# Data Types
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class WalletInfo:
    exchange: str
    wallet_type: str
    label: str
    chain: str = "ethereum"


@dataclass
class WhaleTransferEvent:
    id: str
    tx_hash: str
    from_address: str
    to_address: str
    token: str
    amount_raw: int
    amount_usd: float
    block_number: int
    timestamp: str
    log_index: int
    destination_type: str = "unknown"
    exchange_name: str = ""
    source_type: str = "unknown"
    is_cex_inflow: bool = False
    is_cex_outflow: bool = False


# ─────────────────────────────────────────────────────────────────────────────
# Known Wallet Registry
# ─────────────────────────────────────────────────────────────────────────────

class WalletRegistry:
    """Loads and queries known CEX hot wallet addresses."""

    def __init__(self, file_path: Path) -> None:
        self._wallets: dict[str, WalletInfo] = {}
        self._load(file_path)

    def _load(self, file_path: Path) -> None:
        try:
            with open(file_path, "r") as f:
                raw: dict[str, dict] = json.load(f)
            for addr, info in raw.items():
                # Normalize to lowercase for easy lookup
                self._wallets[addr.lower()] = WalletInfo(
                    exchange=info.get("exchange", "Unknown"),
                    wallet_type=info.get("type", "hot_wallet"),
                    label=info.get("label", addr),
                    chain=info.get("chain", "ethereum"),
                )
            logger.info(
                f"[WALLET_REGISTRY] {len(self._wallets)}ta known CEX wallet load hoise"
            )
        except FileNotFoundError:
            logger.error(f"[WALLET_REGISTRY] {file_path} file pawa gelo na!")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"[WALLET_REGISTRY] JSON parse error: {e}")
            raise

    def lookup(self, address: str) -> Optional[WalletInfo]:
        return self._wallets.get(address.lower())

    def is_known(self, address: str) -> bool:
        return address.lower() in self._wallets

    @property
    def count(self) -> int:
        return len(self._wallets)


# ─────────────────────────────────────────────────────────────────────────────
# Ethereum RPC Manager (with failover)
# ─────────────────────────────────────────────────────────────────────────────

class EthereumRPCManager:
    """Manages AsyncWeb3 connection with automatic failover."""

    def __init__(self, primary_url: str, fallback_url: str) -> None:
        self._primary_url = primary_url
        self._fallback_url = fallback_url
        self._w3: Optional[AsyncWeb3] = None
        self._using_fallback = False

    async def _create_connection(self, url: str) -> AsyncWeb3:
        w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(url))
        # Inject PoA middleware for compatibility with BSC/Polygon if needed
        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        return w3

    async def connect(self) -> AsyncWeb3:
        """Connect to primary, fall back to secondary on failure."""
        try:
            w3 = await self._create_connection(self._primary_url)
            if await w3.is_connected():
                chain_id = await w3.eth.chain_id
                logger.info(
                    f"[ETH] Primary RPC connected. Chain ID: {chain_id}. "
                    f"Whale tracker shuru holo, ETH node connected"
                )
                self._w3 = w3
                self._using_fallback = False
                return w3
            raise ConnectionError("Primary RPC not connected")
        except Exception as e:
            logger.warning(f"[ETH] Primary RPC fail, fallback try korchi: {e}")
            try:
                w3 = await self._create_connection(self._fallback_url)
                if await w3.is_connected():
                    chain_id = await w3.eth.chain_id
                    logger.info(
                        f"[ETH] Fallback RPC connected. Chain ID: {chain_id}"
                    )
                    self._w3 = w3
                    self._using_fallback = True
                    return w3
                raise ConnectionError("Fallback RPC also not connected")
            except Exception as e2:
                logger.error(f"[ETH] Both RPC connections fail hoise! {e2}")
                raise

    async def get_w3(self) -> AsyncWeb3:
        if self._w3 is None:
            await self.connect()
        try:
            # Health check
            await self._w3.eth.block_number  # type: ignore[union-attr]
            return self._w3  # type: ignore[return-value]
        except Exception:
            logger.warning("[ETH] RPC fail, fallback try korchi")
            return await self.connect()

    @property
    def is_fallback(self) -> bool:
        return self._using_fallback


# ─────────────────────────────────────────────────────────────────────────────
# Redis Stream Publisher
# ─────────────────────────────────────────────────────────────────────────────

class RedisStreamPublisher:
    """Async Redis stream publisher for whale events."""

    def __init__(self, url: str, stream_key: str, maxlen: int) -> None:
        self._url = url
        self._stream_key = stream_key
        self._maxlen = maxlen
        self._client: Optional[redis.Redis] = None

    async def connect(self) -> None:
        self._client = await redis.from_url(
            self._url,
            encoding="utf-8",
            decode_responses=True,
            retry_on_timeout=True,
            socket_keepalive=True,
        )
        # Test connection
        await self._client.ping()
        logger.info("[REDIS] Redis connection established for whale stream")

    async def publish(self, event: WhaleTransferEvent) -> str:
        if self._client is None:
            raise RuntimeError("Redis not connected")

        entry_data: dict[str, str] = {
            "id": event.id,
            "tx_hash": event.tx_hash,
            "from_address": event.from_address,
            "to_address": event.to_address,
            "token": event.token,
            "amount_raw": str(event.amount_raw),
            "amount_usd": f"{event.amount_usd:.2f}",
            "block_number": str(event.block_number),
            "timestamp": event.timestamp,
            "log_index": str(event.log_index),
            "destination_type": event.destination_type,
            "exchange_name": event.exchange_name,
            "source_type": event.source_type,
            "is_cex_inflow": "1" if event.is_cex_inflow else "0",
            "is_cex_outflow": "1" if event.is_cex_outflow else "0",
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }

        stream_id = await self._client.xadd(  # type: ignore[misc]
            self._stream_key,
            entry_data,
            maxlen=self._maxlen,
            approximate=True,
        )
        return stream_id

    async def get_stream_length(self) -> int:
        if self._client is None:
            return 0
        return await self._client.xlen(self._stream_key)  # type: ignore[misc]

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()


# ─────────────────────────────────────────────────────────────────────────────
# Transfer Event Decoder
# ─────────────────────────────────────────────────────────────────────────────

def decode_transfer_log(log: LogReceipt, token: str, decimals: int) -> Optional[tuple[str, str, int]]:
    """
    Decode a raw ERC-20 Transfer log into (from_addr, to_addr, value_raw).
    Returns None if decoding fails.
    """
    try:
        topics = log.get("topics", [])
        if len(topics) < 3:
            return None

        # Topics[1] = from (padded 32 bytes), Topics[2] = to (padded 32 bytes)
        from_raw = topics[1]
        to_raw = topics[2]
        data = log.get("data", b"")

        if isinstance(from_raw, bytes):
            from_addr = "0x" + from_raw.hex()[-40:]
        else:
            from_addr = "0x" + str(from_raw)[-40:]

        if isinstance(to_raw, bytes):
            to_addr = "0x" + to_raw.hex()[-40:]
        else:
            to_addr = "0x" + str(to_raw)[-40:]

        # Normalize to checksummed address
        from_addr = AsyncWeb3.to_checksum_address(from_addr)
        to_addr = AsyncWeb3.to_checksum_address(to_addr)

        # Data field = uint256 value (big-endian 32 bytes)
        if isinstance(data, bytes):
            value_raw = int.from_bytes(data[:32], "big") if len(data) >= 32 else 0
        elif isinstance(data, str):
            hex_data = data.replace("0x", "")
            value_raw = int(hex_data[:64], 16) if len(hex_data) >= 64 else 0
        else:
            value_raw = 0

        return from_addr, to_addr, value_raw

    except Exception as e:
        logger.debug(f"[DECODE] Transfer log decode fail: {e}")
        return None


def raw_to_usd(value_raw: int, decimals: int) -> float:
    """Convert raw token amount to USD-equivalent float."""
    divisor = Decimal(10 ** decimals)
    return float(Decimal(value_raw) / divisor)


# ─────────────────────────────────────────────────────────────────────────────
# Block Timestamp Cache
# ─────────────────────────────────────────────────────────────────────────────

class BlockTimestampCache:
    """Caches block timestamps to avoid redundant RPC calls."""

    def __init__(self, max_size: int = 500) -> None:
        self._cache: dict[int, int] = {}
        self._max_size = max_size

    async def get(self, w3: AsyncWeb3, block_number: int) -> int:
        if block_number in self._cache:
            return self._cache[block_number]

        try:
            block = await w3.eth.get_block(block_number)
            timestamp = int(block["timestamp"])
        except Exception:
            timestamp = int(time.time())

        # Evict if full
        if len(self._cache) >= self._max_size:
            oldest = min(self._cache.keys())
            del self._cache[oldest]

        self._cache[block_number] = timestamp
        return timestamp


# ─────────────────────────────────────────────────────────────────────────────
# Main Whale Tracker
# ─────────────────────────────────────────────────────────────────────────────

class WhaleTracker:
    """
    Async Ethereum whale tracker.
    Polls USDT + USDC Transfer events every ~12 seconds and
    publishes large transfers (>= WHALE_THRESHOLD_USD) to Redis.
    """

    def __init__(self) -> None:
        self._wallet_registry = WalletRegistry(KNOWN_WALLETS_FILE)
        self._rpc_manager = EthereumRPCManager(ETH_RPC_URL, ETH_RPC_FALLBACK_URL)
        self._publisher = RedisStreamPublisher(REDIS_URL, REDIS_STREAM_KEY, REDIS_STREAM_MAXLEN)
        self._block_ts_cache = BlockTimestampCache()
        self._last_processed_block: int = 0
        self._is_running: bool = False
        self._total_whales_detected: int = 0
        self._total_transfers_scanned: int = 0

    async def _initialize(self) -> None:
        logger.info("╔══════════════════════════════════════════════════════════╗")
        logger.info("║        CSA Whale Tracker v1.0.0 - Module 2              ║")
        logger.info("╚══════════════════════════════════════════════════════════╝")
        logger.info(f"[INIT] Whale threshold: ${WHALE_THRESHOLD_USD:,.0f} USD")
        logger.info(f"[INIT] Poll interval: {POLL_INTERVAL_SECS}s")
        logger.info(f"[INIT] Known wallets: {self._wallet_registry.count}")

        await self._publisher.connect()
        w3 = await self._rpc_manager.connect()

        # Set starting block to latest - 1 (don't replay history)
        latest = await w3.eth.block_number
        self._last_processed_block = latest - 1
        logger.info(f"[INIT] Starting from block {self._last_processed_block}")

    async def _fetch_transfer_logs(
        self,
        w3: AsyncWeb3,
        from_block: int,
        to_block: int,
    ) -> list[tuple[LogReceipt, str, int]]:
        """
        Fetch Transfer logs for USDT and USDC in a block range.
        Returns list of (log, token_symbol, decimals).
        """
        contracts = [
            (USDT_CONTRACT, "USDT", USDT_DECIMALS),
            (USDC_CONTRACT, "USDC", USDC_DECIMALS),
        ]

        all_logs: list[tuple[LogReceipt, str, int]] = []

        for contract_addr, symbol, decimals in contracts:
            try:
                logs = await w3.eth.get_logs(
                    {
                        "fromBlock": from_block,
                        "toBlock": to_block,
                        "address": contract_addr,
                        "topics": [TRANSFER_TOPIC],
                    }
                )
                for log in logs:
                    all_logs.append((log, symbol, decimals))
            except Exception as e:
                logger.warning(f"[ETH] {symbol} logs fetch fail (block {from_block}-{to_block}): {e}")

        return all_logs

    async def _process_log(
        self,
        log: LogReceipt,
        token: str,
        decimals: int,
        w3: AsyncWeb3,
    ) -> Optional[WhaleTransferEvent]:
        """Decode and classify a single Transfer log."""
        decoded = decode_transfer_log(log, token, decimals)
        if decoded is None:
            return None

        from_addr, to_addr, value_raw = decoded
        amount_usd = raw_to_usd(value_raw, decimals)

        if amount_usd < WHALE_THRESHOLD_USD:
            return None

        # Lookup wallet info
        from_info = self._wallet_registry.lookup(from_addr)
        to_info = self._wallet_registry.lookup(to_addr)

        is_cex_inflow = to_info is not None
        is_cex_outflow = from_info is not None

        destination_type = to_info.wallet_type if to_info else "unknown"
        exchange_name = to_info.exchange if to_info else (from_info.exchange if from_info else "")
        source_type = from_info.wallet_type if from_info else "unknown"

        # Get block timestamp
        block_number = int(log["blockNumber"])
        ts_unix = await self._block_ts_cache.get(w3, block_number)
        timestamp = datetime.fromtimestamp(ts_unix, tz=timezone.utc).isoformat()

        tx_hash = log["transactionHash"]
        if isinstance(tx_hash, bytes):
            tx_hash_str = "0x" + tx_hash.hex()
        else:
            tx_hash_str = str(tx_hash)

        return WhaleTransferEvent(
            id=str(uuid.uuid4()),
            tx_hash=tx_hash_str,
            from_address=from_addr,
            to_address=to_addr,
            token=token,
            amount_raw=value_raw,
            amount_usd=amount_usd,
            block_number=block_number,
            timestamp=timestamp,
            log_index=int(log.get("logIndex", 0)),
            destination_type=destination_type,
            exchange_name=exchange_name,
            source_type=source_type,
            is_cex_inflow=is_cex_inflow,
            is_cex_outflow=is_cex_outflow,
        )

    async def _scan_block_range(
        self, from_block: int, to_block: int
    ) -> list[WhaleTransferEvent]:
        """Scan a block range for whale transfers."""
        w3 = await self._rpc_manager.get_w3()

        logs = await self._fetch_transfer_logs(w3, from_block, to_block)
        self._total_transfers_scanned += len(logs)

        whale_events: list[WhaleTransferEvent] = []
        process_tasks = [self._process_log(log, token, decimals, w3) for log, token, decimals in logs]

        results = await asyncio.gather(*process_tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logger.debug(f"[PROCESS] Log processing error: {result}")
                continue
            if result is not None:
                whale_events.append(result)

        return whale_events

    async def _run_one_cycle(self) -> None:
        """Run a single poll cycle: fetch latest block, scan new blocks."""
        w3 = await self._rpc_manager.get_w3()

        try:
            latest_block = await w3.eth.block_number
        except Exception as e:
            logger.error(f"[ETH] Latest block fetch fail: {e}. RPC fail, fallback try korchi")
            w3 = await self._rpc_manager.connect()
            latest_block = await w3.eth.block_number

        if latest_block <= self._last_processed_block:
            logger.debug(f"[CYCLE] No new blocks (latest={latest_block}, last={self._last_processed_block})")
            return

        from_block = self._last_processed_block + 1
        to_block = latest_block

        # Cap scan range to avoid huge queries
        max_blocks_per_scan = 10
        if to_block - from_block > max_blocks_per_scan:
            to_block = from_block + max_blocks_per_scan
            logger.info(f"[CYCLE] Capping scan to {max_blocks_per_scan} blocks: {from_block}-{to_block}")

        logger.info(
            f"[CYCLE] Block {from_block}-{to_block} scan holo korchi "
            f"({to_block - from_block + 1} block)"
        )

        whale_events = await self._scan_block_range(from_block, to_block)

        blocks_scanned = to_block - from_block + 1
        logger.info(
            f"[SCAN] Block {from_block}-{to_block} scan holo, "
            f"{self._total_transfers_scanned} transfer dekha gelo total"
        )

        if whale_events:
            logger.warning(
                f"[WHALE] {len(whale_events)}ta whale transfer detect hoise! "
                f"Publishing to Redis..."
            )
            for event in whale_events:
                direction = ""
                if event.is_cex_inflow:
                    direction = f"→ {event.exchange_name} (CEX inflow)"
                elif event.is_cex_outflow:
                    direction = f"← {event.exchange_name} (CEX outflow)"

                logger.warning(
                    f"[WHALE] Whale detect hoise! ${event.amount_usd:,.0f} USD "
                    f"{event.token} cex e gese | "
                    f"{event.from_address[:10]}...→{event.to_address[:10]}... "
                    f"{direction} | tx: {event.tx_hash[:18]}..."
                )

                try:
                    stream_id = await self._publisher.publish(event)
                    self._total_whales_detected += 1
                    logger.info(f"[REDIS] Whale event published: {stream_id}")
                except Exception as e:
                    logger.error(f"[REDIS] Publish fail hoise: {e}")

        self._last_processed_block = to_block

        # Stats log every 10 cycles (approximately 2 min)
        if self._total_whales_detected % 10 == 0 or len(whale_events) > 0:
            stream_len = await self._publisher.get_stream_length()
            logger.info(
                f"[STATS] Total whales detected: {self._total_whales_detected} | "
                f"Total transfers scanned: {self._total_transfers_scanned} | "
                f"Stream length: {stream_len}"
            )

    async def start(self) -> None:
        """Main entry point — initializes and runs the polling loop."""
        await self._initialize()
        self._is_running = True

        logger.info("[START] Whale tracker polling loop shuru holo!")

        try:
            while self._is_running:
                cycle_start = time.monotonic()
                try:
                    await self._run_one_cycle()
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"[ERROR] Cycle e unexpected error: {e}", exc_info=True)
                    await asyncio.sleep(5)

                elapsed = time.monotonic() - cycle_start
                wait_time = max(0.0, POLL_INTERVAL_SECS - elapsed)

                if wait_time > 0:
                    await asyncio.sleep(wait_time)

        except asyncio.CancelledError:
            logger.info("[SHUTDOWN] Whale tracker gracefully shutting down...")
        finally:
            await self._publisher.close()
            logger.info("[SHUTDOWN] Whale tracker thama gelo. Resources released.")

    def stop(self) -> None:
        self._is_running = False


# ─────────────────────────────────────────────────────────────────────────────
# Bootstrap
# ─────────────────────────────────────────────────────────────────────────────

async def main() -> None:
    tracker = WhaleTracker()

    loop = asyncio.get_event_loop()

    def _signal_handler() -> None:
        logger.info("[SIGNAL] Shutdown signal received")
        tracker.stop()

    import signal
    loop.add_signal_handler(signal.SIGTERM, _signal_handler)
    loop.add_signal_handler(signal.SIGINT, _signal_handler)

    await tracker.start()


if __name__ == "__main__":
    asyncio.run(main())
