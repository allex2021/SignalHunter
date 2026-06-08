"""
CSA SEC EDGAR Parser - Module 2
Parses SEC EDGAR RSS Atom feeds and individual XML filings.
Handles Form 4 (insider trades), Schedule 13D, and Schedule 13G.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional
from xml.etree import ElementTree as ET

import aiohttp
import feedparser  # type: ignore[import]

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger("edgar-parser")

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

SEC_BASE_URL = "https://www.sec.gov"
SEC_ATOM_BASE = f"{SEC_BASE_URL}/cgi-bin/browse-edgar"

EDGAR_RSS_FEEDS = {
    "form4": f"{SEC_ATOM_BASE}?action=getcurrent&type=4&dateb=&owner=include&count=40&search_text=&output=atom",
    "sc13d": f"{SEC_ATOM_BASE}?action=getcurrent&type=SC+13D&dateb=&owner=include&count=40&search_text=&output=atom",
    "sc13g": f"{SEC_ATOM_BASE}?action=getcurrent&type=SC+13G&dateb=&owner=include&count=40&search_text=&output=atom",
}

# SEC EDGAR XML namespaces
EDGAR_NS = {
    "edgar": "https://www.sec.gov/Archives/edgar/data/",
    "atom": "http://www.w3.org/2005/Atom",
    "xbrl": "http://www.xbrl.org/2003/instance",
}

# Form 4 XML namespace
FORM4_NS = "http://www.sec.gov/Archives/edgar/data/"

# Known crypto-related ticker symbols for enhanced labeling
CRYPTO_TICKERS = {
    "COIN", "MSTR", "RIOT", "MARA", "CLSK", "HIVE", "HUT", "BTBT",
    "CIFR", "CORZ", "WULF", "IREN", "BITF", "SDIG", "BTDR", "HOOD",
    "LCID", "PYPL", "SQ", "SOFI", "DKNG", "AFRM", "UPST",
}

# ─────────────────────────────────────────────────────────────────────────────
# Data Classes
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class SecFiling:
    """Base class for all SEC filings."""
    accession_number: str
    form_type: str
    company_name: str
    cik: str
    filed_at: str  # ISO 8601
    url: str
    ticker: str = ""
    feed_source: str = ""


@dataclass
class Form4Filing(SecFiling):
    """Form 4 — Statement of Changes in Beneficial Ownership (insider trades)."""
    insider_name: str = ""
    insider_title: str = ""
    transaction_type: str = ""   # "P" = Purchase, "S" = Sale, "D" = Disposition
    transaction_date: str = ""
    shares_transacted: float = 0.0
    price_per_share: float = 0.0
    shares_owned_after: float = 0.0
    security_type: str = ""
    transaction_code_label: str = ""


@dataclass
class Schedule13DFiling(SecFiling):
    """Schedule 13D — Acquisition of Beneficial Ownership > 5%."""
    filer_name: str = ""
    percent_owned: float = 0.0
    aggregate_amount: float = 0.0
    purpose_of_transaction: str = ""
    subject_company: str = ""


@dataclass
class Schedule13GFiling(SecFiling):
    """Schedule 13G — Passive beneficial ownership > 5%."""
    filer_name: str = ""
    percent_owned: float = 0.0
    aggregate_amount: float = 0.0
    is_amendment: bool = False
    subject_company: str = ""


# ─────────────────────────────────────────────────────────────────────────────
# Parsing Utilities
# ─────────────────────────────────────────────────────────────────────────────

def _safe_float(value: Any, default: float = 0.0) -> float:
    """Safely convert any value to float."""
    if value is None:
        return default
    try:
        return float(str(value).replace(",", "").strip())
    except (ValueError, TypeError):
        return default


def _safe_text(element: Optional[ET.Element], default: str = "") -> str:
    """Safely extract text from an XML element."""
    if element is None:
        return default
    return (element.text or "").strip()


def _parse_accession_number(url_or_text: str) -> str:
    """Extract SEC accession number from filing URL or text."""
    # Accession numbers look like: 0001234567-24-000001
    import re
    match = re.search(r"(\d{10}-\d{2}-\d{6})", url_or_text)
    if match:
        return match.group(1)
    # Also handle without dashes
    match2 = re.search(r"(\d{18})", url_or_text)
    if match2:
        raw = match2.group(1)
        return f"{raw[:10]}-{raw[10:12]}-{raw[12:]}"
    return url_or_text.split("/")[-1].replace("index.htm", "").strip("/")


def _extract_cik_from_url(url: str) -> str:
    """Extract CIK from EDGAR URL like /Archives/edgar/data/CIK/..."""
    parts = url.split("/")
    try:
        data_idx = parts.index("data")
        return parts[data_idx + 1]
    except (ValueError, IndexError):
        return ""


def _normalize_date(date_str: str) -> str:
    """Normalize various date formats to ISO 8601."""
    if not date_str:
        return datetime.now(timezone.utc).isoformat()

    formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%Y%m%d",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str.strip()[:19], fmt)
            return dt.replace(tzinfo=timezone.utc).isoformat()
        except ValueError:
            continue

    return date_str  # Return as-is if unable to parse


TRANSACTION_CODE_LABELS = {
    "P": "Open market purchase",
    "S": "Open market sale",
    "A": "Grant/award",
    "D": "Sale to issuer",
    "F": "Tax withholding",
    "I": "Discretionary transaction",
    "M": "Exercise of derivative",
    "C": "Conversion",
    "E": "Expiration of short derivative",
    "H": "Expiration of long derivative",
    "O": "Exercise of out-of-money derivative",
    "X": "Exercise of in-money derivative",
    "G": "Gift",
    "L": "Small acquisition (< $10k)",
    "W": "Acquisition by will/inheritance",
    "Z": "Deposit/withdrawal from voting trust",
    "J": "Other",
    "K": "Equity swap",
    "U": "Tender of shares in merger",
}


# ─────────────────────────────────────────────────────────────────────────────
# RSS Feed Parser
# ─────────────────────────────────────────────────────────────────────────────

def parse_atom_feed(feed_text: str, feed_type: str) -> list[SecFiling]:
    """
    Parse an EDGAR Atom RSS feed into a list of SecFiling objects.
    feed_type: 'form4', 'sc13d', 'sc13g'
    """
    if not feed_text or not feed_text.strip():
        logger.warning(f"[EDGAR_PARSER] Empty feed text for {feed_type}")
        return []

    try:
        parsed = feedparser.parse(feed_text)
    except Exception as e:
        logger.error(f"[EDGAR_PARSER] feedparser error on {feed_type}: {e}")
        return []

    filings: list[SecFiling] = []

    for entry in parsed.get("entries", []):
        try:
            filing = _parse_feed_entry(entry, feed_type)
            if filing:
                filings.append(filing)
        except Exception as e:
            logger.debug(f"[EDGAR_PARSER] Entry parse error ({feed_type}): {e}")
            continue

    logger.debug(f"[EDGAR_PARSER] Parsed {len(filings)} filings from {feed_type} feed")
    return filings


def _parse_feed_entry(entry: dict, feed_type: str) -> Optional[SecFiling]:
    """Parse a single Atom feed entry."""
    title = entry.get("title", "")
    link = entry.get("link", "") or entry.get("id", "")
    updated = entry.get("updated", "") or entry.get("published", "")
    summary = entry.get("summary", "")

    if not link:
        return None

    # Extract accession number from link
    accession_number = _parse_accession_number(link)

    # Extract CIK
    cik = _extract_cik_from_url(link)

    # Normalize date
    filed_at = _normalize_date(updated)

    # Extract company name from title: usually "formtype - company (CIK)"
    company_name = ""
    ticker = ""

    import re
    # Title formats: "4 - Company Name (CIK) (Period: YYYY-MM-DD)"
    # or "SC 13D - Company Name (CIK)"
    title_match = re.match(r"^[0-9A-Z\s/]+\s+-\s+(.+?)\s+\(\d+\)", title)
    if title_match:
        company_name = title_match.group(1).strip()

    # Try to extract ticker from summary
    ticker_match = re.search(r"\(([A-Z]{1,5})\)", summary)
    if ticker_match:
        potential_ticker = ticker_match.group(1)
        # Basic sanity: 1-5 uppercase letters
        if 1 <= len(potential_ticker) <= 5:
            ticker = potential_ticker

    if feed_type == "form4":
        return Form4Filing(
            accession_number=accession_number,
            form_type="4",
            company_name=company_name or title,
            cik=cik,
            filed_at=filed_at,
            url=link,
            ticker=ticker,
            feed_source=feed_type,
        )
    elif feed_type == "sc13d":
        return Schedule13DFiling(
            accession_number=accession_number,
            form_type="SC 13D",
            company_name=company_name or title,
            cik=cik,
            filed_at=filed_at,
            url=link,
            ticker=ticker,
            feed_source=feed_type,
            subject_company=company_name,
        )
    elif feed_type == "sc13g":
        return Schedule13GFiling(
            accession_number=accession_number,
            form_type="SC 13G",
            company_name=company_name or title,
            cik=cik,
            filed_at=filed_at,
            url=link,
            ticker=ticker,
            feed_source=feed_type,
            subject_company=company_name,
        )
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Form 4 XML Detail Parser
# ─────────────────────────────────────────────────────────────────────────────

async def enrich_form4_filing(
    filing: Form4Filing,
    session: aiohttp.ClientSession,
) -> Form4Filing:
    """
    Fetch and parse the actual Form 4 XML document to get transaction details.
    Enriches an existing Form4Filing with transaction data.
    """
    # Build the XML document URL from accession number
    # Accession: 0001234567-24-000001 → 000123456724000001
    accession_clean = filing.accession_number.replace("-", "")
    xml_url = (
        f"{SEC_BASE_URL}/Archives/edgar/data/{filing.cik}/"
        f"{accession_clean}/{accession_clean}.xml"
    )

    # Fallback: try the index page
    index_url = (
        f"{SEC_BASE_URL}/Archives/edgar/data/{filing.cik}/"
        f"{accession_clean}/0{accession_clean}-index.htm"
    )

    xml_text = await _fetch_url_text(session, xml_url)
    if not xml_text:
        # Try index page to find actual XML file
        index_text = await _fetch_url_text(session, index_url)
        if index_text:
            xml_url = _find_form4_xml_from_index(index_text, filing.cik, accession_clean)
            if xml_url:
                xml_text = await _fetch_url_text(session, xml_url)

    if not xml_text:
        logger.debug(f"[EDGAR_PARSER] Could not fetch Form 4 XML for {filing.accession_number}")
        return filing

    return _parse_form4_xml(xml_text, filing)


async def _fetch_url_text(
    session: aiohttp.ClientSession,
    url: str,
) -> Optional[str]:
    """Fetch URL and return text content, or None on failure."""
    try:
        headers = {
            "User-Agent": "CSA-SignalAggregator/1.0 research@cryptosignal.example.com",
            "Accept": "application/xml, text/html, */*",
        }
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 200:
                return await resp.text(errors="replace")
            return None
    except asyncio.TimeoutError:
        logger.debug(f"[EDGAR_PARSER] Timeout fetching {url}")
        return None
    except Exception as e:
        logger.debug(f"[EDGAR_PARSER] Fetch error {url}: {e}")
        return None


def _find_form4_xml_from_index(index_html: str, cik: str, accession_clean: str) -> Optional[str]:
    """Find the Form 4 XML file URL from an EDGAR index page."""
    import re
    # Look for .xml file links in the index
    matches = re.findall(r'href="(/Archives/edgar/data/[^"]+\.xml)"', index_html)
    for match in matches:
        if "form4" in match.lower() or accession_clean[:10] in match:
            return f"{SEC_BASE_URL}{match}"
    # Return first XML found
    if matches:
        return f"{SEC_BASE_URL}{matches[0]}"
    return None


def _parse_form4_xml(xml_text: str, filing: Form4Filing) -> Form4Filing:
    """
    Parse Form 4 XML document to extract transaction details.
    Form 4 XML schema: https://www.sec.gov/info/edgar/edgarfm-vol2-v54.pdf
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        logger.debug(f"[EDGAR_PARSER] Form 4 XML parse error: {e}")
        return filing

    # Remove namespace prefix if present
    def strip_ns(tag: str) -> str:
        return tag.split("}")[-1] if "}" in tag else tag

    def find_text(parent: ET.Element, *tags: str) -> str:
        for tag in tags:
            el = parent.find(f".//{tag}")
            if el is not None and el.text:
                return el.text.strip()
        return ""

    # Reporting owner (insider)
    owner_name_el = root.find(".//reportingOwner/reportingOwnerId/rptOwnerName")
    if owner_name_el is not None and owner_name_el.text:
        filing.insider_name = owner_name_el.text.strip()

    owner_title_el = root.find(".//reportingOwner/reportingOwnerRelationship/officerTitle")
    if owner_title_el is not None and owner_title_el.text:
        filing.insider_title = owner_title_el.text.strip()

    # Non-derivative transactions (direct stock purchases/sales)
    nd_transactions = root.findall(".//nonDerivativeTransaction")
    # Derivative transactions (options, warrants)
    d_transactions = root.findall(".//derivativeTransaction")

    all_transactions = nd_transactions + d_transactions

    if not all_transactions:
        return filing

    # Use the first/largest transaction
    best_transaction = None
    best_shares = 0.0

    for txn in all_transactions:
        # Transaction code
        code_el = txn.find(".//transactionCode")
        code = code_el.text.strip() if (code_el is not None and code_el.text) else ""

        # Shares
        shares_el = txn.find(".//transactionShares/value")
        shares = _safe_float(_safe_text(shares_el))

        if shares > best_shares:
            best_shares = shares
            best_transaction = txn
            filing.transaction_type = code
            filing.transaction_code_label = TRANSACTION_CODE_LABELS.get(code, code)

    if best_transaction is not None:
        txn = best_transaction

        # Transaction date
        date_el = txn.find(".//transactionDate/value")
        filing.transaction_date = _normalize_date(_safe_text(date_el))

        # Shares transacted
        shares_el = txn.find(".//transactionShares/value")
        filing.shares_transacted = _safe_float(_safe_text(shares_el))

        # Price per share
        price_el = txn.find(".//transactionPricePerShare/value")
        if price_el is None:
            price_el = txn.find(".//conversionOrExercisePrice/value")
        filing.price_per_share = _safe_float(_safe_text(price_el))

        # Shares owned after
        owned_el = txn.find(".//sharesOwnedFollowingTransaction/value")
        if owned_el is None:
            owned_el = txn.find(".//underlyingSecurityShares/value")
        filing.shares_owned_after = _safe_float(_safe_text(owned_el))

        # Security type
        sec_title_el = txn.find(".//securityTitle/value")
        filing.security_type = _safe_text(sec_title_el, "Common Stock")

    return filing


# ─────────────────────────────────────────────────────────────────────────────
# 13D / 13G Detail Enrichment
# ─────────────────────────────────────────────────────────────────────────────

async def enrich_13d_filing(
    filing: Schedule13DFiling,
    session: aiohttp.ClientSession,
) -> Schedule13DFiling:
    """Fetch and enrich a 13D filing with ownership details."""
    accession_clean = filing.accession_number.replace("-", "")
    xml_url = (
        f"{SEC_BASE_URL}/Archives/edgar/data/{filing.cik}/"
        f"{accession_clean}/{accession_clean}.xml"
    )

    xml_text = await _fetch_url_text(session, xml_url)
    if not xml_text:
        return filing

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return filing

    # Try to get filer name
    filer_el = root.find(".//filerInfo/filer/companyData/conformed-name")
    if filer_el is not None and filer_el.text:
        filing.filer_name = filer_el.text.strip()

    # Try to get percent owned from cover page
    pct_el = root.find(".//coverPage/percentOfClassRepresented")
    if pct_el is not None:
        filing.percent_owned = _safe_float(_safe_text(pct_el))

    return filing


async def enrich_13g_filing(
    filing: Schedule13GFiling,
    session: aiohttp.ClientSession,
) -> Schedule13GFiling:
    """Fetch and enrich a 13G filing with ownership details."""
    accession_clean = filing.accession_number.replace("-", "")
    xml_url = (
        f"{SEC_BASE_URL}/Archives/edgar/data/{filing.cik}/"
        f"{accession_clean}/{accession_clean}.xml"
    )

    xml_text = await _fetch_url_text(session, xml_url)
    if not xml_text:
        return filing

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return filing

    filer_el = root.find(".//filerInfo/filer/companyData/conformed-name")
    if filer_el is not None and filer_el.text:
        filing.filer_name = filer_el.text.strip()

    # Check if it's an amendment
    amendment_el = root.find(".//headerData/formType")
    if amendment_el is not None and "A" in (amendment_el.text or ""):
        filing.is_amendment = True

    pct_el = root.find(".//coverPage/percentOfClassRepresented")
    if pct_el is not None:
        filing.percent_owned = _safe_float(_safe_text(pct_el))

    return filing


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

async def fetch_and_parse_feed(
    session: aiohttp.ClientSession,
    feed_name: str,
    feed_url: str,
) -> list[SecFiling]:
    """
    Fetch an EDGAR Atom feed and parse it into SecFiling objects.
    Returns empty list on any failure.
    """
    headers = {
        "User-Agent": "CSA-SignalAggregator/1.0 research@cryptosignal.example.com",
        "Accept": "application/atom+xml, application/xml, text/xml, */*",
    }

    try:
        async with session.get(
            feed_url,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=20),
        ) as resp:
            if resp.status != 200:
                logger.warning(
                    f"[EDGAR_PARSER] {feed_name} feed HTTP {resp.status}"
                )
                return []
            text = await resp.text(errors="replace")
    except asyncio.TimeoutError:
        logger.warning(f"[EDGAR_PARSER] {feed_name} feed timeout")
        return []
    except aiohttp.ClientError as e:
        logger.warning(f"[EDGAR_PARSER] {feed_name} feed client error: {e}")
        return []

    filings = parse_atom_feed(text, feed_name)
    logger.info(f"[EDGAR_PARSER] {feed_name}: {len(filings)} filings parsed from feed")
    return filings
