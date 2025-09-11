#!/usr/bin/env python3
"""
Guardian Parser Pack - Missing Person Case Data Extraction and Normalization

This module provides comprehensive PDF text extraction and parsing capabilities for
missing person case data from multiple sources including NamUs, NCMEC, and Charley
Project. It normalizes extracted data into a standardized Guardian schema format
with support for geocoding, validation, and multiple output formats.

Key Features:
    - Multi-source PDF text extraction (NamUs, NCMEC, Charley Project)
    - Robust date and demographic data parsing with fallback mechanisms
    - Optional geocoding with caching for location data
    - Schema validation against guardian_schema.json
    - Multiple output formats (JSONL, CSV)
    - Comprehensive error handling and logging

Dependencies:
    Core: pdfminer.six, PyPDF2, python-dateutil, jsonschema, pandas, geopy
    Optional OCR: pytesseract, pillow (requires Tesseract binary)

Usage:
    python parser_pack.py --inputs file1.pdf file2.pdf --geocode
    python parser_pack.py --inputs *.pdf --jsonl output.jsonl --csv output.csv

Author: Joshua Castillo

"""
import re, os, json, csv, sys, logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

# Quiet the usual suspects:
for name in [
    "pdfminer",          # pdfminer.six
    "pdfminer.pdfinterp",
    "pdfminer.psparser",
    "pdfminer.pdffont",
    "fitz",              # PyMuPDF, if  used it anywhere
    "pymupdf",           # alternate name
    "pdfplumber"         # if  routed through pdfplumber
]:
    logging.getLogger(name).setLevel(logging.ERROR)

# Also: default root logger if any remaining noise
logging.getLogger().setLevel(logging.WARNING)

# Graceful imports
try:
    from pdfminer.high_level import extract_text as pdfminer_extract_text
except Exception:
    pdfminer_extract_text = None

try:
    import PyPDF2
except Exception:
    PyPDF2 = None

try:
    import pytesseract
    from PIL import Image
except Exception:
    pytesseract = None
    Image = None

from dateutil import tz
from dateutil.parser import parse as dt_parse
from dateutil import parser as dtp
from jsonschema import Draft7Validator

# ---------- Configuration Constants ----------

# Path to the Guardian schema validation file
GUARDIAN_SCHEMA_PATH: str = os.path.join(os.path.dirname(__file__), "guardian_schema.json")

# Default timezone for date parsing when no timezone is specified
DEFAULT_TZ: str = "America/New_York"

# ---------- Canonical Key Mappings ----------

CANON_MAP = {
    # demographic mappings
    ('demographic', 'age'): ('demographic', 'age_years'),
    ('demographic', 'eyes'): ('demographic', 'eye_color'),
    ('demographic', 'eyes_color'): ('demographic', 'eye_color'),
    ('demographic', 'hair'): ('demographic', 'hair_color'),
    ('demographic', 'height'): ('demographic', 'height_in'),
    ('demographic', 'weight'): ('demographic', 'weight_lb'),
    # spatial
    ('spatial', 'lat'): ('spatial', 'latitude'),
    ('spatial', 'lon'): ('spatial', 'longitude'),
}

# ---------- Helpers: safe regex ----------

def safe_search(pattern: str, text: str, flags: int = 0) -> Optional[re.Match[str]]:
    """
    Perform a regex search that never throws exceptions.
    
    This function provides a safe wrapper around re.search() that catches
    regex compilation errors and returns None instead of raising exceptions.
    
    Args:
        pattern (str): The regex pattern to search for
        text (str): The text to search in
        flags (int): Optional regex flags (default: 0)
        
    Returns:
        Optional[re.Match[str]]: The match object if found, None otherwise
        
    Example:
        >>> safe_search(r"\d+", "abc123def")
        <re.Match object; span=(3, 6), match='123'>
        >>> safe_search(r"[", "invalid pattern")
        None
    """
    try:
        return re.search(pattern, text, flags)
    except re.error:
        return None

def _canonize_keys(rec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Move values from non-canonical keys to canonical ones without overwriting populated canonical fields.
    
    This function ensures consistent field naming across different sources by mapping
    synonymous field names to canonical versions (e.g., 'gender' -> 'sex').
    
    Args:
        rec (Dict[str, Any]): The record to canonicalize
        
    Returns:
        Dict[str, Any]: Record with canonicalized field names
    """
    for (outer, inner), (c_outer, c_inner) in CANON_MAP.items():
        if outer in rec and inner in rec[outer]:
            v = rec[outer].get(inner)
            if v is not None and str(v).strip() != '':
                rec.setdefault(c_outer, {})
                rec[c_outer].setdefault(c_inner, v)
            rec[outer].pop(inner, None)
    return rec

def _enrich_common_fields(rec: Dict[str, Any], full_text: str) -> Dict[str, Any]:
    """
    Lightweight source-agnostic enrichment pass that pulls common attributes
    (height, weight, hair/eye color, DOB, nicknames, phones, case numbers, etc.)
    from raw PDF text and fills gaps in the parsed record.
    
    This function performs a comprehensive text analysis to extract missing
    demographic and case information that may not have been captured by
    source-specific parsers.
    
    Args:
        rec (Dict[str, Any]): The parsed record to enrich
        full_text (str): The raw text from the PDF
        
    Returns:
        Dict[str, Any]: Enriched record with additional extracted fields
        
    Note:
        Never overwrites a non-empty existing value. Only fills gaps.
    """
    txt = full_text or ''
    norm = ' '.join(txt.split())  # normalize whitespace

    def set_if_missing(cat: str, key: str, value: Any) -> None:
        """Set a value only if the target field is missing or empty."""
        if value is None or str(value).strip() == '':
            return
        rec.setdefault(cat, {})
        if rec[cat].get(key) in (None, ''):
            rec[cat][key] = value

    # Sex/Gender
    m = re.search(r"\b(?:Sex|Gender)\s*[:\-]?\s*(Male|Female)\b", norm, re.I)
    if m: 
        set_if_missing("demographic", "sex", m.group(1).title())

    # Age (years)
    m = re.search(r"\bAge(?:\s+at\s+(?:time\s+of\s+disappearance|missing))?\s*[:\-]?\s*(\d{1,2})\b", norm, re.I)
    if m: 
        set_if_missing("demographic", "age_years", int(m.group(1)))

    # Height (ft/in or inches). Accepts ft/feet/' and in/inches/" (also handles curly ' " if present)
    ft_in = re.search(
        r"\b(\d)\s*(?:ft|feet|['\u2019])\s*([0-9]{1,2})\s*(?:in|inches|[\"\u201D])?\b",
        norm, re.I
    )
    inches_only = re.search(r"\bHeight\s*[:\-]?\s*(\d{2,3})\s*(?:in|inches)\b", norm, re.I)
    if ft_in:
        h = int(ft_in.group(1)) * 12 + int(ft_in.group(2))
        set_if_missing("demographic", "height_in", h)
    elif inches_only:
        set_if_missing("demographic", "height_in", int(inches_only.group(1)))

    # Weight (lb)
    m = re.search(r"\bWeight\s*[:\-]?\s*(\d{2,3})\s*(?:lb|lbs|pounds)\b", norm, re.I)
    if m: 
        set_if_missing("demographic", "weight_lb", int(m.group(1)))

    # Hair color
    m = re.search(
        r"\bHair(?:\s*Color)?\s*[:\-]?\s*([A-Za-z /-]+?)\b(?:Eyes?|Eye|Eye\s*Color|Height|Weight|DOB|Date\b)",
        norm, re.I
    )
    if m: 
        set_if_missing("demographic", "hair_color", m.group(1).strip().title())

    # Eye color
    m = re.search(
        r"\bEyes?(?:\s*Color)?\s*[:\-]?\s*([A-Za-z /-]+?)\b(?:Hair|Height|Weight|DOB|Date\b)",
        norm, re.I
    )
    if m: 
        set_if_missing("demographic", "eye_color", m.group(1).strip().title())

    # DOB (normalize several common formats)
    m = re.search(
        r"\b(?:DOB|Date of Birth)\s*[:\-]?\s*([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
        norm, re.I
    )
    if m:
        dob_raw = m.group(1)
        for fmt in ("%B %d, %Y", "%b %d, %Y", "%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y", "%m-%d-%y"):
            try:
                set_if_missing("demographic", "dob", datetime.strptime(dob_raw, fmt).date().isoformat())
                break
            except Exception:
                pass

    # Missing From (city, state)
    m = re.search(r'\b(?:Missing\s+From|Location)\s*[:\-]?\s*([A-Za-z .-]+?),\s*([A-Z]{2})\b', norm, re.I)
    if m:
        set_if_missing('spatial', 'city', m.group(1).strip().title())
        set_if_missing('spatial', 'state', m.group(2).upper())

    # Date of last contact / Missing since
    m = re.search(r'\b(?:Date of Last Contact|Missing Since|Date Missing)\s*[:\-]?\s*([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4})', norm, re.I)
    if m:
        set_if_missing('temporal', 'last_seen_date', m.group(1))

    # Case numbers
    m = re.search(r'\b(?:Case|NamUs|NCMEC)\s*(?:ID|#|Number)\s*[:\-]?\s*([A-Z0-9-]+)\b', norm, re.I)
    if m: 
        set_if_missing('provenance', 'case_number', m.group(1).strip())

    # AKA / Nicknames
    aka = re.findall(r'\b(?:AKA|Alias|Nicknames?)\s*[:\-]?\s*([A-Za-z0-9 .\'-]+)', norm, re.I)
    if aka:
        rec.setdefault('demographic', {})
        if not rec['demographic'].get('aka'):
            rec['demographic']['aka'] = ' | '.join(sorted(set(x.strip() for x in aka if x.strip())))

    # Agency / phone
    m = re.search(r'\b(?:Investigating Agency|Contact)\s*[:\-]?\s*([A-Za-z0-9 .,&\'-]+)', norm, re.I)
    if m: 
        set_if_missing('provenance', 'agency', m.group(1).strip())
    phone = re.search(r'(\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})', norm)
    if phone: 
        set_if_missing('provenance', 'agency_phone', phone.group(1))

    return _canonize_keys(rec)

def parse_date_to_iso_utc(s: str) -> Optional[str]:
    """
    Parse a date string to ISO 8601 UTC format.
    
    This function provides a fallback date parser that converts various
    date formats to ISO 8601 UTC format for temporal field harmonization.
    
    Args:
        s (str): Date string to parse
        
    Returns:
        Optional[str]: ISO 8601 UTC formatted date string or None if parsing fails
        
    Example:
        >>> parse_date_to_iso_utc("12/25/2023")
        "2023-12-25T00:00:00Z"
    """
    try:
        s = (s or "").strip()
        if not s:
            return None
        # VERY tolerant fallback; rely on your robust parser if available.
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%b %d, %Y", "%B %d, %Y"):
            try:
                dt = datetime.strptime(s, fmt)
                return dt.strftime("%Y-%m-%dT00:00:00Z")
            except ValueError:
                pass
    except Exception:
        pass
    return None

def harmonize_record_fields(rec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize synonymous keys to the schema's canonical names and clean duplicates.
    
    This function ensures consistent field naming across different sources by
    mapping synonymous field names to canonical versions and cleaning up
    duplicate or inconsistent data structures. Always converts to canonical keys.
    
    Args:
        rec (Dict[str, Any]): The record to harmonize
        
    Returns:
        Dict[str, Any]: Harmonized record with canonical field names
        
    Note:
        This function modifies the input record in place and also returns it
        for convenience.
    """
    if not rec:
        return rec

    demo = rec.setdefault("demographic", {})
    temp = rec.setdefault("temporal", {})
    spat = rec.setdefault("spatial", {})
    meta = rec.setdefault("case", {})
    narr = rec.setdefault("narrative", {})

    # === bridge writer-style names to canonical names used by CSV ===
    # name
    if "name" in demo and not get_nested(rec, "name.full"):
        rec.setdefault("name", {})["full"] = demo["name"]

    # spatial: last_seen_* → city/state/country
    if "last_seen_city" in spat and "city" not in spat:
        spat["city"] = spat.pop("last_seen_city")
    if "last_seen_state" in spat and "state" not in spat:
        spat["state"] = spat.pop("last_seen_state")
    if "last_seen_country" in spat and "country" not in spat:
        spat["country"] = spat.pop("last_seen_country")

    # temporal: reported_missing_ts → reported_ts
    if "reported_missing_ts" in temp and "reported_ts" not in temp:
        temp["reported_ts"] = temp.pop("reported_missing_ts")

    # case/meta: lift top-level ids and statuses
    # (parsers set case_id at top-level; outcome.case_status)
    if "case_id" in rec and "case_id" not in meta:
        meta["case_id"] = rec.pop("case_id")
    if "outcome" in rec:
        cs = rec["outcome"].get("case_status")
        if cs and "status" not in meta:
            meta["status"] = cs
    # provenance → case.source
    srcs = get_nested(rec, "provenance.sources") or []
    if srcs and "source" not in meta:
        meta["source"] = srcs[0]

    # ---- demographic synonyms (force canonical) ----
    # sex -> gender (always)
    if "sex" in demo:
        val = demo.get("gender") or normalize_gender(demo.get("sex"))
        if val:
            demo["gender"] = val
        demo.pop("sex", None)

    # weight_lb -> weight_lbs (always)
    if "weight_lb" in demo:
        if "weight_lbs" not in demo and demo["weight_lb"] not in (None, ""):
            demo["weight_lbs"] = demo["weight_lb"]
        demo.pop("weight_lb", None)

    # height_inches -> height_in (always)
    if "height_inches" in demo:
        if "height_in" not in demo and demo["height_inches"] not in (None, ""):
            demo["height_in"] = demo["height_inches"]
        demo.pop("height_inches", None)

    # ---- temporal synonyms ----
    if "last_seen_date" in temp:
        val = (temp.pop("last_seen_date") or "").strip()
        ts = parse_date_to_iso_utc(val)
        if ts:
            temp["last_seen_ts"] = ts

    if "reported_date" in temp:
        val = (temp.pop("reported_date") or "").strip()
        ts = parse_date_to_iso_utc(val)
        if ts:
            temp["reported_ts"] = ts

    # ---- spatial normalization ----
    # Accept either last_seen_city/state or city/state; keep both canonical keys available
    city = spat.get("last_seen_city") or spat.get("city")
    state = spat.get("last_seen_state") or spat.get("state")
    country = spat.get("last_seen_country") or spat.get("country")
    if city:  spat["last_seen_city"]  = city
    if state: spat["last_seen_state"] = state
    if country: spat["last_seen_country"] = country

    # lat/lon synonyms
    if "lat" in spat and "last_seen_lat" not in spat:
        spat["last_seen_lat"] = spat.pop("lat")
    if "lon" in spat and "last_seen_lon" not in spat:
        spat["last_seen_lon"] = spat.pop("lon")
    if "lng" in spat and "last_seen_lon" not in spat:
        spat["last_seen_lon"] = spat.pop("lng")

    # ---- case/meta synonyms ----
    if "status" in rec and "status" not in meta:
        meta["status"] = rec.pop("status")
    if "source" in rec and "source" not in meta:
        meta["source"] = rec.pop("source")
    if "case_id" in rec and "case_id" not in meta:
        meta["case_id"] = rec.pop("case_id")

    # If source lives under provenance.sources, surface first value to case.source
    prov_sources = rec.get("provenance", {}).get("sources")
    if isinstance(prov_sources, list) and prov_sources and not meta.get("source"):
        meta["source"] = prov_sources[0]

    # If a simple name was stored in demographic.name, surface to name.full
    if rec.get("demographic", {}).get("name"):
        rec.setdefault("name", {})
        if not rec["name"].get("full"):
            rec["name"]["full"] = rec["demographic"]["name"]

    # Move narrative_osint.incident_summary into narrative.incident_summary if present
    if rec.get("narrative_osint", {}).get("incident_summary"):
        if not narr.get("incident_summary"):
            narr["incident_summary"] = rec["narrative_osint"]["incident_summary"]

    # ---- narrative normalization ----
    # prefer narrative.incident_summary; if empty, pull from narrative_osint.incident_summary
    osint = rec.get("narrative_osint", {})
    if not narr.get("incident_summary") and osint.get("incident_summary"):
        narr["incident_summary"] = osint.get("incident_summary")

    # If incident_summary is list, coalesce
    if isinstance(narr.get("incident_summary"), list):
        narr["incident_summary"] = " ".join([str(x) for x in narr["incident_summary"] if x])

    # ---- de-dupe list-y fields ----
    for path in [("demographic", "aka"), ("case", "categories")]:
        d, key = path
        if isinstance(rec.get(d, {}).get(key), list):
            rec[d][key] = sorted(set(x for x in rec[d][key] if x))

    return rec

# ---------- Hardened Extractors ----------

# Regex patterns for date extraction
MONTH: str = r"(January|February|March|April|May|June|July|August|September|October|November|December)"
MDY: str = rf"{MONTH}\s+\d{{1,2}},\s+\d{{4}}"                 # e.g., September 8, 2025
SLASH: str = r"\b\d{1,2}/\d{1,2}/\d{2,4}\b"                   # e.g., 02/07/1977

def parse_last_seen_ts(text: str) -> Optional[str]:
    """
    Extract and normalize the last seen timestamp from text.
    
    This function searches for various date patterns commonly found in missing
    person case documents and converts them to ISO 8601 date format (YYYY-MM-DD).
    
    Args:
        text (str): The text to search for date patterns
        
    Returns:
        Optional[str]: ISO 8601 formatted date string (YYYY-MM-DD) or None if not found
        
    Supported Patterns:
        - NCMEC: "Missing Since: September 8, 2025"
        - Charley: "Missing Since 02/07/1977" (with line breaks)
        - NamUs: "Date Last Seen: 2025-09-08"
        - Generic: "Last seen on Feb 7, 1977"
        
    Example:
        >>> parse_last_seen_ts("Missing Since: September 8, 2025")
        "2025-09-08"
        >>> parse_last_seen_ts("Missing Since 02/07/1977")
        "1977-02-07"
    """
    # Normalize line endings
    t = text.replace("\r", "")
    
    # NCMEC poster pattern: "Missing Since: September 8, 2025"
    m = re.search(rf"Missing Since:\s*({MDY})", t, re.I)
    if not m:
        # Charley Project pattern: "Missing Since" on one line, date on next
        m = re.search(rf"Missing Since\s*:?\s*(?:\n|\r\n|\s)*({SLASH}|{MDY})", t, re.I)
    if not m:
        # NamUs often: "Date last seen" variants
        m = re.search(rf"(Date\s+Last\s+Seen|Missing\s+Date)\s*:?\s*({SLASH}|{MDY})", t, re.I)
    if not m:
        # Generic "Last seen" patterns
        m = re.search(rf"Last seen[^0-9A-Za-z]{{0,5}}({MDY}|{SLASH})", t, re.I)
    
    if m:
        date_str = m.group(1)
        try:
            # Try multiple date formats in order of preference
            for fmt in ("%B %d, %Y", "%b %d, %Y", "%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y", "%m-%d-%y"):
                try:
                    dt = datetime.strptime(date_str, fmt)
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    continue
            # Fallback to dateutil parser for complex formats
            dt = dtp.parse(date_str, dayfirst=False, fuzzy=True)
            return dt.date().isoformat()
        except Exception:
            pass
    return None

def parse_gender(text: str) -> Optional[str]:
    """
    Extract and normalize gender information from text.
    
    This function searches for gender indicators in various formats commonly
    found in missing person case documents and normalizes them to standard values.
    
    Args:
        text (str): The text to search for gender patterns
        
    Returns:
        Optional[str]: Normalized gender string ("male", "female") or None if not found
        
    Supported Patterns:
        - Charley Project: "Sex: Male" or "Sex Female" (with line breaks)
        - NCMEC: Gender near "Age Now" field
        - Standalone: "Male" or "Female" tokens
        
    Example:
        >>> parse_gender("Sex: Male")
        "male"
        >>> parse_gender("Age Now: 25 Female")
        "female"
    """
    # Normalize line endings
    t = text.replace("\r", "")
    
    # Charley Project: "Sex" header with value on same or next line
    m = re.search(r"^\s*Sex\s*[:\n]\s*(Male|Female)\b", t, re.I | re.M)
    if not m:
        # NCMEC posters place gender near "Age Now"
        m = re.search(r"\b(Age\s*Now\s*:\s*\d+.*?\b)?\b(Male|Female)\b", t, re.I | re.S)
    
    if m:
        # Extract the gender value (group 2 if available, otherwise group 1)
        g = m.group(2) if m.lastindex and m.lastindex >= 2 else m.group(1)
        g = g.strip().lower()
        
        # Normalize to standard values
        if g.startswith("male"):   return "male"
        if g.startswith("female"): return "female"
    
    # Final heuristic: look for standalone tokens (rare but helps)
    if re.search(r"\bFemale\b", t, re.I): return "female"
    if re.search(r"\bMale\b", t, re.I):   return "male"
    
    return None

def backfill(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Safety backfill pass to catch anything per-source extractors missed.
    
    This function performs a final pass over all parsed records to fill in any
    missing critical fields (last_seen_ts, gender) using the hardened extractors
    on the stored raw text. This ensures maximum data extraction coverage.
    
    Args:
        records (List[Dict[str, Any]]): List of parsed case records
        
    Returns:
        List[Dict[str, Any]]: Updated records with backfilled data
        
    Note:
        This function modifies the input records in place and also returns them
        for convenience.
        
    Example:
        >>> records = [{"_fulltext": "Missing Since: Jan 1, 2020", "temporal": {}}]
        >>> backfill(records)
        [{"_fulltext": "...", "temporal": {"last_seen_ts": "2020-01-01"}}]
    """
    for r in records:
        # Get the stored raw text for re-parsing
        t = r.get("_fulltext", "")
        
        # Backfill missing last_seen_ts
        if not r.get("temporal", {}).get("last_seen_ts"):
            ts = parse_last_seen_ts(t)
            r.setdefault("temporal", {})["last_seen_ts"] = ts or ""
        
        # Backfill missing gender
        if not r.get("demographic", {}).get("gender"):
            g = parse_gender(t)
            if g:
                r.setdefault("demographic", {})["gender"] = g
    
    return records

# ---------- Helpers: units & normalization ----------

def to_inches(height_text: str) -> Optional[float]:
    """
    Convert height strings to inches.
    Examples:
      "5' 8\"" -> 68
      "5’8”"   -> 68
      "68 in"  -> 68
      "5'8\" - 5'10\"" -> midpoint (69)
    """
    if not height_text:
        return None
    
    s = height_text.strip()
    
    # Handle ranges like "5'8" - 5'10"" (returns midpoint)
    m = re.findall(r"(\d)\s*['']\s*(\d{1,2})", s)
    if len(m) >= 2:
        vals = []
        for ft, inch in m[:2]:
            vals.append(int(ft) * 12 + int(inch))
        return sum(vals) / len(vals)
    
    # Single feet-inches format (e.g., "5'8\"")
    m = safe_search(r"(\d)\s*['']\s*(\d{1,2})", s)
    if m:
        return int(m.group(1)) * 12 + int(m.group(2))
    
    # Inches explicitly stated (e.g., "68 in", "68 inches")
    m = safe_search(r"(\d{2,3})\s*(?:in|inches)\b", s, re.I)
    if m:
        return float(m.group(1))
    
    return None

def to_pounds(weight_text: str) -> Optional[float]:
    """
    Convert weight strings to pounds; handle ranges.
      "130 - 150 lbs" -> 140
      "100 pounds"    -> 100
    """
    if not weight_text:
        return None
    s = weight_text.strip()
    m = safe_search(r"(\d{2,3})\s*[-–]\s*(\d{2,3})\s*(?:lb|lbs|pounds)\b", s, re.I)
    if m:
        a, b = float(m.group(1)), float(m.group(2))
        return (a + b) / 2.0
    m = safe_search(r"(\d{2,3})\s*(?:lb|lbs|pounds)\b", s, re.I)
    if m:
        return float(m.group(1))
    return None

def to_iso8601(date_text: str, timezone: str = DEFAULT_TZ) -> Optional[str]:
    """
    Parse a variety of date formats to ISO 8601 with timezone.
    If time not present, set to 00:00 in provided timezone.
    """
    if not date_text:
        return None
    try:
        dt = dt_parse(date_text, fuzzy=True, dayfirst=False, yearfirst=False)
        # If no tzinfo, apply DEFAULT_TZ
        if not dt.tzinfo:
            tzinfo = tz.gettz(timezone)
            dt = dt.replace(tzinfo=tzinfo)
        return dt.isoformat()
    except Exception:
        return None

def clamp_lat(lat: Optional[float]) -> Optional[float]:
    if lat is None: return None
    return max(-90.0, min(90.0, float(lat)))

def clamp_lon(lon: Optional[float]) -> Optional[float]:
    if lon is None: return None
    return max(-180.0, min(180.0, float(lon)))

def extract_coords(text: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Extract first lat,lon found like: 37.5007006,-77.5391672
    """
    if not text: return (None, None)
    m = safe_search(r"(-?\d{1,2}\.\d+),\s*(-?\d{1,3}\.\d+)", text)
    if m:
        return clamp_lat(float(m.group(1))), clamp_lon(float(m.group(2)))
    return (None, None)

def normalize_gender(s: str) -> Optional[str]:
    if not s: return None
    s = s.strip().lower()
    if s.startswith("m"): return "male"
    if s.startswith("f"): return "female"
    return None

# ---------- PDF text extraction ----------

def extract_text(pdf_path: str) -> str:
    # Try pdfminer
    if pdfminer_extract_text:
        try:
            return pdfminer_extract_text(pdf_path) or ""
        except Exception:
            pass
    # Try PyPDF2
    if PyPDF2:
        try:
            text = ""
            with open(pdf_path, "rb") as f:
                r = PyPDF2.PdfReader(f)
                for p in r.pages:
                    try:
                        text += p.extract_text() or ""
                    except Exception:
                        continue
            if text.strip():
                return text
        except Exception:
            pass
    # Try OCR (very slow; needs tesseract installed)
    if pytesseract and Image:
        try:
            img = Image.open(pdf_path)
            return pytesseract.image_to_string(img)
        except Exception:
            pass
    return ""

# ---------- extract helpers ----------

DATE_PATTERNS = [
    r"[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}",    # September 8, 2025  | Sep 8 2025
    r"\d{1,2}[/-]\d{1,2}[/-]\d{2,4}",       # 09/08/2025 or 9-8-25
    r"\d{4}[/-]\d{1,2}[/-]\d{1,2}",         # 2025-09-08
]

def find_date_near(text: str, label_regex: str, window: int = 160) -> Optional[str]:
    """
    Find a date near a label. Works even if the value is on next line.
    Returns ISO8601 string or None.
    """
    lab = re.search(label_regex, text, flags=re.I)
    if not lab:
        return None
    # take a window of text after the label and search common date shapes
    start = lab.end()
    chunk = text[start:start+window]
    for dp in DATE_PATTERNS:
        m = re.search(dp, chunk)
        if m:
            iso = to_iso8601(m.group(0))
            if iso:
                return iso
    return None

def grab_after(text: str, label_regex: str, window: int = 160) -> Optional[str]:
    """
    Return a trimmed snippet after a label. Useful for short fields like gender.
    """
    m = re.search(label_regex, text, flags=re.I)
    if not m:
        return None
    snippet = text[m.end(): m.end()+window]
    # collapse whitespace; stop at line break.
    snippet = re.sub(r"\s+", " ", snippet).strip()
    # return up to first punctuation/newline-y break
    snippet = re.split(r"[.;•|\n\r]", snippet)[0].strip()
    return snippet or None

# ---------- Parsers for three layouts ----------

def parse_namus(text: str, case_id: str) -> Dict[str, Any]:
    """
    Targets NamUs form-like PDFs:
      - "Date of Last Contact"
      - "Last Known Location"
      - "Biological Sex"
      - "Height" like "5' 8\" - 5' 10\" (68 - 70 Inches)"
      - "Weight" like "130 - 150 lbs"
      - Optional Google Maps link with lat,lon
    """
    data = {
        "case_id": case_id,
        "demographic": {},
        "spatial": {},
        "temporal": {"timezone": "America/New_York"},
        "outcome": {"case_status": "ongoing"},
        "narrative_osint": {"incident_summary": ""},
        "provenance": {"sources": ["NamUs"], "original_fields": {}}
    }

    # Name fields (best-effort)
    m = safe_search(r"Legal\s+First\s+Name\s*([^\r\n]+)\s+Middle\s+Name\s*([^\r\n]+)\s+Legal\s+Last\s+Name\s*([^\r\n]+)", text, re.S)
    if m:
        name = " ".join([m.group(1).strip(), m.group(2).strip(), m.group(3).strip()]).replace("--","").strip()
        data["demographic"]["name"] = re.sub(r"\s+", " ", name).strip()

        # Sex (Biological Sex or Sex)
    m = re.search(r"(?:Biological\s+Sex|Sex)\s*[:\-]?\s*(Male|Female)\b", text, re.I)
    if m:
        data["demographic"]["gender"] = normalize_gender(m.group(1))


    # Age
    m = safe_search(r"Missing\s+Age[:\s]*([0-9]{1,2})", text, re.I)
    if m:
        try:
            data["demographic"]["age_years"] = float(m.group(1))
        except Exception:
            pass

    # Height (capture line after label)
    m = safe_search(r"Height[:\s]*([^\r\n]+)", text, re.I)
    if m:
        h_in = to_inches(m.group(1))
        if h_in is not None:
            data["demographic"]["height_in"] = h_in

    # Weight
    m = safe_search(r"Weight[:\s]*([^\r\n]+)", text, re.I)
    if m:
        w_lbs = to_pounds(m.group(1))
        if w_lbs is not None:
            data["demographic"]["weight_lbs"] = w_lbs

    # Race/Ethnicity
    m = safe_search(r"Race\s*/\s*Ethnicity[:\s]*([^\r\n]+)", text, re.I)
    if m:
        data["demographic"]["race_ethnicity"] = re.sub(r"\s+", " ", m.group(1)).strip(" ,")

    # Date of Last Contact -> last_seen_ts
    m = re.search(r"Date\s+(?:of\s+)?Last\s+Contact\s*[:\-]?\s*([A-Za-z0-9 ,/\-]{6,40})", text, re.I)
    if m:
        iso = to_iso8601(m.group(1))
        if iso:
            data["temporal"]["last_seen_ts"] = iso

    m = safe_search(r"NamUs\s+Case\s+Created[:\s]*([^\r\n]+)", text, re.I)
    if m:
        iso = to_iso8601(m.group(1))
        if iso:
            data["temporal"]["reported_missing_ts"] = iso

    # Location (free-text line after "Last Known Location ... Location:")
    m = safe_search(r"Last\s+Known\s+Location[\s\S]*?Location[:\s]*([^\r\n]+)", text, re.I)
    if m:
        loc = re.sub(r"\s+", " ", m.group(1)).strip()
        data["spatial"]["last_seen_location"] = loc
        parts = [p.strip() for p in re.split(r",", loc)]
        if len(parts) >= 2:
            data["spatial"]["last_seen_city"] = parts[0]
            data["spatial"]["last_seen_state"] = parts[-1].split()[0]

    # Map coords
    lat, lon = extract_coords(text)
    if lat is not None and lon is not None:
        data["spatial"]["last_seen_lat"] = lat
        data["spatial"]["last_seen_lon"] = lon
    else:
        # Keep placeholders; geocoder may fill these later
        data["spatial"]["last_seen_lat"] = 0.0
        data["spatial"]["last_seen_lon"] = 0.0

    # Circumstances of Disappearance (capture block until next section header)
    # Works on NamUs pages where the heading appears exactly as shown in file.
    m = safe_search(
        r"(?is)Circumstances\s+of\s+Disappearance\s*([\s\S]*?)(?:\n\s*(?:Physical\s+Description|Clothing\s+and\s+Accessories|ADDITIONAL\s+CASE\s+INFO|Transportation|CASE\s+INFORMATION)\b)"
        , text
    )
    if m:
        desc = re.sub(r"\s+", " ", m.group(1)).strip(" :\u00A0")
        if desc:
            data["narrative_osint"]["incident_summary"] = desc

    return data

def parse_ncmec(text: str, case_id: str) -> Dict[str, Any]:
    """
    Targets NCMEC poster:
      - Name in caps
      - "Missing Since: <date>"
      - City, State
      - Age Now, Sex
      - Short clothing/feature description
    """
    data = {
        "case_id": case_id,
        "demographic": {},
        "spatial": {},
        "temporal": {"timezone": "America/New_York"},
        "outcome": {"case_status": "ongoing"},
        "narrative_osint": {"incident_summary": ""},
        "provenance": {"sources": ["NCMEC"], "original_fields": {}}
    }

    # Name (first big line in caps before "Missing Since")
    m = safe_search(r"\n\s*([A-Z][A-Z\s'\-]+)\n\s*Missing Since", text)
    if m:
        data["demographic"]["name"] = " ".join(m.group(1).title().split())

        # Missing Since -> last_seen_ts
    m = re.search(r"Missing\s+Since\s*[:\-]?\s*([A-Za-z0-9 ,/\-]{6,40})", text, re.I)
    if m:
        iso = to_iso8601(m.group(1))
        if iso:
            data["temporal"]["last_seen_ts"] = iso

    # City, State
    city_state = None
    ms = re.search(r"Missing\s+Since\s*:?\s*", text, re.I)
    if ms:
        tail = text[ms.end(): ms.end()+250]
        mcs = re.search(r"\b([A-Za-z .'\-]+),\s*([A-Z]{2})\b", tail)
        if mcs:
            city_state = (mcs.group(1).strip(), mcs.group(2).strip())
    if city_state:
        city, state = city_state
        data["spatial"]["last_seen_location"] = f"{city}, {state}"
        data["spatial"]["last_seen_city"] = city
        data["spatial"]["last_seen_state"] = state


    # Age Now
    m = safe_search(r"Age\s*Now\s*:\s*(\d{1,2})", text, re.I)
    if m:
        data["demographic"]["age_years"] = float(m.group(1))

    # Sex
    m = re.search(r"(?:Sex\s*[:\-]?\s*)?(Female|Male)\b", text, re.I)
    if m:
        data["demographic"]["gender"] = normalize_gender(m.group(1))

    # Short description (clothing / features)
    m = safe_search(r"(?i)(?:last\s+seen\s+wearing|features?)[:\s]*([A-Z0-9 ,.'\-\(\)]+)", text)
    if m:
        desc = m.group(1).strip()
        data["narrative_osint"]["incident_summary"] = desc

    # Universal date fallback
    if "last_seen_ts" not in data["temporal"]:
        # look for a date within 120 chars of key phrases
        kwords = r"(Missing Since|Last Seen|Date of Last Contact|Disappearance)"
        # search windows: label … up to 120 chars … date token
        m = re.search(kwords + r".{0,120}?([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4})", text, re.I | re.S)
        if not m:
            # try date token then label after
            m = re.search(r"([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4}).{0,120}?" + kwords, text, re.I | re.S)
        if m:
            iso = to_iso8601(m.group(1))
            if iso:
                data["temporal"]["last_seen_ts"] = iso

    # Universal date fallback
    if "last_seen_ts" not in data["temporal"]:
        # look for a date within 120 chars of key phrases
        kwords = r"(Missing Since|Last Seen|Date of Last Contact|Disappearance)"
        # search windows: label … up to 120 chars … date token
        m = re.search(kwords + r".{0,120}?([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4})", text, re.I | re.S)
        if not m:
            # try date token then label after
            m = re.search(r"([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4}).{0,120}?" + kwords, text, re.I | re.S)
        if m:
            iso = to_iso8601(m.group(1))
            if iso:
                data["temporal"]["last_seen_ts"] = iso

    # Required lat/lon placeholders (NCMEC posters don't include coords)
    data["spatial"]["last_seen_lat"] = 0.0
    data["spatial"]["last_seen_lon"] = 0.0

    return data

def parse_charley(text: str, case_id: str) -> Dict[str, Any]:
    """
    Targets Charley Project narrative pages:
      - "Missing Since"
      - "Missing From"
      - "Sex", "Race"
      - "Height and Weight"
      - "Details of Disappearance" (long narrative)
    """
    data = {
        "case_id": case_id,
        "demographic": {},
        "spatial": {},
        "temporal": {"timezone": "America/New_York"},
        "outcome": {"case_status": "ongoing"},
        "narrative_osint": {"incident_summary": ""},
        "provenance": {"sources": ["CharleyProject"], "original_fields": {}}
    }

    # Name (title-like pattern, 2-4 words capitalized)
    m = safe_search(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\s*\n", text)
    if m:
        data["demographic"]["name"] = m.group(1).strip()

       # Missing Since (label may be on its own line, value on the next)
    m = re.search(r"Missing\s+Since(?:\s*[:\-])?\s*(?:\n|\r\n|\s)*([A-Za-z0-9 ,/\-]{6,40})", text, re.I)
    if m:
        iso = to_iso8601(m.group(1))
        if iso:
            data["temporal"]["last_seen_ts"] = iso


    # Missing From
    m = safe_search(r"Missing\s+From\s*[:\-]?\s*(?:\n|\r\n|\s)*([A-Za-z .'\-]+),\s*([A-Za-z .'\-]+)", text, re.I)
    if m:
        city, state = m.group(1).strip(), m.group(2).strip()
        data["spatial"]["last_seen_location"] = f"{city}, {state}"
        data["spatial"]["last_seen_city"] = city
        data["spatial"]["last_seen_state"] = state

    # Sex
    m = re.search(r"Sex\s*[:\-]?\s*(?:\n|\r\n|\s)*\b(Female|Male)\b", text, re.I)
    if m:
        data["demographic"]["gender"] = normalize_gender(m.group(1))

    # Height and Weight
    m = safe_search(r"Height\s+and\s+Weight\s*\n\s*([^\r\n]+)", text, re.I)
    if m:
        hw = m.group(1)
        # Height
        hin = to_inches(hw)
        if hin is not None:
            data["demographic"]["height_in"] = hin
        # Weight
        w = to_pounds(hw)
        if w is None:
            m2 = safe_search(r"(\d{2,3})\s*pounds", hw, re.I)
            if m2: w = float(m2.group(1))
        if w is not None:
            data["demographic"]["weight_lbs"] = w

    # Details of Disappearance
    m = safe_search(r"Details\s+of\s+Disappearance\s*\n([\s\S]*?)(?:\n\s*Investigating\s+Agency|\Z)", text, re.I)
    if m:
        desc = re.sub(r"\s+"," ", m.group(1)).strip()
        data["narrative_osint"]["incident_summary"] = desc

    # Universal date fallback
    if "last_seen_ts" not in data["temporal"]:
        # look for a date within 120 chars of key phrases
        kwords = r"(Missing Since|Last Seen|Date of Last Contact|Disappearance)"
        # search windows: label … up to 120 chars … date token
        m = re.search(kwords + r".{0,120}?([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4})", text, re.I | re.S)
        if not m:
            # try date token then label after
            m = re.search(r"([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4}).{0,120}?" + kwords, text, re.I | re.S)
        if m:
            iso = to_iso8601(m.group(1))
            if iso:
                data["temporal"]["last_seen_ts"] = iso

    # Required lat/lon placeholders
    data["spatial"]["last_seen_lat"] = 0.0
    data["spatial"]["last_seen_lon"] = 0.0

    return data

# ---------- Validation ----------

def load_schema(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def validate_guardian(record: Dict[str, Any], schema: Dict[str, Any]) -> List[str]:
    errors = []
    v = Draft7Validator(schema)
    for e in sorted(v.iter_errors(record), key=lambda e: e.path):
        errors.append(f"{list(e.path)}: {e.message}")
    return errors

# ---------- Geocoding  ----------

_GEOCODER = None
_GEOCODE_CACHE = {}

def _init_geocoder():
    """Lazy import geopy and init Nominatim geocoder."""
    global _GEOCODER
    if _GEOCODER is not None:
        return _GEOCODER
    try:
        from geopy.geocoders import Nominatim
        _GEOCODER = Nominatim(user_agent="guardian_parser")
    except Exception:
        _GEOCODER = None
    return _GEOCODER

def load_geocode_cache(path: Optional[str]) -> None:
    global _GEOCODE_CACHE
    if not path:
        _GEOCODE_CACHE = {}
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            _GEOCODE_CACHE = json.load(f)
    except Exception:
        _GEOCODE_CACHE = {}

def save_geocode_cache(path: Optional[str]) -> None:
    if not path:
        return
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(_GEOCODE_CACHE, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def geocode_city_state(city: Optional[str], state: Optional[str], cache_key_extra: str = "", cache_only: bool = False) -> Tuple[Optional[float], Optional[float]]:
    """
    Geocode a (city, state) pair to (lat, lon) using geopy (Nominatim).
    - Uses a JSON cache to avoid repeated calls.
    - Returns (None, None) on failure.
    """
    if not city and not state:
        return (None, None)
    key = f"{(city or '').strip().lower()}|{(state or '').strip().lower()}|{cache_key_extra}"
    if key in _GEOCODE_CACHE:
        val = _GEOCODE_CACHE[key]
        return (val.get("lat"), val.get("lon"))
    if cache_only:
        return (None, None)
    geo = _init_geocoder()
    if not geo:
        return (None, None)
    try:
        q = ", ".join([p for p in [city, state, "USA"] if p])
        loc = geo.geocode(q, timeout=10)
        if loc:
            lat = clamp_lat(loc.latitude)
            lon = clamp_lon(loc.longitude)
            _GEOCODE_CACHE[key] = {"lat": lat, "lon": lon}
            return (lat, lon)
    except Exception:
        pass
    return (None, None)

# ---------- CSV/JSON emit ----------

def get_nested(d: Dict[str, Any], path: str, default: str = "") -> Any:
    """
    Get a nested value from a dictionary using dot notation.
    
    Args:
        d (Dict[str, Any]): The dictionary to search
        path (str): Dot-separated path to the value
        default (str): Default value if path not found
        
    Returns:
        Any: The value at the path or default if not found
        
    Example:
        >>> get_nested({"a": {"b": "value"}}, "a.b")
        "value"
    """
    cur = d or {}
    for p in path.split("."):
        if isinstance(cur, dict):
            cur = cur.get(p)
        else:
            return default
    return cur if cur is not None else default

def flatten_for_csv(rec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flatten a record into a comprehensive CSV row with all commonly requested fields.
    
    This function creates a rich CSV representation with both canonical columns
    and expanded fields, ensuring comprehensive data coverage for analysis.
    Uses tolerant fallback logic to read from both writer-style and canonical locations.
    
    Args:
        rec (Dict[str, Any]): The record to flatten
        
    Returns:
        Dict[str, Any]: Flattened record with comprehensive field coverage
    """
    rec = harmonize_record_fields(rec)

    # Person / name
    full_name = (
        get_nested(rec, "name.full")
        or get_nested(rec, "demographic.name")
    )

    # Gender (already harmonized to demographic.gender)
    gender = get_nested(rec, "demographic.gender")

    # Location (city/state)
    city = (
        get_nested(rec, "spatial.city")
        or get_nested(rec, "spatial.last_seen_city")
    )
    state = (
        get_nested(rec, "spatial.state")
        or get_nested(rec, "spatial.last_seen_state")
    )
    country = (
        get_nested(rec, "spatial.country")
        or get_nested(rec, "spatial.last_seen_country")
    )

    last_seen_location = ", ".join([p for p in [city, state] if p]) or (country or "")

    # Times
    last_seen_ts = get_nested(rec, "temporal.last_seen_ts")
    reported_ts  = (
        get_nested(rec, "temporal.reported_ts")
        or get_nested(rec, "temporal.reported_missing_ts")
    )

    # Case/meta
    source   = get_nested(rec, "case.source") or get_nested(rec, "provenance.sources", [""])[0]
    case_id  = get_nested(rec, "case.case_id") or get_nested(rec, "case_id")
    status   = get_nested(rec, "case.status")  or get_nested(rec, "outcome.case_status")

    row = {
        "source": source,
        "case_id": case_id,
        "case_status": status,

        "full_name": full_name,
        "aka": ( "; ".join(get_nested(rec, "demographic.aka", []))
                 if isinstance(get_nested(rec, "demographic.aka"), list)
                 else get_nested(rec, "demographic.aka") ),
        "dob": get_nested(rec, "demographic.dob"),
        "age_years": get_nested(rec, "demographic.age_years"),
        "gender": gender,
        "hair_color": get_nested(rec, "demographic.hair_color"),
        "eye_color": get_nested(rec, "demographic.eye_color"),

        "height_in": get_nested(rec, "demographic.height_in"),
        "height_cm": get_nested(rec, "demographic.height_cm"),
        "weight_lbs": get_nested(rec, "demographic.weight_lbs") or get_nested(rec, "demographic.weight_lb"),
        "weight_kg": get_nested(rec, "demographic.weight_kg"),

        "last_seen_location": last_seen_location,
        "last_seen_city": city,
        "last_seen_state": state,
        "last_seen_country": country,
        "last_seen_lat": get_nested(rec, "spatial.last_seen_lat"),
        "last_seen_lon": get_nested(rec, "spatial.last_seen_lon"),

        "last_seen_ts": last_seen_ts,
        "reported_ts": reported_ts,

        "incident_summary": get_nested(rec, "narrative.incident_summary") or get_nested(rec, "narrative_osint.incident_summary"),
        "notes": get_nested(rec, "narrative.notes"),

        "categories": (
            "; ".join(get_nested(rec, "case.categories", []))
            if isinstance(get_nested(rec, "case.categories"), list)
            else get_nested(rec, "case.categories")
        ),
    }
    return row

def write_csv(records: List[Dict[str, Any]], output_csv_path: str) -> None:
    """
    Writes all records with a comprehensive set of columns in a stable order.
    
    This function creates a CSV file with a fixed order of commonly requested
    columns plus any additional fields that appear in the data.
    
    Args:
        records (List[Dict[str, Any]]): List of records to write
        output_csv_path (str): Path to the output CSV file
    """
    # Flatten once to find all header keys
    flat = [flatten_for_csv(r) for r in records]
    
    # Fixed order subset (optional) + any extras that appeared
    base_order = [
        "source", "case_id", "case_status",
        "full_name", "aka", "dob", "age_years", "gender", "hair_color", "eye_color",
        "height_in", "height_cm", "weight_lbs", "weight_kg",
        "last_seen_location", "last_seen_city", "last_seen_state", "last_seen_country",
        "last_seen_lat", "last_seen_lon",
        "last_seen_ts", "reported_ts",
        "incident_summary", "notes", "categories",
    ]
    
    # Include any new keys we didn't anticipate (stable order)
    extra = [k for k in sorted(set().union(*[set(d.keys()) for d in flat])) if k not in base_order]
    fieldnames = base_order + extra

    try:
        with open(output_csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            for row in flat:
                w.writerow(row)
    except PermissionError:
        # Fallback if Excel is locking the file
        import time
        import os
        ts = time.strftime("%Y%m%d_%H%M%S")
        alt = os.path.splitext(output_csv_path)[0] + f".{ts}.csv"
        with open(alt, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            for row in flat:
                w.writerow(row)
        print(f"[WARN] Could not write {output_csv_path} (locked?). Wrote {alt} instead.")

# ---------- Runner ----------

def detect_source(text: str) -> str:
    """
    Detect the source type of a missing person case document.
    
    This function analyzes the text content to determine which organization
    or database the document originated from based on characteristic markers.
    
    Args:
        text (str): The extracted text from the PDF document
        
    Returns:
        str: Source identifier ("NamUs", "NCMEC", "Charley", or "Unknown")
        
    Detection Logic:
        - NamUs: Contains "NamUs", "Case Created", or "Date of Last Contact"
        - NCMEC: Contains "Have you seen this child?", "NCMEC", or "Missing Since:"
        - Charley: Contains "The Charley Project", "Details of Disappearance", or "Missing From"
        - Unknown: No characteristic markers found
        
    Example:
        >>> detect_source("NamUs Case Created: 2023-01-01")
        "NamUs"
        >>> detect_source("Missing Since: January 1, 2023")
        "NCMEC"
    """
    # Check for NamUs markers
    if "NamUs" in text or "Case Created" in text or "Date of Last Contact" in text:
        return "NamUs"
    
    # Check for NCMEC markers
    if "Have you seen this child?" in text or "NCMEC" in text or "Missing Since:" in text or "Missing Since :" in text:
        return "NCMEC"
    
    # Check for Charley Project markers
    if "The Charley Project" in text or "Details of Disappearance" in text or "Missing From" in text:
        return "Charley"
    
    return "Unknown"

def parse_pdf(pdf_path: str, case_id: str, do_geocode: bool = False, cache_only: bool = False) -> Dict[str, Any]:
    """
    Parse a PDF document into a standardized Guardian case record.
    
    This is the main entry point for parsing missing person case documents.
    It extracts text, detects the source type, applies source-specific parsing,
    and normalizes the data into the Guardian schema format.
    
    Args:
        pdf_path (str): Path to the PDF file to parse
        case_id (str): Unique identifier for the case
        do_geocode (bool): Whether to attempt geocoding of location data
        cache_only (bool): If True, only use cached geocoding results
        
    Returns:
        Dict[str, Any]: Parsed case record in Guardian schema format
        
    Process Flow:
        1. Extract text from PDF using multiple methods
        2. Normalize text (unicode, whitespace)
        3. Detect source type (NamUs, NCMEC, Charley)
        4. Apply source-specific parsing
        5. Store raw text for backfill processing
        6. Normalize critical fields (dates, gender)
        7. Optional geocoding of location data
        
    Example:
        >>> record = parse_pdf("case.pdf", "GRD-2023-000001", do_geocode=True)
        >>> print(record["demographic"]["name"])
        "John Doe"
    """
    # Extract and normalize text from PDF
    text = extract_text(pdf_path)
    text = _prenormalize(text)
    
    # Detect source type and apply appropriate parser
    source = detect_source(text)
   
    if source == "NamUs":
        rec = parse_namus(text, case_id)
        rec.setdefault("provenance", {}).update({"source_path": pdf_path})

    elif source == "NCMEC":
        rec = parse_ncmec(text, case_id)
        rec.setdefault("provenance", {}).update({"source_path": pdf_path})

    elif source == "Charley":
        rec = parse_charley(text, case_id)
        rec.setdefault("provenance", {}).update({"source_path": pdf_path})

    else:
        # Default to Charley-style narrative for unknown sources
        rec = parse_charley(text, case_id)
        rec.setdefault("provenance", {}).update({"source_path": pdf_path})
        rec["provenance"]["sources"] = ["Unknown"]

    # Store raw text for backfill processing
    rec["_fulltext"] = text

    # Apply source-agnostic enrichment to fill gaps
    rec = _enrich_common_fields(rec, text)

    # Harmonize record fields to canonical names
    rec = harmonize_record_fields(rec)

    # Normalize critical fields into schema before validation
    rec["temporal"] = rec.get("temporal") or {}
    if not rec["temporal"].get("last_seen_ts"):
        rec["temporal"]["last_seen_ts"] = parse_last_seen_ts(text) or ""

    rec["demographic"] = rec.get("demographic") or {}
    rec["demographic"]["gender"] = rec["demographic"].get("gender") or parse_gender(text)

    # ## GEO_HOOK: if lat/lon missing or zero, try geocoding from city/state or free-text location
    if do_geocode:
        lat = rec.get("spatial",{}).get("last_seen_lat")
        lon = rec.get("spatial",{}).get("last_seen_lon")
        needs_geo = (lat is None or lon is None or (lat == 0.0 and lon == 0.0))
        if needs_geo:
            city = rec.get("spatial",{}).get("last_seen_city")
            state = rec.get("spatial",{}).get("last_seen_state")
            glat, glon = geocode_city_state(city, state, cache_key_extra="city_state", cache_only=cache_only)
            if glat is None or glon is None:
                # Try free-text location if available
                loc = rec.get("spatial",{}).get("last_seen_location")
                if loc:
                    parts = [p.strip() for p in (loc.split(",") if isinstance(loc,str) else [])]
                    c2 = parts[0] if parts else city
                    s2 = parts[1] if len(parts) > 1 else state
                    glat, glon = geocode_city_state(c2, s2, cache_key_extra="from_location", cache_only=cache_only)
            if glat is not None and glon is not None:
                rec.setdefault("spatial", {})["last_seen_lat"] = glat
                rec.setdefault("spatial", {})["last_seen_lon"] = glon

    return rec

def _prenormalize(s: str) -> str:
    if not s: return ""
    # normalize unicode quotes/spaces/dashes
    s = s.replace("\u00A0", " ")   # NBSP -> space
    s = s.replace("’", "'").replace("“","\"").replace("”","\"")
    s = s.replace("–","-").replace("—","-")
    # collapse multiple spaces
    s = re.sub(r"[ \t]+", " ", s)
    return s




def main(argv=None):
    import argparse
    parser = argparse.ArgumentParser(description="Guardian Parser Pack")
    parser.add_argument("--inputs", nargs="+", help="PDF files to parse", required=True)
    parser.add_argument("--jsonl", default="guardian_output.jsonl")
    parser.add_argument("--csv", default="guardian_output.csv")
    parser.add_argument("--geocode", action="store_true", help="Attempt to geocode missing lat/lon from city/state")
    parser.add_argument("--geocode-cache", default=str(os.path.join(os.path.dirname(__file__), "geocode_cache.json")), help="Path to a JSON cache for geocoding results")
    args = parser.parse_args(argv)

    schema = load_schema(GUARDIAN_SCHEMA_PATH)
    if args.geocode:
        load_geocode_cache(args.geocode_cache)
    
    # Parse all PDFs first
    records = []
    for idx, pdf in enumerate(args.inputs, start=1):
        case_id = f"GRD-{datetime.now().strftime('%Y')}-{idx:06d}"
        rec = parse_pdf(pdf, case_id, do_geocode=args.geocode, cache_only=False)
        records.append(rec)
    
    # Safety backfill pass to catch anything missed
    records = backfill(records)
    
    # Write JSONL output
    with open(args.jsonl, "w", encoding="utf-8") as jf:
        for rec in records:
            errs = validate_guardian(rec, schema)
            if errs:
                print(f"[WARN] {rec.get('provenance', {}).get('source_path', 'unknown')} failed validation:", *errs, sep="\n  ")
            # Remove _fulltext before writing to JSONL
            rec_clean = {k: v for k, v in rec.items() if k != "_fulltext"}
            jf.write(json.dumps(rec_clean, ensure_ascii=False) + "\n")

    if args.geocode:
        save_geocode_cache(args.geocode_cache)

    # Write CSV using the new dynamic flattener
    write_csv(records, args.csv)

    print(f"Wrote {args.jsonl} and {args.csv}")

if __name__ == "__main__":
    main()
