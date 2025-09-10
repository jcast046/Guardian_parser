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
    "fitz",              # PyMuPDF, if you use it anywhere
    "pymupdf",           # alternate name
    "pdfplumber"         # if you route through pdfplumber
]:
    logging.getLogger(name).setLevel(logging.ERROR)

# Also: default root logger if you see any remaining noise
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

def flatten_for_csv(rec: Dict[str, Any]) -> Dict[str, Any]:
    """Minimal flattener for core fields commonly present"""
    out = {
        "case_id": rec.get("case_id", ""),
        "name": rec.get("demographic",{}).get("name",""),
        "age_years": rec.get("demographic",{}).get("age_years",""),
        "gender": rec.get("demographic",{}).get("gender",""),
        "height_in": rec.get("demographic",{}).get("height_in",""),
        "weight_lbs": rec.get("demographic",{}).get("weight_lbs",""),
        "last_seen_location": rec.get("spatial",{}).get("last_seen_location",""),
        "last_seen_lat": rec.get("spatial",{}).get("last_seen_lat",""),
        "last_seen_lon": rec.get("spatial",{}).get("last_seen_lon",""),
        "last_seen_ts": rec.get("temporal",{}).get("last_seen_ts",""),
        "case_status": rec.get("outcome",{}).get("case_status",""),
        "incident_summary": rec.get("narrative_osint",{}).get("incident_summary","")[:500]
    }
    return out

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
    
    # Write outputs
    csv_rows = []
    with open(args.jsonl, "w", encoding="utf-8") as jf:
        for rec in records:
            errs = validate_guardian(rec, schema)
            if errs:
                print(f"[WARN] {rec.get('provenance', {}).get('source_path', 'unknown')} failed validation:", *errs, sep="\n  ")
            # Remove _fulltext before writing to JSONL
            rec_clean = {k: v for k, v in rec.items() if k != "_fulltext"}
            jf.write(json.dumps(rec_clean, ensure_ascii=False) + "\n")
            csv_rows.append(flatten_for_csv(rec_clean))

    if args.geocode:
        save_geocode_cache(args.geocode_cache)

    # Write CSV
    fieldnames = list(csv_rows[0].keys()) if csv_rows else ["case_id"]
    with open(args.csv, "w", newline="", encoding="utf-8") as cf:
        w = csv.DictWriter(cf, fieldnames=fieldnames)
        w.writeheader()
        for r in csv_rows:
            w.writerow(r)

    print(f"Wrote {args.jsonl} and {args.csv}")

if __name__ == "__main__":
    main()
