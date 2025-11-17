"""Tool implementations for the Guardian agent.

Provides utilities for PDF text extraction, geocoding, validation, and
output writing used by the agent pipeline.
"""
import os
import json
import glob
import sys
from typing import List, Dict
from pathlib import Path

# Add root directory to path to import parser_pack
_root_dir = Path(__file__).parent.parent.parent.resolve()
if str(_root_dir) not in sys.path:
    sys.path.insert(0, str(_root_dir))

# Now import from parser_pack
import parser_pack
from geopy.geocoders import Nominatim
from jsonschema import Draft7Validator, ValidationError

from .protocols import OCRTextReturn, GeocodeReturn, GuardianRow
from .text_clean import clean_pdf_text


CACHE_DIR = os.path.join(_root_dir, "output")
GEO_CACHE = os.path.join(CACHE_DIR, "geocode_cache.json")


def _load(path: str) -> Dict:
    """Load JSON file from path.

    Args:
        path: Path to JSON file.

    Returns:
        Parsed JSON dictionary, empty dictionary if file not found or invalid.
    """
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _save(path: str, data: Dict) -> None:
    """Save dictionary to JSON file.

    Creates parent directories if they do not exist.

    Args:
        path: Path to output JSON file.
        data: Dictionary to serialize.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def list_pdfs(directory: str) -> List[str]:
    """List all PDF files in directory recursively.

    Args:
        directory: Directory path to search.

    Returns:
        Sorted list of PDF file paths.
    """
    return sorted(glob.glob(os.path.join(directory, "**", "*.pdf"), recursive=True))


def extract_text_primary_fallbacks(path: str, force_ocr: bool = False, page_range: str | None = None) -> OCRTextReturn:
    """Extract and clean text from PDF file.

    Uses parser_pack's extract_text function with automatic fallback handling.
    Cleans extracted text for LLM processing.

    Args:
        path: Path to PDF file.
        force_ocr: Unused (parser_pack handles fallbacks automatically).
        page_range: Unused (extracts all pages).

    Returns:
        OCRTextReturn object containing cleaned text, modality, page list,
        and extraction metadata.
    """
    raw_text = parser_pack.extract_text(path)
    
    # Try to get page count and per-page text from PDF
    pages = []
    pages_text = None
    try:
        import PyPDF2
        with open(path, "rb") as f:
            reader = PyPDF2.PdfReader(f)
            num_pages = len(reader.pages)
            pages = list(range(1, num_pages + 1))
            
            # Try to extract per-page text for better header/footer removal
            try:
                pages_text = []
                for page_num in range(num_pages):
                    page_text = reader.pages[page_num].extract_text()
                    pages_text.append(page_text)
            except Exception:
                pages_text = None  # If per-page extraction fails, use None
    except Exception:
        # If can't get page count, estimate from text length
        # or just return empty list
        pages = []
    
    # Clean the text
    text_clean = clean_pdf_text(raw_text, pages_text)
    
    meta = {
        "modality": "pdf",
        "pages": len(pages),
        "source_path": path,
        "char_count_raw": len(raw_text),
        "char_count_clean": len(text_clean)
    }
    
    return OCRTextReturn(text=text_clean, modality="pdf", pages=pages, meta=meta)


def geocode(query: str, state_bias: str = "VA") -> GeocodeReturn:
    """Geocode location query using Nominatim.

    Uses parser_pack's geocoding functions for consistency and caching.

    Args:
        query: Location string to geocode.
        state_bias: State abbreviation to bias geocoding results (default: "VA").

    Returns:
        GeocodeReturn object with lat/lon coordinates or None values if failed.
    """
    # Load parser_pack's geocoding cache
    parser_pack.load_geocode_cache(GEO_CACHE)
    
    # Try to extract city/state from query
    # Simple parsing: assume "City, State" format
    parts = [p.strip() for p in query.split(",")]
    city = parts[0] if parts else None
    state = parts[1] if len(parts) > 1 else state_bias
    
    # Use parser_pack's geocoding function
    lat, lon = parser_pack.geocode_city_state(city, state, cache_key_extra=query, cache_only=False)
    
    # Save cache
    parser_pack.save_geocode_cache(GEO_CACHE)
    
    if lat is not None and lon is not None:
        d = {
            "lat": lat,
            "lon": lon,
            "confidence": 0.9,
            "provider": "nominatim"
        }
    else:
        d = {
            "lat": None,
            "lon": None,
            "confidence": 0.0,
            "provider": "nominatim"
        }
    
    return GeocodeReturn(raw=query, **d)


def geocode_batch(places: List[str], state_bias: str = "VA") -> List[GeocodeReturn]:
    """Geocode multiple places with deduplication.

    Args:
        places: List of location strings to geocode.
        state_bias: State abbreviation to bias geocoding results (default: "VA").

    Returns:
        List of GeocodeReturn objects, one per unique place.
    """
    out, seen = [], set()
    for p in places:
        if not p or p in seen:
            continue
        seen.add(p)
        out.append(geocode(p, state_bias))
    return out


def validate_row(row: GuardianRow, schema_path: str) -> List[str]:
    """Validate GuardianRow against JSON schema.

    Uses Draft7Validator matching the validation approach in parser_pack.py.
    Excludes source_path and audit fields from validation as they are Pydantic
    model fields but not part of the JSON schema.

    Args:
        row: GuardianRow instance to validate.
        schema_path: Path to JSON schema file.

    Returns:
        List of validation error messages formatted as "{path}: {message}".
        Empty list if validation passes.
    """
    errors = []
    try:
        with open(schema_path, "r", encoding="utf-8") as f:
            schema = json.load(f)
        
        # Convert GuardianRow to dict
        row_dict = row.model_dump()
        
        # Extract source_path and audit (not in schema) before validation
        # These fields are required by the Pydantic model but not in the JSON schema
        # We create a copy to avoid modifying the original row object
        validation_dict = row_dict.copy()
        validation_dict.pop("source_path", None)
        validation_dict.pop("audit", None)
        
        # Use Draft7Validator.iter_errors() for detailed error reporting
        validator = Draft7Validator(schema)
        for error in sorted(validator.iter_errors(validation_dict), key=lambda e: e.path):
            # Format error as "{path}: {message}" to match parser_pack format
            error_path = list(error.path) if error.path else ["root"]
            errors.append(f"{error_path}: {error.message}")
        
        return errors
    except Exception as e:
        return [f"Validation error: {str(e)}"]


def write_output(row: GuardianRow | Dict, out_jsonl: str, out_csv: str | None = None):
    """Write GuardianRow or dict to JSONL and optionally CSV files.

    Windows-safe implementation with explicit newline handling.

    Args:
        row: GuardianRow instance or dictionary to write.
        out_jsonl: Path to JSONL output file.
        out_csv: Optional path to CSV output file.
    """
    import io
    
    if out_jsonl:
        # Ensure directory exists
        out_dir = os.path.dirname(out_jsonl)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
        
        # Get data as dict
        if hasattr(row, "model_dump"):
            data = row.model_dump()
        else:
            data = row
        
        # Remove _fulltext if present (not in final output)
        data.pop("_fulltext", None)
        
        # Write JSONL with explicit newline for Windows compatibility
        line = json.dumps(data, ensure_ascii=False)
        with io.open(out_jsonl, "a", encoding="utf-8", newline="\n") as f:
            f.write(line + "\n")
    
    # Write CSV if requested
    if out_csv:
        # Ensure directory exists
        out_dir = os.path.dirname(out_csv)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
        
        # Get data as dict
        if hasattr(row, "model_dump"):
            row_dict = row.model_dump()
        else:
            row_dict = row
        
        # Use parser_pack's flatten_for_csv
        flat_row = parser_pack.flatten_for_csv(row_dict)
        
        # Check if CSV exists to determine if we need a header
        file_exists = os.path.exists(out_csv) and os.path.getsize(out_csv) > 0
        
        import csv
        fieldnames = list(flat_row.keys())
        with open(out_csv, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            if not file_exists:
                writer.writeheader()
            writer.writerow(flat_row)

