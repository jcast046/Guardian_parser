#!/usr/bin/env python3
"""
Post-processor to fix missing temporal.last_seen_ts in existing JSONL output.

This script processes existing Guardian parser output files to extract and normalize
missing date information from text fields. It searches through narrative and provenance
data to find date patterns and converts them to ISO 8601 format.

Key Features:
    - Extracts dates from text fields in existing JSONL records
    - Normalizes dates to YYYY-MM-DD format
    - Removes debug _fulltext fields from output
    - Preserves all other record data unchanged

Usage:
    python fix_dates.py
    
Input:  guardian_output.jsonl
Output: guardian_output.fixed.jsonl

Author: Joshua Castillo
Version: 1.0
"""

import json
import re
import sys
import datetime
from typing import Optional, Dict, Any, List

# Input and output file paths
in_path: str = "guardian_output.jsonl"
out_path: str = "guardian_output.fixed.jsonl"

# Compiled regex patterns for date extraction
# Pattern for wordy dates: "Missing Since: September 8, 2025" or "Last seen on Feb 7, 1977"
DATE_WORDY: re.Pattern[str] = re.compile(
    r'\b(Missing Since|Last seen)\b[^0-9A-Za-z]{0,5}([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4})', 
    re.I
)

# Pattern for slash dates: "Missing Since 02/07/1977" or "Last seen 12-25-2023"
DATE_SLASH: re.Pattern[str] = re.compile(
    r'\b(Missing Since|Last seen)\b[^0-9]{0,5}(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})', 
    re.I
)

def norm_date(s: str) -> Optional[str]:
    """
    Normalize a date string to ISO 8601 format (YYYY-MM-DD).
    
    This function attempts to parse a date string using multiple common formats
    and converts it to the standardized ISO 8601 date format.
    
    Args:
        s (str): Date string to normalize
        
    Returns:
        Optional[str]: ISO 8601 formatted date string (YYYY-MM-DD) or None if parsing fails
        
    Supported Formats:
        - "%B %d, %Y"  -> "September 8, 2025"
        - "%b %d, %Y"  -> "Sep 8, 2025"
        - "%m/%d/%Y"   -> "09/08/2025"
        - "%m-%d-%Y"   -> "09-08-2025"
        - "%m/%d/%y"   -> "09/08/25"
        - "%m-%d-%y"   -> "09-08-25"
        
    Example:
        >>> norm_date("September 8, 2025")
        "2025-09-08"
        >>> norm_date("02/07/1977")
        "1977-02-07"
        >>> norm_date("invalid date")
        None
    """
    # Try multiple date formats in order of preference
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y", "%m-%d-%y"):
        try:
            dt = datetime.datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None

def main() -> None:
    """
    Main processing function to fix missing dates in JSONL records.
    
    This function reads the input JSONL file, processes each record to extract
    missing date information from text fields, and writes the corrected records
    to the output file.
    
    Process Flow:
        1. Read each line from input JSONL file
        2. Parse JSON record
        3. Remove debug _fulltext field
        4. Ensure temporal object structure exists
        5. Extract dates from text fields if last_seen_ts is missing
        6. Write corrected record to output file
        
    Raises:
        FileNotFoundError: If input file doesn't exist
        json.JSONDecodeError: If input contains invalid JSON
        IOError: If unable to read/write files
    """
    try:
        with open(in_path, "r", encoding="utf-8") as fin, open(out_path, "w", encoding="utf-8") as fout:
            for line_num, line in enumerate(fin, 1):
                try:
                    # Parse JSON record
                    o: Dict[str, Any] = json.loads(line)
                    
                    # Remove debug _fulltext field if present
                    o.pop("_fulltext", None)

                    # Ensure temporal object structure exists
                    temporal: Dict[str, Any] = o.get("temporal") or {}
                    o["temporal"] = temporal

                    # Extract missing dates from text fields
                    if not temporal.get("last_seen_ts"):
                        text_fields: List[str] = []
                        
                        # Collect text from fields that may contain date information
                        for k in ("narrative_osint", "provenance", "outcome"):
                            v = o.get(k)
                            if isinstance(v, dict):
                                # Extract string values from nested dictionaries
                                text_fields += [str(x) for x in v.values() 
                                              if isinstance(x, (str, int, float))]
                            elif isinstance(v, str):
                                text_fields.append(v)
                        
                        # Combine all text fields for pattern matching
                        blob: str = " | ".join(text_fields)

                        # Search for date patterns in the combined text
                        m = DATE_WORDY.search(blob) or DATE_SLASH.search(blob)
                        if m:
                            iso: Optional[str] = norm_date(m.group(2))
                            if iso:
                                temporal["last_seen_ts"] = iso

                    # Write corrected record to output
                    fout.write(json.dumps(o, ensure_ascii=False) + "\n")
                    
                except json.JSONDecodeError as e:
                    print(f"Warning: Invalid JSON on line {line_num}: {e}", file=sys.stderr)
                    continue
                except Exception as e:
                    print(f"Warning: Error processing line {line_num}: {e}", file=sys.stderr)
                    continue

        print(f"Successfully processed records and wrote to {out_path}")
        
    except FileNotFoundError:
        print(f"Error: Input file '{in_path}' not found", file=sys.stderr)
        sys.exit(1)
    except IOError as e:
        print(f"Error: Unable to read/write files: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
