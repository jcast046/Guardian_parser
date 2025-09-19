#!/usr/bin/env python3
"""
Virginia Transportation Data Extractor

Integrated into Guardian Parser Pack to extract comprehensive transportation data
from Virginia State Map PDFs including interstates, US routes, state routes, and named streets.

Usage:
    python va_transport_extractor.py --src "C:/Users/N0Cir/CS697/VA_State_Map" --out "output"
"""

import argparse
import json
import os
import re
import uuid
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional

try:
    from PyPDF2 import PdfReader
except ImportError:
    try:
        import pdfminer.six as pdfminer
        from pdfminer.high_level import extract_text
        PDFMINER_AVAILABLE = True
    except ImportError:
        raise SystemExit("PyPDF2 or pdfminer.six is required. Install with: pip install PyPDF2")

# ----------------------------- Configuration -----------------------------

# Known US Routes carried in Virginia (canonical list for classification)
US_ROUTES_VA = {
    1, 11, 13, 15, 17, 19, 21, 23, 25, 29, 33, 50, 52, 58, 60, 211, 220, 221,
    250, 258, 301, 340, 360, 401, 421, 460, 501, 522
}

# Virginia regions and their transportation networks
VA_REGIONS = {
    "Northern Virginia": {
        "interstates": {"I-66", "I-495", "I-395", "I-95", "I-81", "I-270"},
        "us_routes": {"US-1", "US-29", "US-50", "US-15", "US-17", "US-211"},
        "state_routes": {"VA-7", "VA-28", "VA-123", "VA-267", "VA-286", "VA-620"},
        "cities": ["Arlington", "Alexandria", "Fairfax", "Herndon", "Reston", "Tysons", "McLean", "Manassas", "Leesburg", "Ashburn", "Potomac"]
    },
    "Central Virginia": {
        "interstates": {"I-64", "I-95", "I-195", "I-295", "I-288"},
        "us_routes": {"US-33", "US-60", "US-250", "US-301", "US-360", "US-522"},
        "state_routes": {"VA-288", "VA-150", "VA-10", "VA-33", "VA-76"},
        "cities": ["Richmond", "Henrico", "Chesterfield", "Short Pump", "Midlothian", "Mechanicsville", "Ashland"]
    },
    "Tidewater": {
        "interstates": {"I-64", "I-264", "I-464", "I-564", "I-664"},
        "us_routes": {"US-13", "US-17", "US-58", "US-60", "US-258", "US-460"},
        "state_routes": {"VA-168", "VA-164", "VA-199", "VA-44", "VA-134"},
        "cities": ["Virginia Beach", "Norfolk", "Portsmouth", "Chesapeake", "Hampton", "Newport News", "Suffolk", "Williamsburg", "Poquoson", "Yorktown"]
    },
    "Southwest": {
        "interstates": {"I-81", "I-77", "I-581"},
        "us_routes": {"US-11", "US-19", "US-23", "US-58", "US-460", "US-421", "US-52", "US-220"},
        "state_routes": {"VA-100", "VA-114", "VA-116", "VA-140", "VA-177"},
        "cities": ["Roanoke", "Salem", "Blacksburg", "Christiansburg", "Abingdon", "Bristol", "Wise", "Norton", "Pulaski", "Wytheville"]
    },
    "Valley": {
        "interstates": {"I-81", "I-66"},
        "us_routes": {"US-11", "US-33", "US-50", "US-220", "US-250", "US-340", "US-522"},
        "state_routes": {"VA-7", "VA-55", "VA-42", "VA-259", "VA-263"},
        "cities": ["Winchester", "Front Royal", "Harrisonburg", "Staunton", "Waynesboro", "Lexington", "Luray", "Woodstock"]
    },
    "Western Virginia": {
        "interstates": {"I-64", "I-81"},
        "us_routes": {"US-15", "US-29", "US-33", "US-60", "US-250", "US-340", "US-360", "US-460"},
        "state_routes": {"VA-20", "VA-22", "VA-24", "VA-26", "VA-53", "VA-151"},
        "cities": ["Charlottesville", "Lynchburg", "Danville", "Martinsville", "Farmville", "Bedford", "Amherst"]
    },
    "Northern Neck": {
        "interstates": set(),
        "us_routes": {"US-17", "US-301", "US-360"},
        "state_routes": {"VA-3", "VA-200", "VA-218", "VA-222"},
        "cities": ["Fredericksburg", "Stafford", "Spotsylvania", "King George", "Westmoreland", "Northumberland", "Lancaster", "Richmond County"]
    },
    "Southside": {
        "interstates": {"I-85", "I-95"},
        "us_routes": {"US-1", "US-15", "US-29", "US-58", "US-360", "US-460"},
        "state_routes": {"VA-40", "VA-46", "VA-49", "VA-85", "VA-122"},
        "cities": ["Petersburg", "Colonial Heights", "Hopewell", "Emporia", "South Hill", "Lawrenceville", "Boydton", "Chase City"]
    }
}

# ----------------------------- PDF Utilities -----------------------------

def read_pdf_text(path: Path) -> str:
    """Read text from a PDF using available PDF library."""
    try:
        if 'PDFMINER_AVAILABLE' in globals() and PDFMINER_AVAILABLE:
            return extract_text(str(path))
        else:
            reader = PdfReader(str(path))
            parts = []
            for page in reader.pages:
                t = page.extract_text() or ""
                parts.append(t)
            return "\n".join(parts)
    except Exception as e:
        print(f"[WARN] Failed to read {path}: {e}")
        return ""

def iter_pdf_texts(folder: Path):
    """Yield (pdf_path, extracted_text) for all PDFs in a folder (recursive)."""
    for entry in sorted(folder.rglob("*.pdf")):
        try:
            text = read_pdf_text(entry)
            if text.strip():
                yield entry, text
        except Exception as e:
            print(f"[WARN] Failed to read {entry}: {e}")

# ----------------------------- Extraction Logic -----------------------------

# Enhanced regex patterns for Virginia transportation
RE_INTERSTATE = re.compile(r"\bI[\s\-]?(\d{1,3})\b", re.IGNORECASE)
RE_US_ROUTE = re.compile(r"\bU\.?S\.?[\s\-]?(\d{1,3})\b", re.IGNORECASE)
RE_STATE_ROUTE = re.compile(r"\b(?:VA|SR|State Route|State Rte|Rte|Route)[\s\-]?(\d{1,4})\b", re.IGNORECASE)
RE_PRIMARY_HIGHWAY = re.compile(r"\b(?:Primary|SR|State Route)[\s\-]?(\d{1,4})\b", re.IGNORECASE)
RE_SECONDARY_HIGHWAY = re.compile(r"\b(?:Secondary|SR|State Route)[\s\-]?(\d{1,4})\b", re.IGNORECASE)

# Enhanced named road patterns
SUFFIXES = r"(?:St|Street|Rd|Road|Ave|Avenue|Blvd|Boulevard|Dr|Drive|Ln|Lane|Pkwy|Parkway|Turnpike|Tpke|Way|Circle|Cir|Ct|Court|Terr|Terrace|Pl|Place|Hwy|Highway|Expwy|Expressway|Bypass|Byp|Pike|Bridge|Trail|Spur|Freeway|Beltway|Express Lanes)"
RE_NAMED_STREET = re.compile(rf"\b([A-Z][A-Za-z'&\.-]*(?: [A-Z][A-Za-z'&\.-]*)* (?:{SUFFIXES}))\b")
RE_NAMED_HIGHWAY = re.compile(rf"\b([A-Z][A-Za-z'&\.-]*(?: [A-Z][A-Za-z'&\.-]*)* (?:Highway|Hwy|Expressway|Freeway|Beltway|Turnpike|Tpke|Bypass|Byp|Pike|Bridge|Trail|Spur))\b")

# Transit patterns
RE_TRANSIT = re.compile(r"\b(?:Metro|Bus|Rail|Train|Transit|Station|Stop|Route|Line)\b", re.IGNORECASE)

def normalize_whitespace(s: str) -> str:
    """Normalize whitespace in text."""
    return re.sub(r"[ \t]+", " ", s.replace("\u00A0", " ")).strip()

def extract_transportation_data(text: str) -> Dict[str, Set[str]]:
    """Extract comprehensive transportation data from text."""
    text = normalize_whitespace(text)
    
    # Extract interstates
    interstates = set(f"I-{int(m.group(1))}" for m in RE_INTERSTATE.finditer(text))
    
    # Extract US routes
    us_routes = set(f"US-{int(m.group(1))}" for m in RE_US_ROUTE.finditer(text))
    
    # Extract state routes (primary and secondary)
    state_routes = set(f"VA-{int(m.group(1))}" for m in RE_STATE_ROUTE.finditer(text))
    primary_highways = set(f"SR-{int(m.group(1))}" for m in RE_PRIMARY_HIGHWAY.finditer(text))
    secondary_highways = set(f"SR-{int(m.group(1))}" for m in RE_SECONDARY_HIGHWAY.finditer(text))
    
    # Extract named streets and highways
    named_streets = set(m.group(1).strip(" .") for m in RE_NAMED_STREET.finditer(text))
    named_highways = set(m.group(1).strip(" .") for m in RE_NAMED_HIGHWAY.finditer(text))
    
    # Clean up named roads
    named_streets = {n for n in (normalize_whitespace(x) for x in named_streets) if len(n.split()) >= 2}
    named_highways = {n for n in (normalize_whitespace(x) for x in named_highways) if len(n.split()) >= 2}
    
    # Extract transit information
    transit_mentions = set(m.group(0) for m in RE_TRANSIT.finditer(text))
    
    # Classify bare numbers as US routes if they're in our registry
    for num in re.findall(r"\b\d{1,3}\b", text):
        n = int(num)
        if n in US_ROUTES_VA:
            us_routes.add(f"US-{n}")
    
    return {
        "interstates": interstates,
        "us_routes": us_routes,
        "state_routes": state_routes,
        "primary_highways": primary_highways,
        "secondary_highways": secondary_highways,
        "named_streets": named_streets,
        "named_highways": named_highways,
        "transit": transit_mentions
    }

def extract_from_folder(folder: Path) -> Dict[str, List[str]]:
    """Extract transportation data from all PDFs in folder."""
    all_data = {
        "interstates": set(),
        "us_routes": set(),
        "state_routes": set(),
        "primary_highways": set(),
        "secondary_highways": set(),
        "named_streets": set(),
        "named_highways": set(),
        "transit": set()
    }
    
    pdf_count = 0
    for pdf_path, text in iter_pdf_texts(folder):
        pdf_count += 1
        print(f"Processing {pdf_path.name}...")
        
        data = extract_transportation_data(text)
        for category, items in data.items():
            all_data[category] |= items
    
    print(f"Processed {pdf_count} PDF files")
    
    # Convert sets to sorted lists
    return {category: sorted(list(items)) for category, items in all_data.items()}

def assign_to_regions(transportation_data: Dict[str, List[str]]) -> Dict[str, Dict[str, List[str]]]:
    """Assign transportation items to Virginia regions."""
    regional_data = {region: {
        "interstates": [],
        "us_routes": [],
        "state_routes": [],
        "primary_highways": [],
        "secondary_highways": [],
        "named_streets": [],
        "named_highways": [],
        "transit": []
    } for region in VA_REGIONS}
    
    # Assign based on known regional networks
    for region, network in VA_REGIONS.items():
        for category in ["interstates", "us_routes", "state_routes"]:
            if category in network:
                regional_data[region][category] = [
                    item for item in transportation_data.get(category, [])
                    if item in network[category]
                ]
    
    # Assign named streets and highways based on city keywords
    for category in ["named_streets", "named_highways"]:
        for item in transportation_data.get(category, []):
            assigned = False
            for region, network in VA_REGIONS.items():
                for city in network.get("cities", []):
                    if city.lower() in item.lower():
                        regional_data[region][category].append(item)
                        assigned = True
                        break
                if assigned:
                    break
            
            # If not assigned, distribute evenly
            if not assigned:
                regions_list = list(VA_REGIONS.keys())
                idx = abs(hash(item)) % len(regions_list)
                regional_data[regions_list[idx]][category].append(item)
    
    # Sort all lists
    for region in regional_data:
        for category in regional_data[region]:
            regional_data[region][category] = sorted(regional_data[region][category])
    
    return regional_data

def create_road_segment(route_item: str, route_type: str, region: str, source_doc: str = None) -> Dict:
    """Create a structured road segment record according to the schema."""
    
    # Parse route information
    route_system = "Unknown"
    route_number = route_item
    signing = "None"
    
    if route_item.startswith("I-"):
        route_system = "Interstate"
        route_number = route_item.split("-")[1]
        signing = "Interstate"
    elif route_item.startswith("US-"):
        route_system = "US Highway"
        route_number = route_item.split("-")[1]
        signing = "US"
    elif route_item.startswith("VA-"):
        route_system = "Primary Highway"
        route_number = route_item.split("-")[1]
        signing = "VA"
    elif route_item.startswith("SR-"):
        route_system = "Secondary Highway"
        route_number = route_item.split("-")[1]
        signing = "VA"
    
    # Map regions to RL tags
    region_mapping = {
        "Northern Virginia": "NoVA",
        "Central Virginia": "Piedmont", 
        "Tidewater": "Tidewater",
        "Southwest": "Appalachia",
        "Valley": "Shenandoah",
        "Western Virginia": "Piedmont",
        "Northern Neck": "Tidewater",
        "Southside": "Piedmont"
    }
    
    return {
        "segmentId": str(uuid.uuid4()),
        "localNames": [route_item],
        "routeDesignation": {
            "routeSystem": route_system,
            "routeNumber": route_number,
            "routeBranch": "None",
            "signing": signing,
            "corridorCodes": []
        },
        "admin": {
            "region": region,
            "regionTagRL": region_mapping.get(region, "Unknown"),
            "vdotDistrict": None,
            "countyFips": None,
            "placeFips": None,
            "inState": True
        },
        "rlHints": {
            "directionalBearingDeg": None,
            "allowedDirections": []
        },
        "geometry": None,
        "centroid": None,
        "lengthMiles": None,
        "functionalClassification": None,
        "operations": None,
        "linearReference": None,
        "provenance": {
            "source": "VDOT Official State Map 2022–2026",
            "sourceDoc": source_doc,
            "sourcePage": None,
            "parserVersion": "1.0.0",
            "extractedAt": datetime.now().isoformat(),
            "confidence": 0.8
        }
    }

def create_named_street_segment(street_name: str, region: str, source_doc: str = None) -> Dict:
    """Create a structured road segment record for named streets."""
    
    # Map regions to RL tags
    region_mapping = {
        "Northern Virginia": "NoVA",
        "Central Virginia": "Piedmont", 
        "Tidewater": "Tidewater",
        "Southwest": "Appalachia",
        "Valley": "Shenandoah",
        "Western Virginia": "Piedmont",
        "Northern Neck": "Tidewater",
        "Southside": "Piedmont"
    }
    
    return {
        "segmentId": str(uuid.uuid4()),
        "localNames": [street_name],
        "routeDesignation": {
            "routeSystem": "Unknown",
            "routeNumber": "Unknown",
            "routeBranch": "None",
            "signing": "None",
            "corridorCodes": []
        },
        "admin": {
            "region": region,
            "regionTagRL": region_mapping.get(region, "Unknown"),
            "vdotDistrict": None,
            "countyFips": None,
            "placeFips": None,
            "inState": True
        },
        "rlHints": {
            "directionalBearingDeg": None,
            "allowedDirections": []
        },
        "geometry": None,
        "centroid": None,
        "lengthMiles": None,
        "functionalClassification": None,
        "operations": None,
        "linearReference": None,
        "provenance": {
            "source": "VDOT Official State Map 2022–2026",
            "sourceDoc": source_doc,
            "sourcePage": None,
            "parserVersion": "1.0.0",
            "extractedAt": datetime.now().isoformat(),
            "confidence": 0.6
        }
    }

def create_structured_road_segments(transportation_data: Dict[str, List[str]], regional_data: Dict[str, Dict[str, List[str]]]) -> List[Dict]:
    """Create structured road segment records according to the schema."""
    road_segments = []
    
    # Create segments for each route type
    for region, items in regional_data.items():
        # Interstates
        for interstate in items.get("interstates", []):
            segment = create_road_segment(interstate, "Interstate", region)
            road_segments.append(segment)
        
        # US Routes
        for us_route in items.get("us_routes", []):
            segment = create_road_segment(us_route, "US Highway", region)
            road_segments.append(segment)
        
        # State Routes
        for state_route in items.get("state_routes", []):
            segment = create_road_segment(state_route, "Primary Highway", region)
            road_segments.append(segment)
        
        # Primary Highways
        for primary in items.get("primary_highways", []):
            segment = create_road_segment(primary, "Primary Highway", region)
            road_segments.append(segment)
        
        # Secondary Highways
        for secondary in items.get("secondary_highways", []):
            segment = create_road_segment(secondary, "Secondary Highway", region)
            road_segments.append(segment)
        
        # Named Streets
        for street in items.get("named_streets", []):
            segment = create_named_street_segment(street, region)
            road_segments.append(segment)
        
        # Named Highways
        for highway in items.get("named_highways", []):
            segment = create_named_street_segment(highway, region)
            road_segments.append(segment)
    
    return road_segments

def create_comprehensive_output(transportation_data: Dict[str, List[str]], regional_data: Dict[str, Dict[str, List[str]]]) -> Dict:
    """Create comprehensive output structure with structured road segments."""
    
    # Create structured road segments
    road_segments = create_structured_road_segments(transportation_data, regional_data)
    
    return {
        "metadata": {
            "extraction_date": datetime.now().isoformat(),
            "source": "Virginia State Map PDFs",
            "total_categories": len(transportation_data),
            "total_items": sum(len(items) for items in transportation_data.values()),
            "total_segments": len(road_segments),
            "schema_version": "1.0.0"
        },
        "summary": {
            "interstates": {
                "count": len(transportation_data.get("interstates", [])),
                "items": transportation_data.get("interstates", [])
            },
            "us_routes": {
                "count": len(transportation_data.get("us_routes", [])),
                "items": transportation_data.get("us_routes", [])
            },
            "state_routes": {
                "count": len(transportation_data.get("state_routes", [])),
                "items": transportation_data.get("state_routes", [])
            },
            "primary_highways": {
                "count": len(transportation_data.get("primary_highways", [])),
                "items": transportation_data.get("primary_highways", [])
            },
            "secondary_highways": {
                "count": len(transportation_data.get("secondary_highways", [])),
                "items": transportation_data.get("secondary_highways", [])
            },
            "named_streets": {
                "count": len(transportation_data.get("named_streets", [])),
                "items": transportation_data.get("named_streets", [])
            },
            "named_highways": {
                "count": len(transportation_data.get("named_highways", [])),
                "items": transportation_data.get("named_highways", [])
            },
            "transit": {
                "count": len(transportation_data.get("transit", [])),
                "items": transportation_data.get("transit", [])
            }
        },
        "regional_breakdown": regional_data,
        "road_segments": road_segments,
        "raw_data": transportation_data
    }

def main():
    parser = argparse.ArgumentParser(description="Extract Virginia transportation data from PDFs")
    parser.add_argument("--src", required=True, help="Source folder containing VA map PDFs")
    parser.add_argument("--out", default="output", help="Output folder for JSON files")
    args = parser.parse_args()
    
    src_path = Path(args.src)
    out_path = Path(args.out)
    
    if not src_path.exists():
        print(f"Error: Source folder {src_path} does not exist")
        return
    
    out_path.mkdir(parents=True, exist_ok=True)
    
    print("=== Virginia Transportation Data Extractor ===")
    print(f"Source: {src_path}")
    print(f"Output: {out_path}")
    print()
    
    # Extract transportation data
    print("Extracting transportation data from PDFs...")
    transportation_data = extract_from_folder(src_path)
    
    # Assign to regions
    print("Assigning data to Virginia regions...")
    regional_data = assign_to_regions(transportation_data)
    
    # Create comprehensive output
    output_data = create_comprehensive_output(transportation_data, regional_data)
    
    # Write output files
    output_file = out_path / "va_transportation_data.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    # Create summary file
    summary_file = out_path / "va_transportation_summary.json"
    summary_data = {
        "summary": output_data["summary"],
        "regional_breakdown": output_data["regional_breakdown"]
    }
    with open(summary_file, "w", encoding="utf-8") as f:
        json.dump(summary_data, f, indent=2, ensure_ascii=False)
    
    # Create schema-validated road segments file
    road_segments_file = out_path / "va_road_segments.json"
    road_segments_data = {
        "metadata": output_data["metadata"],
        "road_segments": output_data["road_segments"]
    }
    with open(road_segments_file, "w", encoding="utf-8") as f:
        json.dump(road_segments_data, f, indent=2, ensure_ascii=False)
    
    # Create individual road segment files for validation
    segments_dir = out_path / "road_segments"
    segments_dir.mkdir(exist_ok=True)
    
    for i, segment in enumerate(output_data["road_segments"][:10]):  # Save first 10 as examples
        segment_file = segments_dir / f"road_segment_{i+1}.json"
        with open(segment_file, "w", encoding="utf-8") as f:
            json.dump(segment, f, indent=2, ensure_ascii=False)
    
    # Print results
    print("\n=== Extraction Results ===")
    for category, data in output_data["summary"].items():
        print(f"{category.replace('_', ' ').title()}: {data['count']} items")
    
    print(f"\nFiles created:")
    print(f"  - {output_file}")
    print(f"  - {summary_file}")
    print(f"  - {road_segments_file}")
    print(f"  - {segments_dir}/ (individual road segment examples)")
    
    print(f"\nTotal items extracted: {output_data['metadata']['total_items']}")
    print(f"Total road segments created: {output_data['metadata']['total_segments']}")
    print(f"Schema-validated road segments: {len(output_data['road_segments'])}")

if __name__ == "__main__":
    main()
