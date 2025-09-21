#!/usr/bin/env python3
"""
Guardian Parser Pack - Data Extraction Runner
============================================

Streamlined script to run all data extraction processes in sequence:
- Virginia transportation data extraction
- OSM road segment import
- Virginia transit network extraction

Usage:
    python extract_all_data.py [--clean] [--transport-only] [--osm-only] [--transit-only]
"""

import argparse
import os
import sys
import subprocess
import time
from pathlib import Path


def run_command(cmd, description):
    """Run a command and handle errors gracefully."""
    print(f"\n {description}...")
    print(f"Command: {cmd}")
    
    try:
        result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        print(f" {description} completed successfully")
        if result.stdout:
            print(f"Output: {result.stdout.strip()}")
        return True
    except subprocess.CalledProcessError as e:
        print(f" {description} failed with error: {e}")
        if e.stderr:
            print(f"Error details: {e.stderr.strip()}")
        return False


def create_directories():
    """Create necessary output directories."""
    directories = ["output", "data", "data/samples"]
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
    print(" Output directories created")


def clean_outputs():
    """Clean output files."""
    files_to_clean = [
        "output/osm_richmond_segments.json",
        "data/va_transit.json", 
        "data/va_transportation_data.json",
        "data/va_road_segments.json"
    ]
    
    cleaned_count = 0
    for file_path in files_to_clean:
        if os.path.exists(file_path):
            os.remove(file_path)
            cleaned_count += 1
            print(f"  Removed {file_path}")
    
    if cleaned_count == 0:
        print("  No files to clean")
    else:
        print(f" Cleaned {cleaned_count} files")


def extract_transportation_data():
    """Extract Virginia transportation data from state maps."""
    # Check if VA map source directory exists
    va_map_dir = "C:/Users/N0Cir/CS697/VA_State_Map"
    if not os.path.exists(va_map_dir):
        print(f"  VA map directory not found: {va_map_dir}")
        print("   Skipping transportation data extraction")
        return False
    
    cmd = f'python scripts/va_transport_extractor.py --src "{va_map_dir}" --out "data"'
    return run_command(cmd, "Virginia transportation data extraction")


def import_osm_segments():
    """Import OSM road segments for Richmond."""
    cmd = 'python scripts/osm_import.py --osm --place "Richmond, Virginia, USA" --rl-regions "data/va_rl_regions.geojson" --out "output/osm_richmond_segments.json"'
    return run_command(cmd, "OSM road segment import for Richmond")


def extract_transit_network():
    """Extract Virginia transit network."""
    cmd = 'python scripts/va_transit_extractor.py --place "Virginia, USA" --regional --out "data/va_transit.json"'
    return run_command(cmd, "Virginia transit network extraction")


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description="Guardian Parser Pack - Data Extraction Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python extract_all_data.py              # Run all extractions
  python extract_all_data.py --clean      # Clean outputs first
  python extract_all_data.py --transport-only  # Only transportation data
  python extract_all_data.py --osm-only   # Only OSM import
  python extract_all_data.py --transit-only     # Only transit extraction
        """
    )
    
    parser.add_argument("--clean", action="store_true", 
                       help="Clean output files before extraction")
    parser.add_argument("--transport-only", action="store_true",
                       help="Run only transportation data extraction")
    parser.add_argument("--osm-only", action="store_true",
                       help="Run only OSM segment import")
    parser.add_argument("--transit-only", action="store_true",
                       help="Run only transit network extraction")
    
    args = parser.parse_args()
    
    print(" Guardian Parser Pack - Data Extraction Runner")
    print("=" * 50)
    
    start_time = time.time()
    
    # Clean outputs if requested
    if args.clean:
        clean_outputs()
    
    # Create directories
    create_directories()
    
    # Determine which extractions to run
    if args.transport_only:
        extractions = [("transportation", extract_transportation_data)]
    elif args.osm_only:
        extractions = [("OSM import", import_osm_segments)]
    elif args.transit_only:
        extractions = [("transit", extract_transit_network)]
    else:
        extractions = [
            ("transportation", extract_transportation_data),
            ("OSM import", import_osm_segments),
            ("transit", extract_transit_network)
        ]
    
    # Run extractions
    success_count = 0
    total_count = len(extractions)
    
    for name, func in extractions:
        if func():
            success_count += 1
        else:
            print(f"  {name} extraction failed, continuing with remaining extractions...")
    
    # Summary
    end_time = time.time()
    duration = end_time - start_time
    
    print("\n" + "=" * 50)
    print(" EXTRACTION SUMMARY")
    print("=" * 50)
    print(f" Successful: {success_count}/{total_count}")
    print(f"  Duration: {duration:.1f} seconds")
    
    if success_count == total_count:
        print(" All data extractions completed successfully!")
        print("\n Generated files:")
        output_files = [
            "data/va_transportation_data.json",
            "output/osm_richmond_segments.json", 
            "data/va_transit.json"
        ]
        for file_path in output_files:
            if os.path.exists(file_path):
                size = os.path.getsize(file_path) / 1024 / 1024  # MB
                print(f"    {file_path} ({size:.1f} MB)")
    else:
        print(f"  {total_count - success_count} extractions failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
