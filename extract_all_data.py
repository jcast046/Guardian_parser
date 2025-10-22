#!/usr/bin/env python3
"""
Guardian Parser Pack - Data Extraction Runner
============================================

Streamlined script to run all data extraction processes in sequence for the
Guardian Parser Pack system. This script orchestrates the complete data
extraction pipeline including transportation data, OSM imports, and transit
network extraction.

Features:
    - Virginia transportation data extraction from state maps
    - OpenStreetMap road segment import with regional classification
    - Virginia transit network extraction using regional approach
    - Comprehensive error handling and progress reporting
    - Selective execution modes for specific data types
    - Clean mode for fresh data extraction

Dependencies:
    subprocess, argparse, pathlib, os, sys, time

Usage:
    # Run all extractions
    python extract_all_data.py
    
    # Clean and run all extractions
    python extract_all_data.py --clean
    
    # Run specific extractions only
    python extract_all_data.py --transport-only  # Virginia transportation data
    python extract_all_data.py --osm-only        # OSM road segments
    python extract_all_data.py --transit-only   # Transit network

Output:
    Creates comprehensive datasets in the data/ directory:
    - va_transportation_data.json: Virginia road network data
    - va_transit.json: Virginia transit network (2,359+ stations)
    - osm_richmond_segments.json: OSM road segments for Richmond

Author: Joshua Castillo
"""

import argparse
import os
import sys
import subprocess
import time
from pathlib import Path


def run_command(cmd, description, max_retries=2):
    """
    Run a command and handle errors gracefully with retry logic.
    
    Executes a shell command with comprehensive error handling, progress reporting,
    and retry mechanism. Provides detailed output for debugging and monitoring.
    
    Args:
        cmd (str): Shell command to execute
        description (str): Human-readable description of the command
        max_retries (int): Maximum number of retry attempts (default: 2)
        
    Returns:
        bool: True if command succeeded, False if failed after all retries
        
    Note:
        This function uses subprocess.run with shell=True for cross-platform
        compatibility. Commands are executed with timeout protection and retry logic.
    """
    print(f"\n {description}...")
    print(f"Command: {cmd}")
    
    for attempt in range(max_retries + 1):
        try:
            if attempt > 0:
                print(f"  Retry attempt {attempt}/{max_retries}...")
                time.sleep(5)  # Wait 5 seconds before retry
            
            result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True, timeout=1800)
            print(f" {description} completed successfully")
            if result.stdout:
                print(f"Output: {result.stdout.strip()}")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f" {description} failed with error: {e}")
            if e.stderr:
                print(f"Error details: {e.stderr.strip()}")
            if e.stdout:
                print(f"Command output: {e.stdout.strip()}")
            
            if attempt < max_retries:
                print(f"  Will retry in 5 seconds...")
                continue
            else:
                print(f"  Failed after {max_retries + 1} attempts")
                return False
                
        except subprocess.TimeoutExpired as e:
            print(f" {description} timed out after 30 minutes")
            if attempt < max_retries:
                print(f"  Will retry in 5 seconds...")
                continue
            else:
                print(f"  Failed after {max_retries + 1} attempts due to timeout")
                return False
                
        except Exception as e:
            print(f" {description} failed with unexpected error: {e}")
            if attempt < max_retries:
                print(f"  Will retry in 5 seconds...")
                continue
            else:
                print(f"  Failed after {max_retries + 1} attempts due to unexpected error")
                return False
    
    return False


def run_diagnostics():
    """Run system diagnostics to identify potential issues."""
    print("\n Running system diagnostics...")
    
    # Check Python version
    import sys
    print(f" Python version: {sys.version}")
    
    # Check required dependencies
    missing_deps = []
    try:
        import osmnx
        print(f" OSMnx: {osmnx.__version__}")
    except ImportError:
        missing_deps.append("osmnx")
        print(" OSMnx: MISSING")
    
    try:
        import geopandas
        print(f" GeoPandas: {geopandas.__version__}")
    except ImportError:
        missing_deps.append("geopandas")
        print(" GeoPandas: MISSING")
    
    try:
        import shapely
        print(f" Shapely: {shapely.__version__}")
    except ImportError:
        missing_deps.append("shapely")
        print(" Shapely: MISSING")
    
    try:
        import pandas
        print(f" Pandas: {pandas.__version__}")
    except ImportError:
        missing_deps.append("pandas")
        print(" Pandas: MISSING")
    
    # Check required files
    required_files = [
        "data/va_rl_regions.geojson",
        "scripts/osm_import.py",
        "scripts/va_transit_extractor.py",
        "scripts/va_transport_extractor.py"
    ]
    
    missing_files = []
    for file_path in required_files:
        if os.path.exists(file_path):
            print(f" {file_path}: OK")
        else:
            missing_files.append(file_path)
            print(f" {file_path}: MISSING")
    
    # Check VA map directory
    va_map_dir = "C:/Users/N0Cir/CS697/VA_State_Map"
    if os.path.exists(va_map_dir):
        print(f" VA map directory: OK")
    else:
        print(f" VA map directory: MISSING ({va_map_dir})")
    
    # Summary
    if missing_deps or missing_files:
        print(f"\n DIAGNOSTIC SUMMARY:")
        if missing_deps:
            print(f" Missing dependencies: {', '.join(missing_deps)}")
            print(f" Install with: pip install {' '.join(missing_deps)}")
        if missing_files:
            print(f" Missing files: {', '.join(missing_files)}")
        return False
    else:
        print(f"\n All diagnostics passed!")
        return True


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


def test_individual_scripts():
    """Test each extraction script individually to isolate issues."""
    print("\n Testing individual extraction scripts...")
    
    # Test OSM import with a smaller area first
    print("\n1. Testing OSM import with smaller area...")
    cmd = 'python scripts/osm_import.py --osm --place "Alexandria, Virginia, USA" --out "output/test_osm.json"'
    if run_command(cmd, "OSM import test (Alexandria)"):
        print("   OSM import test: PASSED")
        # Clean up test file
        if os.path.exists("output/test_osm.json"):
            os.remove("output/test_osm.json")
    else:
        print("   OSM import test: FAILED")
        return False
    
    # Test transit extraction with a single city
    print("\n2. Testing transit extraction with single city...")
    cmd = 'python scripts/va_transit_extractor.py --place "Richmond, Virginia, USA" --out "output/test_transit.json"'
    if run_command(cmd, "Transit extraction test (Richmond)"):
        print("   Transit extraction test: PASSED")
        # Clean up test file
        if os.path.exists("output/test_transit.json"):
            os.remove("output/test_transit.json")
    else:
        print("   Transit extraction test: FAILED")
        return False
    
    print("\n All individual script tests passed!")
    return True


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
  python extract_all_data.py --diagnose   # Run system diagnostics
  python extract_all_data.py --test       # Test individual scripts
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
    parser.add_argument("--diagnose", action="store_true",
                       help="Run system diagnostics and exit")
    parser.add_argument("--test", action="store_true",
                       help="Test individual scripts with smaller datasets")
    
    args = parser.parse_args()
    
    print(" Guardian Parser Pack - Data Extraction Runner")
    print("=" * 50)
    
    # Run diagnostics if requested
    if args.diagnose:
        if run_diagnostics():
            print("\n System is ready for data extraction!")
            sys.exit(0)
        else:
            print("\n Please fix the issues above before running extractions.")
            sys.exit(1)
    
    # Test individual scripts if requested
    if args.test:
        create_directories()
        if test_individual_scripts():
            print("\n Individual script tests passed! You can now run full extractions.")
            sys.exit(0)
        else:
            print("\n Some script tests failed. Please check the errors above.")
            sys.exit(1)
    
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
