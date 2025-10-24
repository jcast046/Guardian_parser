#!/usr/bin/env python3
"""
Sample execution script for the Guardian Parser Pack.

This script demonstrates how to run the Guardian parser on a collection of PDF files
from multiple sources (NamUs, NCMEC, FBI, Charley Project). It automatically discovers
PDF files in the evidence directories and processes them with geocoding enabled.

Key Features:
    - Automatic PDF discovery from multiple source directories
    - Virtual environment activation
    - Geocoding with caching enabled
    - Structured output to JSONL and CSV formats

Directory Structure Expected:
    evidence/
    ├── namus/           # NamUs case PDFs
    ├── ncmec/           # NCMEC poster PDFs
    ├── FBI/             # FBI missing person posters
    └── the_charley_project/  # Charley Project case PDFs

Output Files:
    - guardian_output.jsonl    # Structured JSON records
    - guardian_output.csv      # Flattened CSV format
    - geocode_cache.json       # Cached geocoding results

Author: Joshua Castillo

"""

import glob
import sys
import pathlib
import subprocess
from typing import List

# Get the root directory of this script
ROOT: pathlib.Path = pathlib.Path(__file__).parent.resolve()

def discover_pdf_files() -> List[str]:
    """
    Discover PDF files from multiple evidence source directories.
    
    Searches for PDF files in the expected evidence directory structure
    and returns a list of all discovered files.
    
    Returns:
        List[str]: List of paths to discovered PDF files
        
    Expected Directory Structure:
        evidence/namus/**/*.pdf
        evidence/ncmec/**/*.pdf
        evidence/FBI/**/*.pdf
        evidence/the_charley_project/**/*.pdf
        
    Note:
        Paths are hardcoded for specific user directory structure.
        Modify paths below to match local setup.
    """
    inputs: List[str] = []
    
    namus_files = glob.glob(r"C:\Users\N0Cir\CS697\evidence\namus\**\*.pdf", recursive=True)
    inputs.extend(namus_files)
    print(f"Found {len(namus_files)} NamUs PDF files")
    
    ncmec_files = glob.glob(r"C:\Users\N0Cir\CS697\evidence\ncmec\**\*.pdf", recursive=True)
    inputs.extend(ncmec_files)
    print(f"Found {len(ncmec_files)} NCMEC PDF files")
    
    fbi_files = glob.glob(r"C:\Users\N0Cir\CS697\evidence\FBI\**\*.pdf", recursive=True)
    inputs.extend(fbi_files)
    print(f"Found {len(fbi_files)} FBI PDF files")
    
    charley_files = glob.glob(r"C:\Users\N0Cir\CS697\evidence\the_charley_project\**\*.pdf", recursive=True)
    inputs.extend(charley_files)
    print(f"Found {len(charley_files)} Charley Project PDF files")
    
    return inputs

def build_parser_command(pdf_files: List[str]) -> List[str]:
    """
    Build the command line arguments for the Guardian parser.
    
    Constructs command line arguments for subprocess execution of the
    Guardian parser with appropriate input and output paths.
    
    Args:
        pdf_files (List[str]): List of PDF file paths to process
        
    Returns:
        List[str]: Command line arguments for subprocess execution
        
    Note:
        Includes virtual environment Python path, parser script, input files,
        output paths, and geocoding options with cache configuration.
    """
    cmd: List[str] = [
        str(ROOT / ".venv/Scripts/python.exe"),
        str(ROOT / "parser_pack.py"),
        "--inputs", *pdf_files,
        "--jsonl", str(ROOT / "output" / "guardian_output.jsonl"),
        "--csv", str(ROOT / "output" / "guardian_output.csv"),
        "--geocode",
        "--geocode-cache", str(ROOT / "output" / "geocode_cache.json")
    ]
    return cmd

def main() -> None:
    """
    Main execution function for the sample run.
    
    Orchestrates the complete PDF processing pipeline:
    1. Discovers PDF files from evidence directories
    2. Builds the parser command with appropriate arguments
    3. Executes the parser with error handling
    4. Reports completion status
    
    Raises:
        SystemExit: On execution errors or missing files
    """
    try:
        pdf_files = discover_pdf_files()
        
        if not pdf_files:
            print("No PDF files found in evidence directories.")
            print("Please ensure the following directories exist and contain PDF files:")
            print("  - C:\\Users\\N0Cir\\CS697\\evidence\\namus\\")
            print("  - C:\\Users\\N0Cir\\CS697\\evidence\\ncmec\\")
            print("  - C:\\Users\\N0Cir\\CS697\\evidence\\FBI\\")
            print("  - C:\\Users\\N0Cir\\CS697\\evidence\\the_charley_project\\")
            return
        
        print(f"Total PDF files to process: {len(pdf_files)}")
        
        cmd = build_parser_command(pdf_files)
        
        print("Executing command:")
        print(" ".join(cmd))
        print("-" * 80)
        
        result = subprocess.run(cmd, check=False)
        
        if result.returncode == 0:
            print("-" * 80)
            print("Processing completed successfully!")
            print(f"Output files created:")
            print(f"   - {ROOT / 'guardian_output.jsonl'}")
            print(f"   - {ROOT / 'guardian_output.csv'}")
            print(f"   - {ROOT / 'geocode_cache.json'}")
        else:
            print("-" * 80)
            print(f"Processing failed with exit code: {result.returncode}")
            print("Check the output above for error details.")
            
    except Exception as e:
        print(f"Error during execution: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
