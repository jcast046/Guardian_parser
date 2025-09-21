# Guardian Parser Pack

The **Guardian Parser Pack** is a comprehensive pipeline for converting unstructured missing person case PDFs (from **NamUs**, **NCMEC**, **FBI**, and **The Charley Project**) into a unified, structured format based on the **Guardian JSON Schema**. It extracts demographic, spatial, temporal, outcome, and narrative/OSINT fields, normalizes them, and outputs both JSONL and CSV files for downstream analysis and synthetic data generation.

The system also provides extensive **Virginia transportation data extraction** including statewide transit networks, road segments, and regional transportation infrastructure.

All data mined will be used for model training in the project  -> [Guardian](https://github.com/jcast046/Guardian) <-

## Features

### Core Parser Functionality
*  **Multi-source parsing** – Handles NamUs, NCMEC, FBI, and Charley Project PDF layouts
*  **Standardization** – Conforms to the Guardian unified schema (`schemas/guardian_case.schema.json`)
*  **Geocoding** – Fills in missing lat/lon from city/state with caching
*  **Post-processing** – Fixes missing `last_seen_ts` from narrative text
*  **Batch execution** – Processes entire evidence folders in one command
*  **Structured output** – JSONL for modeling, CSV for inspection

### Virginia Transportation Data
*  **Statewide transit extraction** – 2,359+ transit stations across Virginia metropolitan areas
*  **Regional approach** – Processes 14+ major Virginia regions separately for comprehensive coverage
*  **Road network data** – Virginia transportation infrastructure with geometry and bearings
*  **OSM integration** – OpenStreetMap road segment import with detailed metadata
*  **Transit network analysis** – Rail, metro, bus stations with regional breakdown

## Directory Layout

```
guardian_parser_pack/
├── parser_pack.py                    # Core parser with enhanced data extraction
├── sample_run.py                     # Batch runner / discovery script
├── extract_all_data.py               # Unified data extraction runner
├── requirements.txt                  # Python dependencies
├── Makefile                          # Build automation (Windows)
├── run_extractions.bat               # Batch extraction script
├── README.md                         # This file
├── data/                             # Data files and outputs
│   ├── va_transportation_data.json   # Virginia road names & highway classes
│   ├── va_transit.json               # Virginia transit network (2,359+ stations)
│   ├── va_road_segments.json         # Virginia road segments with geometry
│   ├── va_transit.json               # Virginia transit data
│   ├── va_transportation_summary.json # Transportation data summary
│   ├── guardian_output.csv           # Parsed data (CSV format)
│   ├── guardian_output.jsonl         # Parsed data (JSONL format)
│   └── output/                       # Output directory
│       ├── geocode_cache.json        # Geocoding cache
│       └── osm_richmond_segments.json # OSM road segments
├── schemas/                          # JSON schemas
│   ├── guardian_case.schema.json     # Guardian case schema
│   ├── guardian_schema.json          # Legacy Guardian schema
│   ├── road_segment.schema.json      # Road segment schema
│   ├── transit_line.schema.json      # Transit line schema
│   └── transit_stop.schema.json      # Transit stop schema
└── scripts/                          # Extraction and import scripts
    ├── va_transport_extractor.py     # Virginia transport data extraction
    ├── va_transit_extractor.py       # Virginia transit network extraction (regional)
    ├── osm_import.py                 # OpenStreetMap road segment import
    └── transform_transit_data.py     # Transit data transformation
```

## Setup

1. Clone or copy this folder into your workspace (e.g., `CS697/guardian_parser_pack/`).
2. Create and activate a virtual environment:

   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1   # Windows PowerShell
   ```
3. Install dependencies:

   ```powershell
   pip install -r requirements.txt
   ```

   For OSM import functionality, install additional dependencies:
   ```powershell
   pip install osmnx geopandas shapely pyproj rtree
   ```

## Usage

### Batch Run 

Runs the parser across **all evidence folders** with geocoding and caching enabled:

```powershell
& .\.venv\Scripts\python.exe .\sample_run.py
```

This will:

* Discover PDFs under `evidence/namus`, `evidence/ncmec`, `evidence/the_charley_project`
* Parse and normalize all cases

### Data Extraction (Streamlined)
```bash
# Run all data extractions in one command
python extract_all_data.py

# Clean and run all extractions
python extract_all_data.py --clean

# Run specific extractions only
python extract_all_data.py --transport-only  # Virginia transportation data
python extract_all_data.py --osm-only        # OSM road segments
python extract_all_data.py --transit-only   # Transit network

# Alternative: Use Makefile (Windows)
make                    # Run all extractions
make transport          # Transportation data only
make osm               # OSM import only
make transit           # Transit extraction only
make clean             # Clean output files

# Alternative: Use batch script (Windows)
run_extractions.bat    # Run all extractions
```

### Virginia Transit Network Extraction

The transit extraction uses a **regional approach** to capture comprehensive statewide data:

```bash
# Extract transit data for all Virginia regions (recommended)
python scripts/va_transit_extractor.py --regional --out "data/va_transit.json"

# Extract transit data for specific regions
python scripts/va_transit_extractor.py --place "Richmond, Virginia, USA" --out "data/richmond_transit.json"

# Extract with custom region list
python scripts/va_transit_extractor.py --regional --out "data/va_transit.json
```

**Regional Coverage**: The system processes 14+ major Virginia metropolitan areas:
- Richmond, Norfolk, Virginia Beach, Hampton, Newport News
- Alexandria, Arlington, Fairfax, Chesapeake, Portsmouth, Suffolk  
- Roanoke, Lynchburg, Northern Virginia

**Transit Data Results**: 
- **2,359+ transit stations** across Virginia
- **Regional breakdown** with station counts per metropolitan area
- **Comprehensive coverage** of bus stops, rail stations, and transit hubs

### Virginia Transportation Data Extraction

```powershell
# Extract Virginia transportation data from state maps
python scripts/va_transport_extractor.py --src "C:/Users/N0Cir/CS697/VA_State_Map" --out "data"

# Extract Virginia transit network 
python scripts/va_transit_extractor.py --regional --out "data/va_transit.json"

# Import OSM road segments for specific areas
python scripts/osm_import.py --osm --place "Richmond, Virginia, USA" --rl-regions "data/va_rl_regions.geojson" --out "output/osm_richmond_segments.json"
```

## Output Formats

* **JSONL** (`data/guardian_output.jsonl`): Each line is a Guardian-conformant JSON object with readable formatting and enhanced field extraction.
* **CSV** (`data/guardian_output.csv`): Flattened table for quick inspection and data analysis.
* **Geocode Cache** (`data/geocode_cache.json`): Stores resolved city/state → lat/lon mappings for performance.

### Virginia Transportation Data

* **Road Names & Highways** (`data/va_transportation_data.json`): Virginia road names, interstates, US routes, state routes with regional categorization
* **Transit Network** (`data/va_transit.json`): 2,359+ transit stations across Virginia with regional breakdown
* **Road Segments** (`data/va_road_segments.json`): Virginia road segments with geometry and metadata
* **OSM Road Segments** (`output/osm_richmond_segments.json`): Detailed street-level road network data with geometry

## Schema

All records conform to the Guardian schema (`schemas/guardian_case.schema.json`).
Key categories:

* `demographic` (name, age, gender, race, height, weight…)
* `spatial` (city, county, state, lat/lon, nearby features)
* `temporal` (last seen, report times, sightings)
* `outcome` (status, recovery details)
* `narrative_osint` (incident summary, witnesses, media, persons of interest)

### Transit Data Schema

Transit data follows specialized schemas:
* `schemas/transit_stop.schema.json` - Transit stop/station schema
* `schemas/transit_line.schema.json` - Transit line/route schema  
* `schemas/road_segment.schema.json` - Road segment schema

## Data Sources

### Missing Person Cases
- **NamUs**: National Missing and Unidentified Persons System
- **NCMEC**: National Center for Missing & Exploited Children  
- **FBI**: Federal Bureau of Investigation missing person posters
- **Charley Project**: Charley Project case files

### Virginia Transportation Data
- **Virginia State Map PDFs**: Official VDOT transportation maps
- **OpenStreetMap**: Community-driven road network data
- **Regional Classification**: 8 Virginia regions (Northern Virginia, Central Virginia, Tidewater, Southwest, Valley, Western Virginia, Northern Neck, Southside)

## Key Features

### Data Extraction
- **Multi-source parsing** from NamUs, NCMEC, FBI, and Charley Project PDFs
- **Comprehensive geocoding** with Virginia-specific overrides
- **Schema validation** ensuring data quality
- **Date/time standardization** with UTC conversion

### Data Normalization
- **Standardized Guardian schema** format
- **Geographic coordinate normalization**
- **Name and demographic data cleaning**
- **Post-processing** for missing temporal data

### Virginia Transportation Integration
- **Road network extraction** with geometry and bearings
- **Route classification** (Interstate, US Highway, Primary/Secondary Highway)
- **Regional tagging** for RL scoring
- **Comprehensive transit network extraction** (2,359+ stations across 14+ regions)
- **Regional approach** for maximum coverage and reliability
- **Enhanced transit detection** with operator and name-based identification


