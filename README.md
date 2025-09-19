# Guardian Parser 

The **Guardian Parser** is a pipeline for converting unstructured missing person case PDFs (from **NamUs**, **NCMEC**, **FBI**, and **The Charley Project**) into a unified, structured format based on the **Guardian JSON Schema**. It extracts demographic, spatial, temporal, outcome, and narrative/OSINT fields, normalizes them, and outputs both JSONL and CSV files for downstream analysis and synthetic data generation. 

All data mined will be used for model training in the project  -> [Guardian](https://github.com/jcast046/Guardian) <-

## Features

*  **Multi-source parsing** – Handles NamUs, NCMEC, and Charley Project PDF layouts
*  **Standardization** – Conforms to the Guardian unified schema (`guardian_schema.json`)
*  **Geocoding** – Fills in missing lat/lon from city/state with caching
*  **Post-processing** – Fixes missing `last_seen_ts` from narrative text (`fix_dates.py`)
*  **Batch execution** – Processes entire evidence folders in one command (`sample_run.py`)
*  **Structured output** – JSONL for modeling, CSV for inspection
*  **Virginia transportation data** – Extracts road networks, transit systems, and regional data
*  **OSM integration** – OpenStreetMap road segment import with geometry and bearings

## Directory Layout

```
guardian_parser_pack/
├── parser_pack.py        # Core parser with enhanced data extraction
├── fix_dates.py          # Post-processor for missing dates
├── sample_run.py         # Batch runner / discovery script
├── guardian_schema.json  # Unified Guardian schema
├── requirements.txt      # Python dependencies
├── .gitignore           # Git ignore rules for clean repository
├── data/                # Data files and outputs
│   ├── va_transportation_data.json # Virginia road names & highway classes
│   ├── va_transit.json            # Virginia transit network (rail/metro + stations)
│   ├── va_rl_regions.geojson      # Virginia regional polygons
│   ├── va_road_segment.schema.json # Road segment schema
│   ├── guardian_output.csv         # Parsed data (CSV format)
│   ├── guardian_output.jsonl       # Parsed data (JSONL format)
│   ├── geocode_cache.json         # Geocoding cache
│   └── samples/                    # Sample data files
│       ├── osm_alexandria_segments.json
│       └── osm_richmond_segments.json
├── scripts/             # Extraction and import scripts
│   ├── va_transport_extractor.py  # Virginia transport data extraction
│   ├── osm_import.py              # OpenStreetMap road segment import
│   └── va_transit_extractor.py    # Virginia transit network extraction

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
* Write outputs to `data/` directory:

  * `data/guardian_output.jsonl` - Structured JSONL with readable formatting
  * `data/guardian_output.csv` - Flattened CSV for analysis
  * `data/geocode_cache.json` - Cached geocoding results

### Virginia Transportation Data Extraction

```powershell
# Extract Virginia transportation data from state maps
python scripts/va_transport_extractor.py --src "C:/Users/N0Cir/CS697/VA_State_Map" --out "data"

# Extract Virginia transit network
python scripts/va_transit_extractor.py --out "data/va_transit.json"

# Import OSM road segments for specific areas
python scripts/osm_import.py --osm --place "Alexandria, Virginia, USA" --rl-regions "data/va_rl_regions.geojson" --out "data/samples/alexandria_segments.json"
```

## Output Formats

* **JSONL** (`data/guardian_output.jsonl`): Each line is a Guardian-conformant JSON object with readable formatting and enhanced field extraction.
* **CSV** (`data/guardian_output.csv`): Flattened table for quick inspection and data analysis.
* **Geocode Cache** (`data/geocode_cache.json`): Stores resolved city/state → lat/lon mappings for performance.

### Virginia Transportation Data

* **Road Names & Highways** (`data/va_transportation_data.json`): Virginia road names, interstates, US routes, state routes with regional categorization
* **Transit Network** (`data/va_transit.json`): Rail/metro lines and stations across Virginia
* **OSM Road Segments** (`data/samples/osm_*_segments.json`): Detailed street-level road network data with geometry

## Schema

All records conform to the Guardian schema (`guardian_schema.json`).
Key categories:

* `demographic` (name, age, gender, race, height, weight…)
* `spatial` (city, county, state, lat/lon, nearby features)
* `temporal` (last seen, report times, sightings)
* `outcome` (status, recovery details)
* `narrative_osint` (incident summary, witnesses, media, persons of interest)

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
- **Transit network extraction** (rail/metro + stations)
