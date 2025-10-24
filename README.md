# Guardian Parser Pack

A data extraction and normalization pipeline for missing person case PDFs and Virginia transportation infrastructure. Converts unstructured PDFs from NamUs, NCMEC, FBI, and Charley Project into standardized Guardian schema format, and extracts comprehensive Virginia transportation data for model training. 

## System Architecture

### Core Components

- **`parser_pack.py`**: Core PDF parser for missing person cases
- **`extract_all_data.py`**: Unified orchestration runner for all data extractions  
- **`sample_run.py`**: Batch discovery and processing script
- **`scripts/`**: Individual extraction tools for transportation data
- **`schemas/`**: JSON schema validation files

### Data Flow

1. **PDF Ingestion**: Discovers PDFs from evidence directories (NamUs, NCMEC, FBI, Charley Project)
2. **Text Extraction**: Multi-engine PDF parsing with OCR fallback
3. **Schema Normalization**: Converts to Guardian case schema format
4. **Geocoding**: Resolves location data with caching
5. **Validation**: Schema validation and data quality checks
6. **Output Generation**: JSONL, CSV, and JSON formats

## Features

### Missing Person Case Processing
- Multi-source PDF parsing (NamUs, NCMEC, FBI, Charley Project)
- Guardian schema standardization (`schemas/guardian_schema.json`)
- Geocoding with caching for location resolution
- Post-processing for missing temporal data
- Batch processing with structured output (JSONL/CSV)

### Virginia Transportation Data
- Statewide transit network extraction (2,359+ stations)
- Regional processing across 8 Virginia regions
- Road network data with geometry and bearings
- OpenStreetMap integration with detailed metadata
- Transportation infrastructure classification

## Directory Structure

```
guardian_parser_pack/
├── parser_pack.py                    # Core PDF parser
├── extract_all_data.py              # Unified extraction runner
├── sample_run.py                    # Batch processing script
├── requirements.txt                 # Python dependencies
├── Makefile                         # Build automation
├── run_extractions.bat             # Windows batch script
├── data/                           # Output directory
│   ├── guardian_output.jsonl       # Parsed case data (JSONL)
│   ├── guardian_output.csv         # Parsed case data (CSV)
│   ├── va_transit.json            # Virginia transit network
│   ├── va_transportation_data.json # Virginia road data
│   ├── va_road_segments.json      # Road segments with geometry
│   └── va_rl_regions.geojson      # Regional boundaries
├── output/                         # Additional outputs
│   ├── geocode_cache.json         # Geocoding cache
│   └── osm_richmond_segments.json # OSM road segments
├── schemas/                        # JSON schemas
│   ├── guardian_schema.json        # Guardian schema
│   ├── road_segment.schema.json    # Road segment schema
│   ├── transit_line.schema.json    # Transit line schema
│   └── transit_stop.schema.json    # Transit stop schema
└── scripts/                        # Extraction scripts
    ├── va_transport_extractor.py   # Virginia transport data
    ├── va_transit_extractor.py     # Transit network extraction
    ├── osm_import.py              # OpenStreetMap import
    └── transform_transit_data.py   # Transit data transformation
```

## Setup

1. **Clone repository**:
   ```bash
   git clone <repository-url>
   cd guardian_parser_pack
   ```

2. **Create virtual environment**:
   ```bash
   python -m venv .venv
   # Windows
   .\.venv\Scripts\Activate.ps1
   # Linux/Mac
   source .venv/bin/activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Optional OSM dependencies** (for transportation data):
   ```bash
   pip install osmnx geopandas shapely pyproj rtree
   ```

## Quick Start

### Prerequisites Check

Run system diagnostics to verify installation:
```bash
python extract_all_data.py --diagnose
```

### Choose Your Workflow

**Option A: Missing Person Case Processing**
- Processes PDFs from NamUs, NCMEC, FBI, Charley Project
- Outputs structured Guardian schema data
- Requires evidence directories with PDF files

**Option B: Virginia Transportation Data Extraction**  
- Extracts transit networks and road data
- Outputs transportation infrastructure datasets
- Optional: Virginia State Map PDFs

**Option C: Complete Data Pipeline**
- Runs both missing person and transportation extractions
- Comprehensive data extraction for model training

## Step-by-Step Workflows

### Workflow A: Missing Person Case Processing

**Step 1: Prepare Evidence Directories**
```bash
# Create evidence directory structure
mkdir -p evidence/namus
mkdir -p evidence/ncmec  
mkdir -p evidence/FBI
mkdir -p evidence/the_charley_project

# Place PDF files in appropriate directories
# evidence/namus/*.pdf
# evidence/ncmec/*.pdf  
# evidence/FBI/*.pdf
# evidence/the_charley_project/*.pdf
```

**Step 2: Run Batch Processing**
```bash
# Automatic discovery and processing
python sample_run.py
```

**Step 3: Verify Outputs**
```bash
# Check generated files
ls -la data/guardian_output.*
ls -la output/geocode_cache.json
```

**Alternative: Direct Processing**
```bash
# Process specific files
python parser_pack.py --inputs file1.pdf file2.pdf --geocode
python parser_pack.py --inputs *.pdf --jsonl output.jsonl --csv output.csv
```

### Workflow B: Virginia Transportation Data Extraction

**Step 1: Prepare Virginia Maps (Optional)**
```bash
# Place Virginia State Map PDFs in directory
# /path/to/va_maps/*.pdf
```

**Step 2: Run Transportation Extraction**
```bash
# Extract all transportation data
python extract_all_data.py

# Or run specific extractions
python extract_all_data.py --transport-only  # Road data only
python extract_all_data.py --transit-only   # Transit network only
python extract_all_data.py --osm-only        # OSM road segments only
```

**Step 3: Verify Transportation Outputs**
```bash
# Check generated files
ls -la data/va_*.json
ls -la output/osm_richmond_segments.json
```

### Workflow C: Complete Data Pipeline

**Step 1: Run All Extractions**
```bash
# Complete pipeline
python extract_all_data.py
```

**Step 2: Process Missing Person Cases**
```bash
# If you have evidence PDFs
python sample_run.py
```

**Step 3: Verify All Outputs**
```bash
# Check all generated files
ls -la data/
ls -la output/
```

## Usage

### Missing Person Case Processing

**Batch processing** (discovers PDFs automatically):
```bash
python sample_run.py
```

**Direct parsing**:
```bash
python parser_pack.py --inputs file1.pdf file2.pdf --geocode
python parser_pack.py --inputs *.pdf --jsonl output.jsonl --csv output.csv
```

### Data Extraction

**All extractions**:
```bash
python extract_all_data.py
```

**Selective extractions**:
```bash
python extract_all_data.py --transport-only  # Virginia transportation data
python extract_all_data.py --osm-only        # OSM road segments  
python extract_all_data.py --transit-only   # Transit network
```

**Alternative methods**:
```bash
# Using Makefile
make                    # All extractions
make transport          # Transportation only
make osm               # OSM import only
make transit           # Transit only
make clean             # Clean outputs

# Using batch script (Windows)
run_extractions.bat
```

### Virginia Transportation Data

**Transit network extraction**:
```bash
python scripts/va_transit_extractor.py --regional --out "data/va_transit.json"
```

**Transportation data extraction**:
```bash
python scripts/va_transport_extractor.py --src "/path/to/va_maps" --out "data"
```

**OSM road segments**:
```bash
python scripts/osm_import.py --osm --place "Richmond, Virginia, USA" --rl-regions "data/va_rl_regions.geojson" --out "output/osm_richmond_segments.json"
```

## Output Formats

### Missing Person Cases
- **JSONL** (`data/guardian_output.jsonl`): Structured case data conforming to Guardian schema
- **CSV** (`data/guardian_output.csv`): Flattened data for analysis
- **Geocode Cache** (`output/geocode_cache.json`): Cached location mappings

### Transportation Data
- **Transit Network** (`data/va_transit.json`): 2,359+ transit stations with regional breakdown
- **Road Data** (`data/va_transportation_data.json`): Virginia road names and highway classifications
- **Road Segments** (`data/va_road_segments.json`): Road segments with geometry and metadata
- **OSM Segments** (`output/osm_richmond_segments.json`): Detailed street-level road network

## Schema

All records conform to the Guardian schema (`schemas/guardian_schema.json`):

- **`demographic`**: Name, age, gender, race, physical characteristics
- **`spatial`**: Location data, coordinates, geographic features
- **`temporal`**: Last seen dates, report times, sighting information
- **`outcome`**: Case status, recovery details
- **`narrative_osint`**: Incident summaries, witness information, media references

Transportation data uses specialized schemas:
- `schemas/transit_stop.schema.json`: Transit stop/station data
- `schemas/transit_line.schema.json`: Transit line/route data
- `schemas/road_segment.schema.json`: Road segment geometry and metadata

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

## Dependencies

### Core Dependencies
- `pdfminer.six`: PDF text extraction
- `PyPDF2`: Alternative PDF processing
- `python-dateutil`: Date parsing and normalization
- `jsonschema`: Schema validation
- `pandas`: Data manipulation
- `geopy`: Geocoding services

### Optional Dependencies
- `pytesseract`, `pillow`: OCR processing (requires Tesseract binary)
- `osmnx`, `geopandas`, `shapely`, `pyproj`, `rtree`: OpenStreetMap integration

## Development

The system supports model training for the [Guardian project](https://github.com/jcast046/Guardian). All extracted data is structured for downstream analysis and synthetic data generation.