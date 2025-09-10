# Guardian Parser Pack

The **Guardian Parser Pack** is a pipeline for converting unstructured missing person case PDFs (from **NamUs**, **NCMEC**, and **The Charley Project**) into a unified, structured format based on the **Guardian JSON Schema**. It extracts demographic, spatial, temporal, outcome, and narrative/OSINT fields, normalizes them, and outputs both JSONL and CSV files for downstream analysis and synthetic data generation. 

All data mined will be used for model training in the project  -> [Guardian](https://github.com/jcast046/Guardian) <-




## Features

*  **Multi-source parsing** – Handles NamUs, NCMEC, and Charley Project PDF layouts
*  **Standardization** – Conforms to the Guardian unified schema (`guardian_schema.json`)
*  **Geocoding** – Fills in missing lat/lon from city/state with caching
*  **Post-processing** – Fixes missing `last_seen_ts` from narrative text (`fix_dates.py`)
*  **Batch execution** – Processes entire evidence folders in one command (`sample_run.py`)
*  **Structured output** – JSONL for modeling, CSV for inspection



## Directory Layout

```
guardian_parser_pack/
├── parser_pack.py        # Core parser
├── fix_dates.py          # Post-processor for missing dates
├── sample_run.py         # Batch runner / discovery script
├── guardian_schema.json  # Unified Guardian schema
├── requirements.txt      # Python dependencies
├── guardian_output.jsonl # Structured JSONL output (created on run)
├── guardian_output.csv   # Flattened CSV output (created on run)
├── geocode_cache.json    # Geocode cache for speed/reproducibility
└── evidence/
    ├── namus/            # NamUs PDFs
    ├── ncmec/            # NCMEC PDFs
    └── the_charley_project/  # Charley Project PDFs
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



## Usage

### Batch Run 

Runs the parser across **all evidence folders** with geocoding and caching enabled:

```powershell
& .\.venv\Scripts\python.exe .\sample_run.py
```

This will:

* Discover PDFs under `evidence/namus`, `evidence/ncmec`, `evidence/the_charley_project`
* Parse and normalize all cases
* Write outputs:

  * `guardian_output.jsonl`
  * `guardian_output.csv`
  * `geocode_cache.json`


## Output Formats

* **JSONL** (`guardian_output.jsonl`): Each line is a Guardian-conformant JSON object.
* **CSV** (`guardian_output.csv`): Flattened table for quick inspection.
* **Geocode Cache** (`geocode_cache.json`): Stores resolved city/state → lat/lon mappings.



## Schema

All records conform to the Guardian schema (`guardian_schema.json`).
Key categories:

* `demographic` (name, age, gender, race, height, weight…)
* `spatial` (city, county, state, lat/lon, nearby features)
* `temporal` (last seen, report times, sightings)
* `outcome` (status, recovery details)
* `narrative_osint` (incident summary, witnesses, media, persons of interest)



