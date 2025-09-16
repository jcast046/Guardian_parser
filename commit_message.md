# Fix parsing issues and implement Virginia geocoding override

## Summary
This commit addresses multiple parsing issues across different data sources (NCMEC, NamUs, FBI) and implements a Virginia-specific geocoding override system that ensures all non-Virginia locations are converted to Virginia localities.

## Major Changes

### 1. NCMEC Parser Improvements
- **Fixed incident summary cutoff**: Enhanced regex patterns to capture longer narratives and contextual information
- **Resolved boilerplate text issues**: Added negative lookaheads and post-processing filters to exclude "Scan, View, & Share" text
- **Fixed poster title capture**: Reordered patterns to prioritize actual incident details over poster titles like "MISSING CHILD [NAME]"
- **Improved behavioral patterns**: Refined regex to focus on actual behaviors and exclude clothing descriptions

### 2. NamUs Parser Enhancements
- **Fixed AKA/aliases extraction**: Replaced broad regex with specific patterns and added validation to filter out demographic keywords
- **Corrected hair color parsing**: Modified regex to capture only valid hair colors and exclude descriptive text
- **Enhanced distinctive features**: Implemented specific patterns for scars, tattoos, and birthmarks with validation and deduplication
- **Fixed agency extraction**: Added specific patterns for law enforcement agencies and validation to exclude administrative text

### 3. FBI Parser Updates
- **Added agency name extraction**: Implemented logic to extract "FBI Field Office" and local law enforcement agency names
- **Fixed location parsing**: Enhanced regex to capture full state names (e.g., "North Carolina" instead of "North")
- **Improved source detection**: Added pattern to correctly identify FBI documents with "Field Office: [Location]"
- **Cleaned agency names**: Added logic to remove duplicate "the" and leading "the" from agency names

### 4. Virginia Geocoding Override System
- **Implemented location boundary checking**: Added `is_location_in_virginia()` function to verify coordinates within VA boundaries
- **Created Virginia override logic**: Added `geocode_city_state_with_va_override()` function to convert non-VA locations to Richmond, VA
- **Updated cache management**: Modified geocoding to cache Virginia coordinates with Virginia locality names
- **Fixed cache filtering**: Corrected Virginia location detection logic to prevent false positives (e.g., "georgia" containing "va")

### 5. Output Organization
- **Moved all output files to output folder**: Updated default paths for JSONL, CSV, and geocode cache files
- **Added directory creation**: Ensured output directory exists before writing files
- **Updated sample runner**: Modified `sample_run.py` to use output folder paths

## Technical Details

### Regex Pattern Improvements
- Added negative lookaheads to exclude boilerplate text
- Implemented more specific patterns for demographic fields
- Enhanced validation with keyword filtering and length constraints
- Added deduplication logic for distinctive features

### Geocoding Architecture
- Virginia boundary check: 36.5°N to 39.5°N, 75.2°W to 83.7°W
- Default Virginia location: Richmond, VA (37.5407, -77.4360)
- Cache key format: "richmond|virginia|{cache_key_extra}"
- Non-Virginia locations return Richmond coordinates but maintain original location text

### File Structure Changes
```
guardian_parser_pack/
├── parser_pack.py (updated)
├── sample_run.py (updated)
└── output/ (all generated files)
    ├── guardian_output.jsonl
    ├── guardian_output.csv
    └── geocode_cache.json
```

## Validation Results
-  NCMEC incident summaries now capture full narratives
-  NamUs fields contain relevant data instead of boilerplate text
-  FBI agency names are properly extracted
-  All non-Virginia locations return Virginia coordinates
-  Geocode cache contains only Virginia locality names
-  Output files are organized in dedicated folder

## Breaking Changes
- Output files now default to `output/` folder instead of root directory
- Geocode cache now uses Virginia locality names for all entries
- Non-Virginia locations in output show Virginia coordinates but maintain original location text

## Files Modified
- `parser_pack.py`: Core parsing logic and geocoding system
- `sample_run.py`: Updated output paths
- `output/geocode_cache.json`: Cleaned of non-Virginia entries
