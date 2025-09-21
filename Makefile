# Guardian Parser Pack - Data Extraction Makefile
# Run all data extraction scripts in sequence

.PHONY: all clean data transport osm transit

# Default target - run all extractions
all: data transport osm transit

# Create output directories
data:
	@echo "Creating output directories..."
	@if not exist "output" mkdir output
	@if not exist "data" mkdir data
	@if not exist "data\samples" mkdir data\samples

# Extract Virginia transportation data from state maps
transport: data
	@echo "Extracting Virginia transportation data..."
	@if exist "C:\Users\N0Cir\CS697\VA_State_Map" (
		python scripts\va_transport_extractor.py --src "C:\Users\N0Cir\CS697\VA_State_Map" --out "data"
		@echo "✅ Virginia transportation data extracted"
	) else (
		@echo "⚠️  VA map directory not found, skipping transportation extraction"
	)

# Import OSM road segments for Richmond
osm: data
	@echo "Importing OSM road segments for Richmond..."
	python scripts\osm_import.py --osm --place "Richmond, Virginia, USA" --rl-regions "data\va_rl_regions.geojson" --out "output\osm_richmond_segments.json"
	@echo "✅ OSM road segments imported"

# Extract Virginia transit network
transit: data
	@echo "Extracting Virginia transit network..."
	python scripts\va_transit_extractor.py --place "Virginia, USA" --regional --out "data\va_transit.json"
	@echo "✅ Virginia transit network extracted"

# Clean output files
clean:
	@echo "Cleaning output files..."
	@if exist "output\osm_richmond_segments.json" del "output\osm_richmond_segments.json"
	@if exist "data\va_transit.json" del "data\va_transit.json"
	@if exist "data\va_transportation_data.json" del "data\va_transportation_data.json"
	@echo "✅ Output files cleaned"

# Help target
help:
	@echo "Guardian Parser Pack - Data Extraction"
	@echo "======================================"
	@echo "Available targets:"
	@echo "  all       - Run all data extractions (default)"
	@echo "  data      - Create output directories"
	@echo "  transport - Extract Virginia transportation data"
	@echo "  osm       - Import OSM road segments for Richmond"
	@echo "  transit   - Extract Virginia transit network"
	@echo "  clean     - Clean output files"
	@echo "  help      - Show this help message"
	@echo ""
	@echo "Usage:"
	@echo "  make          # Run all extractions"
	@echo "  make transport # Run only transportation extraction"
	@echo "  make clean     # Clean output files"
