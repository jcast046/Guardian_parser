@echo off
REM Guardian Parser Pack - Data Extraction Batch Script
REM Run all data extraction scripts in sequence

echo  Guardian Parser Pack - Data Extraction Runner
echo ================================================

REM Create output directories
echo Creating output directories...
if not exist "output" mkdir output
if not exist "data" mkdir data
if not exist "data\samples" mkdir data\samples
echo  Output directories created

REM Extract Virginia transportation data
echo.
echo  Extracting Virginia transportation data...
if exist "C:\Users\N0Cir\CS697\VA_State_Map" (
    python scripts\va_transport_extractor.py --src "C:\Users\N0Cir\CS697\VA_State_Map" --out "data"
    if %errorlevel% neq 0 (
        echo  Transportation data extraction failed
        goto :error
    )
    echo  Virginia transportation data extracted
) else (
    echo   VA map directory not found, skipping transportation extraction
)

REM Import OSM road segments for Richmond
echo.
echo  Importing OSM road segments for Richmond...
python scripts\osm_import.py --osm --place "Richmond, Virginia, USA" --rl-regions "data\va_rl_regions.geojson" --out "output\osm_richmond_segments.json"
if %errorlevel% neq 0 (
    echo  OSM import failed
    goto :error
)
echo  OSM road segments imported

REM Extract Virginia transit network
echo.
echo  Extracting Virginia transit network...
python scripts\va_transit_extractor.py --place "Virginia, USA" --regional --out "data\va_transit.json"
if %errorlevel% neq 0 (
    echo  Transit network extraction failed
    goto :error
)
echo  Virginia transit network extracted

REM Success summary
echo.
echo ================================================
echo  EXTRACTION SUMMARY
echo ================================================
echo  All data extractions completed successfully!
echo.
echo  Generated files:
echo     data\va_transportation_data.json
echo     output\osm_richmond_segments.json
echo     data\va_transit.json
echo.
echo  Ready for Guardian model training!
goto :end

:error
echo.
echo  Data extraction failed. Check the error messages above.
exit /b 1

:end
pause
