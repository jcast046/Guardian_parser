#!/usr/bin/env python3
"""
Transform Virginia Transit Data

Convert the current va_transit.json into a statewide, schema-stable, versioned dataset
that conforms to the transit_line.schema.json and transit_stop.schema.json schemas.
"""

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

def assign_region(lat: float, lon: float) -> str:
    """Assign Virginia region based on coordinates."""
    # Simplified regional assignment based on coordinates
    if lat > 38.5 and lon < -77.0:
        return "Northern Virginia"
    elif lat > 37.5 and lon < -77.5:
        return "Central Virginia"
    elif lat > 36.5 and lon < -76.0:
        return "Tidewater"
    elif lat < 37.0 and lon < -80.0:
        return "Southwest"
    elif lat > 38.0 and lon > -79.0:
        return "Valley"
    elif lat < 37.5 and lon > -78.0:
        return "Southside"
    else:
        return "Unknown"

def assign_region_tag_rl(region: str) -> str:
    """Map region to RL tag."""
    mapping = {
        "Northern Virginia": "NoVA",
        "Central Virginia": "Piedmont", 
        "Tidewater": "Tidewater",
        "Southwest": "Appalachia",
        "Valley": "Shenandoah",
        "Western Virginia": "Shenandoah",
        "Northern Neck": "Tidewater",
        "Southside": "Piedmont",
        "Unknown": "Unknown"
    }
    return mapping.get(region, "Unknown")

def transform_station_to_stop(station: Dict[str, Any]) -> Dict[str, Any]:
    """Transform a station record to conform to transit_stop.schema.json."""
    coords = station.get("geometry", {}).get("coordinates", [0, 0])
    lat, lon = coords[1], coords[0]
    region = assign_region(lat, lon)
    
    # Extract name from tags or use "Unnamed"
    name = station.get("name", "Unnamed")
    if name == "Unnamed" and station.get("tags", {}).get("name"):
        name = station["tags"]["name"]
    
    # Determine stop type
    stop_type = station.get("type", "bus_stop")
    if station.get("tags", {}).get("railway") == "station":
        stop_type = "station"
    elif station.get("tags", {}).get("railway") == "halt":
        stop_type = "halt"
    elif station.get("tags", {}).get("public_transport") == "platform":
        stop_type = "platform"
    
    return {
        "id": station.get("id", str(uuid.uuid4())),
        "name": name,
        "type": stop_type,
        "operator": station.get("operator"),
        "network": station.get("network"),
        "geometry": {
            "type": "Point",
            "coordinates": coords
        },
        "platforms": [],  # Could be extracted from tags
        "lines": [],  # Would need to be linked to lines
        "facilities": {
            "shelter": station.get("tags", {}).get("shelter") == "yes",
            "bench": station.get("tags", {}).get("bench") == "yes",
            "lighting": station.get("tags", {}).get("lighting") == "yes",
            "ticketMachine": station.get("tags", {}).get("ticket_machine") == "yes",
            "waitingRoom": station.get("tags", {}).get("waiting_room") == "yes",
            "restroom": station.get("tags", {}).get("toilets") == "yes",
            "parking": station.get("tags", {}).get("parking") == "yes",
            "bikeRack": station.get("tags", {}).get("bicycle_parking") == "yes",
            "wifi": station.get("tags", {}).get("wifi") == "yes"
        },
        "accessibility": {
            "wheelchairAccessible": station.get("tags", {}).get("wheelchair") == "yes",
            "audioAnnouncements": station.get("tags", {}).get("tactile_paving") == "yes",
            "visualAnnouncements": station.get("tags", {}).get("visual_announcements") == "yes",
            "elevatorAccess": station.get("tags", {}).get("elevator") == "yes",
            "tactilePaving": station.get("tags", {}).get("tactile_paving") == "yes",
            "rampAccess": station.get("tags", {}).get("ramp") == "yes"
        },
        "admin": {
            "region": region,
            "regionTagRL": assign_region_tag_rl(region),
            "countyFips": None,  # Could be geocoded
            "placeFips": None,    # Could be geocoded
            "inState": True
        },
        "tags": station.get("tags", {}),
        "provenance": {
            "source": "OpenStreetMap via OSMnx",
            "sourceDoc": None,
            "parserVersion": "1.0.0",
            "extractedAt": datetime.utcnow().isoformat() + "Z",
            "confidence": 0.8  # Default confidence
        }
    }

def create_transit_line_from_stops(stops: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Create a transit line from a group of stops (simplified)."""
    if not stops:
        return None
    
    # Group stops by operator/network
    lines = {}
    for stop in stops:
        operator = stop.get("operator") or "Unknown"
        network = stop.get("network") or "Unknown"
        key = f"{operator}_{network}"
        
        if key not in lines:
            lines[key] = {
                "operator": operator,
                "network": network,
                "stops": []
            }
        lines[key]["stops"].extend(stop.get("lines", []))
    
    # Create line records (simplified - would need more complex logic for real lines)
    transit_lines = []
    for key, line_data in lines.items():
        if len(line_data["stops"]) > 1:  # Only create lines with multiple stops
            transit_lines.append({
                "id": str(uuid.uuid4()),
                "name": f"{line_data['operator']} {line_data['network']}",
                "type": "bus",  # Default to bus
                "operator": line_data["operator"],
                "network": line_data["network"],
                "routeNumber": None,
                "color": None,
                "geometry": {
                    "type": "LineString",
                    "coordinates": []  # Would need to be calculated
                },
                "lengthMiles": None,
                "stops": [stop["id"] for stop in line_data["stops"]],
                "servicePatterns": None,
                "admin": {
                    "region": "Unknown",
                    "regionTagRL": "Unknown",
                    "countyFips": None,
                    "placeFips": None,
                    "inState": True
                },
                "accessibility": None,
                "provenance": {
                    "source": "OpenStreetMap via OSMnx",
                    "sourceDoc": None,
                    "parserVersion": "1.0.0",
                    "extractedAt": datetime.utcnow().isoformat() + "Z",
                    "confidence": 0.6
                }
            })
    
    return transit_lines

def transform_transit_data(input_file: str, output_file: str):
    """Transform the transit data to schema-stable format."""
    
    print(f"Loading transit data from {input_file}...")
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    print(f"Found {data['metadata']['total_stations']} stations, {data['metadata']['total_lines']} lines")
    
    # Transform stations to stops
    print("Transforming stations to schema-compliant stops...")
    stops = []
    for station in data.get("stations", []):
        stop = transform_station_to_stop(station)
        stops.append(stop)
    
    # Create transit lines (simplified)
    print("Creating transit lines...")
    lines = create_transit_line_from_stops(stops)
    
    # Create the new dataset structure
    dataset = {
        "metadata": {
            "version": "1.0.0",
            "schema_version": "2024-09-19",
            "extraction_date": datetime.utcnow().isoformat() + "Z",
            "source": "OpenStreetMap via OSMnx",
            "coverage": "Virginia, USA",
            "total_stops": len(stops),
            "total_lines": len(lines) if lines else 0,
            "regions": list(set(stop["admin"]["region"] for stop in stops)),
            "operators": list(set(stop["operator"] for stop in stops if stop["operator"])),
            "networks": list(set(stop["network"] for stop in stops if stop["network"]))
        },
        "stops": stops,
        "lines": lines if lines else []
    }
    
    # Write the transformed data
    print(f"Writing transformed data to {output_file}...")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(dataset, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Transformation complete!")
    print(f"   - {len(stops)} stops created")
    print(f"   - {len(lines) if lines else 0} lines created")
    print(f"   - Regions: {', '.join(dataset['metadata']['regions'])}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Transform Virginia transit data to schema-stable format")
    parser.add_argument("--input", default="data/va_transit.json", help="Input transit data file")
    parser.add_argument("--output", default="data/va_transit_v1.json", help="Output transformed data file")
    
    args = parser.parse_args()
    
    transform_transit_data(args.input, args.output)
