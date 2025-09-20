#!/usr/bin/env python3
"""
Virginia Transit Data Extractor

Extract transit network (rail/metro + key bus hubs) from OpenStreetMap
for Virginia using OSMnx. Creates va_transit.json with rail lines and stations.

Usage:
    python va_transit_extractor.py --out "output/va_transit.json"
"""

import argparse
import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any

try:
    import geopandas as gpd
    import osmnx as ox
    import pandas as pd
    from shapely.geometry import LineString, MultiLineString, Point
except ImportError as e:
    print(f"Missing dependencies: {e}")
    print("Install with: pip install osmnx geopandas shapely pyproj rtree")
    exit(1)

def extract_transit_network(place: str = "Virginia, USA") -> Dict[str, Any]:
    """Extract transit network from OSM for Virginia."""
    
    print(f"[INFO] Fetching transit network for {place}...")
    
    # Get transit infrastructure
    G = ox.graph_from_place(place, network_type="all", simplify=False)
    
    # Convert to GeoDataFrames
    nodes, edges = ox.convert.graph_to_gdfs(G)
    
    # Filter for transit-related features
    transit_nodes = []
    transit_edges = []
    
    # Extract rail lines and stations
    for idx, row in nodes.iterrows():
        tags = row.to_dict()
        
        # Check for railway stations, stops, platforms
        if any(tag in tags for tag in ['railway', 'public_transport', 'amenity']):
            node_type = "station"
            if tags.get('railway') in ['station', 'halt', 'stop']:
                node_type = "rail_station"
            elif tags.get('public_transport') in ['station', 'platform']:
                node_type = "transit_station"
            elif tags.get('amenity') == 'bus_station':
                node_type = "bus_station"
            elif tags.get('highway') == 'bus_stop':
                node_type = "bus_stop"
            else:
                continue
            
            # Clean tags - replace NaN with null for JSON compatibility
            clean_tags = {}
            for k, v in tags.items():
                if k not in ['geometry', 'osmid']:
                    if pd.isna(v) or v == 'NaN':
                        clean_tags[k] = None
                    else:
                        clean_tags[k] = v
            
            transit_nodes.append({
                "id": str(uuid.uuid4()),
                "name": tags.get('name', 'Unnamed'),
                "type": node_type,
                "operator": tags.get('operator', None),
                "network": tags.get('network', None),
                "geometry": {
                    "type": "Point",
                    "coordinates": [row.geometry.x, row.geometry.y]
                },
                "tags": clean_tags
            })
    
    # Extract rail lines
    for idx, row in edges.iterrows():
        tags = row.to_dict()
        
        if tags.get('railway') in ['rail', 'subway', 'light_rail', 'tram']:
            # Get line geometry
            geom = row.geometry
            if isinstance(geom, (LineString, MultiLineString)):
                if isinstance(geom, MultiLineString):
                    # Use the longest segment
                    longest = max(geom.geoms, key=lambda x: x.length)
                    coords = list(longest.coords)
                else:
                    coords = list(geom.coords)
                
                # Clean tags - replace NaN with null for JSON compatibility
                clean_tags = {}
                for k, v in tags.items():
                    if k not in ['geometry', 'osmid']:
                        if pd.isna(v) or v == 'NaN':
                            clean_tags[k] = None
                        else:
                            clean_tags[k] = v
                
                transit_edges.append({
                    "id": str(uuid.uuid4()),
                    "name": tags.get('name', 'Unnamed'),
                    "type": tags.get('railway'),
                    "operator": tags.get('operator', None),
                    "network": tags.get('network', None),
                    "geometry": {
                        "type": "LineString",
                        "coordinates": coords
                    },
                    "tags": clean_tags
                })
    
    return {
        "metadata": {
            "extraction_date": datetime.now().isoformat(),
            "source": "OpenStreetMap via OSMnx",
            "place": place,
            "total_stations": len(transit_nodes),
            "total_lines": len(transit_edges)
        },
        "stations": transit_nodes,
        "lines": transit_edges
    }

def main():
    parser = argparse.ArgumentParser(description="Extract Virginia transit network from OSM")
    parser.add_argument("--place", default="Virginia, USA", help="Place name for OSM extraction")
    parser.add_argument("--out", default="output/va_transit.json", help="Output JSON file")
    args = parser.parse_args()
    
    # Extract transit data
    transit_data = extract_transit_network(args.place)
    
    # Write output
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(transit_data, f, indent=2, ensure_ascii=False)
    
    print(f"[OK] Wrote {transit_data['metadata']['total_stations']} stations and {transit_data['metadata']['total_lines']} lines -> {out_path}")

if __name__ == "__main__":
    main()
