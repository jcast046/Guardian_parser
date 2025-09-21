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

def extract_transit_network_regional(regions: List[str] = None) -> Dict[str, Any]:
    """Extract transit network from OSM for Virginia by major metropolitan areas."""
    
    if regions is None:
        # Major Virginia metropolitan areas with transit systems
        # Using broader regional names and alternative search terms
        regions = [
            "Richmond, Virginia, USA",
            "Norfolk, Virginia, USA", 
            "Virginia Beach, Virginia, USA",
            "Hampton, Virginia, USA",
            "Newport News, Virginia, USA",
            "Alexandria, Virginia, USA",
            "Arlington, Virginia, USA",
            "Fairfax, Virginia, USA",
            "Roanoke, Virginia, USA",
            "Lynchburg, Virginia, USA",
            # Add some broader regional searches
            "Northern Virginia, USA",
            # Try alternative names for Hampton Roads area
            "Chesapeake, Virginia, USA",
            "Portsmouth, Virginia, USA",
            "Suffolk, Virginia, USA"
        ]
    
    print(f"[INFO] Fetching transit networks for {len(regions)} Virginia regions...")
    
    all_stations = []
    all_lines = []
    region_metadata = []
    
    for region in regions:
        try:
            print(f"[INFO] Processing {region}...")
            
            # Get transit infrastructure for this region with multiple network types
            G = None
            try:
                # Try with all network types first
                G = ox.graph_from_place(region, network_type="all", simplify=False)
            except:
                try:
                    # Fallback to drive network if all fails
                    G = ox.graph_from_place(region, network_type="drive", simplify=False)
                except:
                    print(f"[WARNING] Could not fetch data for {region}, trying alternative approach...")
                    # Try with a broader search area
                    try:
                        # Extract city name and try with just the city
                        city_name = region.split(',')[0].strip()
                        G = ox.graph_from_place(f"{city_name}, Virginia, USA", network_type="all", simplify=False)
                    except:
                        print(f"[WARNING] Could not fetch data for {region}, skipping...")
                        continue
            
            # Convert to GeoDataFrames
            nodes, edges = ox.convert.graph_to_gdfs(G)
            
            # Filter for transit-related features
            region_stations = []
            region_lines = []
            
            # Extract rail lines and stations
            for idx, row in nodes.iterrows():
                tags = row.to_dict()
                
                # Check for transit-related features with comprehensive criteria
                is_transit = False
                node_type = "station"
                
                # Railway stations and stops
                if tags.get('railway') in ['station', 'halt', 'stop', 'platform']:
                    is_transit = True
                    node_type = "rail_station"
                # Public transport stations and platforms
                elif tags.get('public_transport') in ['station', 'platform', 'stop_position']:
                    is_transit = True
                    node_type = "transit_station"
                # Bus stations and stops - this is the most common type
                elif tags.get('highway') == 'bus_stop':
                    is_transit = True
                    node_type = "bus_stop"
                elif tags.get('amenity') == 'bus_station':
                    is_transit = True
                    node_type = "bus_station"
                # Additional transit-related tags
                elif tags.get('railway') in ['subway_entrance', 'tram_stop']:
                    is_transit = True
                    node_type = "transit_station"
                # Check for transit operators/networks
                elif any(operator in str(tags.get('operator', '')).lower() for operator in 
                        ['grtc', 'hampton roads transit', 'hrt', 'wmata', 'metro', 'vre', 'amtrak', 'valley metro', 'pulaski']):
                    is_transit = True
                    node_type = "transit_station"
                # Check for transit-related names
                elif any(name in str(tags.get('name', '')).lower() for name in 
                        ['bus stop', 'transit', 'metro', 'station', 'depot', 'terminal']):
                    is_transit = True
                    node_type = "transit_station"
                # Check for any highway tags that might be transit-related
                elif tags.get('highway') in ['bus_stop', 'bus_station']:
                    is_transit = True
                    node_type = "bus_stop"
                
                if not is_transit:
                    continue
                
                # Clean tags - replace NaN with null for JSON compatibility
                clean_tags = {}
                for k, v in tags.items():
                    if k not in ['geometry', 'osmid']:
                        if pd.isna(v) or v == 'NaN':
                            clean_tags[k] = None
                        else:
                            clean_tags[k] = v
                    
                    station = {
                        "id": str(uuid.uuid4()),
                        "name": tags.get('name', 'Unnamed'),
                        "type": node_type,
                        "operator": tags.get('operator', None),
                        "network": tags.get('network', None),
                        "geometry": {
                            "type": "Point",
                            "coordinates": [row.geometry.x, row.geometry.y]
                        },
                        "tags": clean_tags,
                        "region": region
                    }
                    region_stations.append(station)
                    all_stations.append(station)
            
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
                        
                        line = {
                            "id": str(uuid.uuid4()),
                            "name": tags.get('name', 'Unnamed'),
                            "type": tags.get('railway'),
                            "operator": tags.get('operator', None),
                            "network": tags.get('network', None),
                            "geometry": {
                                "type": "LineString",
                                "coordinates": coords
                            },
                            "tags": clean_tags,
                            "region": region
                        }
                        region_lines.append(line)
                        all_lines.append(line)
            
            # Store region metadata
            region_metadata.append({
                "region": region,
                "stations": len(region_stations),
                "lines": len(region_lines)
            })
            
            print(f"[OK] {region}: {len(region_stations)} stations, {len(region_lines)} lines")
            
        except Exception as e:
            print(f"[ERROR] Failed to process {region}: {e}")
            continue
    
    return {
        "metadata": {
            "extraction_date": datetime.now().isoformat(),
            "source": "OpenStreetMap via OSMnx (Regional)",
            "place": "Virginia, USA (Regional)",
            "total_stations": len(all_stations),
            "total_lines": len(all_lines),
            "regions_processed": len(region_metadata),
            "region_breakdown": region_metadata
        },
        "stations": all_stations,
        "lines": all_lines
    }

def extract_single_place(place: str) -> Dict[str, Any]:
    """Extract transit network from OSM for a single place."""
    
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
        
        # Check for transit-related features with broader criteria
        is_transit = False
        node_type = "station"
        
        # Railway stations and stops
        if tags.get('railway') in ['station', 'halt', 'stop', 'platform']:
            is_transit = True
            node_type = "rail_station"
        # Public transport stations and platforms
        elif tags.get('public_transport') in ['station', 'platform', 'stop_position']:
            is_transit = True
            node_type = "transit_station"
        # Bus stations and stops
        elif tags.get('amenity') == 'bus_station' or tags.get('highway') == 'bus_stop':
            is_transit = True
            node_type = "bus_station" if tags.get('amenity') == 'bus_station' else "bus_stop"
        # Additional transit-related tags
        elif tags.get('railway') in ['subway_entrance', 'tram_stop']:
            is_transit = True
            node_type = "transit_station"
        # Check for transit operators/networks
        elif any(operator in str(tags.get('operator', '')).lower() for operator in 
                ['grtc', 'hampton roads transit', 'hrt', 'wmata', 'metro', 'vre', 'amtrak']):
            is_transit = True
            node_type = "transit_station"
        # Check for transit-related names
        elif any(name in str(tags.get('name', '')).lower() for name in 
                ['bus stop', 'transit', 'metro', 'station', 'depot']):
            is_transit = True
            node_type = "transit_station"
        
        if not is_transit:
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
    parser.add_argument("--regional", action="store_true", help="Use regional extraction for large areas")
    args = parser.parse_args()
    
    if args.regional or args.place == "Virginia, USA":
        # Use regional approach for large areas
        transit_data = extract_transit_network_regional()
    else:
        # Use single place extraction - create a simple wrapper
        transit_data = extract_single_place(args.place)
    
    # Write output
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(transit_data, f, indent=2, ensure_ascii=False)
    
    print(f"[OK] Wrote {transit_data['metadata']['total_stations']} stations and {transit_data['metadata']['total_lines']} lines -> {out_path}")

if __name__ == "__main__":
    main()
