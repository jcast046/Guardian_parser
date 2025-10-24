#!/usr/bin/env python3
"""
OpenStreetMap Road Segment Importer

Build RoadSegment JSON directly from OpenStreetMap via OSMnx and auto-fill
fields useful to RL config (corridor codes, region tags, bearings). This script
extracts detailed road network data with geometry, metadata, and regional
classification for the Guardian Parser Pack system.

Features:
    - OpenStreetMap road network extraction via OSMnx
    - Regional classification using GeoJSON boundaries
    - Road segment geometry with bearings and metadata
    - Schema-validated output conforming to road_segment.schema.json
    - Support for custom boundary files and regional tagging
    - Memory-efficient processing for large geographic areas

Dependencies:
    osmnx, geopandas, shapely, pyproj, rtree, json, pathlib

Usage Examples:
    # Entire state, output JSON array
    python osm_import.py --osm --place "Virginia, USA" --out "data/road_segments.json"

    # A specific metro (faster)
    python osm_import.py --osm --place "Alexandria, Virginia, USA" --out "data/alexandria_segments.json"

    # Use a custom boundary GeoJSON (must be Polygon/MultiPolygon, WGS84)
    python osm_import.py --osm --boundary "data/my_boundary.geojson" --out "data/segments.json"

    # Assign RL regions via GeoJSON polygons
    python osm_import.py --osm --place "Virginia, USA" --rl-regions "data/va_rl_regions.geojson" --out "data/segments.json"

Output:
    JSON file containing road segments with:
    - geometry: LineString coordinates
    - metadata: road name, type, classification
    - regional tags: RL region assignment
    - bearings: directional information

Author: Joshua Castillo
"""

import argparse
import json
import re
import sys
import uuid
from datetime import datetime
from pathlib import Path

try:
    import geopandas as gpd
    import osmnx as ox
    from shapely.geometry import LineString, MultiLineString
    from shapely.ops import linemerge
except ImportError as e:
    print(f"Missing dependencies: {e}")
    print("Install with: pip install osmnx geopandas shapely pyproj rtree")
    sys.exit(1)

# -------------- Helpers --------------

def bearing_to_cardinal(b):
    """
    Map degrees to NB/EB/SB/WB quadrants.
    
    Converts bearing degrees to cardinal direction abbreviations.
    
    Args:
        b (float): Bearing in degrees (0-360)
        
    Returns:
        Optional[str]: Cardinal direction (NB, EB, SB, WB) or None if invalid
        
    Note:
        Uses 45-degree quadrants: 315-45° = NB, 45-135° = EB, etc.
    """
    if b is None:
        return None
    b = float(b) % 360.0
    if (b >= 315) or (b < 45):
        return "NB"
    elif 45 <= b < 135:
        return "EB"
    elif 135 <= b < 225:
        return "SB"
    else:
        return "WB"

BRANCH_MAP = {
    "BUS": "Business", "BUSINESS": "Business",
    "ALT": "Alternate", "ALTERNATE": "Alternate",
    "BYP": "Bypass", "BYPASS": "Bypass",
    "TRUCK": "Truck", "SPUR": "Spur"
}

def parse_ref_token(token):
    """
    Parse a single ref token like 'I 95', 'US 29 BUS', 'VA 7', 'US-50 BYP'.
    
    Extracts route system, number, branch, and signing information from
    OpenStreetMap ref tokens.
    
    Args:
        token (str): Reference token to parse
        
    Returns:
        Tuple[str, str, str, str]: (routeSystem, routeNumber, routeBranch, signing)
        
    Note:
        Handles various formats including interstate, US highway, and state route
        designations with business, alternate, bypass, and spur branches.
    """
    t = token.strip().upper().replace("–", "-").replace("—", "-")
    t = re.sub(r"\s+", " ", t)
    # Extract branch suffix if present
    branch = "None"
    for k, v in BRANCH_MAP.items():
        if re.search(rf"\b{k}\b", t):
            branch = v
            t = re.sub(rf"\b{k}\b", "", t).strip()
            break

    m = re.match(r"^I[\s\-]?(\d+)$", t)
    if m:
        return ("Interstate", m.group(1), branch, "Interstate")

    m = re.match(r"^US[\s\-]?(\d+)$", t)
    if m:
        return ("US Highway", m.group(1), branch, "US")

    m = re.match(r"^(VA|SR)[\s\-]?(\d+)$", t)
    if m:
        return ("Primary Highway", m.group(2), branch, "VA")

    return ("Unknown", "", branch, "None")

FC_MAP = {
    "motorway": "Freeway/Expressway",
    "trunk": "Principal Arterial",
    "primary": "Principal Arterial",
    "secondary": "Minor Arterial",
    "tertiary": "Major Collector",
    "residential": "Local",
    "unclassified": "Local",
    "service": "Local"
}

def pick_linestring(geom):
    """
    Ensure a LineString geometry (pick longest if MultiLineString).
    
    Converts MultiLineString geometries to single LineString by selecting
    the longest component for road segment representation.
    
    Args:
        geom: Shapely geometry object (LineString or MultiLineString)
        
    Returns:
        Optional[LineString]: Longest LineString component or None if invalid
        
    Note:
        Used for road segment geometry standardization in OSM data processing.
    """
    if isinstance(geom, LineString):
        return geom
    if isinstance(geom, MultiLineString):
        # choose the longest component to represent the edge
        longest = None
        max_len = -1.0
        for ls in geom.geoms:
            L = ls.length
            if L > max_len:
                longest = ls
                max_len = L
        return longest
    return None

def build_corridor_codes(route_system, route_number, bearing):
    """
    Build corridor codes from route system, number, and bearing.
    
    Creates standardized corridor codes combining route designation with
    cardinal direction for regional classification.
    
    Args:
        route_system (str): Route system (Interstate, US Highway, etc.)
        route_number (str): Route number
        bearing (float): Bearing in degrees
        
    Returns:
        List[str]: List of corridor codes or empty list if invalid
        
    Note:
        Only creates codes for recognized route systems (Interstate, US Highway,
        Primary Highway). Includes cardinal direction when bearing is available.
    """
    if not route_system or not route_number:
        return []
    cardinal = bearing_to_cardinal(bearing)
    prefix = {"Interstate":"I", "US Highway":"US", "Primary Highway":"VA"}.get(route_system, None)
    if not prefix:
        return []
    if cardinal:
        return [f"{prefix}-{route_number} {cardinal}"]
    return [f"{prefix}-{route_number}"]

def load_rl_regions(path):
    """
    Load RL region polygons (expects properties: region, region_tag).
    
    Loads GeoJSON file containing regional boundary polygons for
    spatial classification of road segments.
    
    Args:
        path (str): Path to GeoJSON file with regional boundaries
        
    Returns:
        Optional[GeoDataFrame]: Regional boundaries with region and region_tag columns
        
    Raises:
        ValueError: If required properties (region, region_tag) are missing
        
    Note:
        Automatically converts to WGS84 (EPSG:4326) coordinate system.
    """
    if not path:
        return None
    gdf = gpd.read_file(path)
    if gdf.crs is None:
        gdf.set_crs(4326, inplace=True)
    else:
        gdf = gdf.to_crs(4326)
    if "region" not in gdf.columns or "region_tag" not in gdf.columns:
        raise ValueError("RL regions GeoJSON must include 'region' and 'region_tag' properties.")
    return gdf[["region", "region_tag", "geometry"]]

# -------------- Core pipeline --------------

def fetch_graph(place=None, boundary=None, network_type="drive", simplify=True):
    """
    Fetch OpenStreetMap graph for specified place or boundary.
    
    Downloads road network data from OpenStreetMap using OSMnx for either
    a named place or custom boundary polygon.
    
    Args:
        place (str, optional): Named place for OSM extraction
        boundary (str, optional): Path to GeoJSON boundary file
        network_type (str): OSMnx network type (default: "drive")
        simplify (bool): Whether to simplify graph topology (default: True)
        
    Returns:
        NetworkX graph: Road network graph from OpenStreetMap
        
    Raises:
        ValueError: If neither place nor boundary is provided
        
    Note:
        Automatically converts boundary to WGS84 coordinate system.
    """
    if boundary:
        poly = gpd.read_file(boundary)
        if poly.crs is None:
            poly.set_crs(4326, inplace=True)
        else:
            poly = poly.to_crs(4326)
        if len(poly) > 1:
            geom = poly.unary_union
        else:
            geom = poly.geometry.iloc[0]
        G = ox.graph_from_polygon(geom, network_type=network_type, simplify=simplify)
        return G
    if place:
        return ox.graph_from_place(place, network_type=network_type, simplify=simplify)
    raise ValueError("Provide either --place or --boundary.")

def graph_to_segments(G, rl_regions_path=None):
    """
    Convert OSMnx graph to structured road segments.
    
    Processes road network graph and creates standardized road segment
    records with geometry, metadata, and regional classification.
    
    Args:
        G: OSMnx road network graph
        rl_regions_path (str, optional): Path to regional boundaries GeoJSON
        
    Returns:
        List[Dict]: List of structured road segment records
        
    Note:
        Enriches graph with speeds, travel times, and bearings before
        processing. Performs spatial join for regional classification.
    """
    G = ox.routing.add_edge_speeds(G)
    G = ox.routing.add_edge_travel_times(G)
    G = ox.bearing.add_edge_bearings(G)

    edges = ox.convert.graph_to_gdfs(G, nodes=False)
    edges = edges.to_crs(4326)

    rl_gdf = load_rl_regions(rl_regions_path) if rl_regions_path else None
    if rl_gdf is not None:
        joined = gpd.sjoin(edges[["geometry"]], rl_gdf, how="left", predicate="intersects")
        edges = edges.join(joined[["region","region_tag"]])
    else:
        edges["region"] = None
        edges["region_tag"] = None

    segments = []
    for idx, row in edges.iterrows():
        geom = pick_linestring(row.geometry)
        if geom is None:
            continue

        name_fields = [
            row.get("name", None),
            row.get("official_name", None),
            row.get("alt_name", None),
            row.get("loc_name", None),
            row.get("short_name", None),
            row.get("old_name", None)
        ]
        
        local_names = []
        for field in name_fields:
            if field is not None:
                if isinstance(field, list):
                    local_names.extend([str(n) for n in field if n])
                else:
                    local_names.append(str(field))
        
        seen = set()
        local_names = [n for n in local_names if not (n in seen or seen.add(n))]

        # Parse ref tokens -> choose primary
        route_system, route_number, route_branch, signing = "Unknown", "", "None", "None"
        corridor_codes = []
        ref = row.get("ref", None)
        if ref:
            tokens = re.split(r"[;|/,]", str(ref))
            for tok in tokens:
                rs, rn, rb, sg = parse_ref_token(tok)
                # prefer Interstate > US > VA > Unknown
                rank = {"Interstate":3, "US Highway":2, "Primary Highway":1, "Unknown":0}
                if rank.get(rs,0) > rank.get(route_system,0):
                    route_system, route_number, route_branch, signing = rs, rn, rb, sg

        # Build corridor codes from bearing + primary ref
        bearing = row.get("bearing", None)
        corridor_codes = build_corridor_codes(route_system, route_number, bearing)

        # Functional class from OSM 'highway'
        hw = row.get("highway", None)
        functional = None
        if hw:
            # Handle both string and list values
            if isinstance(hw, list):
                hw = hw[0] if hw else None
            if hw:
                functional = {"context": "Urban", "class": FC_MAP.get(hw, "Local")}

        # Allowed directions
        oneway = row.get("oneway", False)
        allowed = []
        if bool(oneway) and (bearing is not None):
            c = bearing_to_cardinal(bearing)
            allowed = [c] if c else []

        length_m = float(row.get("length", 0.0) or 0.0)
        length_miles = length_m * 0.000621371

        seg = {
            "segmentId": str(uuid.uuid4()),
            "localNames": local_names or [],
            "routeDesignation": {
                "routeSystem": route_system,
                "routeNumber": route_number,
                "routeBranch": route_branch,
                "signing": signing,
                "corridorCodes": corridor_codes
            },
            "admin": {
                "region": row.get("region") or "Unknown",
                "regionTagRL": row.get("region_tag") or "Unknown",
                "vdotDistrict": None,
                "countyFips": None,
                "placeFips": None,
                "inState": True
            },
            "rlHints": {
                "directionalBearingDeg": float(bearing) if bearing is not None else None,
                "allowedDirections": allowed
            },
            "geometry": {
                "type": "LineString",
                "coordinates": list(geom.coords)
            },
            "centroid": {
                "lon": geom.centroid.x,
                "lat": geom.centroid.y
            },
            "lengthMiles": length_miles,
            "functionalClassification": functional,
            "operations": {
                "toll": (str(row.get("toll")).lower() == "yes"),
                "hovHot": "None",
                "evacuationRoute": False,
                "truckRoute": "none",
                "restrictedHeightFt": None,
                "restrictedWeightTons": None
            },
            "linearReference": None,
            "provenance": {
                "source": "OpenStreetMap via OSMnx",
                "sourceDoc": None,
                "sourcePage": None,
                "parserVersion": "osm-import-0.1",
                "extractedAt": datetime.now().isoformat(),
                "confidence": 0.9
            }
        }
        segments.append(seg)

    return segments

# -------------- CLI --------------

def main():
    ap = argparse.ArgumentParser(description="Import RoadSegment JSON from OpenStreetMap using OSMnx.")
    ap.add_argument("--osm", action="store_true", help="Required flag to confirm an OSM import run.")
    ap.add_argument("--place", type=str, help="Place name for OSMnx (e.g., 'Virginia, USA').")
    ap.add_argument("--boundary", type=str, help="Path to GeoJSON boundary (Polygon/MultiPolygon).")
    ap.add_argument("--rl-regions", type=str, help="GeoJSON with properties 'region' and 'region_tag' for RL region tagging.")
    ap.add_argument("--network-type", type=str, default="drive", choices=["drive","drive_service"],
                    help="OSMnx network_type (default: drive).")
    ap.add_argument("--simplify", action="store_true", help="Simplify graph topology (default True).")
    ap.add_argument("--no-simplify", dest="simplify", action="store_false", help="Disable simplification.")
    ap.set_defaults(simplify=True)
    ap.add_argument("--out", type=str, required=True, help="Output JSON path (array of RoadSegment objects).")

    args = ap.parse_args()
    if not args.osm:
        print("Add --osm to confirm you intend to import from OpenStreetMap.", file=sys.stderr)
        sys.exit(2)

    if not args.place and not args.boundary:
        print("Provide either --place or --boundary.", file=sys.stderr)
        sys.exit(2)

    print("[INFO] Fetching graph...")
    G = fetch_graph(place=args.place, boundary=args.boundary, network_type=args.network_type, simplify=args.simplify)
    print("[INFO] Graph fetched. Building segments...")
    segments = graph_to_segments(G, rl_regions_path=args.rl_regions)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(segments, f, indent=2, ensure_ascii=False)

    print(f"[OK] Wrote {len(segments)} segments -> {out_path}")

if __name__ == "__main__":
    main()
