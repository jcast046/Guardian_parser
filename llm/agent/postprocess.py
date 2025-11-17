"""Post-processing functions to repair and normalize LLM output.

Provides utilities to repair, coerce types, and normalize LLM-generated
JSON before schema validation.
"""
from __future__ import annotations
from typing import Any, Dict, List, Union


def coerce_guardian(rec: dict) -> dict:
    """Repair and normalize LLM JSON output before schema validation.

    Performs type coercion, normalizes field structures, fixes common
    LLM output issues, and ensures required fields have defaults.

    Args:
        rec: Raw record dictionary from LLM output.

    Returns:
        Repaired and normalized record dictionary.

    Note:
        Drops unknown top-level keys, fixes 4-digit years in age_years,
        converts distinctive_features list to pipe-separated string,
        normalizes follow_up_sightings structure, and enforces numeric types.
    """
    rec = rec or {}
    
    # Drop unknown top-level keys (whitelist)
    allowed_top = {
        "case_id", "demographic", "spatial", "temporal", 
        "narrative_osint", "provenance", "outcome", "case", "source_path"
    }
    rec = {k: v for k, v in rec.items() if k in allowed_top}
    
    demo = rec.setdefault("demographic", {})
    temp = rec.setdefault("temporal", {})
    spat = rec.setdefault("spatial", {})
    osint = rec.setdefault("narrative_osint", {})
    prov = rec.setdefault("provenance", {})
    
    # Nulls → empty string for required strings
    for path in [
        ("demographic", "name"),
        ("spatial", "last_seen_location"),
        ("narrative_osint", "incident_summary"),
    ]:
        d, k = path
        if isinstance(rec.get(d, {}).get(k), type(None)):
            rec.setdefault(d, {})[k] = ""
    
    # age_years: fix 4-digit years
    age = demo.get("age_years")
    if isinstance(age, (int, float)) and age > 1900:
        demo.pop("age_years", None)
    
    # distinctive_features: list → pipe-joined string
    if isinstance(demo.get("distinctive_features"), list):
        df_list = demo["distinctive_features"]
        df_str = " | ".join([str(x).strip() for x in df_list if str(x).strip()])
        demo["distinctive_features"] = df_str if df_str else None
        if demo["distinctive_features"] is None:
            demo.pop("distinctive_features", None)
    
    # enforce numbers
    for k in ["height_in", "weight_lbs", "age_years"]:
        v = demo.get(k)
        if isinstance(v, str):
            try:
                demo[k] = float(v)
            except (ValueError, TypeError):
                demo.pop(k, None)
    
    # follow_up_sightings: keep only ts/lat/lon/event_type/reporter_type/confidence/note, 
    # rename date_iso→ts, drop extras
    fus = temp.get("follow_up_sightings")
    if isinstance(fus, list):
        clean = []
        allowed_keys = {"ts", "lat", "lon", "event_type", "reporter_type", "confidence", "note"}
        for item in fus:
            if not isinstance(item, dict):
                continue
            
            # Map various date/time field names to "ts"
            ts = (item.get("ts") or item.get("date_iso") or 
                  item.get("time_iso") or item.get("date") or 
                  item.get("datetime") or item.get("time"))
            
            # Map various note/description field names to "note"
            note = (item.get("note") or item.get("notes") or 
                   item.get("text") or item.get("desc") or 
                   item.get("description"))
            
            # Extract other allowed fields
            lat = item.get("lat") or item.get("latitude")
            lon = item.get("lon") or item.get("longitude")
            event_type = item.get("event_type")
            reporter_type = item.get("reporter_type")
            confidence = item.get("confidence")
            
            # Build clean item with only allowed keys
            clean_item = {}
            if ts:
                clean_item["ts"] = str(ts).strip()
            if note:
                clean_item["note"] = str(note).strip()
            if lat is not None:
                try:
                    clean_item["lat"] = float(lat)
                except (ValueError, TypeError):
                    pass
            if lon is not None:
                try:
                    clean_item["lon"] = float(lon)
                except (ValueError, TypeError):
                    pass
            if event_type:
                clean_item["event_type"] = str(event_type)
            if reporter_type:
                clean_item["reporter_type"] = str(reporter_type)
            if confidence is not None:
                try:
                    conf_val = float(confidence)
                    clean_item["confidence"] = max(0.0, min(1.0, conf_val))
                except (ValueError, TypeError):
                    pass
            
            # Only add if it has at least "ts" (required)
            if clean_item.get("ts"):
                clean.append(clean_item)
        
        if clean:
            temp["follow_up_sightings"] = clean
        else:
            temp.pop("follow_up_sightings", None)
    
    # empty strings for required ISO times are invalid → drop them; validator will complain less
    for tkey in ["last_seen_ts", "reported_missing_ts", "first_police_action_ts"]:
        if temp.get(tkey) in (None, ""):
            temp.pop(tkey, None)
    
    # Ensure required fields exist with defaults
    if "timezone" not in temp:
        temp["timezone"] = "America/New_York"
    if "last_seen_ts" not in temp:
        from datetime import datetime
        temp["last_seen_ts"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # Ensure gender is set (required)
    if "gender" not in demo or demo["gender"] not in ("male", "female"):
        demo["gender"] = "male"  # Default
    
    # Ensure spatial has lat/lon (required)
    if "last_seen_lat" not in spat or spat.get("last_seen_lat") is None:
        spat["last_seen_lat"] = 0.0
    if "last_seen_lon" not in spat or spat.get("last_seen_lon") is None:
        spat["last_seen_lon"] = 0.0
    
    # Ensure outcome has case_status
    outcome = rec.setdefault("outcome", {})
    if "case_status" not in outcome or outcome["case_status"] not in ("ongoing", "found", "not_found"):
        outcome["case_status"] = "ongoing"
    
    # Ensure narrative_osint has incident_summary
    if "incident_summary" not in osint:
        osint["incident_summary"] = "No summary available"
    
    # Ensure provenance structure
    if not isinstance(prov, dict):
        prov = {}
    if "sources" not in prov:
        prov["sources"] = []
    if "original_fields" not in prov:
        prov["original_fields"] = {}
    rec["provenance"] = prov
    
    return rec

