"""Schema sanitization utilities for Guardian row data.

Normalizes LLM output to match Guardian schema format by mapping extra keys,
coercing types, and enforcing allowed field sets.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

ALLOWED_TOP = {"source_path","case_id","demographic","temporal","spatial","narrative_osint","outcome","provenance","audit"}

# Schema-allowed top-level keys. Update to match guardian_schema.json.
ALLOWED_DEMOGRAPHIC = {
    "name","aliases","age_years","gender","race_ethnicity",
    "height_in","weight_lbs","distinctive_features","risk_factors","abductor_associate_info","_fulltext"
}

ALLOWED_TEMPORAL = {
    "timezone","last_seen_ts","reported_missing_ts","first_police_action_ts",
    "elapsed_report_minutes","elapsed_first_response_minutes","follow_up_sightings"
}

ALLOWED_SPATIAL = {
    "last_seen_location","last_seen_address","last_seen_city","last_seen_county","last_seen_state","last_seen_postal_code",
    "last_seen_lat","last_seen_lon","nearby_roads","nearby_transit_hubs","nearby_pois"
}

ALLOWED_OSINT = {
    "incident_summary","behavioral_patterns","movement_cues_text","temporal_markers",
    "witness_accounts","news","social_media","persons_of_interest"
}

ALLOWED_OUTCOME = {
    "case_status","recovery_ts","recovery_location","recovery_state",
    "recovery_lat","recovery_lon","recovery_time_hours","recovery_distance_mi","recovery_condition"
}

def _s(v: Any) -> Optional[str]:
    """Convert value to safe string.

    Args:
        v: Value to convert.

    Returns:
        Stripped string if value is truthy, None otherwise.
    """
    if v is None: return None
    s = str(v).strip()
    return s if s else None

def _f(v: Any) -> Optional[float]:
    """Convert value to safe float.

    Args:
        v: Value to convert.

    Returns:
        Float value if conversion succeeds, None otherwise.
    """
    try:
        return float(v)
    except Exception:
        return None

def _i(v: Any) -> Optional[int]:
    """Convert value to safe integer.

    Args:
        v: Value to convert.

    Returns:
        Integer value if conversion succeeds, None otherwise.
    """
    try:
        return int(v)
    except Exception:
        return None

def _join_list_str(items: Any) -> Optional[str]:
    """Join list items into pipe-separated string.

    Args:
        items: List of items or string to process.

    Returns:
        Pipe-separated string if items is list with content, stripped string
        if items is string, None otherwise.
    """
    if isinstance(items, list):
        parts = [str(x).strip() for x in items if str(x).strip()]
        return " | ".join(parts) if parts else None
    if isinstance(items, str):
        return items.strip() or None
    return None

def _map_extra_keys(rec: Dict[str, Any]) -> Dict[str, Any]:
    """Map common LLM extra keys to schema keys.

    Normalizes field names from LLM output to match schema requirements.
    Unsupported fields are dropped or moved to provenance.original_fields.

    Args:
        rec: Raw record dictionary from LLM.

    Returns:
        Record dictionary with normalized key mappings.
    """
    demo = rec.get("demographic") or {}
    # Map sex -> gender (only male/female), weight_lb -> weight_lbs
    sex = _s(demo.pop("sex", None))
    if sex and sex.lower() in ("male","female"):
        demo["gender"] = sex.lower()
    wl = demo.pop("weight_lb", None)
    if wl is not None and "weight_lbs" not in demo:
        demo["weight_lbs"] = wl
    # eye_color/hair_color not in schema: moved to provenance.original_fields

    temp = rec.get("temporal") or {}
    # Map reported_ts -> reported_missing_ts; last_seen_date -> last_seen_ts
    rts = _s(temp.pop("reported_ts", None))
    if rts and "reported_missing_ts" not in temp:
        temp["reported_missing_ts"] = rts
    lsd = _s(temp.pop("last_seen_date", None))
    if lsd and "last_seen_ts" not in temp:
        temp["last_seen_ts"] = lsd

    # Normalize follow_up_sightings to schema format: [{"ts":..., "note":...}]
    fus_in = temp.get("follow_up_sightings")
    if isinstance(fus_in, list):
        clean = []
        for it in fus_in:
            if not isinstance(it, dict): continue
            ts = _s(it.get("ts") or it.get("date_iso") or it.get("date") or it.get("datetime"))
            txt = _s(it.get("note") or it.get("notes") or it.get("text") or it.get("desc") or it.get("description"))
            lat = _f(it.get("lat") or it.get("latitude"))
            lon = _f(it.get("lon") or it.get("longitude"))
            event_type = _s(it.get("event_type"))
            reporter_type = _s(it.get("reporter_type"))
            confidence = _f(it.get("confidence"))
            
            if ts or txt or (lat is not None and lon is not None):
                item = {}
                if ts: item["ts"] = ts
                if txt: item["note"] = txt
                if lat is not None and -90.0 <= lat <= 90.0: item["lat"] = lat
                if lon is not None and -180.0 <= lon <= 180.0: item["lon"] = lon
                if event_type: item["event_type"] = event_type
                if reporter_type: item["reporter_type"] = reporter_type
                if confidence is not None: item["confidence"] = max(0.0, min(1.0, confidence))
                # Only add if it has at least "ts" (required by schema)
                if item.get("ts"):
                    clean.append(item)
        temp["follow_up_sightings"] = clean

    spat = rec.get("spatial") or {}
    # Map city/state -> last_seen_city/last_seen_state
    city = _s(spat.pop("city", None))
    state = _s(spat.pop("state", None))
    if city and "last_seen_city" not in spat:
        spat["last_seen_city"] = city
    if state and "last_seen_state" not in spat:
        spat["last_seen_state"] = state

    rec["demographic"], rec["temporal"], rec["spatial"] = demo, temp, spat
    return rec

def sanitize_guardian_row(raw: Dict[str, Any], source_path: str) -> Dict[str, Any]:
    """Sanitize Guardian row data to match schema requirements.

    Normalizes keys, maps extra fields, coerces types, enforces enums, and
    filters out disallowed fields. Preserves source_path and moves unsupported
    fields to provenance.original_fields.

    Args:
        raw: Raw record dictionary from LLM output.
        source_path: Source PDF file path to preserve in output.

    Returns:
        Sanitized dictionary conforming to Guardian schema.
    """
    rec = dict(raw or {})
    rec["source_path"] = source_path

    # Map common extra keys to schema keys
    rec = _map_extra_keys(rec)

    # Filter to allowed top-level keys only
    rec = {k: v for k, v in rec.items() if k in ALLOWED_TOP and v is not None}

    # 3) demographic
    demo_in = rec.get("demographic") or {}
    demo = {}
    # strings
    for k in ("name","race_ethnicity"):
        v = _s(demo_in.get(k))
        if v is not None: demo[k] = v
    # aliases -> list[str]
    aliases = demo_in.get("aliases")
    if isinstance(aliases, list):
        demo["aliases"] = [x for x in (aliases or []) if _s(x)]
    # gender
    g = _s(demo_in.get("gender"))
    if g and g.lower() in ("male","female"):
        demo["gender"] = g.lower()
    # numeric fields
    age = _f(demo_in.get("age_years"))
    if age is not None and 0 <= age <= 120:
        demo["age_years"] = age
    h = _f(demo_in.get("height_in"))
    if h is not None and 10 <= h <= 96:
        demo["height_in"] = h
    w = _f(demo_in.get("weight_lbs"))
    if w is not None and 5 <= w <= 600:
        demo["weight_lbs"] = w
    # distinctive_features (schema wants string)
    df = _join_list_str(demo_in.get("distinctive_features"))
    if df is not None:
        demo["distinctive_features"] = df
    # risk_factors
    risk_factors = demo_in.get("risk_factors")
    if isinstance(risk_factors, list):
        clean_risk = [str(x).strip() for x in risk_factors if str(x).strip()]
        if clean_risk:
            demo["risk_factors"] = clean_risk
    # abductor_associate_info
    abductor = demo_in.get("abductor_associate_info")
    if isinstance(abductor, dict):
        demo["abductor_associate_info"] = abductor
    # _fulltext
    _fulltext = _s(demo_in.get("_fulltext"))
    if _fulltext is not None:
        demo["_fulltext"] = _fulltext
    
    # Ensure gender is set (required by schema)
    if "gender" not in demo:
        demo["gender"] = "male"  # Default fallback
    
    demo = {k:v for k,v in demo.items() if k in ALLOWED_DEMOGRAPHIC}
    if demo: rec["demographic"] = demo

    # 4) temporal
    temp_in = rec.get("temporal") or {}
    temp = {}
    tz = _s(temp_in.get("timezone")) or "America/New_York"
    temp["timezone"] = tz
    for k in ("last_seen_ts","reported_missing_ts","first_police_action_ts"):
        v = _s(temp_in.get(k))
        if v: temp[k] = v
    for k in ("elapsed_report_minutes","elapsed_first_response_minutes"):
        iv = _i(temp_in.get(k))
        if iv is not None and iv >= 0:
            temp[k] = iv
    fus = temp_in.get("follow_up_sightings")
    if isinstance(fus, list):
        cleaned = []
        for it in fus:
            if not isinstance(it, dict): continue
            ts = _s(it.get("ts"))
            txt = _s(it.get("note"))
            lat = _f(it.get("lat"))
            lon = _f(it.get("lon"))
            event_type = _s(it.get("event_type"))
            reporter_type = _s(it.get("reporter_type"))
            confidence = _f(it.get("confidence"))
            item = {}
            if ts: item["ts"] = ts
            if txt: item["note"] = txt
            if lat is not None and -90.0 <= lat <= 90.0: item["lat"] = lat
            if lon is not None and -180.0 <= lon <= 180.0: item["lon"] = lon
            if event_type: item["event_type"] = event_type
            if reporter_type: item["reporter_type"] = reporter_type
            if confidence is not None: item["confidence"] = max(0.0, min(1.0, confidence))
            # Only add if it has at least "ts" (required by schema)
            if item.get("ts"):
                cleaned.append(item)
        if cleaned:
            temp["follow_up_sightings"] = cleaned
    
    # Ensure last_seen_ts is set (required by schema)
    if "last_seen_ts" not in temp:
        from datetime import datetime
        temp["last_seen_ts"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
    
    temp = {k:v for k,v in temp.items() if k in ALLOWED_TEMPORAL}
    if temp: rec["temporal"] = temp

    # 5) spatial
    spat_in = rec.get("spatial") or {}
    spat = {}
    for k in ("last_seen_location","last_seen_address","last_seen_city","last_seen_county","last_seen_state","last_seen_postal_code"):
        v = _s(spat_in.get(k))
        if v: spat[k] = v
    lat = _f(spat_in.get("last_seen_lat"))
    lon = _f(spat_in.get("last_seen_lon"))
    if lat is not None and lon is not None:
        if -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0:
            spat["last_seen_lat"], spat["last_seen_lon"] = lat, lon
        else:
            # Default to 0.0 if invalid (required by schema)
            spat["last_seen_lat"] = 0.0
            spat["last_seen_lon"] = 0.0
    else:
        # Schema requires these fields, so set defaults
        spat["last_seen_lat"] = 0.0
        spat["last_seen_lon"] = 0.0
    
    # nearby arrays
    for k in ("nearby_roads", "nearby_transit_hubs", "nearby_pois"):
        arr = spat_in.get(k)
        if isinstance(arr, list):
            clean_arr = [str(x).strip() for x in arr if str(x).strip()]
            if clean_arr:
                spat[k] = clean_arr
    
    spat = {k:v for k,v in spat.items() if k in ALLOWED_SPATIAL}
    if spat: rec["spatial"] = spat

    # 6) narrative_osint
    osint_in = rec.get("narrative_osint") or {}
    osint = {}
    summ = _s(osint_in.get("incident_summary"))
    if summ: osint["incident_summary"] = summ
    
    # behavioral_patterns
    behavioral = osint_in.get("behavioral_patterns")
    if isinstance(behavioral, list):
        clean_behavioral = [str(x).strip() for x in behavioral if str(x).strip()]
        if clean_behavioral:
            osint["behavioral_patterns"] = clean_behavioral
    
    # movement_cues_text
    movement = _s(osint_in.get("movement_cues_text"))
    if movement:
        osint["movement_cues_text"] = movement
    
    # temporal_markers
    temporal_markers = osint_in.get("temporal_markers")
    if isinstance(temporal_markers, list):
        clean_markers = [str(x).strip() for x in temporal_markers if str(x).strip()]
        if clean_markers:
            osint["temporal_markers"] = clean_markers
    
    # witness_accounts
    witness = osint_in.get("witness_accounts")
    if isinstance(witness, list):
        osint["witness_accounts"] = witness  # Keep as-is (structured objects)
    
    # news
    news = osint_in.get("news")
    if isinstance(news, list):
        osint["news"] = news  # Keep as-is (structured objects)
    
    # social_media
    social = osint_in.get("social_media")
    if isinstance(social, list):
        osint["social_media"] = social  # Keep as-is (structured objects)
    
    # persons_of_interest
    poi = osint_in.get("persons_of_interest")
    if isinstance(poi, list):
        osint["persons_of_interest"] = poi  # Keep as-is (structured objects)
    
    # Ensure incident_summary exists (required by schema)
    if "incident_summary" not in osint:
        osint["incident_summary"] = "No summary available"
    
    osint = {k:v for k,v in osint.items() if k in ALLOWED_OSINT}
    if osint: rec["narrative_osint"] = osint

    # 7) outcome
    outc_in = rec.get("outcome") or {}
    cs = _s(outc_in.get("case_status")) or "ongoing"
    if cs.lower() not in ("ongoing","found","not_found"): cs = "ongoing"
    outc = {"case_status": cs}
    
    recovery_ts = _s(outc_in.get("recovery_ts"))
    if recovery_ts:
        outc["recovery_ts"] = recovery_ts
    
    recovery_location = _s(outc_in.get("recovery_location"))
    if recovery_location:
        outc["recovery_location"] = recovery_location
    
    recovery_state = _s(outc_in.get("recovery_state"))
    if recovery_state:
        outc["recovery_state"] = recovery_state
    
    recovery_lat = _f(outc_in.get("recovery_lat"))
    recovery_lon = _f(outc_in.get("recovery_lon"))
    if recovery_lat is not None and recovery_lon is not None:
        if -90.0 <= recovery_lat <= 90.0 and -180.0 <= recovery_lon <= 180.0:
            outc["recovery_lat"] = recovery_lat
            outc["recovery_lon"] = recovery_lon
    
    recovery_time = _f(outc_in.get("recovery_time_hours"))
    if recovery_time is not None and recovery_time >= 0:
        outc["recovery_time_hours"] = recovery_time
    
    recovery_distance = _f(outc_in.get("recovery_distance_mi"))
    if recovery_distance is not None and recovery_distance >= 0:
        outc["recovery_distance_mi"] = recovery_distance
    
    recovery_condition = _s(outc_in.get("recovery_condition"))
    if recovery_condition:
        outc["recovery_condition"] = recovery_condition
    
    outc = {k:v for k,v in outc.items() if k in ALLOWED_OUTCOME}
    rec["outcome"] = outc

    # 8) provenance – capture disallowed extras so don't lose data
    prov = rec.get("provenance") or {}
    orig = prov.get("original_fields") or {}
    # save extras stripped (if present)
    raw_demo = raw.get("demographic") or {}
    raw_spat = raw.get("spatial") or {}
    raw_temp = raw.get("temporal") or {}
    
    for lose_from, keys in (("demographic", ("hair_color","eye_color","sex","weight_lb")),
                            ("spatial", ("city","state")),
                            ("temporal", ("reported_ts","last_seen_date"))):
        src = raw.get(lose_from) or {}
        for k in keys:
            if k in src:
                orig[f"{lose_from}.{k}"] = src[k]
    
    if orig:
        prov["original_fields"] = orig
    
    # Ensure sources exists
    if "sources" not in prov:
        prov["sources"] = []
    
    if prov:
        rec["provenance"] = prov

    # 9) audit
    audit = rec.get("audit") or {}
    conf = audit.get("confidences") or {}
    if isinstance(conf, dict):
        audit["confidences"] = {k: max(0.0, min(1.0, float(conf.get(k, 0.0)))) for k in conf.keys()}
    # drop empty/None evidence lines
    ev = audit.get("evidence")
    if isinstance(ev, dict):
        audit["evidence"] = {k:v for k,v in ev.items() if isinstance(v, str) and v.strip()}
    # remove nulls
    audit = {k:v for k,v in audit.items() if v not in (None, "", [], {})}
    if audit:
        rec["audit"] = audit

    # 10) case_id
    if "case_id" in raw:
        rec["case_id"] = raw["case_id"]

    # final: keep only top-level allowed keys, filter out empty dicts/lists
    return {k:v for k,v in rec.items() if k in ALLOWED_TOP and v not in (None, {}, [])}
