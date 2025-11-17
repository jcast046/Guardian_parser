"""Simplified agent with deterministic orchestration.

LLM is used only for extraction and summarization, not orchestration.
"""
import os
import json
import sys
from datetime import datetime
from typing import List, Optional, Dict, Any
from pathlib import Path

from .llm_client import LLMClient
from .protocols import GuardianRow
from . import tools
from .schema_sanitize import sanitize_guardian_row
from .postprocess import coerce_guardian

# Import legacy normalizers from parser_pack
_root_dir = Path(__file__).parent.parent.parent.resolve()
if str(_root_dir) not in sys.path:
    sys.path.insert(0, str(_root_dir))
import parser_pack


PROMPTS_DIR = os.path.join(os.path.dirname(__file__), "prompts")


def _read(name: str) -> str:
    """Read prompt file from prompts directory.

    Args:
        name: Name of the prompt file to read.

    Returns:
        Contents of the prompt file as a string.
    """
    with open(os.path.join(PROMPTS_DIR, name), "r", encoding="utf-8") as f:
        return f.read()


def _repair_with_validator_feedback(
    row: Dict[str, Any],
    validation_errors: List[str],
    client: LLMClient,
    extract_prompt: str,
    source_path: str,
    doc_text: str = ""
) -> Optional[Dict[str, Any]]:
    """Repair JSON output using validator error feedback.

    Args:
        row: Current row dictionary that failed validation.
        validation_errors: List of validation error messages.
        client: LLMClient instance for repair requests.
        extract_prompt: Original extraction prompt for context.
        source_path: Source PDF path to preserve in repaired output.
        doc_text: Original document text (unused in current implementation).

    Returns:
        Repaired row dictionary or None if repair failed.
    """
    try:
        # Preserve source_path before sending to LLM
        preserved_source_path = row.get("source_path") or source_path
        
        # Create repair prompt
        repair_prompt = f"""You produced the JSON below. The validator errors follow. Return a corrected JSON that fixes ONLY the errors, without adding new keys.

Errors:
{chr(10).join(f"- {err}" for err in validation_errors[:10])}

Current JSON:
{json.dumps(row, ensure_ascii=False, indent=2)}

Rules:
- Change 4-digit ages into computed ages or remove age_years.
- For follow_up_sightings, keep only {{ "ts", "lat", "lon", "event_type", "reporter_type", "confidence", "note" }} per item.
- Use "ts" NOT "date_iso", use "note" NOT "notes".
- Fix type mismatches (numbers vs strings, nulls where not allowed).
- Do NOT include source_path in output (it will be added automatically).
- Output a single JSON object, no prose."""

        repair_messages = [
            {"role": "system", "content": extract_prompt},
            {"role": "user", "content": repair_prompt}
        ]
        
        repaired_data = client.chat_json(repair_messages)
        if isinstance(repaired_data, dict):
            # Ensure source_path is preserved
            repaired_data["source_path"] = preserved_source_path
            return repaired_data
        return None
    except Exception as e:
        print(f"    Repair failed: {str(e)}")
        return None


def _sanitize_extracted(extracted: dict) -> dict:
    """Normalize keys, drop unknown fields, coerce types, and enforce enums.

    Args:
        extracted: Raw extracted data dictionary from LLM.

    Returns:
        Sanitized dictionary with normalized keys and coerced types.
    """
    out = {"demographic":{}, "temporal":{}, "spatial":{}, "narrative_osint":{}, "outcome":{}, "provenance":{}, "audit":{}}

    # --- demographic ---
    d = extracted.get("demographic", {}) or {}
    def fnum(x):
        try:
            return float(x)
        except Exception:
            return None
    out["demographic"]["name"] = d.get("name")
    out["demographic"]["aliases"] = d.get("aliases") or []
    
    # Age: reject 4-digit values (likely birth years) >= 1000
    age = fnum(d.get("age_years"))
    if age is not None and age >= 1000:
        age = None  # Treat as birth year, not age
    out["demographic"]["age_years"] = age
    
    g = (d.get("gender") or "").lower()
    out["demographic"]["gender"] = g if g in ("male","female") else None
    out["demographic"]["race_ethnicity"] = d.get("race_ethnicity")
    out["demographic"]["height_in"] = fnum(d.get("height_in"))
    out["demographic"]["weight_lbs"] = fnum(d.get("weight_lbs"))
    
    # distinctive_features: convert array to string if needed
    df = d.get("distinctive_features")
    if isinstance(df, list):
        # Join list items with semicolon
        df_str = "; ".join([str(x).strip() for x in df if str(x).strip()])
        out["demographic"]["distinctive_features"] = df_str if df_str else None
    else:
        out["demographic"]["distinctive_features"] = df
    
    # _fulltext: only include if present, don't set to None
    _fulltext = d.get("_fulltext")
    if _fulltext is not None and str(_fulltext).strip():
        out["demographic"]["_fulltext"] = _fulltext

    # --- temporal ---
    t = extracted.get("temporal", {}) or {}
    out["temporal"]["timezone"] = t.get("timezone") or "America/New_York"
    def iso(s):
        if not s or not isinstance(s, str): return None
        s = s.strip()
        return s if s else None
    out["temporal"]["last_seen_ts"] = iso(t.get("last_seen_ts"))
    out["temporal"]["reported_missing_ts"] = iso(t.get("reported_missing_ts"))
    out["temporal"]["first_police_action_ts"] = iso(t.get("first_police_action_ts"))
    for k in ("elapsed_report_minutes","elapsed_first_response_minutes"):
        v = t.get(k)
        try:
            out["temporal"][k] = int(v) if v not in (None,"") else None
        except Exception:
            out["temporal"][k] = None
    out["temporal"]["follow_up_sightings"] = t.get("follow_up_sightings") or []

    # --- spatial ---
    s = extracted.get("spatial", {}) or {}
    out["spatial"]["last_seen_location"] = s.get("last_seen_location")
    out["spatial"]["last_seen_city"] = s.get("last_seen_city")
    out["spatial"]["last_seen_state"] = s.get("last_seen_state") or "VA"
    out["spatial"]["last_seen_lat"] = None
    out["spatial"]["last_seen_lon"] = None
    lr = s.get("locations_raw")
    out["spatial"]["locations_raw"] = [x for x in (lr or []) if isinstance(x,str)]

    # --- narrative_osint ---
    n = extracted.get("narrative_osint", {}) or {}
    out["narrative_osint"]["incident_summary"] = n.get("incident_summary")
    spans = n.get("narrative_spans") or []
    out["narrative_osint"]["narrative_spans"] = [sp for sp in spans if isinstance(sp, str) and sp.strip()]

    # --- outcome/provenance/audit ---
    o = extracted.get("outcome", {}) or {}
    cs = (o.get("case_status") or "ongoing").lower()
    out["outcome"]["case_status"] = cs if cs in ("ongoing","found","not_found") else "ongoing"
    out["provenance"] = extracted.get("provenance", {}) or {}
    a = extracted.get("audit", {}) or {}
    # clamp confidences
    conf = a.get("confidences") or {}
    conf_clean = {}
    for k in ("demographic","temporal","spatial","narrative_osint"):
        try:
            v = conf.get(k, 0.0)
            conf_clean[k] = max(0.0, min(1.0, float(v)))
        except (ValueError, TypeError):
            conf_clean[k] = 0.0
    a["confidences"] = conf_clean
    out["audit"] = a
    return out


def run_agent_simple(
    input_dir: str,
    out_jsonl: str,
    out_csv: Optional[str],
    schema_path: str,
    backend: str = "ollama",
    model_path: Optional[str] = None,
    ollama_model: str = "llama3.2",
    max_retries: int = 3
) -> tuple[bool, int, Optional[str]]:
    """Run simplified agent with deterministic orchestration.

    Uses LLM only for extraction and summarization. Orchestration is
    deterministic and follows a fixed pipeline.

    Args:
        input_dir: Directory containing PDF files to process.
        out_jsonl: Path to output JSONL file.
        out_csv: Optional path to output CSV file.
        schema_path: Path to JSON schema file for validation.
        backend: LLM backend to use ("ollama" or "llama").
        model_path: Path to GGUF model file (required for llama backend).
        ollama_model: Ollama model name (required for ollama backend).
        max_retries: Maximum number of retries for LLM calls.

    Returns:
        Tuple containing:
            - success: True if processing completed successfully.
            - records_processed: Number of records successfully processed.
            - error_message: Error message if processing failed, None otherwise.
    """
    records_processed = 0
    errors = []
    
    try:
        # Initialize LLM client with low temperature for deterministic extraction
        client = LLMClient(
            backend=backend,
            model_path=model_path,
            ollama_model=ollama_model,
            temperature=0.1,
            json_mode=True
        )
        
        # Load prompts
        extract_prompt = _read("extract_guardian_schema.txt")
        summarize_prompt = _read("summarize_case.txt")
        
        # Step 1: List all PDFs
        pdf_paths = tools.list_pdfs(input_dir)
        if not pdf_paths:
            return False, 0, f"No PDF files found in {input_dir}"
        
        print(f"Found {len(pdf_paths)} PDF file(s) to process")
        
        # Step 2: Process each PDF deterministically
        for pdf_path in pdf_paths:
            try:
                print(f"Processing: {os.path.basename(pdf_path)}")
                
                # 2a. Extract text (deterministic)
                text_result = tools.extract_text_primary_fallbacks(pdf_path)
                if not text_result.text or len(text_result.text.strip()) < 10:
                    errors.append(f"{pdf_path}: No text extracted")
                    continue
                
                # For VSP detection, use raw extracted text (before cleaning)
                # Get raw text directly from parser_pack for accurate VSP detection
                raw_text = parser_pack.extract_text(pdf_path)
                if not raw_text or len(raw_text.strip()) < 10:
                    # Fallback to cleaned text if raw extraction fails
                    raw_text = text_result.text
                
                # Normalize text for source detection (detect_source expects _prenormalize'd text)
                normalized_text = parser_pack._prenormalize(raw_text)
                
                # Check if this is a VSP document (contains multiple cases)
                source = parser_pack.detect_source(normalized_text)
                
                if source == "VSP":
                    # VSP documents contain multiple cases - use legacy parser directly
                    # The LLM extraction is unreliable for VSP multi-case documents
                    print(f"  Detected VSP document with multiple cases")
                    print(f"  Using legacy parser for VSP document (more reliable for multi-case documents)")
                    
                    # Use the legacy parse_pdf_vsp function which handles all cases correctly
                    # The legacy parser includes geocoding, case splitting, and proper field extraction
                    try:
                        base_case_id = f"GRD-{datetime.now().strftime('%Y')}-{records_processed + 1:06d}"
                        vsp_records = parser_pack.parse_pdf_vsp(pdf_path, base_case_id, do_geocode=True, cache_only=False)
                        
                        if vsp_records:
                            print(f"  Legacy parser extracted {len(vsp_records)} cases from VSP document")
                            
                            # Process each VSP record from legacy parser
                            for idx, vsp_record in enumerate(vsp_records):
                                try:
                                    # Convert legacy record to GuardianRow format
                                    # The legacy parser already returns records in the correct format
                                    row_dict = vsp_record.copy()
                                    
                                    # Ensure case_id is properly formatted
                                    case_id = row_dict.get("case_id")
                                    if not case_id or not case_id.startswith("GRD-"):
                                        # Generate case_id if missing or invalid
                                        case_num = records_processed + idx + 1
                                        case_id = f"GRD-{datetime.now().strftime('%Y')}-{case_num:06d}"
                                        row_dict["case_id"] = case_id
                                    
                                    # Ensure provenance.sources includes "VSP"
                                    prov = row_dict.setdefault("provenance", {})
                                    sources = prov.setdefault("sources", [])
                                    if "VSP" not in sources:
                                        sources.insert(0, "VSP")
                                    
                                    # Remove _fulltext and any other non-schema fields before sanitization
                                    # Explicitly preserve source_path before sanitization
                                    
                                    # Clean and validate the record using sanitize_guardian_row
                                    # This function will ensure all required fields are present and properly formatted
                                    clean_row = sanitize_guardian_row(row_dict, source_path=pdf_path)
                                    
                                    # Validate and write
                                    try:
                                        row_obj = GuardianRow(**clean_row)
                                        validation_errors = tools.validate_row(row_obj, schema_path)
                                        if validation_errors:
                                            # Log validation errors but continue with writing
                                            error_msg = f"{pdf_path} (VSP case {case_id}): Validation warnings: {', '.join(validation_errors[:2])}"
                                            errors.append(error_msg)
                                            print(f"    Warning: {error_msg}")
                                        
                                        # Write output (even if there are validation warnings)
                                        tools.write_output(row_obj, out_jsonl, out_csv)
                                        records_processed += 1
                                        
                                    except Exception as e:
                                        error_msg = f"{pdf_path} (VSP case {case_id}): Validation/write failed: {str(e)}"
                                        errors.append(error_msg)
                                        print(f"    Error: {error_msg}")
                                        import traceback
                                        traceback.print_exc()
                                        continue
                                    
                                except Exception as e:
                                    error_msg = f"{pdf_path} (VSP case {idx + 1}): Processing failed: {str(e)}"
                                    errors.append(error_msg)
                                    print(f"    Error: {error_msg}")
                                    import traceback
                                    traceback.print_exc()
                                    continue
                            
                            print(f"  [OK] Processed {len(vsp_records)} VSP cases ({records_processed} total records)")
                        else:
                            errors.append(f"{pdf_path}: Legacy VSP parser returned no cases")
                        
                        # Skip LLM processing for VSP documents
                        continue
                        
                    except Exception as e:
                        errors.append(f"{pdf_path}: VSP parsing failed: {str(e)}")
                        import traceback
                        traceback.print_exc()
                        continue
                
                # 2b. Extract structured data (LLM) for single-case documents
                extracted_data = None
                for retry in range(max_retries):
                    try:
                        extract_messages = [
                            {"role": "system", "content": extract_prompt},
                            {"role": "user", "content": f"DOC_TEXT START\n{text_result.text[:50000]}\nDOC_TEXT END"}
                        ]
                        extracted_data = client.chat_json(extract_messages)
                        if isinstance(extracted_data, dict) and extracted_data:
                            break
                    except Exception as e:
                        if retry == max_retries - 1:
                            errors.append(f"{pdf_path}: Extraction failed after {max_retries} retries: {str(e)}")
                            extracted_data = None
                        else:
                            continue
                
                if not extracted_data:
                    continue
                
                # Ensure extracted_data is a dict
                if not isinstance(extracted_data, dict):
                    errors.append(f"{pdf_path}: Extracted data is not a dict")
                    continue
                
                # Sanitize extracted data to normalize keys, drop unknowns, coerce types, and enforce enums
                san = _sanitize_extracted(extracted_data)
                
                # 2c. Build current_row with sanitized data
                year = datetime.now().strftime("%Y")
                case_num = records_processed + 1
                case_id = f"GRD-{year}-{case_num:06d}"
                
                current_row = {
                    "source_path": pdf_path,
                    "case_id": case_id,
                    "demographic": san["demographic"],
                    "temporal": san["temporal"],
                    "spatial": san["spatial"],
                    "narrative_osint": san["narrative_osint"],
                    "outcome": san["outcome"],
                    "provenance": san["provenance"],
                    "audit": san["audit"]
                }
                
                # Ensure last_seen_ts is never empty (required field)
                # The sanitizer sets it to None if empty, so we need a fallback
                if not current_row["temporal"].get("last_seen_ts"):
                    current_row["temporal"]["last_seen_ts"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                
                # Ensure gender is set (required field)
                if not current_row["demographic"].get("gender"):
                    current_row["demographic"]["gender"] = "male"  # Default fallback
                
                # Ensure last_seen_lat/lon are set (required fields - schema requires them)
                # They're set to None by sanitizer, but schema requires numbers
                if current_row["spatial"].get("last_seen_lat") is None:
                    current_row["spatial"]["last_seen_lat"] = 0.0
                if current_row["spatial"].get("last_seen_lon") is None:
                    current_row["spatial"]["last_seen_lon"] = 0.0
                
                # 2d. Synthesize last_seen_location from city/state if missing
                if not current_row["spatial"].get("last_seen_location"):
                    city = current_row["spatial"].get("last_seen_city")
                    state = current_row["spatial"].get("last_seen_state")
                    if city and state:
                        current_row["spatial"]["last_seen_location"] = f"{city}, {state}"
                    elif city:
                        current_row["spatial"]["last_seen_location"] = city
                    elif state:
                        current_row["spatial"]["last_seen_location"] = state
                
                # 2e. Geocode locations (deterministic)
                # Extract location strings from spatial data if available
                # Preserve locations_raw during geocoding - don't remove it yet
                location_queries = []
                if current_row["spatial"].get("last_seen_location"):
                    location_queries.append(current_row["spatial"]["last_seen_location"])
                elif current_row["spatial"].get("last_seen_city") and current_row["spatial"].get("last_seen_state"):
                    location_queries.append(f"{current_row['spatial']['last_seen_city']}, {current_row['spatial']['last_seen_state']}")
                
                # Use locations_raw for geocoding if available
                locations_raw = current_row["spatial"].get("locations_raw", [])
                if locations_raw:
                    if isinstance(locations_raw, list):
                        location_queries.extend(locations_raw)
                    elif isinstance(locations_raw, str):
                        location_queries.append(locations_raw)
                
                # Geocode using deterministic geocode_batch (don't let LLM free-compose)
                if location_queries:
                    try:
                        geocoded = tools.geocode_batch(location_queries, state_bias="VA")
                        
                        # Update last_seen_lat/lon only if a good geocoding result exists
                        if geocoded and len(geocoded) > 0 and geocoded[0].lat is not None and geocoded[0].lon is not None:
                            current_row["spatial"]["last_seen_lat"] = geocoded[0].lat
                            current_row["spatial"]["last_seen_lon"] = geocoded[0].lon
                    except (IndexError, Exception) as e:
                        # Geocoding failed or returned empty results - continue with default coordinates (0.0, 0.0)
                        pass
                
                # Remove locations_raw after geocoding (it's not in the schema)
                # Do this before validation so schema validation doesn't fail
                current_row["spatial"].pop("locations_raw", None)
                
                # 2f. Summarize (LLM)
                try:
                    context = {
                        "demographic": current_row.get("demographic", {}),
                        "temporal": current_row.get("temporal", {}),
                        "spatial": current_row.get("spatial", {}),
                        "narrative_spans": current_row.get("narrative_osint", {}).get("narrative_spans", [])
                    }
                    summ_messages = [
                        {"role": "system", "content": summarize_prompt},
                        {"role": "user", "content": json.dumps({"context": context})}
                    ]
                    summary_result = client.chat_json(summ_messages)
                    
                    if isinstance(summary_result, dict):
                        if "summary" in summary_result:
                            current_row["narrative_osint"]["incident_summary"] = summary_result["summary"]
                        # Note: "timeline" is not in schema, so don't add it
                except Exception as e:
                    errors.append(f"{pdf_path}: Summarization failed: {str(e)}")
                    # Continue without summary
                    if not current_row["narrative_osint"].get("incident_summary"):
                        current_row["narrative_osint"]["incident_summary"] = "No summary available"
                
                # Ensure incident_summary exists (required or at least expected)
                if not current_row["narrative_osint"].get("incident_summary"):
                    current_row["narrative_osint"]["incident_summary"] = "No summary available"
                
                # Remove non-schema fields before sanitization
                # narrative_spans is not in the schema, so remove it
                current_row["narrative_osint"].pop("narrative_spans", None)
                
                # 2f. Post-process and sanitize for schema compliance
                # First apply coerce_guardian for basic repairs
                current_row = coerce_guardian(current_row)
                # Then apply schema sanitizer for final cleanup
                clean_row = sanitize_guardian_row(current_row, source_path=pdf_path)
                
                # 2g. Validate (deterministic) - must pass before writing
                validation_passed = False
                validation_errors = []
                try:
                    row_obj = GuardianRow(**clean_row)
                    validation_errors = tools.validate_row(row_obj, schema_path)
                    if validation_errors:
                        # Try validator-guided repair (one retry)
                        print(f"  [RETRY] Validation failed, attempting repair: {os.path.basename(pdf_path)}")
                        repaired_row = _repair_with_validator_feedback(clean_row, validation_errors, client, extract_prompt, pdf_path, text_result.text[:50000])
                        if repaired_row:
                            # Re-coerce and re-sanitize repaired row
                            repaired_row = coerce_guardian(repaired_row)
                            repaired_row = sanitize_guardian_row(repaired_row, source_path=pdf_path)
                            try:
                                row_obj = GuardianRow(**repaired_row)
                                validation_errors = tools.validate_row(row_obj, schema_path)
                                if not validation_errors:
                                    clean_row = repaired_row
                                    validation_passed = True
                                else:
                                    # Still failed after repair
                                    error_details = "; ".join(validation_errors[:5])
                                    error_msg = f"{pdf_path}: Schema validation failed after repair: {error_details}"
                                    errors.append(error_msg)
                                    print(f"  [SKIPPED] Validation failed after repair: {os.path.basename(pdf_path)}")
                                    validation_passed = False
                            except Exception as e:
                                error_msg = f"{pdf_path}: Record creation failed after repair: {str(e)}"
                                errors.append(error_msg)
                                validation_passed = False
                        else:
                            # Repair failed
                            error_details = "; ".join(validation_errors[:5])
                            error_msg = f"{pdf_path}: Schema validation failed: {error_details}"
                            if len(validation_errors) > 5:
                                error_msg += f" (and {len(validation_errors) - 5} more errors)"
                            errors.append(error_msg)
                            print(f"  [SKIPPED] Validation failed: {os.path.basename(pdf_path)}")
                            validation_passed = False
                    else:
                        validation_passed = True
                except Exception as e:
                    # Pydantic validation or other error occurred
                    error_msg = f"{pdf_path}: Record creation/validation failed: {str(e)}"
                    errors.append(error_msg)
                    print(f"  [SKIPPED] Record creation failed: {os.path.basename(pdf_path)}")
                    print(f"    Error: {str(e)}")
                    validation_passed = False
                
                # 2h. Apply legacy normalizers if validation passed
                if validation_passed:
                    try:
                        # Convert to dict for normalizer functions
                        row_dict = clean_row.copy()
                        # Store _fulltext at top level if available
                        if text_result.text:
                            row_dict["_fulltext"] = text_result.text
                        
                        # Apply legacy normalizers
                        row_dict = parser_pack.harmonize_record_fields(row_dict)
                        row_dict = parser_pack._enrich_common_fields(row_dict, text_result.text)
                        
                        # Re-apply sanitizer to strip any disallowed keys added by normalizers
                        # Preserve source_path from clean_row
                        row_dict = sanitize_guardian_row(row_dict, source_path=pdf_path)
                        
                        # Re-create GuardianRow with normalized and sanitized data
                        # Remove _fulltext before creating row (not in schema)
                        _fulltext = row_dict.pop("_fulltext", None)
                        row_obj = GuardianRow(**row_dict)
                        
                        # Final validation after normalization and sanitization
                        final_errors = tools.validate_row(row_obj, schema_path)
                        if final_errors:
                            # If normalization introduced errors, use original clean_row
                            error_details = "; ".join(final_errors[:3])
                            print(f"  [WARN] Normalization introduced errors, using original: {error_details}")
                            row_obj = GuardianRow(**clean_row)
                    except Exception as e:
                        # If normalization fails, use original clean_row
                        print(f"  [WARN] Normalization failed, using original: {str(e)}")
                        row_obj = GuardianRow(**clean_row)
                
                # 2i. Write output (deterministic) - only if validation passed
                if validation_passed:
                    try:
                        tools.write_output(row_obj, out_jsonl, out_csv)
                        records_processed += 1
                        print(f"  [OK] Processed {records_processed} record(s)")
                    except Exception as e:
                        errors.append(f"{pdf_path}: Write failed: {str(e)}")
                        print(f"  [ERROR] Write failed: {os.path.basename(pdf_path)}")
                        continue
                else:
                    # Skip writing this record
                    continue
                    
            except Exception as e:
                errors.append(f"{pdf_path}: Processing failed: {str(e)}")
                continue
        
        # Return results
        if records_processed > 0:
            error_msg = None
            if errors:
                error_msg = f"Processed {records_processed} record(s) with {len(errors)} error(s): " + "; ".join(errors[:5])
            return True, records_processed, error_msg
        else:
            error_msg = "No records processed. Errors: " + "; ".join(errors[:10]) if errors else "No records processed"
            return False, 0, error_msg
    
    except Exception as e:
        error_msg = f"Agent error: {str(e)}"
        if errors:
            error_msg += ". Additional errors: " + "; ".join(errors[:5])
        return False, records_processed, error_msg

