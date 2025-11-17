"""Main agent orchestration loop for Guardian PDF processing.

Provides LLM-driven agent with tool-calling capabilities for processing
PDF documents through extraction, geocoding, summarization, and validation.
"""
import os
import sys
import json
from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path

from .llm_client import LLMClient
from .protocols import AgentAction, GuardianRow
from . import tools


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


def run_agent(
    input_dir: str,
    out_jsonl: str,
    out_csv: Optional[str],
    schema_path: str,
    backend: str = "ollama",
    model_path: Optional[str] = None,
    ollama_model: str = "llama3.2",
    max_steps: int = 60
    ) -> tuple[bool, int, Optional[str]]:
    """Run agent to process PDFs.

    Uses LLM-driven agent with tool-calling to orchestrate PDF processing
    through extraction, geocoding, summarization, and validation steps.

    Args:
        input_dir: Directory containing PDF files to process.
        out_jsonl: Path to output JSONL file.
        out_csv: Optional path to output CSV file.
        schema_path: Path to JSON schema file for validation.
        backend: LLM backend to use ("ollama" or "llama").
        model_path: Path to GGUF model file (required for llama backend).
        ollama_model: Ollama model name (required for ollama backend).
        max_steps: Maximum number of agent action steps.

    Returns:
        Tuple containing:
            - success: True if processing completed successfully.
            - records_processed: Number of records successfully processed.
            - error_message: Error message if processing failed, None otherwise.
    """
    records_processed = 0
    try:
        # Initialize LLM client
        client = LLMClient(
            backend=backend,
            model_path=model_path,
            ollama_model=ollama_model,
            json_mode=True
        )
        
        # Load system prompt - try simple version first
        try:
            system_prompt = _read("system_orchestrator_simple.txt").format(input_dir=input_dir)
        except FileNotFoundError:
            system_prompt = _read("system_orchestrator.txt").format(input_dir=input_dir)
        
        # Initialize conversation with explicit example
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Start processing PDFs in {input_dir}. Your first action must be: {{\"type\": \"list_pdfs\", \"args\": {{\"directory\": \"{input_dir}\"}}}}"}
        ]
        
        records_processed = 0
        current_pdf = None
        current_text = None
        current_row = None
        
        # Agent loop
        for step in range(max_steps):
            # Get action from LLM
            try:
                plan = client.chat_json(messages)
                
                # Ensure we have a type field - if not, try to extract from nested structure
                if "type" not in plan:
                    # Try to find the action in common wrapper keys
                    for key in ["result", "action", "response", "data", "output"]:
                        if key in plan and isinstance(plan[key], dict) and "type" in plan[key]:
                            plan = plan[key]
                            break
                    
                    # If still no type, check if it's a list
                    if "type" not in plan and isinstance(plan, list) and len(plan) > 0:
                        if isinstance(plan[0], dict) and "type" in plan[0]:
                            plan = plan[0]
                    
                    # Final check
                    if "type" not in plan:
                        return False, records_processed, f"LLM response missing 'type' field. Got: {str(plan)[:500]}"
                
                action = AgentAction(**plan)
            except Exception as e:
                error_detail = f"Failed to get agent action at step {step}: {str(e)}"
                if 'plan' in locals():
                    error_detail += f"\nLLM returned: {plan}"
                return False, records_processed, error_detail
            
            # Handle action
            if action.type == "list_pdfs":
                directory = action.args.get("directory", input_dir)
                paths = tools.list_pdfs(directory)
                messages.append({
                    "role": "tool",
                    "content": json.dumps({
                        "tool": "list_pdfs",
                        "result": paths,
                        "message": f"Found {len(paths)} PDF(s). Process each one with: ocr_text -> extract_json -> geocode_batch -> summarize -> validate -> write_output"
                    })
                })
            
            elif action.type == "ocr_text":
                path = action.args.get("path")
                if not path:
                    messages.append({
                        "role": "tool",
                        "content": json.dumps({"tool": "ocr_text", "result": "error: path required"})
                    })
                else:
                    force_ocr = action.args.get("force_ocr", False)
                    page_range = action.args.get("page_range")
                    ret = tools.extract_text_primary_fallbacks(path, force_ocr, page_range)
                    current_pdf = path
                    current_text = ret.text
                    messages.append({
                        "role": "tool",
                        "content": json.dumps({"tool": "ocr_text", "result": ret.model_dump()})
                    })
            
            elif action.type == "extract_json":
                if not current_text:
                    messages.append({
                        "role": "tool",
                        "content": json.dumps({
                            "tool": "extract_json",
                            "result": "error",
                            "message": "No text available. Call ocr_text first."
                        })
                    })
                else:
                    # Ask LLM to extract structured data
                    extract_prompt = _read("extract_guardian_schema.txt")
                    extract_messages = [
                        {"role": "system", "content": extract_prompt},
                        {"role": "user", "content": f"DOC_TEXT START\n{current_text[:50000]}\nDOC_TEXT END"}
                    ]
                    try:
                        extracted = client.chat_json(extract_messages)
                        # Ensure extracted is a dict
                        if not isinstance(extracted, dict):
                            raise ValueError(f"Extracted data must be a dict, got {type(extracted)}")
                        
                        # Merge into current_row or create new
                        if current_row:
                            current_row.update(extracted)
                        else:
                            current_row = extracted
                        
                        # Ensure it's a dict
                        if not isinstance(current_row, dict):
                            current_row = {}
                        
                        # Ensure required fields
                        if "case_id" not in current_row or not current_row["case_id"]:
                            # Generate case_id
                            year = datetime.now().strftime("%Y")
                            case_num = records_processed + 1
                            current_row["case_id"] = f"GRD-{year}-{case_num:06d}"
                        
                        if "source_path" not in current_row:
                            current_row["source_path"] = current_pdf or ""
                        
                        # Ensure nested dicts exist
                        for key in ["demographic", "temporal", "spatial", "narrative_osint", "outcome", "provenance", "audit"]:
                            if key not in current_row:
                                current_row[key] = {}
                        
                        # Set defaults for required schema fields
                        if "timezone" not in current_row.get("temporal", {}):
                            current_row.setdefault("temporal", {})["timezone"] = "America/New_York"
                        if "last_seen_ts" not in current_row.get("temporal", {}):
                            current_row.setdefault("temporal", {})["last_seen_ts"] = ""
                        if "case_status" not in current_row.get("outcome", {}):
                            current_row.setdefault("outcome", {})["case_status"] = "ongoing"
                        if "locations_raw" not in current_row.get("spatial", {}):
                            current_row.setdefault("spatial", {})["locations_raw"] = []
                        if "last_seen_lat" not in current_row.get("spatial", {}):
                            current_row.setdefault("spatial", {})["last_seen_lat"] = 0.0
                        if "last_seen_lon" not in current_row.get("spatial", {}):
                            current_row.setdefault("spatial", {})["last_seen_lon"] = 0.0
                        if "gender" not in current_row.get("demographic", {}):
                            current_row.setdefault("demographic", {})["gender"] = ""  # Will be validated
                        if "incident_summary" not in current_row.get("narrative_osint", {}):
                            current_row.setdefault("narrative_osint", {})["incident_summary"] = ""
                        
                        messages.append({
                            "role": "tool",
                            "content": json.dumps({
                                "tool": "extract_json",
                                "result": "success",
                                "message": "Data extracted successfully. Continue with geocode_batch."
                            })
                        })
                    except Exception as e:
                        messages.append({
                            "role": "tool",
                            "content": json.dumps({
                                "tool": "extract_json",
                                "result": "error",
                                "message": f"Extraction failed: {str(e)}. Try again or use fail action."
                            })
                        })
            
            elif action.type == "geocode_batch":
                places = action.args.get("places", [])
                if not places and current_row and "locations_raw" in current_row.get("spatial", {}):
                    places = current_row["spatial"]["locations_raw"]
                
                if places:
                    geos = [g.model_dump() for g in tools.geocode_batch(places, action.args.get("state_bias", "VA"))]
                    # Update spatial.locations_geocoded
                    if current_row:
                        if "locations_geocoded" not in current_row.get("spatial", {}):
                            current_row.setdefault("spatial", {})["locations_geocoded"] = []
                        current_row["spatial"]["locations_geocoded"] = geos
                        
                        # Also update last_seen_lat/lon if we have a primary location
                        if geos and geos[0].get("lat") and geos[0].get("lon"):
                            current_row["spatial"]["last_seen_lat"] = geos[0]["lat"]
                            current_row["spatial"]["last_seen_lon"] = geos[0]["lon"]
                    
                    messages.append({
                        "role": "tool",
                        "content": json.dumps({"tool": "geocode_batch", "result": geos})
                    })
                else:
                    messages.append({
                        "role": "tool",
                        "content": json.dumps({"tool": "geocode_batch", "result": []})
                    })
            
            elif action.type == "summarize":
                if not current_row:
                    messages.append({
                        "role": "tool",
                        "content": json.dumps({"tool": "summarize", "result": "error: no row to summarize"})
                    })
                else:
                    summ_prompt = _read("summarize_case.txt")
                    context = {
                        "demographic": current_row.get("demographic", {}),
                        "temporal": current_row.get("temporal", {}),
                        "spatial": current_row.get("spatial", {}),
                        "narrative_spans": current_row.get("narrative_osint", {}).get("narrative_spans", [])
                    }
                    summ_messages = [
                        {"role": "system", "content": summ_prompt},
                        {"role": "user", "content": json.dumps({"context": context})}
                    ]
                    try:
                        summary_result = client.chat_json(summ_messages)
                        # Update narrative_osint
                        if "narrative_osint" not in current_row:
                            current_row["narrative_osint"] = {}
                        current_row["narrative_osint"]["incident_summary"] = summary_result.get("summary", "")
                        current_row["narrative_osint"]["timeline"] = summary_result.get("timeline", [])
                        messages.append({
                            "role": "tool",
                            "content": json.dumps({"tool": "summarize", "result": "success"})
                        })
                    except Exception as e:
                        messages.append({
                            "role": "tool",
                            "content": json.dumps({"tool": "summarize", "result": f"error: {str(e)}"})
                        })
            
            elif action.type == "validate":
                if not current_row:
                    messages.append({
                        "role": "tool",
                        "content": json.dumps({"tool": "validate", "result": "error: no row to validate"})
                    })
                else:
                    try:
                        row_obj = GuardianRow(**current_row)
                        errors = tools.validate_row(row_obj, schema_path)
                        if errors:
                            messages.append({
                                "role": "tool",
                                "content": json.dumps({"tool": "validate", "result": errors})
                            })
                        else:
                            messages.append({
                                "role": "tool",
                                "content": json.dumps({"tool": "validate", "result": []})
                            })
                    except Exception as e:
                        messages.append({
                            "role": "tool",
                            "content": json.dumps({"tool": "validate", "result": [f"Error: {str(e)}"]})
                        })
            
            elif action.type == "write_output":
                # Use row from args if provided, otherwise use current_row
                row_data = action.args.get("row")
                if not row_data:
                    row_data = current_row
                
                if not row_data:
                    messages.append({
                        "role": "tool",
                        "content": json.dumps({
                            "tool": "write_output",
                            "result": "error",
                            "message": "No row data available. You must complete: ocr_text -> extract_json -> geocode_batch -> summarize -> validate before write_output. Current status: " + 
                                     (f"PDF={current_pdf}, " if current_pdf else "No PDF loaded, ") +
                                     (f"Text extracted, " if current_text else "No text extracted, ") +
                                     (f"Row extracted" if current_row else "No row extracted")
                        })
                    })
                else:
                    try:
                        # Ensure row_data is a dict (not a string)
                        if isinstance(row_data, str):
                            row_data = json.loads(row_data)
                        
                        # Ensure it's a dict
                        if not isinstance(row_data, dict):
                            raise ValueError(f"row_data must be a dict, got {type(row_data)}")
                        
                        # Create GuardianRow from dict
                        row_obj = GuardianRow(**row_data)
                        tools.write_output(row_obj, out_jsonl, out_csv)
                        records_processed += 1
                        messages.append({
                            "role": "tool",
                            "content": json.dumps({"tool": "write_output", "result": "ok"})
                        })
                        # Reset for next PDF
                        current_row = None
                        current_pdf = None
                        current_text = None
                    except Exception as e:
                        error_msg = f"error: {str(e)}"
                        messages.append({
                            "role": "tool",
                            "content": json.dumps({"tool": "write_output", "result": error_msg})
                        })
            
            elif action.type == "finish":
                messages.append({
                    "role": "tool",
                    "content": json.dumps({"tool": "finish", "result": "done"})
                })
                break
            
            elif action.type == "fail":
                error_msg = action.args.get("reason", "Agent reported failure")
                return False, records_processed, error_msg
            
            else:
                messages.append({
                    "role": "tool",
                    "content": json.dumps({"tool": "unknown", "result": f"Unknown action type: {action.type}"})
                })
        
        # If we exited loop without finish, check if we processed anything
        if records_processed > 0:
            return True, records_processed, None
        else:
            # Provide more context about what went wrong
            error_detail = "Agent did not complete successfully. "
            if current_row:
                error_detail += "Row was extracted but not written. "
            elif current_text:
                error_detail += "Text was extracted but row was not created. "
            elif current_pdf:
                error_detail += f"Started processing {current_pdf} but did not complete. "
            else:
                error_detail += "No PDFs were processed. Check if list_pdfs found any files."
            return False, records_processed, error_detail
    
    except Exception as e:
        return False, records_processed, f"Agent error: {str(e)}"

