#!/usr/bin/env python3
"""IDE-friendly script to run the Guardian LLM agent.

This script can be run directly from IDEs like VS Code or PyCharm.
Provides configuration section for easy modification of input/output paths
and LLM backend settings.
"""
import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.resolve()
sys.path.insert(0, str(project_root))

from guardian_parser_pack.agent_api import run_agent_api

# ============================================================================
# CONFIGURATION - Edit these paths as needed
# ============================================================================

# Input directory containing PDF files
INPUT_DIR = r"C:\Users\N0Cir\CS697\evidence"

# Output files (will be created if they don't exist)
OUTPUT_JSONL = project_root / "output" / "guardian_llm_output.jsonl"
OUTPUT_CSV = project_root / "output" / "guardian_llm_output.csv"

# Schema file path
SCHEMA_PATH = project_root / "schemas" / "guardian_schema.json"

# LLM Configuration
BACKEND = "ollama"  # Options: "ollama" or "llama"
OLLAMA_MODEL = "llama3.2"  # Change to "llama3.1:8b-instruct" for better results
MODEL_PATH = None  # Only needed for llama backend: "models/Llama3_2-3B-Instruct/model.gguf"

# Fallback options
FALLBACK_ON_ERROR = False  # Set to True to use deterministic parser if agent fails

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Run the Guardian LLM agent.

    Processes PDF files from the configured input directory and writes
    results to JSONL and CSV output files.

    Returns:
        Exit code: 0 on success, 1 on failure, 130 on keyboard interrupt.
    """
    print("=" * 70)
    print("Guardian LLM Parser Agent")
    print("=" * 70)
    print(f"Input directory: {INPUT_DIR}")
    print(f"Output JSONL: {OUTPUT_JSONL}")
    print(f"Output CSV: {OUTPUT_CSV}")
    print(f"Schema: {SCHEMA_PATH}")
    print(f"Backend: {BACKEND}")
    print(f"Model: {OLLAMA_MODEL}")
    print("=" * 70)
    print()
    
    # Ensure output directory exists
    OUTPUT_JSONL.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    
    # Run the agent
    try:
        success, records_processed, error_message = run_agent_api(
            input_dir=str(INPUT_DIR),
            out_jsonl=str(OUTPUT_JSONL),
            out_csv=str(OUTPUT_CSV),
            schema_path=str(SCHEMA_PATH) if SCHEMA_PATH.exists() else None,
            backend=BACKEND,
            model_path=MODEL_PATH,
            ollama_model=OLLAMA_MODEL,
            fallback_on_error=FALLBACK_ON_ERROR
        )
        
        # Report results
        print()
        print("=" * 70)
        print("RESULTS")
        print("=" * 70)
        
        if success:
            print(f"[SUCCESS] Successfully processed {records_processed} record(s)")
            print(f"  Output JSONL: {OUTPUT_JSONL}")
            print(f"  Output CSV: {OUTPUT_CSV}")
            if error_message and "error" in error_message.lower():
                print(f"  Note: {error_message}")
            return 0
        else:
            print(f"[FAILED] Failed: {error_message}")
            print(f"  Processed {records_processed} record(s) before failure")
            return 1
            
    except KeyboardInterrupt:
        print("\n\n[WARNING] Process interrupted by user")
        return 130
    except Exception as e:
        print(f"\n\n[ERROR] Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        print("=" * 70)


if __name__ == "__main__":
    sys.exit(main())

