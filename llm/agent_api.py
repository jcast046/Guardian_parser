"""Programmatic API for the Guardian agent system.

Provides high-level interface for running the Guardian LLM agent
programmatically without CLI dependencies.
"""
import os
import sys
import subprocess
from pathlib import Path
from typing import Optional, Tuple

from .agent.llm_agent_simple import run_agent_simple as run_agent


def run_agent_api(
    input_dir: str,
    out_jsonl: Optional[str] = None,
    out_csv: Optional[str] = None,
    schema_path: Optional[str] = None,
    backend: str = "ollama",
    model_path: Optional[str] = None,
    ollama_model: str = "llama3.2",
    max_steps: int = 60,
    fallback_on_error: bool = False
) -> Tuple[bool, int, Optional[str]]:
    """Run the Guardian agent programmatically.

    Args:
        input_dir: Directory containing PDF files to process.
        out_jsonl: Path to output JSONL file (default: data/guardian_output.jsonl).
        out_csv: Path to output CSV file (default: data/guardian_output.csv).
        schema_path: Path to JSON schema file (default: schemas/guardian_schema.json).
        backend: LLM backend to use ("ollama" or "llama", default: "ollama").
        model_path: Path to GGUF model file (required for llama backend).
        ollama_model: Ollama model name (default: "llama3.2").
        max_steps: Maximum number of agent steps (default: 60, unused in simplified agent).
        fallback_on_error: If True, call sample_run.py via subprocess on failure.

    Returns:
        Tuple containing:
            - success: True if processing completed successfully.
            - records_processed: Number of records successfully processed.
            - error_message: Error message if processing failed, None otherwise.

    Example:
        >>> from guardian_parser_pack.agent_api import run_agent_api
        >>> success, count, error = run_agent_api("evidence/")
        >>> if success:
        ...     print(f"Processed {count} records")
        ... else:
        ...     print(f"Failed: {error}")
    """
    # Get root directory (parent of guardian_parser_pack package)
    root_dir = Path(__file__).parent.parent.resolve()
    
    # Set defaults
    if out_jsonl is None:
        out_jsonl = str(root_dir / "data" / "guardian_output.jsonl")
    if out_csv is None:
        out_csv = str(root_dir / "data" / "guardian_output.csv")
    if schema_path is None:
        schema_path = str(root_dir / "schemas" / "guardian_schema.json")
    
    # Ensure output directories exist
    os.makedirs(os.path.dirname(out_jsonl), exist_ok=True)
    if out_csv:
        os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    
    # Run agent (simplified version - max_steps is ignored, processes all PDFs)
    try:
        success, records_processed, error_message = run_agent(
            input_dir=input_dir,
            out_jsonl=out_jsonl,
            out_csv=out_csv,
            schema_path=schema_path,
            backend=backend,
            model_path=model_path,
            ollama_model=ollama_model,
            max_retries=3  # Simplified agent uses max_retries instead of max_steps
        )
        
        # If failed and fallback requested, call sample_run.py
        if not success and fallback_on_error:
            try:
                sample_run_path = root_dir / "sample_run.py"
                if sample_run_path.exists():
                    # Call sample_run.py via subprocess
                    result = subprocess.run(
                        [sys.executable, str(sample_run_path)],
                        cwd=str(root_dir),
                        capture_output=True,
                        text=True
                    )
                    if result.returncode == 0:
                        return True, records_processed, "Agent failed but fallback succeeded"
                    else:
                        return False, records_processed, f"Agent and fallback both failed: {error_message}"
                else:
                    return success, records_processed, error_message
            except Exception as e:
                return False, records_processed, f"Agent failed and fallback error: {str(e)}"
        
        return success, records_processed, error_message
    
    except Exception as e:
        error_msg = f"Agent API error: {str(e)}"
        if fallback_on_error:
            try:
                sample_run_path = root_dir / "sample_run.py"
                if sample_run_path.exists():
                    result = subprocess.run(
                        [sys.executable, str(sample_run_path)],
                        cwd=str(root_dir),
                        capture_output=True,
                        text=True
                    )
                    if result.returncode == 0:
                        return True, 0, "Agent error but fallback succeeded"
            except Exception:
                pass
        return False, 0, error_msg

