#!/usr/bin/env python3
"""CLI wrapper for the Guardian agent system.

Provides command-line interface for running the Guardian LLM agent
to process PDF documents.
"""
import argparse
import sys
from pathlib import Path

from .agent_api import run_agent_api


def main():
    """CLI entry point for Guardian agent.

    Parses command-line arguments and runs the agent API.

    Returns:
        Exit code: 0 on success, 1 on failure, 130 on keyboard interrupt.
    """
    parser = argparse.ArgumentParser(
        description="Guardian Agent - LLM-powered PDF processing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Use Ollama (default)
  python -m guardian_parser_pack.run_agent --input-dir evidence/ --backend ollama

  # Use llama.cpp with local GGUF model
  python -m guardian_parser_pack.run_agent --input-dir evidence/ --backend llama --model-path models/Llama3_2-3B-Instruct/model.gguf

  # With fallback to deterministic parser
  python -m guardian_parser_pack.run_agent --input-dir evidence/ --fallback-deterministic
        """
    )
    
    parser.add_argument(
        "--input-dir",
        required=True,
        help="Directory containing PDF files to process"
    )
    
    parser.add_argument(
        "--out-jsonl",
        default=None,
        help="Path to output JSONL file (default: data/guardian_output.jsonl)"
    )
    
    parser.add_argument(
        "--out-csv",
        default=None,
        help="Path to output CSV file (default: data/guardian_output.csv)"
    )
    
    parser.add_argument(
        "--schema",
        default=None,
        help="Path to JSON schema file (default: schemas/guardian_schema.json)"
    )
    
    parser.add_argument(
        "--backend",
        choices=["ollama", "llama"],
        default="ollama",
        help="LLM backend to use (default: ollama)"
    )
    
    parser.add_argument(
        "--model-path",
        default=None,
        help="Path to GGUF model file (for llama backend)"
    )
    
    parser.add_argument(
        "--ollama-model",
        default="llama3.2",
        help="Ollama model name (default: llama3.2)"
    )
    
    parser.add_argument(
        "--max-steps",
        type=int,
        default=60,
        help="Maximum number of agent steps (ignored in simplified agent, kept for compatibility)"
    )
    
    parser.add_argument(
        "--fallback-deterministic",
        action="store_true",
        help="Call sample_run.py via subprocess if agent fails"
    )
    
    args = parser.parse_args()
    
    # Run agent
    success, records_processed, error_message = run_agent_api(
        input_dir=args.input_dir,
        out_jsonl=args.out_jsonl,
        out_csv=args.out_csv,
        schema_path=args.schema,
        backend=args.backend,
        model_path=args.model_path,
        ollama_model=args.ollama_model,
        max_steps=args.max_steps,
        fallback_on_error=args.fallback_deterministic
    )
    
    # Report results
    if success:
        print(f"Successfully processed {records_processed} record(s)")
        if error_message and "fallback" in error_message.lower():
            print(f"Note: {error_message}")
        sys.exit(0)
    else:
        print(f"Failed: {error_message}", file=sys.stderr)
        print(f"Processed {records_processed} record(s) before failure", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

