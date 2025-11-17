"""Guardian Parser Pack - Agentic LLM Integration.

Provides LLM-powered agent for processing missing person case documents
with structured extraction and normalization.
"""

__version__ = "0.1.0"

# Export main API function
from .agent_api import run_agent_api

__all__ = ["run_agent_api"]

