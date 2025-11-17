"""Protocol definitions for the Guardian agent system.

Pydantic models defining data structures for agent actions, tool I/O,
and Guardian row format.
"""
from __future__ import annotations
from pydantic import BaseModel, Field
from typing import List, Optional, Literal, Dict


# ---- tool I/O models ----

class ListPdfsArgs(BaseModel):
    """Arguments for list_pdfs tool."""
    directory: str


class OCRTextArgs(BaseModel):
    """Arguments for ocr_text tool."""
    path: str
    force_ocr: bool = False
    page_range: Optional[str] = None


class OCRTextReturn(BaseModel):
    """Return value from ocr_text tool."""
    text: str
    modality: Literal["pdf", "image"] = "pdf"
    pages: List[int] = []
    meta: Dict = Field(default_factory=dict)


class GeocodeArgs(BaseModel):
    """Arguments for geocode tool."""
    query: str
    state_bias: Optional[str] = "VA"


class GeocodeReturn(BaseModel):
    """Return value from geocode tool."""
    raw: str
    lat: Optional[float] = None
    lon: Optional[float] = None
    confidence: float = 0.0
    provider: str = "nominatim"


# ---- final row (matches full guardian_schema.json structure) ----

class GuardianRow(BaseModel):
    """Guardian case record matching guardian_schema.json structure."""
    source_path: str
    case_id: str
    
    demographic: Dict = Field(default_factory=dict)
    temporal: Dict = Field(default_factory=dict)
    spatial: Dict = Field(default_factory=dict)
    narrative_osint: Dict = Field(default_factory=dict)
    outcome: Dict = Field(default_factory=dict)
    provenance: Dict = Field(default_factory=dict)
    audit: Dict = Field(default_factory=dict)


# ---- agent action protocol ----

class AgentAction(BaseModel):
    """Agent action specification for tool-calling."""
    type: Literal[
        "list_pdfs", "ocr_text", "extract_json", "geocode_batch",
        "summarize", "validate", "write_output", "finish", "fail"
    ]
    args: Dict = Field(default_factory=dict)

