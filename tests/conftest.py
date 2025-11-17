"""Pytest configuration and shared fixtures for Guardian Parser Pack tests.

Provides common fixtures for schema validation, mock LLM clients,
sample data, and utility functions used across test modules.
"""
import os
import json
import tempfile
import shutil
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List
from unittest.mock import Mock, MagicMock, patch
import pytest
from jsonschema import Draft7Validator


# Get project root directory
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
SCHEMA_PATH = PROJECT_ROOT / "schemas" / "guardian_schema.json"


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test outputs.

    Returns:
        Path to temporary directory (automatically cleaned up after test).
    """
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def temp_output_dir(temp_dir):
    """Create a temporary output directory structure.

    Args:
        temp_dir: Temporary directory fixture.

    Returns:
        Path to temporary output directory.
    """
    output_dir = Path(temp_dir) / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    return str(output_dir)


@pytest.fixture
def schema_path():
    """Return path to guardian_schema.json.

    Returns:
        Path to schema file as string.
    """
    return str(SCHEMA_PATH)


@pytest.fixture
def schema():
    """Load and return the Guardian schema.

    Returns:
        Parsed JSON schema dictionary.
    """
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def schema_validator(schema):
    """Create a Draft7Validator instance for the schema.

    Args:
        schema: Schema dictionary fixture.

    Returns:
        Draft7Validator instance configured with schema.
    """
    return Draft7Validator(schema)


@pytest.fixture
def mock_llm_client():
    """Create a mock LLM client.

    Returns:
        Mock LLMClient instance with default configuration.
    """
    client = Mock()
    client.chat_json = Mock(return_value={})
    client.backend = "ollama"
    client.ollama_model = "llama3.2"
    client.temperature = 0.1
    client.json_mode = True
    return client


@pytest.fixture
def mock_ollama_response():
    """Create a mock Ollama API response factory.

    Returns:
        Factory function that creates mock response objects with specified
        content and status code.
    """
    def _create_response(content: Dict[str, Any], status_code: int = 200):
        response = Mock()
        response.status_code = status_code
        response.json = Mock(return_value={
            "message": {
                "content": json.dumps(content)
            }
        })
        response.text = json.dumps(content)
        response.raise_for_status = Mock()
        return response
    return _create_response


@pytest.fixture
def sample_text_pdf(temp_dir):
    """Create a minimal text-based PDF file for testing.

    Args:
        temp_dir: Temporary directory fixture.

    Returns:
        Path to sample PDF file as string.
    """
    # Create a simple text file that simulates a PDF (for testing purposes)
    # In real tests, this would be a proper PDF, but for unit tests we can use text
    pdf_path = Path(temp_dir) / "sample.pdf"
    pdf_path.write_text("Sample PDF content for testing")
    return str(pdf_path)


@pytest.fixture
def sample_namus_text():
    """Sample NamUs text for testing."""
    return """
    NamUs Case Created: 2023-01-15
    Date of Last Contact: January 10, 2023
    Name: John Doe
    Age: 25
    Sex: Male
    Race: White
    Height: 5'10"
    Weight: 180 lbs
    Missing From: Richmond, VA
    """


@pytest.fixture
def sample_ncmec_text():
    """Sample NCMEC text for testing."""
    return """
    Have you seen this child?
    NCMEC
    Missing Since: January 10, 2023
    Name: Jane Smith
    Age: 12
    Sex: Female
    Missing From: Virginia
    """


@pytest.fixture
def sample_vsp_text():
    """Sample VSP text for testing."""
    return """
    MISSING PERSONS
    Missing From: Richmond, Virginia
    Missing Since: January 10, 2023
    Contact: Virginia State Police
    VAA23-1234
    """


@pytest.fixture
def sample_fbi_text():
    """Sample FBI text for testing."""
    return """
    FBI
    www.fbi.gov
    Federal Bureau of Investigation
    If you have any information concerning this person
    Field Office: Richmond
    """


@pytest.fixture
def sample_charley_text():
    """Sample Charley Project text for testing."""
    return """
    The Charley Project
    Details of Disappearance
    Missing From: Virginia
    """


@pytest.fixture
def sample_unknown_text():
    """Sample unknown source text for testing."""
    return """
    Some random text that doesn't match any known source patterns.
    Name: Test Person
    Age: 30
    """


@pytest.fixture
def mock_geocode_cache():
    """Create a mock geocoding cache."""
    return {
        "Richmond, VA": {"lat": 37.5407, "lon": -77.4360},
        "Virginia": {"lat": 37.7693, "lon": -78.1697},
    }


@pytest.fixture
def mock_pytesseract():
    """Mock pytesseract for OCR fallback testing."""
    with patch("parser_pack.pytesseract") as mock_tesseract:
        mock_tesseract.image_to_string = Mock(return_value="OCR extracted text")
        yield mock_tesseract


def compare_json_semantic(actual: Dict[str, Any], expected: Dict[str, Any], 
                          ignore_keys: Optional[List[str]] = None) -> Tuple[bool, List[str]]:
    """Compare two JSON objects semantically (order-independent).

    Args:
        actual: The actual JSON object to compare.
        expected: The expected JSON object to compare against.
        ignore_keys: Optional list of keys to ignore during comparison.

    Returns:
        Tuple containing:
            - is_equal: True if objects match semantically.
            - differences: List of difference descriptions if not equal.
    """
    if ignore_keys is None:
        ignore_keys = []
    
    differences = []
    
    def _normalize(obj, path=""):
        """Normalize JSON object for comparison."""
        if isinstance(obj, dict):
            # Sort keys and filter ignored keys
            normalized = {}
            for k, v in sorted(obj.items()):
                if k not in ignore_keys:
                    full_path = f"{path}.{k}" if path else k
                    normalized[k] = _normalize(v, full_path)
            return normalized
        elif isinstance(obj, list):
            # Sort lists if all elements are comparable
            try:
                return sorted([_normalize(item, f"{path}[{i}]") for i, item in enumerate(obj)])
            except TypeError:
                # If not sortable, just normalize elements
                return [_normalize(item, f"{path}[{i}]") for i, item in enumerate(obj)]
        else:
            return obj
    
    def _compare(a, b, path=""):
        """Recursively compare two objects."""
        if type(a) != type(b):
            differences.append(f"{path}: Type mismatch - {type(a).__name__} vs {type(b).__name__}")
            return False
        
        if isinstance(a, dict):
            all_keys = set(a.keys()) | set(b.keys())
            for key in all_keys:
                if key in ignore_keys:
                    continue
                new_path = f"{path}.{key}" if path else key
                if key not in a:
                    differences.append(f"{new_path}: Missing in actual")
                    return False
                if key not in b:
                    differences.append(f"{new_path}: Missing in expected")
                    return False
                if not _compare(a[key], b[key], new_path):
                    return False
            return True
        elif isinstance(a, list):
            if len(a) != len(b):
                differences.append(f"{path}: List length mismatch - {len(a)} vs {len(b)}")
                return False
            for i, (a_item, b_item) in enumerate(zip(a, b)):
                if not _compare(a_item, b_item, f"{path}[{i}]"):
                    return False
            return True
        else:
            if a != b:
                differences.append(f"{path}: Value mismatch - {a} vs {b}")
                return False
            return True
    
    # Normalize both objects
    norm_actual = _normalize(actual)
    norm_expected = _normalize(expected)
    
    # Compare
    is_equal = _compare(norm_actual, norm_expected)
    
    return is_equal, differences


@pytest.fixture
def json_comparator():
    """Return the JSON comparison function.

    Returns:
        compare_json_semantic function for use in tests.
    """
    return compare_json_semantic

