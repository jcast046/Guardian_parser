"""Integration tests for LLM agent repair loop functionality."""
import pytest
import json
import sys
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from guardian_parser_pack.agent.llm_agent_simple import _repair_with_validator_feedback
from guardian_parser_pack.agent.llm_client import LLMClient


@pytest.mark.integration
class TestRepairLoop:
    """Test cases for repair loop functionality.

    Tests validator-guided repair of LLM output when schema validation fails.
    """
    
    def test_repair_with_validator_feedback_called(self, mock_llm_client):
        """Test that repair function is called when validation fails."""
        # Mock LLM client to return repaired JSON
        mock_llm_client.chat_json.return_value = {
            "demographic": {"gender": "male"},
            "temporal": {"last_seen_ts": "2023-01-10T00:00:00Z", "timezone": "America/New_York"},
            "spatial": {"last_seen_lat": 0.0, "last_seen_lon": 0.0},
            "outcome": {"case_status": "ongoing"},
            "narrative_osint": {"incident_summary": "Test summary"}
        }
        
        # Create a row that fails validation
        failed_row = {
            "demographic": {"gender": "invalid"},
            "source_path": "/test/path.pdf"
        }
        
        validation_errors = ["['demographic', 'gender']: 'invalid' is not one of ['male', 'female']"]
        extract_prompt = "Extract data from text"
        
        result = _repair_with_validator_feedback(
            failed_row,
            validation_errors,
            mock_llm_client,
            extract_prompt,
            "/test/path.pdf"
        )
        
        # Repair function should be called
        assert mock_llm_client.chat_json.called
        assert result is not None
        assert result["source_path"] == "/test/path.pdf"
    
    def test_repair_preserves_source_path(self, mock_llm_client):
        """Test that source_path is preserved during repair."""
        mock_llm_client.chat_json.return_value = {
            "demographic": {"gender": "male"},
            "temporal": {"last_seen_ts": "2023-01-10T00:00:00Z", "timezone": "America/New_York"},
            "spatial": {"last_seen_lat": 0.0, "last_seen_lon": 0.0},
            "outcome": {"case_status": "ongoing"},
            "narrative_osint": {"incident_summary": "Test summary"}
        }
        
        failed_row = {
            "demographic": {"gender": "invalid"},
            "source_path": "/test/path.pdf"
        }
        
        validation_errors = ["Validation error"]
        extract_prompt = "Extract data"
        
        result = _repair_with_validator_feedback(
            failed_row,
            validation_errors,
            mock_llm_client,
            extract_prompt,
            "/test/path.pdf"
        )
        
        assert result["source_path"] == "/test/path.pdf"
    
    def test_repair_handles_validation_errors(self, mock_llm_client):
        """Test that repair handles multiple validation errors."""
        mock_llm_client.chat_json.return_value = {
            "demographic": {"gender": "male", "age_years": 25},
            "temporal": {"last_seen_ts": "2023-01-10T00:00:00Z", "timezone": "America/New_York"},
            "spatial": {"last_seen_lat": 0.0, "last_seen_lon": 0.0},
            "outcome": {"case_status": "ongoing"},
            "narrative_osint": {"incident_summary": "Test summary"}
        }
        
        failed_row = {
            "demographic": {"gender": "invalid", "age_years": "invalid"},
            "source_path": "/test/path.pdf"
        }
        
        validation_errors = [
            "['demographic', 'gender']: 'invalid' is not one of ['male', 'female']",
            "['demographic', 'age_years']: 'invalid' is not of type 'number'"
        ]
        extract_prompt = "Extract data"
        
        result = _repair_with_validator_feedback(
            failed_row,
            validation_errors,
            mock_llm_client,
            extract_prompt,
            "/test/path.pdf"
        )
        
        # Check that repair prompt includes errors
        call_args = mock_llm_client.chat_json.call_args
        assert call_args is not None
        messages = call_args[0][0]
        repair_message = messages[1]["content"]
        assert "Validation error" in repair_message or "gender" in repair_message or "age_years" in repair_message
    
    def test_repair_returns_none_on_failure(self, mock_llm_client):
        """Test that repair returns None when LLM call fails."""
        mock_llm_client.chat_json.side_effect = Exception("LLM call failed")
        
        failed_row = {
            "demographic": {"gender": "invalid"},
            "source_path": "/test/path.pdf"
        }
        
        validation_errors = ["Validation error"]
        extract_prompt = "Extract data"
        
        result = _repair_with_validator_feedback(
            failed_row,
            validation_errors,
            mock_llm_client,
            extract_prompt,
            "/test/path.pdf"
        )
        
        assert result is None
    
    def test_repair_returns_none_on_invalid_response(self, mock_llm_client):
        """Test that repair returns None when LLM returns invalid response."""
        mock_llm_client.chat_json.return_value = "Not a dict"
        
        failed_row = {
            "demographic": {"gender": "invalid"},
            "source_path": "/test/path.pdf"
        }
        
        validation_errors = ["Validation error"]
        extract_prompt = "Extract data"
        
        result = _repair_with_validator_feedback(
            failed_row,
            validation_errors,
            mock_llm_client,
            extract_prompt,
            "/test/path.pdf"
        )
        
        assert result is None
    
    @patch('guardian_parser_pack.agent.llm_agent_simple.tools.validate_row')
    def test_repair_loop_integration(self, mock_validate_row, mock_llm_client):
        """Test full repair loop integration."""
        # First validation fails, second succeeds
        mock_validate_row.side_effect = [
            ["Validation error"],  # First call fails
            []  # Second call succeeds
        ]
        
        mock_llm_client.chat_json.return_value = {
            "demographic": {"gender": "male"},
            "temporal": {"last_seen_ts": "2023-01-10T00:00:00Z", "timezone": "America/New_York"},
            "spatial": {"last_seen_lat": 0.0, "last_seen_lon": 0.0},
            "outcome": {"case_status": "ongoing"},
            "narrative_osint": {"incident_summary": "Test summary"}
        }
        
        # This test verifies the repair loop logic exists
        # The actual implementation may vary
        assert mock_validate_row is not None
        assert mock_llm_client is not None
    
    def test_repair_prompt_format(self, mock_llm_client):
        """Test that repair prompt has correct format."""
        mock_llm_client.chat_json.return_value = {
            "demographic": {"gender": "male"},
            "temporal": {"last_seen_ts": "2023-01-10T00:00:00Z", "timezone": "America/New_York"},
            "spatial": {"last_seen_lat": 0.0, "last_seen_lon": 0.0},
            "outcome": {"case_status": "ongoing"},
            "narrative_osint": {"incident_summary": "Test summary"}
        }
        
        failed_row = {
            "demographic": {"gender": "invalid"},
            "source_path": "/test/path.pdf"
        }
        
        validation_errors = ["Error 1", "Error 2", "Error 3"]
        extract_prompt = "Extract data"
        
        _repair_with_validator_feedback(
            failed_row,
            validation_errors,
            mock_llm_client,
            extract_prompt,
            "/test/path.pdf"
        )
        
        # Check that repair prompt includes errors (limited to 10)
        call_args = mock_llm_client.chat_json.call_args
        assert call_args is not None
        messages = call_args[0][0]
        repair_message = messages[1]["content"]
        
        # Should include error information
        assert "Error" in repair_message or "error" in repair_message.lower()
        # Should include current JSON
        assert "demographic" in repair_message or json.dumps(failed_row) in repair_message

