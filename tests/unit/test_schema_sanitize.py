"""Unit tests for schema sanitization functionality."""
import pytest
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from guardian_parser_pack.agent.schema_sanitize import sanitize_guardian_row


@pytest.mark.unit
class TestSchemaSanitize:
    """Test cases for sanitize_guardian_row function.

    Tests normalization, type coercion, field mapping, validation,
    and schema compliance of LLM output.
    """
    
    def test_sanitize_messy_llm_input(self):
        """Test sanitization of messy LLM-style input."""
        messy_input = {
            "sex": "M",
            "age": "around 25",
            "age_years": "30",
            "extra_field": "foo",
            "demographic": {
                "name": "John Doe",
                "sex": "male",
                "age_years": "25",
                "weight_lb": 180,
                "hair_color": "brown",
                "eye_color": "blue"
            },
            "temporal": {
                "reported_ts": "2023-01-15T10:00:00Z",
                "last_seen_date": "2023-01-10T08:00:00Z"
            },
            "spatial": {
                "city": "Richmond",
                "state": "VA"
            }
        }
        
        source_path = "/test/path.pdf"
        result = sanitize_guardian_row(messy_input, source_path)
        
        # Check that source_path is preserved
        assert result["source_path"] == source_path
        
        # Check demographic fields
        assert result["demographic"]["name"] == "John Doe"
        assert result["demographic"]["gender"] == "male"
        assert result["demographic"]["age_years"] == 25.0
        assert result["demographic"]["weight_lbs"] == 180.0
        
        # Check that extra fields are stored in provenance
        assert "provenance" in result
        assert "original_fields" in result["provenance"]
        # Hair and eye color should be in original_fields
        orig_fields = result["provenance"]["original_fields"]
        assert "demographic.hair_color" in orig_fields or "demographic.eye_color" in orig_fields
    
    def test_type_coercion_age_string_to_number(self):
        """Test type coercion of age from string to number."""
        input_data = {
            "demographic": {
                "age_years": "30"
            }
        }
        
        result = sanitize_guardian_row(input_data, "/test/path.pdf")
        assert result["demographic"]["age_years"] == 30.0
        assert isinstance(result["demographic"]["age_years"], float)
    
    def test_type_coercion_age_invalid(self):
        """Test that invalid age values are rejected."""
        input_data = {
            "demographic": {
                "age_years": "invalid"
            }
        }
        
        result = sanitize_guardian_row(input_data, "/test/path.pdf")
        # Invalid age should not be in result
        assert "age_years" not in result.get("demographic", {}) or result["demographic"].get("age_years") is None
    
    def test_age_validation_reject_birth_year(self):
        """Test that 4-digit birth years (>= 1000) are rejected."""
        input_data = {
            "demographic": {
                "age_years": 1990  # Birth year, not age
            }
        }
        
        result = sanitize_guardian_row(input_data, "/test/path.pdf")
        # Birth year should be rejected (not in valid age range 0-120)
        assert result["demographic"].get("age_years") is None or result["demographic"].get("age_years") < 120
    
    def test_age_validation_valid_range(self):
        """Test that valid ages (0-120) are accepted."""
        input_data = {
            "demographic": {
                "age_years": 25
            }
        }
        
        result = sanitize_guardian_row(input_data, "/test/path.pdf")
        assert result["demographic"]["age_years"] == 25.0
    
    def test_field_mapping_sex_to_gender(self):
        """Test field mapping from sex to gender."""
        input_data = {
            "demographic": {
                "sex": "M"
            }
        }
        
        result = sanitize_guardian_row(input_data, "/test/path.pdf")
        assert result["demographic"]["gender"] == "male"
    
    def test_field_mapping_sex_female(self):
        """Test field mapping from sex to gender for female."""
        input_data = {
            "demographic": {
                "sex": "F"
            }
        }
        
        result = sanitize_guardian_row(input_data, "/test/path.pdf")
        assert result["demographic"]["gender"] == "female"
    
    def test_field_mapping_weight_lb_to_weight_lbs(self):
        """Test field mapping from weight_lb to weight_lbs."""
        input_data = {
            "demographic": {
                "weight_lb": 180
            }
        }
        
        result = sanitize_guardian_row(input_data, "/test/path.pdf")
        assert result["demographic"]["weight_lbs"] == 180.0
    
    def test_field_mapping_reported_ts_to_reported_missing_ts(self):
        """Test field mapping from reported_ts to reported_missing_ts."""
        input_data = {
            "temporal": {
                "reported_ts": "2023-01-15T10:00:00Z"
            }
        }
        
        result = sanitize_guardian_row(input_data, "/test/path.pdf")
        assert result["temporal"]["reported_missing_ts"] == "2023-01-15T10:00:00Z"
    
    def test_field_mapping_last_seen_date_to_last_seen_ts(self):
        """Test field mapping from last_seen_date to last_seen_ts."""
        input_data = {
            "temporal": {
                "last_seen_date": "2023-01-10T08:00:00Z"
            }
        }
        
        result = sanitize_guardian_row(input_data, "/test/path.pdf")
        assert result["temporal"]["last_seen_ts"] == "2023-01-10T08:00:00Z"
    
    def test_field_mapping_city_state_to_last_seen(self):
        """Test field mapping from city/state to last_seen_city/last_seen_state."""
        input_data = {
            "spatial": {
                "city": "Richmond",
                "state": "VA"
            }
        }
        
        result = sanitize_guardian_row(input_data, "/test/path.pdf")
        assert result["spatial"]["last_seen_city"] == "Richmond"
        assert result["spatial"]["last_seen_state"] == "VA"
    
    def test_extra_fields_in_provenance(self):
        """Test that extra fields are stored in provenance.original_fields."""
        input_data = {
            "demographic": {
                "hair_color": "brown",
                "eye_color": "blue",
                "sex": "M"
            },
            "spatial": {
                "city": "Richmond",
                "state": "VA"
            },
            "temporal": {
                "reported_ts": "2023-01-15T10:00:00Z",
                "last_seen_date": "2023-01-10T08:00:00Z"
            }
        }
        
        result = sanitize_guardian_row(input_data, "/test/path.pdf")
        
        # Check that original_fields contains mapped fields
        assert "provenance" in result
        assert "original_fields" in result["provenance"]
        orig_fields = result["provenance"]["original_fields"]
        
        # Check that mapped fields are in original_fields
        assert "demographic.hair_color" in orig_fields
        assert "demographic.eye_color" in orig_fields
        assert "demographic.sex" in orig_fields
        assert "spatial.city" in orig_fields
        assert "spatial.state" in orig_fields
        assert "temporal.reported_ts" in orig_fields
        assert "temporal.last_seen_date" in orig_fields
    
    def test_gender_required_field(self):
        """Test that gender is required and defaults to male if missing."""
        input_data = {
            "demographic": {}
        }
        
        result = sanitize_guardian_row(input_data, "/test/path.pdf")
        # Gender should default to "male" if not provided
        assert result["demographic"]["gender"] == "male"
    
    def test_last_seen_ts_required_field(self):
        """Test that last_seen_ts is required and has a default."""
        input_data = {
            "temporal": {}
        }
        
        result = sanitize_guardian_row(input_data, "/test/path.pdf")
        # last_seen_ts should be set (default timestamp)
        assert "last_seen_ts" in result["temporal"]
        assert result["temporal"]["last_seen_ts"] is not None
    
    def test_incident_summary_required_field(self):
        """Test that incident_summary is required in narrative_osint."""
        input_data = {
            "narrative_osint": {}
        }
        
        result = sanitize_guardian_row(input_data, "/test/path.pdf")
        # incident_summary should default to "No summary available"
        assert result["narrative_osint"]["incident_summary"] == "No summary available"
    
    def test_case_status_default(self):
        """Test that case_status defaults to ongoing."""
        input_data = {
            "outcome": {}
        }
        
        result = sanitize_guardian_row(input_data, "/test/path.pdf")
        assert result["outcome"]["case_status"] == "ongoing"
    
    def test_follow_up_sightings_normalization(self):
        """Test normalization of follow_up_sightings array."""
        input_data = {
            "temporal": {
                "follow_up_sightings": [
                    {
                        "date_iso": "2023-01-15T10:00:00Z",
                        "notes": "Sighting note",
                        "latitude": 37.5407,
                        "longitude": -77.4360
                    }
                ]
            }
        }
        
        result = sanitize_guardian_row(input_data, "/test/path.pdf")
        
        # Check that fields are normalized
        sightings = result["temporal"]["follow_up_sightings"]
        assert len(sightings) == 1
        assert sightings[0]["ts"] == "2023-01-15T10:00:00Z"
        assert sightings[0]["note"] == "Sighting note"
        assert sightings[0]["lat"] == 37.5407
        assert sightings[0]["lon"] == -77.4360
    
    def test_spatial_lat_lon_required(self):
        """Test that spatial lat/lon are required and default to 0.0 if missing."""
        input_data = {
            "spatial": {}
        }
        
        result = sanitize_guardian_row(input_data, "/test/path.pdf")
        # lat/lon should default to 0.0 if not provided
        assert result["spatial"]["last_seen_lat"] == 0.0
        assert result["spatial"]["last_seen_lon"] == 0.0
    
    def test_spatial_lat_lon_invalid_range(self):
        """Test that invalid lat/lon values are corrected to 0.0."""
        input_data = {
            "spatial": {
                "last_seen_lat": 91.0,  # Invalid (should be -90 to 90)
                "last_seen_lon": -181.0  # Invalid (should be -180 to 180)
            }
        }
        
        result = sanitize_guardian_row(input_data, "/test/path.pdf")
        # Invalid coordinates should default to 0.0
        assert result["spatial"]["last_seen_lat"] == 0.0
        assert result["spatial"]["last_seen_lon"] == 0.0
    
    def test_height_weight_validation(self):
        """Test validation of height and weight ranges."""
        input_data = {
            "demographic": {
                "height_in": 5.0,  # Too small (should be >= 10)
                "weight_lbs": 3.0  # Too small (should be >= 5)
            }
        }
        
        result = sanitize_guardian_row(input_data, "/test/path.pdf")
        # Values below minimum should not be included
        assert result["demographic"].get("height_in") is None or result["demographic"]["height_in"] >= 10
        assert result["demographic"].get("weight_lbs") is None or result["demographic"]["weight_lbs"] >= 5
    
    def test_distinctive_features_list_to_string(self):
        """Test that distinctive_features list is converted to string."""
        input_data = {
            "demographic": {
                "distinctive_features": ["tattoo", "scar", "birthmark"]
            }
        }
        
        result = sanitize_guardian_row(input_data, "/test/path.pdf")
        # List should be converted to string with separator
        features = result["demographic"]["distinctive_features"]
        assert isinstance(features, str)
        assert "tattoo" in features
        assert "scar" in features

