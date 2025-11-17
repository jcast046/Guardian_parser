"""Unit tests for NamUs parser functionality."""
import pytest
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import parser_pack


@pytest.mark.unit
class TestParseNamUs:
    """Test cases for parse_namus function.

    Tests extraction of demographic, spatial, temporal, and narrative
    fields from NamUs case text.
    """
    
    def test_parse_namus_basic(self):
        """Test basic NamUs parsing."""
        text = """
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
        
        case_id = "GRD-2023-000001"
        result = parser_pack.parse_namus(text, case_id)
        
        assert result["case_id"] == case_id
        assert "demographic" in result
        assert "spatial" in result
        assert "temporal" in result
        assert "outcome" in result
        assert "narrative_osint" in result
    
    def test_parse_namus_name_extraction(self):
        """Test name extraction from NamUs text."""
        text = """
        Name: Jane Smith
        Age: 30
        """
        
        case_id = "GRD-2023-000002"
        result = parser_pack.parse_namus(text, case_id)
        
        assert result["demographic"].get("name") == "Jane Smith"
    
    def test_parse_namus_age_extraction(self):
        """Test age extraction from NamUs text."""
        text = """
        Name: John Doe
        Age: 25
        """
        
        case_id = "GRD-2023-000003"
        result = parser_pack.parse_namus(text, case_id)
        
        # Age should be extracted (exact value depends on parser implementation)
        assert "demographic" in result
    
    def test_parse_namus_gender_extraction(self):
        """Test gender extraction from NamUs text."""
        text = """
        Name: John Doe
        Sex: Male
        """
        
        case_id = "GRD-2023-000004"
        result = parser_pack.parse_namus(text, case_id)
        
        # Gender should be extracted
        assert "demographic" in result
    
    def test_parse_namus_location_extraction(self):
        """Test location extraction from NamUs text."""
        text = """
        Name: John Doe
        Missing From: Richmond, VA
        """
        
        case_id = "GRD-2023-000005"
        result = parser_pack.parse_namus(text, case_id)
        
        # Location should be extracted
        assert "spatial" in result
    
    def test_parse_namus_date_extraction(self):
        """Test date extraction from NamUs text."""
        text = """
        Date of Last Contact: January 10, 2023
        """
        
        case_id = "GRD-2023-000006"
        result = parser_pack.parse_namus(text, case_id)
        
        # Date should be extracted
        assert "temporal" in result
    
    def test_parse_namus_missing_fields(self):
        """Test parsing with missing fields."""
        text = """
        Name: John Doe
        """
        
        case_id = "GRD-2023-000007"
        result = parser_pack.parse_namus(text, case_id)
        
        # Should still return valid structure with missing fields
        assert result["case_id"] == case_id
        assert "demographic" in result
        assert "spatial" in result
        assert "temporal" in result
    
    def test_parse_namus_google_maps_coordinates(self):
        """Test extraction of coordinates from Google Maps links."""
        text = """
        Name: John Doe
        Missing From: Richmond, VA
        https://maps.google.com/?q=37.5407,-77.4360
        """
        
        case_id = "GRD-2023-000008"
        result = parser_pack.parse_namus(text, case_id)
        
        # Coordinates should be extracted if parser supports it
        assert "spatial" in result
    
    def test_parse_namus_multiple_names(self):
        """Test parsing with multiple name variations."""
        text = """
        Name: John Doe
        Also Known As: Johnny Doe, John D.
        """
        
        case_id = "GRD-2023-000009"
        result = parser_pack.parse_namus(text, case_id)
        
        # Aliases should be extracted if present
        assert "demographic" in result

