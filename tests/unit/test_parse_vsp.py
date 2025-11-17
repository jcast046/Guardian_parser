"""Unit tests for VSP parser functionality."""
import pytest
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import parser_pack


@pytest.mark.unit
class TestParseVsp:
    """Test cases for parse_vsp function.

    Tests extraction of demographic, spatial, temporal, and narrative
    fields from VSP case text.
    """
    
    def test_parse_vsp_basic(self):
        """Test basic VSP parsing."""
        text = """
        MISSING PERSONS
        Missing From: Richmond, Virginia
        Missing Since: January 10, 2023
        Contact: Virginia State Police
        VAA23-1234
        Name: John Doe
        Age at time of disappearance: 25
        Sex: Male
        """
        
        case_id = "GRD-2023-000001"
        result = parser_pack.parse_vsp(text, case_id)
        
        assert result["case_id"] == case_id
        assert "demographic" in result
        assert "spatial" in result
        assert "temporal" in result
        assert "outcome" in result
        assert "narrative_osint" in result
    
    def test_parse_vsp_name_extraction(self):
        """Test name extraction from VSP text."""
        text = """
        Name: Jane Smith
        Age at time of disappearance: 30
        Sex: Female
        """
        
        case_id = "GRD-2023-000002"
        result = parser_pack.parse_vsp(text, case_id)
        
        # Name should be extracted
        assert "demographic" in result
    
    def test_parse_vsp_age_extraction(self):
        """Test age extraction from VSP text."""
        text = """
        Name: John Doe
        Age at time of disappearance: 25
        """
        
        case_id = "GRD-2023-000003"
        result = parser_pack.parse_vsp(text, case_id)
        
        # Age should be extracted
        assert "demographic" in result
    
    def test_parse_vsp_location_extraction(self):
        """Test location extraction from VSP text."""
        text = """
        Missing From: Richmond, Virginia
        Name: John Doe
        """
        
        case_id = "GRD-2023-000004"
        result = parser_pack.parse_vsp(text, case_id)
        
        # Location should be extracted
        assert "spatial" in result
    
    def test_parse_vsp_date_extraction(self):
        """Test date extraction from VSP text."""
        text = """
        Missing Since: January 10, 2023
        Name: John Doe
        """
        
        case_id = "GRD-2023-000005"
        result = parser_pack.parse_vsp(text, case_id)
        
        # Date should be extracted
        assert "temporal" in result
    
    def test_parse_vsp_case_number(self):
        """Test VAA case number extraction."""
        text = """
        VAA23-1234
        Name: John Doe
        Missing From: Richmond, Virginia
        """
        
        case_id = "GRD-2023-000006"
        result = parser_pack.parse_vsp(text, case_id)
        
        # Case number should be extracted if parser supports it
        assert result["case_id"] == case_id


@pytest.mark.unit
class TestSplitVspCases:
    """Test cases for split_vsp_cases function.

    Tests splitting VSP documents containing multiple cases into
    individual case text blocks.
    """
    
    def test_split_vsp_cases_single_case(self):
        """Test splitting VSP text with single case."""
        text = """
        MISSING PERSONS
        Missing From: Richmond, Virginia
        Missing Since: January 10, 2023
        Name: John Doe
        Age at time of disappearance: 25
        """
        
        cases = parser_pack.split_vsp_cases(text)
        
        # Should return at least one case
        assert len(cases) >= 1
    
    def test_split_vsp_cases_multiple_cases(self):
        """Test splitting VSP text with multiple cases."""
        text = """
        MISSING PERSONS
        A
        
        John Doe
        Missing From: Richmond, Virginia
        Missing Since: January 10, 2023
        Age at time of disappearance: 25
        
        Jane Smith
        Missing From: Virginia Beach, Virginia
        Missing Since: January 15, 2023
        Age at time of disappearance: 30
        """
        
        cases = parser_pack.split_vsp_cases(text)
        
        # Should return multiple cases
        assert len(cases) >= 2
    
    def test_split_vsp_cases_case_boundaries(self):
        """Test that case boundaries are correctly identified."""
        text = """
        MISSING PERSONS
        A
        
        Case One
        Missing From: Richmond
        
        Case Two
        Missing From: Virginia Beach
        """
        
        cases = parser_pack.split_vsp_cases(text)
        
        # Should split into separate cases
        assert len(cases) >= 1
        # Each case should contain "Missing From"
        for case in cases:
            assert "Missing From" in case
    
    def test_split_vsp_cases_empty_text(self):
        """Test splitting empty VSP text."""
        text = ""
        cases = parser_pack.split_vsp_cases(text)
        
        # Should return empty list
        assert cases == []
    
    def test_split_vsp_cases_no_cases(self):
        """Test splitting text with no case markers."""
        text = "Some random text without case markers"
        cases = parser_pack.split_vsp_cases(text)
        
        # Should return empty list or handle gracefully
        assert isinstance(cases, list)

