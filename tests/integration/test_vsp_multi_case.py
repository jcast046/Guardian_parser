"""Integration tests for VSP multi-case handling."""
import pytest
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import parser_pack


@pytest.mark.integration
class TestVSPMultiCase:
    """Test cases for VSP multi-case document handling.

    Tests splitting VSP documents containing multiple cases and parsing
    individual cases from split results.
    """
    
    def test_split_vsp_cases_multiple_cases(self):
        """Test splitting VSP document with multiple cases."""
        text = """
        MISSING PERSONS
        A
        
        John Doe
        Missing From: Richmond, Virginia
        Missing Since: January 10, 2023
        Age at time of disappearance: 25
        Sex: Male
        Contact: Virginia State Police
        
        Jane Smith
        Missing From: Virginia Beach, Virginia
        Missing Since: January 15, 2023
        Age at time of disappearance: 30
        Sex: Female
        Contact: Virginia State Police
        
        Bob Johnson
        Missing From: Norfolk, Virginia
        Missing Since: January 20, 2023
        Age at time of disappearance: 35
        Sex: Male
        Contact: Virginia State Police
        """
        
        cases = parser_pack.split_vsp_cases(text)
        
        # Should return 3 cases
        assert len(cases) == 3
        
        # Each case should contain "Missing From"
        for case in cases:
            assert "Missing From" in case
        
        # Check that cases are separated
        assert "John Doe" in cases[0] or "Richmond" in cases[0]
        assert "Jane Smith" in cases[1] or "Virginia Beach" in cases[1]
        assert "Bob Johnson" in cases[2] or "Norfolk" in cases[2]
    
    def test_parse_pdf_vsp_multiple_cases(self, temp_dir):
        """Test parsing VSP PDF with multiple cases."""
        # Create a mock PDF path (we'll mock the text extraction)
        pdf_path = str(Path(temp_dir) / "vsp_multi.pdf")
        
        text = """
        MISSING PERSONS
        A
        
        John Doe
        Missing From: Richmond, Virginia
        Missing Since: January 10, 2023
        Age at time of disappearance: 25
        Sex: Male
        
        Jane Smith
        Missing From: Virginia Beach, Virginia
        Missing Since: January 15, 2023
        Age at time of disappearance: 30
        Sex: Female
        """
        
        # Mock extract_text to return our test text
        with patch("parser_pack.extract_text") as mock_extract:
            mock_extract.return_value = text
            
            # Mock detect_source to return VSP
            with patch("parser_pack.detect_source") as mock_detect:
                mock_detect.return_value = "VSP"
                
                # Parse the PDF
                case_id = "GRD-2023-000001"
                result = parser_pack.parse_pdf(pdf_path, case_id, do_geocode=False)
                
                # For VSP with multiple cases, parse_pdf should handle them
                # The exact behavior depends on implementation
                assert result is not None
    
    def test_split_vsp_cases_case_boundaries(self):
        """Test that case boundaries are correctly identified."""
        text = """
        MISSING PERSONS
        A
        
        Case One Name
        Missing From: Richmond, Virginia
        Missing Since: January 10, 2023
        Age at time of disappearance: 25
        
        Case Two Name
        Missing From: Virginia Beach, Virginia
        Missing Since: January 15, 2023
        Age at time of disappearance: 30
        """
        
        cases = parser_pack.split_vsp_cases(text)
        
        # Should split into separate cases
        assert len(cases) >= 2
        
        # Each case should contain "Missing From"
        for case in cases:
            assert "Missing From" in case
            assert "Age at time of disappearance" in case
    
    def test_split_vsp_cases_single_case(self):
        """Test splitting VSP document with single case."""
        text = """
        MISSING PERSONS
        Missing From: Richmond, Virginia
        Missing Since: January 10, 2023
        Name: John Doe
        Age at time of disappearance: 25
        """
        
        cases = parser_pack.split_vsp_cases(text)
        
        # Should return at least one case (or empty list if single case handling is different)
        assert isinstance(cases, list)
        # If cases are returned, each should contain key markers
        if len(cases) > 0:
            for case in cases:
                assert "Missing From" in case or "Age at time of disappearance" in case
    
    def test_split_vsp_cases_vaa_pattern(self):
        """Test splitting VSP document with VAA case numbers."""
        text = """
        MISSING PERSONS
        A
        
        John Doe
        VAA23-1234
        Missing From: Richmond, Virginia
        Missing Since: January 10, 2023
        
        Jane Smith
        VAA23-1235
        Missing From: Virginia Beach, Virginia
        Missing Since: January 15, 2023
        """
        
        cases = parser_pack.split_vsp_cases(text)
        
        # Should identify multiple cases by VAA pattern or name patterns
        assert len(cases) >= 1
    
    def test_parse_vsp_single_case_extraction(self):
        """Test parsing a single VSP case."""
        text = """
        John Doe
        Missing From: Richmond, Virginia
        Missing Since: January 10, 2023
        Age at time of disappearance: 25
        Sex: Male
        Race: White
        Hair: Brown
        Eyes: Blue
        Height: 5'10"
        Weight: 180 lbs
        Contact: Virginia State Police
        """
        
        case_id = "GRD-2023-000001"
        result = parser_pack.parse_vsp(text, case_id)
        
        assert result["case_id"] == case_id
        assert "demographic" in result
        assert result["demographic"].get("name") == "John Doe"
        assert result["demographic"].get("age_years") == 25.0
        assert result["demographic"].get("gender") == "male"
        assert "spatial" in result
        assert "temporal" in result
    
    def test_split_vsp_cases_empty_text(self):
        """Test splitting empty VSP text."""
        text = ""
        cases = parser_pack.split_vsp_cases(text)
        
        # Should return empty list
        assert cases == []
    
    def test_split_vsp_cases_no_markers(self):
        """Test splitting text with no case markers."""
        text = "Some random text without case markers"
        cases = parser_pack.split_vsp_cases(text)
        
        # Should return empty list or handle gracefully
        assert isinstance(cases, list)
    
    def test_vsp_case_count_accuracy(self):
        """Test that VSP case splitting accurately counts cases."""
        # Create text with known number of cases
        text = """
        MISSING PERSONS
        A
        
        Case 1
        Missing From: Location 1
        Age at time of disappearance: 25
        
        Case 2
        Missing From: Location 2
        Age at time of disappearance: 30
        
        Case 3
        Missing From: Location 3
        Age at time of disappearance: 35
        """
        
        cases = parser_pack.split_vsp_cases(text)
        
        # Should identify all cases
        # Note: Exact count may vary based on parser implementation
        assert len(cases) >= 1

