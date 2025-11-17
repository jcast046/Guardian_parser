"""
Unit tests for source detection functionality.
"""
import pytest
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import parser_pack


@pytest.mark.unit
class TestDetectSource:
    """Test cases for detect_source function.

    Tests detection of document sources (NamUs, NCMEC, FBI, VSP, Charley)
    based on characteristic text patterns.
    """
    
    def test_detect_namus(self, sample_namus_text):
        """Test detection of NamUs source."""
        result = parser_pack.detect_source(sample_namus_text)
        assert result == "NamUs"
    
    def test_detect_namus_case_created(self):
        """Test detection of NamUs by Case Created marker."""
        text = "Case Created: 2023-01-15"
        result = parser_pack.detect_source(text)
        assert result == "NamUs"
    
    def test_detect_namus_date_of_last_contact(self):
        """Test detection of NamUs by Date of Last Contact marker."""
        text = "Date of Last Contact: January 10, 2023"
        result = parser_pack.detect_source(text)
        assert result == "NamUs"
    
    def test_detect_ncmec(self, sample_ncmec_text):
        """Test detection of NCMEC source."""
        result = parser_pack.detect_source(sample_ncmec_text)
        assert result == "NCMEC"
    
    def test_detect_ncmec_have_you_seen(self):
        """Test detection of NCMEC by 'Have you seen this child?' marker."""
        text = "Have you seen this child? Missing Since: January 10, 2023"
        result = parser_pack.detect_source(text)
        assert result == "NCMEC"
    
    def test_detect_ncmec_missing_since(self):
        """Test detection of NCMEC by Missing Since marker."""
        text = "Missing Since: January 10, 2023"
        result = parser_pack.detect_source(text)
        assert result == "NCMEC"
    
    def test_detect_vsp(self, sample_vsp_text):
        """Test detection of VSP source."""
        result = parser_pack.detect_source(sample_vsp_text)
        assert result == "VSP"
    
    def test_detect_vsp_missing_persons_header(self):
        """Test detection of VSP by MISSING PERSONS header."""
        text = "MISSING PERSONS\nMissing From: Richmond\nContact: VSP"
        result = parser_pack.detect_source(text)
        assert result == "VSP"
    
    def test_detect_vsp_vaa_pattern(self):
        """Test detection of VSP by VAA case number pattern."""
        text = "MISSING PERSONS\nVAA23-1234\nMissing From: Richmond\nContact: VSP"
        result = parser_pack.detect_source(text)
        assert result == "VSP"
    
    def test_detect_vsp_multiple_cases(self):
        """Test detection of VSP by multiple Missing From markers."""
        text = "MISSING PERSONS\nMissing From: Richmond\nContact: VSP\nMissing From: Virginia Beach\nContact: VSP"
        result = parser_pack.detect_source(text)
        assert result == "VSP"
    
    def test_detect_fbi(self, sample_fbi_text):
        """Test detection of FBI source."""
        result = parser_pack.detect_source(sample_fbi_text)
        assert result == "FBI"
    
    def test_detect_fbi_www_fbi_gov(self):
        """Test detection of FBI by www.fbi.gov marker."""
        text = "FBI\nwww.fbi.gov\nField Office: Richmond"
        result = parser_pack.detect_source(text)
        assert result == "FBI"
    
    def test_detect_fbi_federal_bureau(self):
        """Test detection of FBI by Federal Bureau of Investigation marker."""
        text = "Federal Bureau of Investigation\nField Office: Richmond"
        result = parser_pack.detect_source(text)
        assert result == "FBI"
    
    def test_detect_fbi_field_office(self):
        """Test detection of FBI by Field Office pattern."""
        text = "FBI Field Office: Richmond"
        result = parser_pack.detect_source(text)
        assert result == "FBI"
    
    def test_detect_fbi_information_concerning(self):
        """Test detection of FBI by 'information concerning this person' marker."""
        text = "If you have any information concerning this person"
        result = parser_pack.detect_source(text)
        assert result == "FBI"
    
    def test_detect_charley(self, sample_charley_text):
        """Test detection of Charley Project source."""
        result = parser_pack.detect_source(sample_charley_text)
        assert result == "Charley"
    
    def test_detect_charley_charley_project(self):
        """Test detection of Charley Project by name marker."""
        text = "The Charley Project\nDetails of Disappearance"
        result = parser_pack.detect_source(text)
        assert result == "Charley"
    
    def test_detect_charley_details_of_disappearance(self):
        """Test detection of Charley Project by Details of Disappearance marker."""
        text = "Details of Disappearance\nMissing From: Virginia"
        result = parser_pack.detect_source(text)
        assert result == "Charley"
    
    def test_detect_unknown(self, sample_unknown_text):
        """Test detection of unknown source."""
        result = parser_pack.detect_source(sample_unknown_text)
        assert result == "Unknown"
    
    def test_detect_unknown_empty_text(self):
        """Test detection of unknown source with empty text."""
        result = parser_pack.detect_source("")
        assert result == "Unknown"
    
    def test_detect_priority_vsp_over_ncmec(self):
        """Test that VSP is detected before NCMEC when both markers present."""
        text = "MISSING PERSONS\nMissing From: Richmond\nContact: VSP\nMissing Since: January 10"
        result = parser_pack.detect_source(text)
        assert result == "VSP"
    
    def test_detect_priority_vsp_over_charley(self):
        """Test that VSP is detected before Charley when both markers present."""
        text = "MISSING PERSONS\nMissing From: Richmond\nContact: VSP\nMissing From: Virginia"
        result = parser_pack.detect_source(text)
        assert result == "VSP"
    
    def test_detect_ncmec_not_vsp(self):
        """Test that NCMEC is detected when Missing Since is present but not VSP format."""
        text = "Missing Since: January 10, 2023\nName: John Doe"
        result = parser_pack.detect_source(text)
        assert result == "NCMEC"

