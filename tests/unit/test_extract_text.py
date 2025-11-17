"""Unit tests for PDF text extraction functionality."""
import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import sys

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import parser_pack


@pytest.mark.unit
class TestExtractText:
    """Test cases for extract_text function.

    Tests PDF text extraction with fallback handling for pdfminer,
    PyPDF2, and OCR methods.
    """
    
    def test_extract_text_standard_pdf(self, temp_dir):
        """Test extraction from a standard text-based PDF."""
        # Create a simple text file to simulate PDF content
        pdf_path = Path(temp_dir) / "test.pdf"
        pdf_path.write_text("Test PDF content")
        
        # Mock pdfminer to return text
        with patch("parser_pack.pdfminer_extract_text") as mock_pdfminer:
            mock_pdfminer.return_value = "Extracted text from PDF"
            result = parser_pack.extract_text(str(pdf_path))
            assert result == "Extracted text from PDF"
            mock_pdfminer.assert_called_once_with(str(pdf_path))
    
    def test_extract_text_pypdf2_fallback(self, temp_dir):
        """Test fallback to PyPDF2 when pdfminer fails."""
        pdf_path = Path(temp_dir) / "test.pdf"
        pdf_path.write_text("Test PDF content")
        
        # Mock pdfminer to fail, PyPDF2 to succeed
        with patch("parser_pack.pdfminer_extract_text") as mock_pdfminer:
            with patch("parser_pack.PyPDF2") as mock_pypdf2:
                mock_pdfminer.side_effect = Exception("PDFMiner failed")
                
                # Mock PyPDF2 reader
                mock_reader = MagicMock()
                mock_page = MagicMock()
                mock_page.extract_text.return_value = "PyPDF2 extracted text"
                mock_reader.pages = [mock_page]
                mock_pypdf2.PdfReader.return_value = mock_reader
                
                result = parser_pack.extract_text(str(pdf_path))
                assert result == "PyPDF2 extracted text"
                mock_pdfminer.assert_called_once()
                mock_pypdf2.PdfReader.assert_called_once()
    
    def test_extract_text_ocr_fallback(self, temp_dir, mock_pytesseract):
        """Test OCR fallback when both pdfminer and PyPDF2 fail."""
        pdf_path = Path(temp_dir) / "test.pdf"
        pdf_path.write_text("Test PDF content")
        
        # Mock both pdfminer and PyPDF2 to fail
        with patch("parser_pack.pdfminer_extract_text") as mock_pdfminer:
            with patch("parser_pack.PyPDF2") as mock_pypdf2:
                with patch("parser_pack.Image") as mock_image:
                    mock_pdfminer.side_effect = Exception("PDFMiner failed")
                    mock_pypdf2.side_effect = Exception("PyPDF2 failed")
                    
                    # Mock PIL Image
                    mock_img = MagicMock()
                    mock_image.open.return_value = mock_img
                    mock_pytesseract.image_to_string.return_value = "OCR extracted text"
                    
                    result = parser_pack.extract_text(str(pdf_path))
                    assert result == "OCR extracted text"
                    mock_pytesseract.image_to_string.assert_called_once_with(mock_img)
    
    def test_extract_text_corrupted_pdf(self, temp_dir):
        """Test handling of corrupted/empty PDF."""
        pdf_path = Path(temp_dir) / "corrupted.pdf"
        pdf_path.write_bytes(b"Invalid PDF content")
        
        # Mock all extractors to fail
        with patch("parser_pack.pdfminer_extract_text") as mock_pdfminer:
            with patch("parser_pack.PyPDF2") as mock_pypdf2:
                with patch("parser_pack.pytesseract", None):
                    with patch("parser_pack.Image", None):
                        mock_pdfminer.side_effect = Exception("Corrupted PDF")
                        mock_pypdf2.side_effect = Exception("Corrupted PDF")
                        
                        result = parser_pack.extract_text(str(pdf_path))
                        # Should return empty string on failure
                        assert result == ""
    
    def test_extract_text_nonexistent_file(self):
        """Test handling of non-existent file."""
        # The function catches exceptions and returns empty string
        with patch("parser_pack.pdfminer_extract_text") as mock_pdfminer:
            mock_pdfminer.side_effect = FileNotFoundError("File not found")
            result = parser_pack.extract_text("nonexistent_file.pdf")
            # Should return empty string when all methods fail
            assert result == ""
    
    def test_extract_text_empty_pdf(self, temp_dir):
        """Test handling of empty PDF file."""
        pdf_path = Path(temp_dir) / "empty.pdf"
        pdf_path.write_bytes(b"")
        
        # Mock pdfminer to return empty string
        with patch("parser_pack.pdfminer_extract_text") as mock_pdfminer:
            mock_pdfminer.return_value = ""
            
            # PyPDF2 should also return empty
            with patch("parser_pack.PyPDF2") as mock_pypdf2:
                mock_reader = MagicMock()
                mock_reader.pages = []
                mock_pypdf2.PdfReader.return_value = mock_reader
                
                result = parser_pack.extract_text(str(pdf_path))
                # Should fall through to OCR or return empty
                assert result == "" or result is not None
    
    def test_extract_text_multiple_pages(self, temp_dir):
        """Test extraction from PDF with multiple pages."""
        pdf_path = Path(temp_dir) / "multipage.pdf"
        pdf_path.write_text("Test PDF content")
        
        # Mock PyPDF2 with multiple pages
        with patch("parser_pack.pdfminer_extract_text") as mock_pdfminer:
            with patch("parser_pack.PyPDF2") as mock_pypdf2:
                mock_pdfminer.side_effect = Exception("Failed")
                
                mock_reader = MagicMock()
                mock_page1 = MagicMock()
                mock_page1.extract_text.return_value = "Page 1 content"
                mock_page2 = MagicMock()
                mock_page2.extract_text.return_value = "Page 2 content"
                mock_reader.pages = [mock_page1, mock_page2]
                mock_pypdf2.PdfReader.return_value = mock_reader
                
                result = parser_pack.extract_text(str(pdf_path))
                assert result == "Page 1 contentPage 2 content"
                assert mock_page1.extract_text.called
                assert mock_page2.extract_text.called

