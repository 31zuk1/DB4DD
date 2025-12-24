"""
PDF text extraction with multiple fallback strategies.
"""
import tempfile
import logging
import subprocess
from pathlib import Path
from typing import Optional

try:
    import pdfminer.high_level
    PDFMINER_AVAILABLE = True
except ImportError:
    PDFMINER_AVAILABLE = False

try:
    import fitz  # PyMuPDF
    PYMUPDF_AVAILABLE = True
except ImportError:
    PYMUPDF_AVAILABLE = False

logger = logging.getLogger(__name__)

class PDFProcessor:
    """PDF text extraction with fallback strategies."""
    
    def __init__(self):
        self.strategies = []
        
        if PDFMINER_AVAILABLE:
            self.strategies.append(self._extract_with_pdfminer)
            
        if PYMUPDF_AVAILABLE:
            self.strategies.append(self._extract_with_pymupdf)
            
        self.strategies.append(self._extract_with_tesseract)
        
        logger.info(f"Initialized with {len(self.strategies)} extraction strategies")
    
    def extract(self, pdf_path: Path) -> str:
        """Extract text from PDF using fallback strategies."""
        logger.info(f"Extracting text from {pdf_path.name}")
        
        for i, strategy in enumerate(self.strategies):
            try:
                text = strategy(pdf_path)
                if text and text.strip():
                    logger.info(f"Successfully extracted {len(text)} chars using strategy {i+1}")
                    return text
                else:
                    logger.warning(f"Strategy {i+1} returned empty text")
            except Exception as e:
                logger.warning(f"Strategy {i+1} failed: {e}")
                continue
        
        logger.error(f"All extraction strategies failed for {pdf_path.name}")
        return ""
    
    def _extract_with_pdfminer(self, pdf_path: Path) -> str:
        """Extract text using PDFMiner."""
        return pdfminer.high_level.extract_text(str(pdf_path))
    
    def _extract_with_pymupdf(self, pdf_path: Path) -> str:
        """Extract text using PyMuPDF."""
        doc = fitz.open(str(pdf_path))
        text = ""
        for page in doc:
            text += page.get_text()
        doc.close()
        return text
    
    def _extract_with_tesseract(self, pdf_path: Path) -> str:
        """Extract text using Tesseract OCR."""
        try:
            import fitz  # Need PyMuPDF for image conversion
        except ImportError:
            logger.error("PyMuPDF required for Tesseract OCR fallback")
            return ""
        
        doc = fitz.open(str(pdf_path))
        text = ""
        
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            for page_num in range(len(doc)):
                page = doc[page_num]
                pix = page.get_pixmap()
                img_path = temp_path / f"page_{page_num}.png"
                pix.save(str(img_path))
                
                try:
                    result = subprocess.run([
                        'tesseract', str(img_path), 'stdout', '-l', 'jpn'
                    ], capture_output=True, text=True, timeout=30)
                    
                    if result.returncode == 0:
                        text += result.stdout + "\n"
                    else:
                        logger.warning(f"Tesseract failed for page {page_num}")
                        
                except (subprocess.TimeoutExpired, FileNotFoundError) as e:
                    logger.warning(f"Tesseract error on page {page_num}: {e}")
                    continue
        
        doc.close()
        return text