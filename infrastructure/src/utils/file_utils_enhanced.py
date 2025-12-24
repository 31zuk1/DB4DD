"""
DB4DD (Data Base for Digital Democracy) - Enhanced File Utilities
Advanced file parsing and metadata extraction for Japanese government documents.
Supports both デジタル庁 and こども家庭庁 directory structures.
"""
import re
import json
import logging
import unicodedata
from pathlib import Path
from typing import Tuple, Optional, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

# Import base class from original file_utils
from .file_utils import ProcessedDatabase as BaseProcessedDatabase

class FileMetadata:
    """Metadata extracted from government document filenames."""
    
    def __init__(self):
        self.meeting_name: Optional[str] = None
        self.round_num: Optional[str] = None
        self.date: Optional[str] = None
        self.ministry: Optional[str] = None
        self.document_type: Optional[str] = None
        self.fiscal_year: Optional[str] = None
        self.additional_info: Optional[str] = None
        self.is_valid: bool = False
        self.pattern_used: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metadata to dictionary."""
        return {
            'meeting_name': self.meeting_name,
            'round_num': self.round_num,
            'date': self.date,
            'ministry': self.ministry,
            'document_type': self.document_type,
            'fiscal_year': self.fiscal_year,
            'additional_info': self.additional_info,
            'is_valid': self.is_valid,
            'pattern_used': self.pattern_used
        }
    
    def get_formatted_date(self) -> Optional[str]:
        """Get formatted date string (YYYY-MM-DD)."""
        if self.date and len(self.date) == 8:
            return f"{self.date[:4]}-{self.date[4:6]}-{self.date[6:]}"
        return None

class EnhancedFileParser:
    """Enhanced file parser with multiple pattern support."""
    
    # Define patterns in priority order
    PATTERNS = [
        # Standard pattern: {meeting}_第{N}回_{YYYYMMDD}_{optional}
        {
            'name': 'standard',
            'pattern': r'^(.+?)_第(\d+)回_(\d{8})(?:_(.*))?$',
            'groups': ['meeting_name', 'round_num', 'date', 'additional_info']
        },
        # Fiscal year pattern: {meeting}_{fiscal_year}{period}
        {
            'name': 'fiscal_year',
            'pattern': r'^(.+?)_(令和|平成)(\d+)年度?(全体|概要|上期|下期|第[1-4]四半期)?$',
            'groups': ['meeting_name', 'era', 'year', 'period']
        },
        # Simple report pattern: {report_name} (for direct files under meeting folders)
        {
            'name': 'simple_report',
            'pattern': r'^([^_]+(?:について|報告書|まとめ|概要|資料))$',
            'groups': ['document_name']
        },
        # Meeting with date only: {meeting}_{YYYYMMDD}_{optional}
        {
            'name': 'date_only',
            'pattern': r'^(.+?)_(\d{8})(?:_(.*))?$',
            'groups': ['meeting_name', 'date', 'additional_info']
        },
        # Notification pattern: {type}_{content}
        {
            'name': 'notification',
            'pattern': r'^(通知|別添|参考資料|Q&A|活用について)(?:_(.*))?$',
            'groups': ['document_type', 'content']
        }
    ]
    
    # Document type keywords
    DOC_TYPE_KEYWORDS = {
        '議事次第': 'agenda',
        '議事録': 'minutes',
        '議事概要': 'summary',
        '議事要旨': 'abstract',
        '資料': 'material',
        '参考資料': 'reference',
        '報告書': 'report',
        '概要': 'overview',
        '通知': 'notification',
        '別添': 'attachment',
        '構成員': 'member',
        '名簿': 'roster',
        '設置要綱': 'establishment',
        '運営要領': 'operation',
        'Q&A': 'qa',
        'まとめ': 'summary_report'
    }
    
    @classmethod
    def parse_filename(cls, filename: str, pdf_path: Path) -> FileMetadata:
        """Parse filename with enhanced pattern matching."""
        metadata = FileMetadata()
        
        # Determine ministry from path with Unicode normalization
        path_str = unicodedata.normalize('NFC', str(pdf_path))
        digital_ministry = unicodedata.normalize('NFC', 'デジタル庁')
        kodomo_ministry = unicodedata.normalize('NFC', 'こども家庭庁')
        
        if digital_ministry in path_str:
            metadata.ministry = 'デジタル庁'
        elif kodomo_ministry in path_str:
            metadata.ministry = 'こども家庭庁'
        
        # Extract meeting name from parent directory if possible
        parent_dir = pdf_path.parent.name
        if parent_dir and '_第' in parent_dir:
            # This is likely a round-specific folder
            meeting_match = re.match(r'^(.+?)_第\d+回_\d{8}$', parent_dir)
            if meeting_match:
                default_meeting = meeting_match.group(1)
            else:
                default_meeting = pdf_path.parent.parent.name
        else:
            # Direct child of meeting folder
            default_meeting = parent_dir
        
        # Try each pattern
        for pattern_info in cls.PATTERNS:
            pattern = pattern_info['pattern']
            match = re.match(pattern, filename)
            
            if match:
                metadata.pattern_used = pattern_info['name']
                groups = match.groups()
                
                # Process based on pattern type
                if pattern_info['name'] == 'standard':
                    metadata.meeting_name = groups[0]
                    metadata.round_num = groups[1].zfill(2)
                    metadata.date = groups[2]
                    metadata.additional_info = groups[3]
                    metadata.is_valid = True
                    
                elif pattern_info['name'] == 'fiscal_year':
                    metadata.meeting_name = groups[0] or default_meeting
                    era = groups[1]
                    year = int(groups[2])
                    # Convert Japanese era to Western year
                    if era == '令和':
                        western_year = 2018 + year
                    else:  # 平成
                        western_year = 1988 + year
                    metadata.fiscal_year = f"{western_year}"
                    metadata.additional_info = groups[3]
                    metadata.is_valid = True
                    
                elif pattern_info['name'] == 'simple_report':
                    metadata.meeting_name = default_meeting
                    metadata.document_type = 'report'
                    metadata.additional_info = groups[0]
                    metadata.is_valid = True
                    
                elif pattern_info['name'] == 'date_only':
                    metadata.meeting_name = groups[0]
                    metadata.date = groups[1]
                    metadata.additional_info = groups[2]
                    metadata.is_valid = True
                    
                elif pattern_info['name'] == 'notification':
                    metadata.meeting_name = default_meeting
                    metadata.document_type = groups[0]
                    metadata.additional_info = groups[1]
                    metadata.is_valid = True
                
                # Extract document type from additional info
                if metadata.additional_info:
                    for keyword, doc_type in cls.DOC_TYPE_KEYWORDS.items():
                        if keyword in metadata.additional_info:
                            metadata.document_type = doc_type
                            break
                
                break
        
        # If no pattern matched, use fallback
        if not metadata.is_valid:
            metadata.meeting_name = default_meeting
            metadata.additional_info = filename.replace('.pdf', '')
            metadata.is_valid = True  # Mark as valid to process
            metadata.pattern_used = 'fallback'
            logger.warning(f"Using fallback parsing for: {filename}")
        
        return metadata

class EnhancedProcessedDatabase(BaseProcessedDatabase):
    """Enhanced database with metadata storage."""
    
    def mark_with_metadata(self, key: str, status: str, metadata: FileMetadata):
        """Mark a file as processed with metadata."""
        self.data[key] = {
            'status': status,
            'timestamp': datetime.now().isoformat(),
            'metadata': metadata.to_dict()
        }
        self.save()
    
    def get_metadata(self, key: str) -> Optional[Dict[str, Any]]:
        """Get metadata for a processed file."""
        if key in self.data:
            return self.data[key].get('metadata')
        return None

class PDFWithMetadata:
    """Wrapper class to hold PDF path and metadata."""
    def __init__(self, path: Path, metadata: FileMetadata):
        self.path = path
        self.metadata = metadata
        
    def __str__(self):
        return str(self.path)
    
    def __getattr__(self, name):
        # Delegate attribute access to the Path object
        return getattr(self.path, name)

def find_pdfs_enhanced(data_root: Path, meeting_filter: Optional[str] = None, 
                      round_filter: Optional[int] = None, 
                      ministry_filter: Optional[str] = None) -> list:
    """Find PDF files with enhanced filtering."""
    pdfs = []
    parser = EnhancedFileParser()
    
    for pdf_path in data_root.rglob('*.pdf'):
        # Skip hidden files and temp files
        if pdf_path.name.startswith('.') or pdf_path.name.startswith('~'):
            continue
            
        metadata = parser.parse_filename(pdf_path.name, pdf_path)
        
        # Apply filters
        if ministry_filter:
            if not metadata.ministry or ministry_filter not in metadata.ministry:
                continue
                
        if meeting_filter:
            if not metadata.meeting_name or meeting_filter.lower() not in metadata.meeting_name.lower():
                continue
        
        if round_filter is not None:
            if not metadata.round_num or int(metadata.round_num) != round_filter:
                continue
        
        # Create wrapper object with metadata
        pdf_with_meta = PDFWithMetadata(pdf_path, metadata)
        pdfs.append(pdf_with_meta)
    
    return sorted(pdfs, key=lambda x: str(x.path))

def generate_output_filename(metadata: FileMetadata) -> str:
    """Generate appropriate output filename based on metadata."""
    parts = []
    
    # Add meeting name
    if metadata.meeting_name:
        parts.append(metadata.meeting_name)
    
    # Add round number
    if metadata.round_num:
        parts.append(f"第{metadata.round_num}回")
    
    # Add date or fiscal year
    if metadata.date:
        formatted_date = metadata.get_formatted_date()
        if formatted_date:
            parts.append(formatted_date)
    elif metadata.fiscal_year:
        parts.append(f"{metadata.fiscal_year}年度")
        if metadata.additional_info:
            parts.append(metadata.additional_info)
    
    # Add document type
    if metadata.document_type:
        parts.append(metadata.document_type)
    elif metadata.additional_info and not metadata.fiscal_year:
        # Clean up additional info for filename
        clean_info = metadata.additional_info.replace('.pdf', '').replace('.', '_')
        parts.append(clean_info)
    
    # Join with underscores and add .md extension
    filename = '_'.join(parts) + '.md'
    
    # Sanitize filename
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    
    return filename

# Keep backward compatibility
ProcessedDatabase = EnhancedProcessedDatabase
parse_filename = lambda filename: EnhancedFileParser.parse_filename(filename, Path(filename))