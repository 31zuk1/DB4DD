"""
File handling utilities.
"""
import re
import json
import logging
from pathlib import Path
from typing import Tuple, Optional

logger = logging.getLogger(__name__)

class ProcessedDatabase:
    """Simple JSON-based database to track processed files."""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.data = {}
        self.load()
    
    def load(self):
        """Load the database from file."""
        if self.db_path.exists():
            try:
                with open(self.db_path, 'r', encoding='utf-8') as f:
                    self.data = json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load processed DB: {e}")
                self.data = {}
    
    def save(self):
        """Save the database to file."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.db_path, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)
    
    def is_processed(self, key: str) -> bool:
        """Check if a file has been processed."""
        return key in self.data
    
    def mark(self, key: str, status: str):
        """Mark a file as processed with given status."""
        self.data[key] = {'status': status, 'timestamp': None}
        self.save()
    
    def clear(self):
        """Clear all processed records."""
        self.data.clear()
        self.save()

def parse_filename(filename: str) -> Tuple[str, str, str]:
    """Parse meeting filename to extract meeting name, round, and date."""
    # Pattern: {meeting_name}_第{N}回_{YYYYMMDD}_{additional_info}
    pattern = r'^(.+?)_第(\d+)回_(\d{8})(?:_.*)?$'
    match = re.match(pattern, filename)
    
    if match:
        meeting_name = match.group(1)
        round_num = match.group(2).zfill(2)  # Pad with zero if needed
        date = match.group(3)
        return meeting_name, round_num, date
    else:
        logger.warning(f"Could not parse filename: {filename}")
        # Return None to indicate invalid filename that should be skipped
        return None, None, None

def cleanup_cache(cache_dir: Path, days: int):
    """Remove cache files older than specified days."""
    import time
    cutoff = time.time() - (days * 24 * 60 * 60)
    
    removed_count = 0
    for cache_file in cache_dir.glob('*.json'):
        try:
            if cache_file.stat().st_mtime < cutoff:
                cache_file.unlink()
                removed_count += 1
        except Exception as e:
            logger.warning(f"Failed to remove cache file {cache_file}: {e}")
    
    logger.info(f"Removed {removed_count} old cache files")

def find_pdfs(data_root: Path, meeting_filter: Optional[str] = None, 
              round_filter: Optional[int] = None) -> list:
    """Find PDF files based on filters."""
    pdfs = []
    
    for pdf_path in data_root.rglob('*.pdf'):
        meeting, round_num, date = parse_filename(pdf_path.stem)
        
        # Skip files that don't match the expected naming pattern
        if meeting is None or round_num is None or date is None:
            logger.info(f"Skipping invalid filename: {pdf_path.name}")
            continue
            
        if meeting_filter:
            if meeting_filter.lower() not in meeting.lower():
                continue
        
        if round_filter is not None:
            if int(round_num) != round_filter:
                continue
        
        pdfs.append(pdf_path)
    
    return sorted(pdfs)