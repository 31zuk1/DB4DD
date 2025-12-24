#!/usr/bin/env python3
"""
DB4DD - Text Cache Based Processing Module
Processes cached text files instead of PDFs to generate AI-powered summaries.
Uses existing text files in text_cache/ directory for more efficient processing.
"""
import os
import sys
import argparse
import logging
from pathlib import Path
from dotenv import load_dotenv
from tqdm import tqdm
from datetime import datetime
import json
from collections import defaultdict
from typing import Dict, List, Optional

# Load environment variables
load_dotenv(Path(__file__).parent / '.env')

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent))

# Import our modules
from core.rate_limiter import AdaptiveRateLimiter, RequestMonitor
from core.api_client import APIClient
from processing.text_summarizer import TextSummarizer
from output.markdown_generator import MarkdownGenerator
from utils.file_utils_enhanced import (
    EnhancedProcessedDatabase, 
    EnhancedFileParser,
    FileMetadata
)
from utils.file_utils import cleanup_cache

# Initialize logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s %(levelname)s %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration from environment
TEXT_CACHE_ROOT = Path('./text_cache')
BASE_VAULT_ROOT = Path(os.getenv('VAULT_ROOT', './DB'))
CACHE_DIR = Path(os.getenv('CACHE_DIR', './.cache'))
CHUNK_SIZE = int(os.getenv('CHUNK_CHARS', '2000'))
MAX_WORKERS = int(os.getenv('MAX_WORKERS', '4'))

# Fixed date for output - use DB_0823 as requested
VAULT_DATE = '0823'
VAULT_ROOT = BASE_VAULT_ROOT / f"DB_{VAULT_DATE}"

class TextCacheSession:
    """Represents a group of text files from the same meeting session."""
    def __init__(self, session_key: str, session_dir: Path):
        self.session_key = session_key
        self.session_dir = session_dir
        self.text_files: List[Path] = []
        self.metadata: Optional[FileMetadata] = None
        self.meeting_name = None
        self.round_num = None
        self.date = None
    
    def add_text_file(self, text_file: Path):
        """Add a text file to this session group."""
        self.text_files.append(text_file)
        
        # Parse metadata from the first file
        if self.metadata is None:
            self._parse_metadata_from_filename(text_file)
    
    def _parse_metadata_from_filename(self, text_file: Path):
        """Parse metadata from text filename."""
        # Example: EBPM研究会_第01回_20230621_議事次第.txt
        filename = text_file.stem  # Remove .txt extension
        parts = filename.split('_')
        
        if len(parts) >= 3:
            self.meeting_name = parts[0]
            
            # Extract round number
            if '第' in parts[1] and '回' in parts[1]:
                round_str = parts[1].replace('第', '').replace('回', '')
                try:
                    self.round_num = int(round_str)
                except:
                    pass
            
            # Extract date
            if len(parts[2]) == 8 and parts[2].isdigit():
                try:
                    year = int(parts[2][:4])
                    month = int(parts[2][4:6])
                    day = int(parts[2][6:8])
                    self.date = f"{year}-{month:02d}-{day:02d}"
                except:
                    pass
            
            # Create metadata object
            self.metadata = FileMetadata()
            self.metadata.meeting_name = self.meeting_name
            self.metadata.round_num = str(self.round_num) if self.round_num else None
            self.metadata.date = self.date.replace('-', '') if self.date else None
            self.metadata.is_valid = True
    
    def get_session_name(self) -> str:
        """Get the session name for output filename."""
        parts = []
        if self.meeting_name:
            parts.append(self.meeting_name)
        if self.round_num:
            parts.append(f"第{self.round_num:02d}回")
        if self.date:
            parts.append(self.date.replace('-', ''))
        return "_".join(parts) if parts else self.session_key
    
    def get_combined_text(self) -> str:
        """Read and combine all text files in this session."""
        combined_text = []
        
        for text_file in sorted(self.text_files):
            try:
                with open(text_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    if content.strip():  # Only add non-empty content
                        # Add a header for each file
                        file_type = text_file.stem.split('_')[-1]  # e.g., 議事次第, 資料1
                        combined_text.append(f"\n=== {file_type} ===\n")
                        combined_text.append(content)
                        combined_text.append("\n" + "="*50 + "\n")
            except Exception as e:
                logger.warning(f"Failed to read {text_file}: {e}")
        
        return "\n".join(combined_text)

class TextCacheProcessor:
    """Process cached text files to generate summaries."""
    
    def __init__(self, args):
        self.args = args
        
        # Initialize core components
        self.rate_limiter = AdaptiveRateLimiter()
        self.monitor = RequestMonitor()
        self.api_client = APIClient(CACHE_DIR, self.rate_limiter, self.monitor)
        self.text_summarizer = TextSummarizer(self.api_client, CHUNK_SIZE)
        self.markdown_generator = MarkdownGenerator()
        self.processed_db = EnhancedProcessedDatabase(CACHE_DIR / 'processed_text_sessions.json')
        
        # Create output directory
        VAULT_ROOT.mkdir(parents=True, exist_ok=True)
        logger.info(f"Output directory: {VAULT_ROOT}")
    
    def find_text_sessions(self) -> Dict[str, TextCacheSession]:
        """Find and group text files by session."""
        sessions = {}
        
        # Determine which ministries to process
        if self.args.ministry:
            ministries = [self.args.ministry]
        else:
            # Process all available ministries
            ministries = [d.name for d in TEXT_CACHE_ROOT.iterdir() if d.is_dir()]
        
        for ministry in ministries:
            ministry_path = TEXT_CACHE_ROOT / ministry
            if not ministry_path.exists():
                logger.warning(f"Ministry path not found: {ministry_path}")
                continue
            
            # Find all meeting directories
            for meeting_dir in ministry_path.iterdir():
                if not meeting_dir.is_dir():
                    continue
                
                # Skip if specific meeting filter is set
                if self.args.meeting and self.args.meeting not in str(meeting_dir):
                    continue
                
                # Find all session directories
                for session_dir in meeting_dir.iterdir():
                    if not session_dir.is_dir():
                        continue
                    
                    # Create session key
                    session_key = f"{ministry}/{meeting_dir.name}/{session_dir.name}"
                    
                    # Check if already processed
                    if not self.args.overwrite and self.processed_db.is_processed(session_key):
                        logger.info(f"Skipping already processed: {session_key}")
                        continue
                    
                    # Create session group
                    session = TextCacheSession(session_key, session_dir)
                    
                    # Find all text files in session directory
                    text_files = list(session_dir.glob("*.txt"))
                    if text_files:
                        for text_file in text_files:
                            session.add_text_file(text_file)
                        sessions[session_key] = session
                        logger.info(f"Found session with {len(text_files)} text files: {session_key}")
        
        return sessions
    
    def process_session(self, session: TextCacheSession) -> bool:
        """Process a single session of text files."""
        try:
            logger.info(f"Processing session: {session.session_key}")
            
            # Get combined text from all files
            combined_text = session.get_combined_text()
            
            if not combined_text.strip():
                logger.warning(f"No text content found for session: {session.session_key}")
                return False
            
            # Create output path - fix duplicate ministry in path
            parts = session.session_key.split('/')
            ministry = parts[0]
            
            output_dir = VAULT_ROOT / ministry
            output_dir.mkdir(parents=True, exist_ok=True)
            
            output_filename = f"{session.get_session_name()}.md"
            output_path = output_dir / output_filename
            
            # Skip if file exists and not overwriting
            if output_path.exists() and not self.args.overwrite:
                logger.info(f"Output already exists, skipping: {output_path}")
                return True
            
            logger.info(f"Text length: {len(combined_text)} characters")
            logger.info(f"Generating AI summary...")
            
            # Generate summary
            # Create metadata if not exists
            if not session.metadata:
                session.metadata = FileMetadata()
                session.metadata.meeting_name = session.meeting_name
                session.metadata.round_num = str(session.round_num) if session.round_num else None
                session.metadata.date = session.date.replace('-', '') if session.date else None
                session.metadata.is_valid = True
            
            # Generate AI-powered summary using existing framework
            summary_result = self.text_summarizer.power_summary(combined_text)
            
            if not summary_result:
                logger.warning(f"Failed to get summary result for: {session.session_key}")
                return False
            
            # Generate markdown using the structured result
            logger.info(f"Creating markdown file: {output_filename} in {output_dir}")
            
            # Prepare metadata for markdown generation
            meeting_name = session.metadata.meeting_name or "Unknown Meeting"
            round_num = session.metadata.round_num or "1"
            date_str = session.metadata.date or "20230101"
            source_files = ", ".join([f.name for f in session.text_files])
            
            # Use the markdown generator with the summary result
            markdown_content = self.markdown_generator.generate_markdown(
                summary_result,
                meeting_name,
                int(round_num) if str(round_num).isdigit() else 1,
                date_str,
                source_files
            )
            
            # Write output
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(markdown_content)
            
            # Mark as processed
            self.processed_db.mark(session.session_key, 'completed')
            
            logger.info(f"✅ Successfully processed: {output_filename}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to process session {session.session_key}: {e}")
            try:
                logger.debug(f"  Target directory: {output_dir}")
                logger.debug(f"  Target filename: {output_filename}")
            except:
                pass
            return False
    
    def run(self):
        """Main processing loop."""
        logger.info("=" * 80)
        logger.info("DB4DD Text Cache Processor - Starting")
        logger.info(f"Text cache directory: {TEXT_CACHE_ROOT}")
        logger.info(f"Output directory: {VAULT_ROOT}")
        logger.info("=" * 80)
        
        # Find all text sessions
        sessions = self.find_text_sessions()
        
        if not sessions:
            logger.warning("No text sessions found to process")
            return
        
        logger.info(f"Found {len(sessions)} sessions to process")
        
        # Process each session
        success_count = 0
        fail_count = 0
        
        with tqdm(total=len(sessions), desc="Processing sessions") as pbar:
            for session_key, session in sessions.items():
                try:
                    if self.process_session(session):
                        success_count += 1
                    else:
                        fail_count += 1
                except KeyboardInterrupt:
                    logger.info("Processing interrupted by user")
                    break
                except Exception as e:
                    logger.error(f"Unexpected error processing {session_key}: {e}")
                    fail_count += 1
                finally:
                    pbar.update(1)
        
        # Print summary
        logger.info("=" * 80)
        logger.info("Processing complete!")
        logger.info(f"Successfully processed: {success_count} sessions")
        if fail_count > 0:
            logger.warning(f"Failed: {fail_count} sessions")
        logger.info(f"Output directory: {VAULT_ROOT}")
        logger.info("=" * 80)

def main():
    parser = argparse.ArgumentParser(
        description='Process cached text files to generate AI summaries'
    )
    parser.add_argument(
        '--ministry', 
        type=str, 
        help='Process only specific ministry (e.g., "こども家庭庁")'
    )
    parser.add_argument(
        '--meeting', 
        type=str, 
        help='Process only specific meeting type'
    )
    parser.add_argument(
        '--overwrite', 
        action='store_true', 
        help='Overwrite existing output files'
    )
    parser.add_argument(
        '--cleanup-cache',
        type=int,
        metavar='DAYS',
        help='Clean up cache files older than N days'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be processed without actually processing'
    )
    
    args = parser.parse_args()
    
    # Handle cache cleanup
    if args.cleanup_cache is not None:
        cleanup_cache(CACHE_DIR, args.cleanup_cache)
        return
    
    # Dry run mode
    if args.dry_run:
        processor = TextCacheProcessor(args)
        sessions = processor.find_text_sessions()
        logger.info(f"Dry run mode - would process {len(sessions)} sessions:")
        for session_key in sessions:
            logger.info(f"  - {session_key}")
        return
    
    # Run the processor
    processor = TextCacheProcessor(args)
    processor.run()

if __name__ == "__main__":
    main()