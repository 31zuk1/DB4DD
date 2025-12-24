#!/usr/bin/env python3
"""
DB4DD (Data Base for Digital Democracy) - Main Processing Module
Clean, modular architecture for automated government document processing.
Empowering digital democracy through accessible government data.
"""
import os
import sys
import argparse
import logging
from pathlib import Path
from dotenv import load_dotenv
from tqdm import tqdm
from datetime import datetime

# Load environment variables
load_dotenv(Path(__file__).parent / '.env')

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent))

# Import our modules
from core.rate_limiter import AdaptiveRateLimiter, RequestMonitor
from core.api_client import APIClient
from processing.pdf_processor import PDFProcessor
from processing.text_summarizer import TextSummarizer
from output.markdown_generator import MarkdownGenerator
from utils.file_utils import ProcessedDatabase, parse_filename, cleanup_cache, find_pdfs

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# Configuration from environment
DATA_ROOT = Path(os.getenv('DATA_ROOT', './data'))
BASE_VAULT_ROOT = Path(os.getenv('VAULT_ROOT', './DB'))
CACHE_DIR = Path(os.getenv('CACHE_DIR', './.cache'))
CHUNK_SIZE = int(os.getenv('CHUNK_CHARS', '3000'))
MAX_WORKERS = int(os.getenv('MAX_WORKERS', '4'))

# Generate date-based vault directory
def get_dated_vault_root():
    """Generate vault root with today's date folder."""
    today = datetime.now().strftime('%Y%m%d')
    dated_vault = BASE_VAULT_ROOT / f"DB_{today}"
    dated_vault.mkdir(parents=True, exist_ok=True)
    return dated_vault

VAULT_ROOT = get_dated_vault_root()

class GovMeetTracker:
    """Main application class for processing government meeting PDFs."""
    
    def __init__(self, args):
        self.args = args
        
        # Initialize core components
        self.rate_limiter = AdaptiveRateLimiter()
        self.monitor = RequestMonitor()
        self.api_client = APIClient(CACHE_DIR, self.rate_limiter, self.monitor)
        self.pdf_processor = PDFProcessor()
        self.text_summarizer = TextSummarizer(self.api_client, CHUNK_SIZE)
        self.markdown_generator = MarkdownGenerator()
        self.processed_db = ProcessedDatabase(CACHE_DIR / 'processed.json')
        
        # Configure rate limiting based on arguments
        self._configure_rate_limiting()
    
    def _configure_rate_limiting(self):
        """Configure rate limiting based on command line arguments."""
        if self.args.aggressive:
            self.rate_limiter.configure(
                self.args.rate_limit_rpm,
                self.args.rate_limit_tpm,
                50  # Start very aggressive
            )
            logger.info(f"Aggressive mode: {self.args.rate_limit_rpm} RPM, {self.args.rate_limit_tpm} TPM")
        else:
            self.rate_limiter.configure(
                min(self.args.rate_limit_rpm, 3000),
                min(self.args.rate_limit_tpm, 150000)
            )
            logger.info(f"Conservative mode: {self.rate_limiter.rate_info.requests_per_minute} RPM")
    
    def cleanup_cache(self):
        """Clean up old cache files."""
        if self.args.cleanup_cache:
            cleanup_cache(CACHE_DIR, self.args.cleanup_cache)
    
    def clear_vault(self):
        """Clear the vault and processed database."""
        if self.args.clean:
            if VAULT_ROOT.exists():
                import shutil
                shutil.rmtree(VAULT_ROOT)
                logger.info(f"Cleared vault: {VAULT_ROOT}")
            self.processed_db.clear()
            logger.info("Cleared processed database")
    
    def find_pdfs_to_process(self):
        """Find PDFs that need processing."""
        pdfs = find_pdfs(DATA_ROOT, self.args.meeting, self.args.round)
        
        if not pdfs:
            logger.error(f"No PDFs found in {DATA_ROOT}")
            return []
        
        logger.info(f"Found {len(pdfs)} PDFs")
        return pdfs
    
    def dry_run(self, pdfs):
        """Show what would be processed without actually processing."""
        if self.args.dry_run:
            logger.info("Dry run mode - showing what would be processed:")
            for pdf in pdfs:
                meta = parse_filename(pdf.stem)
                print('→', pdf.name, '⇒', meta)
            return True
        return False
    
    def process_single_pdf(self, pdf_path: Path) -> bool:
        """Process a single PDF file."""
        try:
            # Extract text
            text = self.pdf_processor.extract(pdf_path)
            if not text.strip():
                self.processed_db.mark(str(pdf_path), 'empty')
                return False
            
            logger.info(f"Processing {pdf_path.name} ({len(text)} chars)")
            
            # Generate summary
            summary = self.text_summarizer.power_summary(text, nocache=self.args.nocache)
            
            # Parse filename and generate output
            meeting, round_num, date = parse_filename(pdf_path.stem)
            
            # Skip files that don't match the expected naming pattern
            if meeting is None or round_num is None or date is None:
                logger.warning(f"Skipping invalid filename: {pdf_path.name}")
                self.processed_db.mark(str(pdf_path), 'error')
                return False
            
            # Create output directory
            out_dir = VAULT_ROOT / meeting
            out_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate markdown
            markdown_content = self.markdown_generator.generate_markdown(
                summary, meeting, int(round_num), date, pdf_path.name
            )
            
            # Write markdown file
            output_file = out_dir / f"{meeting}_第{round_num}回_{date[:4]}-{date[4:6]}-{date[6:]}.md"
            output_file.write_text(markdown_content, encoding='utf-8')
            
            self.processed_db.mark(str(pdf_path), 'success')
            return True
            
        except Exception as e:
            logger.error(f"Failed to process {pdf_path.name}: {e}")
            self.processed_db.mark(str(pdf_path), 'error')
            return False
    
    def run(self):
        """Main execution method."""
        # Handle cleanup operations
        self.cleanup_cache()
        self.clear_vault()
        
        # Find PDFs to process
        pdfs = self.find_pdfs_to_process()
        if not pdfs:
            return
        
        # Handle dry run
        if self.dry_run(pdfs):
            return
        
        # Main processing loop
        bar = tqdm(total=len(pdfs), desc='PDF')
        processed_count = 0
        
        for pdf in pdfs:
            # Skip if already processed (unless overwrite)
            if not self.args.overwrite and self.processed_db.is_processed(str(pdf)):
                bar.update(1)
                continue
            
            # Log progress every 5 files
            if processed_count > 0 and processed_count % 5 == 0:
                self.monitor.log_status(self.rate_limiter)
            
            # Process the PDF
            if self.process_single_pdf(pdf):
                processed_count += 1
            
            bar.update(1)
        
        bar.close()
        
        # Final statistics
        self.monitor.log_status(self.rate_limiter)
        logger.info(f'✅ All done. Processed {processed_count} files.')

def create_argument_parser():
    """Create and configure argument parser."""
    parser = argparse.ArgumentParser(description='Process government meeting PDFs')
    
    # Processing options
    parser.add_argument('--meeting', help='Filter by meeting name')
    parser.add_argument('--round', type=int, help='Filter by round number')
    parser.add_argument('--overwrite', action='store_true', help='Reprocess existing files')
    parser.add_argument('--nocache', action='store_true', help='Disable API caching')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be processed')
    
    # Maintenance options
    parser.add_argument('--clean', action='store_true', help='Clear vault and processed DB')
    parser.add_argument('--cleanup-cache', type=int, metavar='DAYS', 
                       help='Remove cache files older than N days')
    
    # Performance options
    parser.add_argument('--workers', type=int, default=MAX_WORKERS, help='Number of workers')
    parser.add_argument('--aggressive', action='store_true', 
                       help='最大並列度で処理（レート制限ギリギリ）')
    parser.add_argument('--rate-limit-rpm', type=int, default=5000, 
                       help='分間リクエスト数制限')
    parser.add_argument('--rate-limit-tpm', type=int, default=200000, 
                       help='分間トークン数制限')
    
    return parser

def main():
    """Main entry point."""
    parser = create_argument_parser()
    args = parser.parse_args()
    
    try:
        app = GovMeetTracker(args)
        app.run()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()