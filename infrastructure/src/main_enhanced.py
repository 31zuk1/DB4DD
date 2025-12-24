#!/usr/bin/env python3
"""
DB4DD (Data Base for Digital Democracy) - Enhanced Processing Module
Automated processing of Japanese government meeting documents with AI-powered summarization.
Supports both デジタル庁 and こども家庭庁 directory structures.
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
from utils.file_utils_enhanced import (
    EnhancedProcessedDatabase, 
    EnhancedFileParser,
    find_pdfs_enhanced,
    generate_output_filename,
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
DATA_ROOT = Path(os.getenv('DATA_ROOT', './data'))
BASE_VAULT_ROOT = Path(os.getenv('VAULT_ROOT', './DB'))
CACHE_DIR = Path(os.getenv('CACHE_DIR', './.cache'))
CHUNK_SIZE = int(os.getenv('CHUNK_CHARS', '2000'))
MAX_WORKERS = int(os.getenv('MAX_WORKERS', '4'))

# Fixed date for DB_0728
VAULT_DATE = '0728'
VAULT_ROOT = BASE_VAULT_ROOT / f"DB_{VAULT_DATE}"

class EnhancedGovMeetTracker:
    """Enhanced application class with better exception handling."""
    
    def __init__(self, args):
        self.args = args
        
        # Initialize core components
        self.rate_limiter = AdaptiveRateLimiter()
        self.monitor = RequestMonitor()
        self.api_client = APIClient(CACHE_DIR, self.rate_limiter, self.monitor)
        self.pdf_processor = PDFProcessor()
        self.text_summarizer = TextSummarizer(self.api_client, CHUNK_SIZE)
        self.markdown_generator = MarkdownGenerator()
        self.processed_db = EnhancedProcessedDatabase(CACHE_DIR / 'processed.json')
        self.file_parser = EnhancedFileParser()
        
        # Statistics tracking
        self.stats = {
            'total_files': 0,
            'processed': 0,
            'skipped': 0,
            'errors': 0,
            'by_ministry': {},
            'by_pattern': {}
        }
        
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
    
    def setup_vault_structure(self):
        """Set up the Obsidian vault structure."""
        logger.info(f"Setting up vault structure at: {VAULT_ROOT}")
        
        # Create main vault directory
        VAULT_ROOT.mkdir(parents=True, exist_ok=True)
        
        # Create .obsidian directory with basic configuration
        obsidian_dir = VAULT_ROOT / '.obsidian'
        obsidian_dir.mkdir(exist_ok=True)
        
        # Create basic app.json
        app_config = {
            "legacyEditor": False,
            "livePreview": True,
            "defaultViewMode": "preview",
            "showFrontmatter": True,
            "showLineNumber": True,
            "spellcheck": True,
            "useTab": False,
            "tabSize": 2
        }
        (obsidian_dir / 'app.json').write_text(
            json.dumps(app_config, ensure_ascii=False, indent=2)
        )
        
        # Create workspace configuration
        workspace_config = {
            "main": {
                "id": "main",
                "type": "split",
                "children": [{
                    "id": "root",
                    "type": "leaf",
                    "state": {
                        "type": "markdown",
                        "state": {
                            "file": "index.md",
                            "mode": "preview"
                        }
                    }
                }],
                "direction": "vertical"
            },
            "left": {
                "id": "left",
                "type": "split",
                "children": [{
                    "id": "file-explorer",
                    "type": "leaf",
                    "state": {
                        "type": "file-explorer",
                        "state": {}
                    }
                }],
                "direction": "horizontal",
                "width": 300
            },
            "active": "root"
        }
        (obsidian_dir / 'workspace.json').write_text(
            json.dumps(workspace_config, ensure_ascii=False, indent=2)
        )
        
        logger.info("Vault structure created successfully")
    
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
        pdfs = find_pdfs_enhanced(
            DATA_ROOT, 
            self.args.meeting, 
            self.args.round,
            self.args.ministry
        )
        
        if not pdfs:
            logger.error(f"No PDFs found in {DATA_ROOT}")
            return []
        
        self.stats['total_files'] = len(pdfs)
        logger.info(f"Found {len(pdfs)} PDFs")
        
        # Count by ministry
        for pdf in pdfs:
            if pdf.metadata.ministry:
                ministry = pdf.metadata.ministry
                self.stats['by_ministry'][ministry] = self.stats['by_ministry'].get(ministry, 0) + 1
        
        return pdfs
    
    def dry_run(self, pdfs):
        """Show what would be processed without actually processing."""
        if self.args.dry_run:
            logger.info("Dry run mode - showing what would be processed:")
            print("\nFiles to process:")
            print("-" * 80)
            
            for pdf in pdfs[:20]:  # Show first 20 files
                meta = pdf.metadata
                print(f"📄 {pdf.name}")
                print(f"   Ministry: {meta.ministry or 'Unknown'}")
                print(f"   Meeting: {meta.meeting_name or 'Unknown'}")
                if meta.round_num:
                    print(f"   Round: {meta.round_num}")
                if meta.date:
                    print(f"   Date: {meta.get_formatted_date()}")
                print(f"   Pattern: {meta.pattern_used}")
                print()
            
            if len(pdfs) > 20:
                print(f"... and {len(pdfs) - 20} more files")
            
            print("\nStatistics:")
            print("-" * 40)
            for ministry, count in self.stats['by_ministry'].items():
                print(f"{ministry}: {count} files")
            
            return True
        return False
    
    def generate_enhanced_markdown(self, summary: str, metadata: FileMetadata, 
                                 pdf_path: Path) -> str:
        """Generate markdown with enhanced metadata."""
        lines = []
        
        # Enhanced frontmatter
        lines.append("---")
        lines.append(f"title: \"{metadata.meeting_name or 'Unknown Meeting'}\"")
        if metadata.round_num:
            lines.append(f"round: {metadata.round_num}")
        if metadata.date:
            lines.append(f"date: {metadata.get_formatted_date()}")
        if metadata.fiscal_year:
            lines.append(f"fiscal_year: {metadata.fiscal_year}")
        lines.append(f"ministry: {metadata.ministry or 'Unknown'}")
        if metadata.document_type:
            lines.append(f"document_type: {metadata.document_type}")
        lines.append(f"source_file: \"{pdf_path.name}\"")
        lines.append(f"processed_date: {datetime.now().strftime('%Y-%m-%d')}")
        
        # Tags
        tags = []
        if metadata.ministry:
            tags.append(f"#{metadata.ministry.replace(' ', '_')}")
        if metadata.meeting_name:
            # Simplify meeting name for tag
            simple_name = metadata.meeting_name.split('_')[0].replace(' ', '_')
            tags.append(f"#{simple_name}")
        if metadata.document_type:
            tags.append(f"#type/{metadata.document_type}")
        if metadata.fiscal_year:
            tags.append(f"#year/{metadata.fiscal_year}")
        
        if tags:
            lines.append(f"tags: [{', '.join(tags)}]")
        else:
            lines.append("tags: []")
        lines.append("---")
        lines.append("")
        
        # Title
        title_parts = []
        if metadata.meeting_name:
            title_parts.append(metadata.meeting_name)
        if metadata.round_num:
            title_parts.append(f"第{metadata.round_num}回")
        if metadata.date:
            title_parts.append(metadata.get_formatted_date())
        elif metadata.fiscal_year:
            title_parts.append(f"{metadata.fiscal_year}年度")
        
        lines.append(f"# {' - '.join(title_parts)}")
        lines.append("")
        
        # Metadata section
        lines.append("## 📋 基本情報")
        lines.append("")
        lines.append(f"- **省庁**: {metadata.ministry or 'Unknown'}")
        lines.append(f"- **会議名**: {metadata.meeting_name or 'Unknown'}")
        if metadata.round_num:
            lines.append(f"- **回次**: 第{metadata.round_num}回")
        if metadata.date:
            lines.append(f"- **開催日**: {metadata.get_formatted_date()}")
        if metadata.document_type:
            lines.append(f"- **文書種別**: {metadata.document_type}")
        lines.append(f"- **元ファイル**: [[{pdf_path.name}]]")
        lines.append("")
        
        # Summary section
        lines.append("## 📝 要約")
        lines.append("")
        
        # Handle both string and dict summary formats
        if isinstance(summary, dict):
            if 'summary' in summary:
                lines.append(summary['summary'])
            else:
                lines.append(str(summary))
        else:
            lines.append(str(summary))
        lines.append("")
        
        # Links section
        lines.append("## 🔗 関連リンク")
        lines.append("")
        lines.append(f"- [[{metadata.ministry or 'Unknown'}]]")
        if metadata.meeting_name:
            lines.append(f"- [[{metadata.meeting_name}]]")
        lines.append("")
        
        return '\n'.join(lines)
    
    def process_single_pdf(self, pdf_path: Path) -> bool:
        """Process a single PDF file with enhanced error handling."""
        try:
            # Get metadata from wrapper object
            metadata = pdf_path.metadata
            
            # Update pattern statistics
            if metadata.pattern_used:
                self.stats['by_pattern'][metadata.pattern_used] = \
                    self.stats['by_pattern'].get(metadata.pattern_used, 0) + 1
            
            # Extract text (use actual path)
            text = self.pdf_processor.extract(pdf_path.path)
            if not text.strip():
                logger.warning(f"Empty PDF: {pdf_path.name}")
                self.processed_db.mark_with_metadata(str(pdf_path.path), 'empty', metadata)
                self.stats['skipped'] += 1
                return False
            
            logger.info(f"Processing {pdf_path.name} ({len(text)} chars)")
            
            # Generate summary
            try:
                summary = self.text_summarizer.power_summary(text, nocache=self.args.nocache)
            except Exception as e:
                logger.error(f"Failed to generate summary for {pdf_path.name}: {e}")
                # Use fallback summary
                summary = f"エラー: 要約の生成に失敗しました。\n\n原文の最初の500文字:\n{text[:500]}..."
            
            # Create output directory structure
            if metadata.ministry:
                ministry_dir = VAULT_ROOT / metadata.ministry
                ministry_dir.mkdir(exist_ok=True)
                
                if metadata.meeting_name:
                    out_dir = ministry_dir / metadata.meeting_name
                else:
                    out_dir = ministry_dir / "その他"
            else:
                out_dir = VAULT_ROOT / "分類不明"
            
            out_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate markdown content
            markdown_content = self.generate_enhanced_markdown(summary, metadata, pdf_path.path)
            
            # Generate output filename
            output_filename = generate_output_filename(metadata)
            output_file = out_dir / output_filename
            
            # Handle duplicate filenames
            if output_file.exists():
                base_name = output_file.stem
                counter = 1
                while output_file.exists():
                    output_file = out_dir / f"{base_name}_{counter}.md"
                    counter += 1
            
            # Write markdown file
            output_file.write_text(markdown_content, encoding='utf-8')
            logger.info(f"Created: {output_file.relative_to(VAULT_ROOT)}")
            
            self.processed_db.mark_with_metadata(str(pdf_path.path), 'success', metadata)
            self.stats['processed'] += 1
            return True
            
        except Exception as e:
            logger.error(f"Failed to process {pdf_path.name}: {e}", exc_info=True)
            self.processed_db.mark_with_metadata(str(pdf_path.path), 'error', metadata)
            self.stats['errors'] += 1
            return False
    
    def create_index_files(self):
        """Create index files for the vault."""
        logger.info("Creating index files...")
        
        # Main index
        index_content = [
            "# 政府会議資料データベース",
            "",
            f"最終更新: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "",
            "## 📊 統計情報",
            "",
            f"- **総ファイル数**: {self.stats['total_files']}",
            f"- **処理済み**: {self.stats['processed']}",
            f"- **スキップ**: {self.stats['skipped']}",
            f"- **エラー**: {self.stats['errors']}",
            "",
            "## 🏛️ 省庁別",
            ""
        ]
        
        for ministry, count in sorted(self.stats['by_ministry'].items()):
            index_content.append(f"### [[{ministry}]]")
            index_content.append(f"- ファイル数: {count}")
            index_content.append("")
        
        index_content.extend([
            "## 📑 処理パターン統計",
            ""
        ])
        
        for pattern, count in sorted(self.stats['by_pattern'].items()):
            index_content.append(f"- {pattern}: {count} files")
        
        index_file = VAULT_ROOT / "index.md"
        index_file.write_text('\n'.join(index_content), encoding='utf-8')
        
        # Create ministry index files
        for ministry_dir in VAULT_ROOT.iterdir():
            if ministry_dir.is_dir() and not ministry_dir.name.startswith('.'):
                self._create_ministry_index(ministry_dir)
    
    def _create_ministry_index(self, ministry_dir: Path):
        """Create index file for a ministry."""
        ministry_name = ministry_dir.name
        meetings = {}
        
        # Collect all meetings
        for meeting_dir in ministry_dir.iterdir():
            if meeting_dir.is_dir():
                md_files = list(meeting_dir.glob('*.md'))
                if md_files:
                    meetings[meeting_dir.name] = len(md_files)
        
        # Create index content
        index_content = [
            f"# {ministry_name}",
            "",
            f"会議数: {len(meetings)}",
            f"総ファイル数: {sum(meetings.values())}",
            "",
            "## 会議一覧",
            ""
        ]
        
        for meeting, count in sorted(meetings.items()):
            index_content.append(f"### [[{meeting}]]")
            index_content.append(f"- 資料数: {count}")
            index_content.append("")
        
        index_file = ministry_dir / "index.md"
        index_file.write_text('\n'.join(index_content), encoding='utf-8')
    
    def run(self):
        """Main execution method."""
        # Handle cleanup operations
        self.cleanup_cache()
        self.clear_vault()
        
        # Set up vault structure
        self.setup_vault_structure()
        
        # Find PDFs to process
        pdfs = self.find_pdfs_to_process()
        if not pdfs:
            return
        
        # Handle dry run
        if self.dry_run(pdfs):
            return
        
        # Main processing loop
        bar = tqdm(total=len(pdfs), desc='Processing PDFs')
        
        for pdf in pdfs:
            # Skip if already processed (unless overwrite)
            if not self.args.overwrite and self.processed_db.is_processed(str(pdf.path)):
                self.stats['skipped'] += 1
                bar.update(1)
                continue
            
            # Log progress every 10 files
            if self.stats['processed'] > 0 and self.stats['processed'] % 10 == 0:
                self.monitor.log_status(self.rate_limiter)
            
            # Process the PDF
            self.process_single_pdf(pdf)
            bar.update(1)
        
        bar.close()
        
        # Create index files
        self.create_index_files()
        
        # Final statistics
        logger.info("=" * 60)
        logger.info("Processing complete!")
        logger.info(f"Total files: {self.stats['total_files']}")
        logger.info(f"Processed: {self.stats['processed']}")
        logger.info(f"Skipped: {self.stats['skipped']}")
        logger.info(f"Errors: {self.stats['errors']}")
        logger.info("=" * 60)
        
        # Save final statistics
        stats_file = VAULT_ROOT / 'processing_stats.json'
        stats_file.write_text(
            json.dumps(self.stats, ensure_ascii=False, indent=2)
        )

def create_argument_parser():
    """Create and configure argument parser."""
    parser = argparse.ArgumentParser(
        description='Process government meeting PDFs with enhanced exception handling'
    )
    
    # Processing options
    parser.add_argument('--meeting', help='Filter by meeting name')
    parser.add_argument('--round', type=int, help='Filter by round number')
    parser.add_argument('--ministry', help='Filter by ministry (デジタル庁/こども家庭庁)')
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
        app = EnhancedGovMeetTracker(args)
        app.run()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()