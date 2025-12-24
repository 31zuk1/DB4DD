#!/usr/bin/env python3
"""
DB4DD (Data Base for Digital Democracy) - Session-Based Processing Module
Automated processing of Japanese government meeting documents with AI-powered summarization.
Groups PDFs by meeting session and creates one markdown file per session.
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
from typing import Dict, List

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

class SessionGroup:
    """Represents a group of PDFs from the same meeting session."""
    def __init__(self, session_key: str, session_dir: Path):
        self.session_key = session_key
        self.session_dir = session_dir
        self.pdfs: List = []
        self.metadata: FileMetadata = None
    
    def add_pdf(self, pdf_wrapper):
        """Add a PDF to this session group."""
        self.pdfs.append(pdf_wrapper)
        # Use the first PDF's metadata as the session metadata
        if self.metadata is None:
            self.metadata = pdf_wrapper.metadata
    
    def get_session_name(self) -> str:
        """Get the session name for output filename."""
        if self.metadata:
            parts = []
            if self.metadata.meeting_name:
                parts.append(self.metadata.meeting_name)
            if self.metadata.round_num:
                parts.append(f"第{self.metadata.round_num}回")
            if self.metadata.date:
                formatted_date = self.metadata.get_formatted_date()
                if formatted_date:
                    parts.append(formatted_date)
            return "_".join(parts)
        return self.session_key

class SessionBasedGovMeetTracker:
    """Session-based processing application class."""
    
    def __init__(self, args):
        self.args = args
        
        # Initialize core components
        self.rate_limiter = AdaptiveRateLimiter()
        self.monitor = RequestMonitor()
        self.api_client = APIClient(CACHE_DIR, self.rate_limiter, self.monitor)
        self.pdf_processor = PDFProcessor()
        self.text_summarizer = TextSummarizer(self.api_client, CHUNK_SIZE)
        self.markdown_generator = MarkdownGenerator()
        self.processed_db = EnhancedProcessedDatabase(CACHE_DIR / 'processed_sessions.json')
        self.file_parser = EnhancedFileParser()
        
        # Statistics tracking
        self.stats = {
            'total_sessions': 0,
            'total_pdfs': 0,
            'processed_sessions': 0,
            'processed_pdfs': 0,
            'skipped_sessions': 0,
            'errors': 0,
            'by_ministry': {},
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
    
    def group_pdfs_by_session(self) -> Dict[str, SessionGroup]:
        """Group PDFs by meeting session (folder)."""
        pdfs = find_pdfs_enhanced(
            DATA_ROOT, 
            self.args.meeting, 
            self.args.round,
            self.args.ministry
        )
        
        if not pdfs:
            logger.error(f"No PDFs found in {DATA_ROOT}")
            return {}
        
        sessions = defaultdict(lambda: None)
        
        for pdf_wrapper in pdfs:
            # Get the session directory (parent of the PDF)
            session_dir = pdf_wrapper.path.parent
            session_key = session_dir.name
            
            # Create session group if it doesn't exist
            if sessions[session_key] is None:
                sessions[session_key] = SessionGroup(session_key, session_dir)
            
            # Add PDF to the session
            sessions[session_key].add_pdf(pdf_wrapper)
        
        self.stats['total_sessions'] = len(sessions)
        self.stats['total_pdfs'] = len(pdfs)
        
        # Count by ministry
        for session in sessions.values():
            if session.metadata and session.metadata.ministry:
                ministry = session.metadata.ministry
                self.stats['by_ministry'][ministry] = self.stats['by_ministry'].get(ministry, 0) + 1
        
        logger.info(f"Found {len(sessions)} sessions with {len(pdfs)} PDFs total")
        return dict(sessions)
    
    def dry_run(self, sessions: Dict[str, SessionGroup]):
        """Show what would be processed without actually processing."""
        if self.args.dry_run:
            logger.info("Dry run mode - showing what would be processed:")
            print("\nSessions to process:")
            print("-" * 80)
            
            session_list = list(sessions.values())[:10]  # Show first 10 sessions
            for session in session_list:
                meta = session.metadata
                print(f"📁 {session.session_key}")
                print(f"   Ministry: {meta.ministry or 'Unknown'}")
                print(f"   Meeting: {meta.meeting_name or 'Unknown'}")
                if meta.round_num:
                    print(f"   Round: {meta.round_num}")
                if meta.date:
                    print(f"   Date: {meta.get_formatted_date()}")
                print(f"   PDFs: {len(session.pdfs)} files")
                print()
            
            if len(sessions) > 10:
                print(f"... and {len(sessions) - 10} more sessions")
            
            print("\nStatistics:")
            print("-" * 40)
            print(f"Total sessions: {self.stats['total_sessions']}")
            print(f"Total PDFs: {self.stats['total_pdfs']}")
            for ministry, count in self.stats['by_ministry'].items():
                print(f"{ministry}: {count} sessions")
            
            return True
        return False
    
    def generate_session_markdown(self, session: SessionGroup, all_summaries: List[Dict]) -> str:
        """Generate markdown content for a session with all its PDFs."""
        lines = []
        
        meta = session.metadata
        
        # デジタル庁形式のフロントマター
        lines.append("---")
        
        # date (クォート付き)
        if meta.date:
            lines.append(f"date: '{meta.get_formatted_date()}'")
        
        # meeting (title から変更)
        if meta.meeting_name:
            lines.append(f"meeting: {meta.meeting_name}")
        
        # round (数値のまま)
        if meta.round_num:
            round_val = int(meta.round_num) if meta.round_num.isdigit() else meta.round_num
            lines.append(f"round: {round_val}")
        
        # source_pdf (最初のPDFファイル)
        if session.pdfs:
            lines.append(f"source_pdf: {session.pdfs[0].path.name}")
        
        # status
        lines.append("status: completed")
        
        # tags (配列形式)
        tags = []
        if meta.ministry:
            tags.append(meta.ministry)
        if meta.meeting_name:
            # 会議名から適切なタグを生成
            simple_name = meta.meeting_name.split('_')[0] if '_' in meta.meeting_name else meta.meeting_name
            if simple_name != meta.ministry:
                tags.append(simple_name)
        
        if tags:
            lines.append("tags:")
            for tag in tags:
                lines.append(f"- {tag}")
        
        lines.append("---")
        lines.append("")
        
        # Title
        title_parts = []
        if meta.meeting_name:
            title_parts.append(meta.meeting_name)
        if meta.round_num:
            title_parts.append(f"第{meta.round_num}回")
        if meta.date:
            title_parts.append(meta.get_formatted_date())
        elif meta.fiscal_year:
            title_parts.append(f"{meta.fiscal_year}年度")
        
        lines.append(f"# {' - '.join(title_parts)}")
        lines.append("")
        
        # Basic information
        lines.append("## 📋 基本情報")
        lines.append("")
        lines.append(f"- **省庁**: {meta.ministry or 'Unknown'}")
        lines.append(f"- **会議名**: {meta.meeting_name or 'Unknown'}")
        if meta.round_num:
            lines.append(f"- **回次**: 第{meta.round_num}回")
        if meta.date:
            lines.append(f"- **開催日**: {meta.get_formatted_date()}")
        lines.append(f"- **資料ファイル数**: {len(session.pdfs)}")
        lines.append("")
        
        # Files list
        lines.append("## 📄 資料一覧")
        lines.append("")
        for i, pdf_wrapper in enumerate(session.pdfs, 1):
            lines.append(f"{i}. {pdf_wrapper.path.name}")
        lines.append("")
        
        # Combined summary from all documents
        lines.append("## 📝 統合要約")
        lines.append("")
        
        if all_summaries:
            # Create a combined summary from all individual summaries
            combined_text = "\n\n---\n\n".join([
                str(summary.get('summary', str(summary)) if isinstance(summary, dict) else str(summary))
                for summary in all_summaries
            ])
            lines.append(combined_text)
        else:
            lines.append("要約の生成に失敗しました。")
        lines.append("")
        
        # Document details
        lines.append("## 📑 資料詳細")
        lines.append("")
        
        for i, (pdf_wrapper, summary) in enumerate(zip(session.pdfs, all_summaries), 1):
            lines.append(f"### {i}. {pdf_wrapper.path.name}")
            lines.append("")
            
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
        lines.append(f"- [[{meta.ministry or 'Unknown'}]]")
        if meta.meeting_name:
            lines.append(f"- [[{meta.meeting_name}]]")
        lines.append("")
        
        return '\n'.join(lines)
    
    def process_session(self, session: SessionGroup) -> bool:
        """Process a complete session (all PDFs in the session folder)."""
        try:
            session_name = session.get_session_name()
            logger.info(f"Processing session: {session_name} ({len(session.pdfs)} PDFs)")
            
            all_summaries = []
            
            # Process each PDF in the session
            for pdf_wrapper in session.pdfs:
                try:
                    # Extract text
                    text = self.pdf_processor.extract(pdf_wrapper.path)
                    if not text.strip():
                        logger.warning(f"Empty PDF: {pdf_wrapper.path.name}")
                        all_summaries.append("このファイルは空です。")
                        continue
                    
                    logger.info(f"  - Processing {pdf_wrapper.path.name} ({len(text)} chars)")
                    
                    # Generate summary for this PDF
                    try:
                        summary = self.text_summarizer.power_summary(text, nocache=self.args.nocache)
                        all_summaries.append(summary)
                    except Exception as e:
                        logger.error(f"Failed to generate summary for {pdf_wrapper.path.name}: {e}")
                        fallback_summary = f"エラー: 要約の生成に失敗しました。\n\n原文の最初の500文字:\n{text[:500]}..."
                        all_summaries.append(fallback_summary)
                    
                    self.stats['processed_pdfs'] += 1
                    
                except Exception as e:
                    logger.error(f"Failed to process PDF {pdf_wrapper.path.name}: {e}")
                    all_summaries.append(f"エラー: {pdf_wrapper.path.name}の処理に失敗しました。")
            
            # Create output directory structure
            meta = session.metadata
            if meta.ministry:
                out_dir = VAULT_ROOT / meta.ministry
            else:
                out_dir = VAULT_ROOT / "分類不明"
            
            out_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate markdown content for the entire session
            markdown_content = self.generate_session_markdown(session, all_summaries)
            
            # Generate output filename
            output_filename = f"{session_name}.md"
            output_file = out_dir / output_filename
            
            # Handle duplicate filenames
            if output_file.exists() and not self.args.overwrite:
                base_name = output_file.stem
                counter = 1
                while output_file.exists():
                    output_file = out_dir / f"{base_name}_{counter}.md"
                    counter += 1
            
            # Write markdown file
            output_file.write_text(markdown_content, encoding='utf-8')
            logger.info(f"Created: {output_file.relative_to(VAULT_ROOT)}")
            
            # Mark session as processed
            self.processed_db.mark_with_metadata(session.session_key, 'success', meta)
            self.stats['processed_sessions'] += 1
            return True
            
        except Exception as e:
            logger.error(f"Failed to process session {session.session_key}: {e}", exc_info=True)
            self.processed_db.mark_with_metadata(session.session_key, 'error', session.metadata)
            self.stats['errors'] += 1
            return False
    
    def create_index_files(self):
        """Create index files for the vault."""
        logger.info("Creating index files...")
        
        # Main index
        index_content = [
            "# 政府会議資料データベース（セッション統合版）",
            "",
            f"最終更新: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "",
            "## 📊 統計情報",
            "",
            f"- **総セッション数**: {self.stats['total_sessions']}",
            f"- **総PDFファイル数**: {self.stats['total_pdfs']}",
            f"- **処理済みセッション**: {self.stats['processed_sessions']}",
            f"- **処理済みPDFファイル**: {self.stats['processed_pdfs']}",
            f"- **スキップセッション**: {self.stats['skipped_sessions']}",
            f"- **エラー**: {self.stats['errors']}",
            "",
            "## 🏛️ 省庁別",
            ""
        ]
        
        for ministry, count in sorted(self.stats['by_ministry'].items()):
            index_content.append(f"### [[{ministry}]]")
            index_content.append(f"- セッション数: {count}")
            index_content.append("")
        
        index_file = VAULT_ROOT / "index.md"
        index_file.write_text('\n'.join(index_content), encoding='utf-8')
        
        # Create ministry index files
        for ministry_dir in VAULT_ROOT.iterdir():
            if ministry_dir.is_dir() and not ministry_dir.name.startswith('.'):
                self._create_ministry_index(ministry_dir)
    
    def _create_ministry_index(self, ministry_dir: Path):
        """Create index file for a ministry."""
        ministry_name = ministry_dir.name
        md_files = list(ministry_dir.glob('*.md'))
        
        # Create index content
        index_content = [
            f"# {ministry_name}",
            "",
            f"会議セッション数: {len(md_files)}",
            "",
            "## 会議セッション一覧",
            ""
        ]
        
        for md_file in sorted(md_files):
            session_name = md_file.stem
            index_content.append(f"- [[{session_name}]]")
        
        index_file = ministry_dir / "index.md"
        index_file.write_text('\n'.join(index_content), encoding='utf-8')
    
    def run(self):
        """Main execution method."""
        # Handle cleanup operations
        self.cleanup_cache()
        self.clear_vault()
        
        # Set up vault structure
        self.setup_vault_structure()
        
        # Group PDFs by session
        sessions = self.group_pdfs_by_session()
        if not sessions:
            return
        
        # Handle dry run
        if self.dry_run(sessions):
            return
        
        # Main processing loop
        bar = tqdm(total=len(sessions), desc='Processing Sessions')
        
        for session_key, session in sessions.items():
            # Skip if already processed (unless overwrite)
            if not self.args.overwrite and self.processed_db.is_processed(session_key):
                self.stats['skipped_sessions'] += 1
                bar.update(1)
                continue
            
            # Log progress every 5 sessions
            if self.stats['processed_sessions'] > 0 and self.stats['processed_sessions'] % 5 == 0:
                self.monitor.log_status(self.rate_limiter)
            
            # Process the session
            self.process_session(session)
            bar.update(1)
        
        bar.close()
        
        # Create index files
        self.create_index_files()
        
        # Final statistics
        logger.info("=" * 60)
        logger.info("Processing complete!")
        logger.info(f"Total sessions: {self.stats['total_sessions']}")
        logger.info(f"Total PDFs: {self.stats['total_pdfs']}")
        logger.info(f"Processed sessions: {self.stats['processed_sessions']}")
        logger.info(f"Processed PDFs: {self.stats['processed_pdfs']}")
        logger.info(f"Skipped sessions: {self.stats['skipped_sessions']}")
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
        description='Process government meeting PDFs with session-based approach'
    )
    
    # Processing options
    parser.add_argument('--meeting', help='Filter by meeting name')
    parser.add_argument('--round', type=int, help='Filter by round number')
    parser.add_argument('--ministry', help='Filter by ministry (デジタル庁/こども家庭庁)')
    parser.add_argument('--overwrite', action='store_true', help='Reprocess existing sessions')
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
        app = SessionBasedGovMeetTracker(args)
        app.run()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()