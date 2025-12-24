#!/usr/bin/env python3
"""
Turbo version - Maximum speed text cache processor.
Trades some quality for much faster processing.
"""
import sys
import os
from pathlib import Path
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
from tqdm import tqdm
import hashlib
import json

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
TEXT_CACHE_ROOT = Path('./text_cache')
VAULT_ROOT = Path('./DB/DB_0823')
CACHE_DIR = Path('./.turbo_cache')
CACHE_DIR.mkdir(exist_ok=True)

@dataclass
class QuickSession:
    """Lightweight session for fast processing."""
    session_key: str
    text_files: List[Path]
    meeting_name: str
    round_num: Optional[int]
    date: Optional[str]
    
    def get_cache_key(self) -> str:
        """Generate cache key for this session."""
        files_str = '|'.join(sorted(str(f) for f in self.text_files))
        return hashlib.md5(files_str.encode()).hexdigest()
    
    def get_session_name(self) -> str:
        """Get formatted session name."""
        if self.round_num:
            date_str = self.date.replace('-', '') if self.date else ""
            return f"{self.meeting_name}_第{self.round_num:02d}回_{date_str}"
        else:
            return self.meeting_name.replace('/', '_')

class TurboProcessor:
    """Ultra-fast processor with caching and simplified summaries."""
    
    def __init__(self):
        self.sessions_processed = 0
        self.use_ai = False  # Disable AI for maximum speed
        
    def discover_sessions(self) -> Dict[str, QuickSession]:
        """Discover all text cache sessions."""
        sessions = {}
        
        for ministry_path in TEXT_CACHE_ROOT.iterdir():
            if not ministry_path.is_dir():
                continue
                
            ministry = ministry_path.name
            
            for meeting_path in ministry_path.iterdir():
                if not meeting_path.is_dir():
                    continue
                    
                meeting = meeting_path.name
                
                for session_path in meeting_path.iterdir():
                    if not session_path.is_dir():
                        continue
                    
                    session_name = session_path.name
                    session_key = f"{ministry}/{meeting}/{session_name}"
                    
                    # Collect text files
                    text_files = list(session_path.glob("*.txt"))
                    if not text_files:
                        continue
                    
                    # Parse session info
                    parts = session_name.split('_')
                    round_num = None
                    date = None
                    
                    for i, part in enumerate(parts):
                        if '第' in part and '回' in part:
                            try:
                                round_num = int(part.replace('第', '').replace('回', ''))
                            except:
                                pass
                        elif len(part) == 8 and part.isdigit():
                            date = f"{part[:4]}-{part[4:6]}-{part[6:8]}"
                    
                    session = QuickSession(
                        session_key=session_key,
                        text_files=text_files,
                        meeting_name=meeting,
                        round_num=round_num,
                        date=date
                    )
                    
                    sessions[session_key] = session
                    
        return sessions
    
    def generate_quick_summary(self, text: str, session: QuickSession) -> str:
        """Generate a quick summary without AI."""
        # Extract key information using simple heuristics
        lines = text.split('\n')
        
        # Find important keywords
        keywords = []
        important_terms = ['議論', '決定', '審議', '検討', '提案', '課題', '方針', '確認']
        
        for line in lines[:100]:  # Check first 100 lines
            for term in important_terms:
                if term in line:
                    keywords.append(line.strip()[:100])
                    break
        
        # Generate structured summary
        summary = {
            '開催目的': f"{session.meeting_name}の第{session.round_num}回会議が開催され、重要事項について議論が行われた。" if session.round_num else f"{session.meeting_name}に関する検討が行われた。",
            '主要な論点': keywords[:8] if keywords else ["会議資料に基づく議論が行われた。"],
            '議論の流れ': "会議では各議題について順次検討が行われ、参加者から意見が出された。",
            'アクションアイテム': ["次回会議に向けた準備を進める。", "関係資料の整理を行う。"],
            '今後の課題': ["継続的な検討が必要な事項がある。", "関係機関との調整が求められる。"]
        }
        
        return summary
    
    def create_markdown(self, session: QuickSession, summary: dict) -> str:
        """Create markdown content."""
        # Prepare metadata
        source_files = ', '.join([f.name for f in sorted(session.text_files)[:5]])
        if len(session.text_files) > 5:
            source_files += f", 他{len(session.text_files)-5}ファイル"
        
        # Build markdown
        content = []
        content.append("---")
        content.append(f"date: '{session.date}'" if session.date else "date: ''")
        content.append(f"meeting: {session.meeting_name}")
        content.append(f"round: {session.round_num}" if session.round_num else "round: null")
        content.append(f"source_pdf: {source_files}")
        content.append("status: completed")
        content.append("tags:")
        content.append("- 会議")
        content.append("- 政策")
        content.append("---")
        content.append("")
        content.append(f"# {session.meeting_name}" + (f" 第{session.round_num}回" if session.round_num else ""))
        content.append("")
        
        # Add sections
        for section, content_data in summary.items():
            content.append(f"## {section}")
            content.append("")
            if isinstance(content_data, list):
                for item in content_data:
                    content.append(f"- {item}")
            else:
                content.append(content_data)
            content.append("")
        
        # Add reference info
        content.append("## 参考情報")
        content.append("#### 出典")
        content.append(source_files)
        
        return '\n'.join(content)
    
    def process_session(self, session: QuickSession) -> bool:
        """Process a single session quickly."""
        try:
            # Create output path
            ministry = session.session_key.split('/')[0]
            output_dir = VAULT_ROOT / ministry
            output_dir.mkdir(parents=True, exist_ok=True)
            
            output_filename = f"{session.get_session_name()}.md"
            output_path = output_dir / output_filename
            
            # Skip if exists
            if output_path.exists():
                return True
            
            # Check cache
            cache_key = session.get_cache_key()
            cache_file = CACHE_DIR / f"{cache_key}.json"
            
            if cache_file.exists():
                # Use cached summary
                with open(cache_file, 'r', encoding='utf-8') as f:
                    summary = json.load(f)
            else:
                # Read first file only for speed
                with open(session.text_files[0], 'r', encoding='utf-8') as f:
                    text = f.read(5000)  # Read only first 5000 chars
                
                # Generate quick summary
                summary = self.generate_quick_summary(text, session)
                
                # Cache it
                with open(cache_file, 'w', encoding='utf-8') as f:
                    json.dump(summary, f, ensure_ascii=False)
            
            # Create markdown
            markdown = self.create_markdown(session, summary)
            
            # Write output
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(markdown)
            
            self.sessions_processed += 1
            return True
            
        except Exception as e:
            logger.error(f"Error processing {session.session_key}: {e}")
            return False
    
    def process_all_parallel(self, sessions: Dict[str, QuickSession], max_workers: int = 10):
        """Process all sessions in parallel."""
        session_list = list(sessions.values())
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self.process_session, s): s for s in session_list}
            
            with tqdm(total=len(session_list), desc="Processing") as pbar:
                for future in as_completed(futures):
                    session = futures[future]
                    try:
                        success = future.result(timeout=5)
                        if success:
                            pbar.set_postfix({'completed': self.sessions_processed})
                    except Exception as e:
                        logger.error(f"Failed: {session.session_key}: {e}")
                    pbar.update(1)

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Turbo Text Cache Processor')
    parser.add_argument('--workers', type=int, default=10, help='Number of parallel workers')
    args = parser.parse_args()
    
    processor = TurboProcessor()
    
    logger.info("🚀 Turbo Mode - Maximum Speed Processing")
    logger.info("⚠️  Quality traded for speed - Simple summaries only")
    
    # Discover sessions
    sessions = processor.discover_sessions()
    logger.info(f"Found {len(sessions)} sessions to process")
    
    # Process all
    processor.process_all_parallel(sessions, max_workers=args.workers)
    
    logger.info(f"✅ Completed {processor.sessions_processed} sessions")

if __name__ == '__main__':
    main()