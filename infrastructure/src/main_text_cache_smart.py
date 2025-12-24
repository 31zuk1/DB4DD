#!/usr/bin/env python3
"""
Smart processor with size-based skipping and logging.
"""
import sys
import os
from pathlib import Path
import json
from datetime import datetime
sys.path.insert(0, str(Path(__file__).parent))

from main_from_text_cache import TextCacheProcessor
import logging

logger = logging.getLogger(__name__)

class SmartProcessor(TextCacheProcessor):
    """Process with intelligent size handling."""
    
    def __init__(self, args):
        super().__init__(args)
        self.threshold_kb = int(os.environ.get('SIZE_THRESHOLD_KB', '500'))
        self.threshold_bytes = self.threshold_kb * 1024
        self.skipped_sessions = []
        self.skip_log_file = Path('skipped_huge_sessions.log')
        
    def process_session(self, session):
        """Process only if not too large."""
        # Calculate total size
        total_size = sum(f.stat().st_size for f in session.text_files)
        total_chars = 0
        
        # Skip if too large
        if total_size > self.threshold_bytes:
            # Count characters for logging
            for f in session.text_files[:3]:  # Sample first 3 files
                try:
                    with open(f, 'r', encoding='utf-8') as file:
                        total_chars += len(file.read())
                except:
                    pass
            
            skip_info = {
                'timestamp': datetime.now().isoformat(),
                'session': session.session_key,
                'files': len(session.text_files),
                'size_kb': total_size / 1024,
                'estimated_chars': total_chars * (len(session.text_files) / 3) if total_chars > 0 else 0,
                'reason': f'Size {total_size/1024:.1f}KB exceeds threshold {self.threshold_kb}KB'
            }
            
            self.skipped_sessions.append(skip_info)
            
            # Log to file
            with open(self.skip_log_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(skip_info, ensure_ascii=False) + '\n')
            
            logger.warning(f"⚠️ SKIPPED (too large): {session.session_key} - {total_size/1024:.1f}KB > {self.threshold_kb}KB")
            
            # Create placeholder file
            ministry = session.session_key.split('/')[0]
            output_dir = Path('DB/DB_0823') / ministry
            output_dir.mkdir(parents=True, exist_ok=True)
            
            placeholder_file = output_dir / f"{session.get_session_name()}_SKIPPED_TOO_LARGE.txt"
            with open(placeholder_file, 'w', encoding='utf-8') as f:
                f.write(f"Session skipped due to size: {total_size/1024:.1f}KB\n")
                f.write(f"Threshold: {self.threshold_kb}KB\n")
                f.write(f"Files: {len(session.text_files)}\n")
                f.write(f"Session: {session.session_key}\n")
                f.write(f"To process this file, run with higher threshold:\n")
                f.write(f"SIZE_THRESHOLD_KB=1000 python3 src/main_from_text_cache.py --meeting '{session.meeting_name}'\n")
            
            return True  # Mark as "processed" (skipped)
        
        # Process normally
        return super().process_session(session)
    
    def report_skipped(self):
        """Report all skipped sessions."""
        if self.skipped_sessions:
            print("\n" + "="*70)
            print(f"スキップされたセッション: {len(self.skipped_sessions)}個")
            print("="*70)
            for skip in self.skipped_sessions:
                print(f"- {skip['session']}: {skip['size_kb']:.1f}KB")
            print(f"\n詳細は {self.skip_log_file} を参照")
            
            # Save summary
            summary_file = Path('skipped_sessions_summary.json')
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump(self.skipped_sessions, f, ensure_ascii=False, indent=2)
            print(f"サマリーを {summary_file} に保存")

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--overwrite', action='store_true')
    parser.add_argument('--ministry', type=str, help='Filter by ministry')
    parser.add_argument('--meeting', type=str, help='Filter by meeting')
    args = parser.parse_args()
    
    print(f"処理開始 - 閾値: {os.environ.get('SIZE_THRESHOLD_KB', '500')}KB")
    
    processor = SmartProcessor(args)
    sessions = processor.find_text_sessions()
    
    # Apply filters
    if args.ministry:
        sessions = {k: v for k, v in sessions.items() if args.ministry in k}
    if args.meeting:
        sessions = {k: v for k, v in sessions.items() if args.meeting in k}
    
    # Process each session
    from tqdm import tqdm
    logger.info(f"Found {len(sessions)} sessions to process")
    
    with tqdm(total=len(sessions), desc="Processing sessions") as pbar:
        for session_key, session in sessions.items():
            try:
                processor.process_session(session)
            except Exception as e:
                logger.error(f"Failed to process {session_key}: {e}")
            pbar.update(1)
    
    processor.report_skipped()