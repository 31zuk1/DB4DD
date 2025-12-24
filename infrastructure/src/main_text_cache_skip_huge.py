#!/usr/bin/env python3
"""
Skip huge files version - Process only normal sized sessions.
"""
import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from main_from_text_cache import TextCacheProcessor
import logging

logger = logging.getLogger(__name__)

class SkipHugeProcessor(TextCacheProcessor):
    """Skip sessions larger than threshold."""
    
    def process_session(self, session):
        """Process only if not too large."""
        # Calculate total size
        total_size = sum(f.stat().st_size for f in session.text_files)
        
        # Skip if too large (>500KB)
        if total_size > 500000:
            logger.warning(f"⚠️ Skipping huge session ({total_size/1000:.0f}KB): {session.session_key}")
            return True  # Return True to mark as "processed" (skipped)
        
        # Process normally
        return super().process_session(session)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--overwrite', action='store_true')
    args = parser.parse_args()
    
    processor = SkipHugeProcessor(args)
    sessions = processor.discover_sessions_from_text_cache()
    
    # Filter out already processed
    existing = set(f.stem for f in Path('DB/DB_0823').glob('*/*.md'))
    sessions_to_process = {k: v for k, v in sessions.items() 
                          if v.get_session_name() not in existing}
    
    logger.info(f"スキップ済み: {len(existing)}, 処理対象: {len(sessions_to_process)}")
    
    processor.process_all_sessions(sessions_to_process)