#!/usr/bin/env python3
"""
Fast text cache processor with parallel processing for multiple sessions.
"""
import sys
import os
from pathlib import Path
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
from tqdm import tqdm

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from main_from_text_cache import TextCacheProcessor, TextCacheSession

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)

class FastTextCacheProcessor(TextCacheProcessor):
    """Fast version with parallel session processing."""
    
    def process_all_sessions(self, sessions: dict) -> None:
        """Process all sessions with parallel execution."""
        session_list = list(sessions.values())
        
        if not session_list:
            logger.info("No sessions to process")
            return
        
        logger.info(f"Found {len(session_list)} sessions to process")
        
        # Process sessions in parallel (limit to 3 to avoid API rate limits)
        max_parallel = min(3, len(session_list))
        
        with ThreadPoolExecutor(max_workers=max_parallel) as executor:
            # Submit all tasks
            future_to_session = {
                executor.submit(self.process_session, session): session
                for session in session_list
            }
            
            # Track progress
            with tqdm(total=len(session_list), desc="Processing sessions") as pbar:
                for future in as_completed(future_to_session):
                    session = future_to_session[future]
                    try:
                        success = future.result(timeout=300)  # 5 minute timeout per session
                        if success:
                            logger.info(f"✅ Completed: {session.session_key}")
                        else:
                            logger.error(f"❌ Failed: {session.session_key}")
                    except Exception as e:
                        logger.error(f"❌ Error processing {session.session_key}: {e}")
                    finally:
                        pbar.update(1)

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Fast Text Cache Processor')
    parser.add_argument('--ministry', type=str, help='Filter by specific ministry')
    parser.add_argument('--meeting', type=str, help='Filter by specific meeting name')
    parser.add_argument('--overwrite', action='store_true', help='Overwrite existing files')
    parser.add_argument('--parallel', type=int, default=3, help='Number of parallel sessions to process')
    args = parser.parse_args()
    
    processor = FastTextCacheProcessor(args)
    
    # Discover sessions using parent class method
    sessions = processor.discover_sessions_from_text_cache()
    
    if args.ministry:
        sessions = {k: v for k, v in sessions.items() if args.ministry in k}
        logger.info(f"Filtered to {len(sessions)} sessions for ministry: {args.ministry}")
    
    if args.meeting:
        sessions = {k: v for k, v in sessions.items() if args.meeting in k}
        logger.info(f"Filtered to {len(sessions)} sessions for meeting: {args.meeting}")
    
    # Process all sessions
    processor.process_all_sessions(sessions)
    
    logger.info("All processing complete!")

if __name__ == '__main__':
    main()