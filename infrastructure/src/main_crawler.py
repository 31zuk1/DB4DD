#!/usr/bin/env python3
"""
Crawler Entry Point
Runs the crawler engine with infrastructure-aware configuration.
"""

import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

# Setup path to include src/
ROOT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT_DIR))

# Load environment
load_dotenv(ROOT_DIR / '.env')

from crawler import CrawlerEngine

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%H:%M:%S"
)

def main():
    # Define paths relative to infrastructure/
    data_dir = ROOT_DIR / 'data'
    output_dir = data_dir / 'raw' / 'crawler_downloads'
    state_file = data_dir / 'crawler_state.json'
    
    # Initialize and run
    crawler = CrawlerEngine(
        output_base_dir=output_dir,
        state_file=state_file
    )
    crawler.run()

if __name__ == "__main__":
    main()
