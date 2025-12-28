
import os
import sys
import re
import asyncio
import logging
import shutil
import fitz  # PyMuPDF
from pathlib import Path
from typing import Optional, Tuple
from pydantic import BaseModel, Field
from dotenv import load_dotenv
import csv

# Ensure src is in pythonpath
sys.path.append(str(Path(__file__).parent))

# Load environment variables
load_dotenv(Path(__file__).parent / '.env')

from core.api_client import APIClient
from core.rate_limiter import AdaptiveRateLimiter, RequestMonitor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("organize_pdfs.log")
    ]
)
logger = logging.getLogger(__name__)

# --- Configuration ---
PROJECT_INFRA_ROOT = Path(__file__).resolve().parent.parent
SOURCE_DIR = PROJECT_INFRA_ROOT / "data/raw/crawler_downloads/master_raw"
TARGET_DIR = PROJECT_INFRA_ROOT / "data/input/crawled"
CACHE_DIR = PROJECT_INFRA_ROOT / "vaults/.cache"
CSV_LOG_PATH = PROJECT_INFRA_ROOT / "src/rename_map.csv"

# --- Pydantic Model for Extraction ---
class MeetingMetadata(BaseModel):
    meeting_name: str = Field(..., description="正式な会議名。例: 'デジタル社会推進会議'。スペースは含めないでください。")
    round_number: Optional[int] = Field(None, description="開催回数。例: '第5回'なら 5。不明な場合はNone。")
    document_name: str = Field(..., description="資料名。例: '議事次第', '資料1', '参考資料3'。")

# --- Helper Functions ---

def extract_date_from_filename(filename: str) -> Optional[str]:
    """Extract YYYYMMDD from crawler filename (hash_YYYYMMDD_slug.pdf)."""
    match = re.search(r'_(\d{8})_', filename)
    if match:
        return match.group(1)
    return None

def read_first_pages(pdf_path: Path, max_pages: int = 2) -> str:
    """Read first N pages of PDF."""
    try:
        doc = fitz.open(pdf_path)
        text = ""
        for i in range(min(max_pages, len(doc))):
            text += doc[i].get_text() + "\n"
        doc.close()
        return text[:4000] # Limit context size
    except Exception as e:
        logger.error(f"Failed to read PDF {pdf_path}: {e}")
        return ""

async def process_file(pdf_path: Path, api_client: APIClient, semaphore: asyncio.Semaphore, csv_writer: object, dry_run: bool = False):
    """Process a single PDF file."""
    async with semaphore:
        try:
            filename = pdf_path.name
            
            # 1. Check existing format
            # If it already matches {Meeting}_第{N}回_{Date}_... skip it
            if "第" in filename and "回" in filename and extract_date_from_filename(filename):
                logger.info(f"Skipping already organized file: {filename}")
                return

            # 2. Extract Date from filename
            date = extract_date_from_filename(filename)
            if not date:
                logger.warning(f"Could not extract date from filename: {filename}")
                return # Cannot proceed without date

            # 3. Read Content
            text = read_first_pages(pdf_path)
            if not text.strip():
                logger.warning(f"Empty text in PDF: {filename}")
                return

            # 4. LLM Extraction
            messages = [
                {"role": "system", "content": "You are a helper that extracts meeting metadata from Japanese government documents."},
                {"role": "user", "content": f"以下のテキストは会議資料の冒頭です。会議名、開催回数、資料名を抽出してください。\n\nfilename: {filename}\ntext:\n{text}"}
            ]
            
            try:
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(
                    None, 
                    lambda: api_client.structured_chat(messages, MeetingMetadata)
                )
            except Exception as e:
                logger.error(f"LLM Error for {filename}: {e}")
                return

            # 5. Construct New Filename
            safe_meeting = re.sub(r'[\\/:*?"<>|]', '', result.meeting_name).strip()
            safe_doc = re.sub(r'[\\/:*?"<>|]', '', result.document_name).strip()
            
            round_str = f"第{result.round_number:02d}回" if result.round_number else "回数不明"
            
            new_filename = f"{safe_meeting}_{round_str}_{date}_{safe_doc}.pdf"
            
            logger.info(f"Proposed Name: {filename} -> {new_filename}")
            
            if csv_writer:
                csv_writer.writerow([filename, new_filename, result.meeting_name, result.round_number, result.document_name])

            if dry_run:
                return

            # 6. Move/Copy
            # Create subdirectory for round
            round_dir_name = f"第{result.round_number:02d}回" if result.round_number else "回数不明"
            dest_dir = TARGET_DIR / safe_meeting / round_dir_name
            dest_dir.mkdir(parents=True, exist_ok=True)
            dest_path = dest_dir / new_filename
            
            # Handle duplicate dest path
            if dest_path.exists():
                stem = dest_path.stem
                new_filename = f"{stem}_{filename[:8]}.pdf"
                dest_path = dest_dir / new_filename
            
            shutil.copy2(pdf_path, dest_path)
            logger.info(f"Copied to: {dest_path}")

        except Exception as e:
            logger.error(f"Failed to process {pdf_path}: {e}")

async def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0, help="Limit number of files to process")
    parser.add_argument("--dry-run", action="store_true", help="Do not move files, just log")
    args = parser.parse_args()

    # Setup
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    if not args.dry_run:
        TARGET_DIR.mkdir(parents=True, exist_ok=True)
    
    # Init API Client
    rate_limiter = AdaptiveRateLimiter()
    monitor = RequestMonitor()
    client = APIClient(CACHE_DIR, rate_limiter, monitor)
    
    # Find files
    files = list(SOURCE_DIR.glob("*.pdf"))
    logger.info(f"Found {len(files)} PDFs in {SOURCE_DIR}")
    
    if args.limit > 0:
        files = files[:args.limit]
        logger.info(f"Limiting to first {args.limit} files")

    if not files:
        logger.warning("No files found to organize.")
        return

    # Semaphore for concurrency
    sem = asyncio.Semaphore(10)
    
    if args.dry_run:
        logger.info("DRY RUN: No files will be moved.")
    
    # Init CSV
    csv_file = None
    csv_writer = None
    if not args.dry_run:
        file_exists = CSV_LOG_PATH.exists()
        csv_file = open(CSV_LOG_PATH, mode='a', newline='', encoding='utf-8')
        csv_writer = csv.writer(csv_file)
        if not file_exists:
            csv_writer.writerow(['Original Filename', 'New Filename', 'Meeting Name', 'Round', 'Document Name'])

    try:
        tasks = [process_file(f, client, sem, csv_writer, args.dry_run) for f in files]
        logger.info("Starting organization...")
        await asyncio.gather(*tasks)
    finally:
        if csv_file:
            csv_file.close()

    logger.info("Organization complete.")

if __name__ == "__main__":
    asyncio.run(main())
