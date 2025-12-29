import asyncio
import os
import shutil
import logging
from pathlib import Path
import sys
from dotenv import load_dotenv

# Add src to path to import core modules
sys.path.append(str(Path(__file__).resolve().parent.parent))

from core.api_client import APIClient
from core.rate_limiter import AdaptiveRateLimiter, RequestMonitor
from organize_pdfs import MeetingMetadata, read_first_pages, extract_date_from_filename
import unicodedata
import re
import difflib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("rescue_operation.log")
    ]
)
logger = logging.getLogger(__name__)

# Config
load_dotenv(Path(__file__).resolve().parent.parent / '.env')
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent # infrastructure/src/tools -> infrastructure/src -> infrastructure -> Project -> Root? No.
# __file__ = infrastructure/src/tools/rescue_generic_folders.py
# parent = infrastructure/src/tools
# parent.parent = infrastructure/src
# parent.parent.parent = infrastructure
# parent.parent.parent.parent = DB4DD (Project Root)

# Actually, let's just use relative to cwd if we run from project root, or be careful.
# If cwd is DB4DD:
TARGET_DIR = Path("data/input/crawled/デジタル庁")
CACHE_DIR = Path("vaults/.cache")

# List of suffixes or exact names to rescue FROM
# We will check if folder name ENDS WITH these or IS one of these
GENERIC_CANDIDATES = [
    "有識者会議",
    "作業部会",
    "中間とりまとめ",
    "本検討会",
    "報告書",
    "リスト",
    "第9回作業部会"
]

async def rescue_file(pdf_path: Path, api_client: APIClient, semaphore: asyncio.Semaphore):
    async with semaphore:
        try:
            logger.info(f"Rescuing: {pdf_path.name}")
            
            # 1. Read Content
            text = read_first_pages(pdf_path)
            if not text.strip():
                logger.warning(f"Empty text, skipping: {pdf_path.name}")
                return

            # 2. LLM Re-Extraction
            messages = [
                {"role": "system", "content": "You are a helper that extracts meeting metadata from Japanese government documents. The current file is misclassified in a generic folder. Extract the SPECIFIC meeting name exactly as written. Do not use generic names like '有識者会議'. If the document title is '第X回 〇〇検討会', valid meeting name is '〇〇検討会'."},
                {"role": "user", "content": f"以下のテキストは会議資料の冒頭です。具体的で正式な会議名を抽出してください。\n\nfilename: {pdf_path.name}\ntext:\n{text}"}
            ]
            
            try:
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(
                    None, 
                    lambda: api_client.structured_chat(messages, MeetingMetadata)
                )
            except Exception as e:
                logger.error(f"LLM Error for {pdf_path.name}: {e}")
                return

            # 3. Construct New Path (Reuse organize_pdfs logic)
            normalized_meeting = unicodedata.normalize('NFKC', result.meeting_name)
            safe_meeting = re.sub(r'[\\/:*?"<>|]', '', normalized_meeting).strip()
            
            # Fuzzy match check
            existing_folders = [d.name for d in TARGET_DIR.iterdir() if d.is_dir()]
            matches = difflib.get_close_matches(safe_meeting, existing_folders, n=1, cutoff=0.9)
            if matches:
                 logger.info(f"  Mapped '{safe_meeting}' -> Existing '{matches[0]}'")
                 safe_meeting = matches[0]
            
            round_str = f"第{result.round_number:02d}回" if result.round_number else "回数不明"
            
            # We don't verify if new meeting name is still generic here (trusting prompt), 
            # but if it maps to the SAME generic folder, we should skip to avoid loop
            current_parent_name = pdf_path.parent.parent.name # input/crawled/Digital/Generic/Round/File
            if safe_meeting == current_parent_name:
                logger.warning(f"  LLM returned same generic name '{safe_meeting}', skipping.")
                return

            dest_dir = TARGET_DIR / safe_meeting / round_str
            dest_dir.mkdir(parents=True, exist_ok=True)
            
            # Rename file? The filename usually has generic name too?
            # Let's regenerate filename to be safe
            date = extract_date_from_filename(pdf_path.name)
            if not date: date = "00000000"
            
            normalized_doc = unicodedata.normalize('NFKC', result.document_name)
            safe_doc = re.sub(r'[\\/:*?"<>|]', '', normalized_doc).strip()
            new_filename = f"{safe_meeting}_{round_str}_{date}_{safe_doc}.pdf"
            
            dest_path = dest_dir / new_filename
            
            if dest_path.exists():
                new_filename = f"{dest_path.stem}_rescued.pdf"
                dest_path = dest_dir / new_filename
                
            shutil.move(str(pdf_path), str(dest_path))
            logger.info(f"  Moved to: {safe_meeting}/{round_str}/")

        except Exception as e:
            logger.error(f"Failed to rescue {pdf_path}: {e}")

async def main():
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    rate_limiter = AdaptiveRateLimiter()
    monitor = RequestMonitor()
    client = APIClient(CACHE_DIR, rate_limiter, monitor)
    
    sem = asyncio.Semaphore(5)
    tasks = []

    # Iterate over ALL top-level directories and check if they match candidates
    for folder in TARGET_DIR.iterdir():
        if not folder.is_dir():
            continue
            
        # Match condition: Exact match or Ends With (for things like 'XXXX報告書')
        is_candidate = False
        if folder.name in GENERIC_CANDIDATES:
            is_candidate = True
        
        # Also rescue from "Xについて" if we want? Maybe too aggressive.
        # Let's stick to the list for now but allow "contains" if needed.
        
        if is_candidate:
            logger.info(f"Scanning generic folder: {folder.name}")
            # Recursive find all PDFs
            pdfs = list(folder.rglob("*.pdf"))
            
            for pdf in pdfs:
                tasks.append(rescue_file(pdf, client, sem))
            
    if not tasks:
        logger.info("No files found in generic folders.")
        return

    logger.info(f"Starting rescue of {len(tasks)} files...")
    await asyncio.gather(*tasks)
    
    # Cleanup empty generic folders
    for folder in TARGET_DIR.iterdir():
        if not folder.is_dir(): continue
        if folder.name in GENERIC_CANDIDATES:
             # Check emptiness
             if not any(folder.rglob("*")):
                 try:
                     shutil.rmtree(folder)
                     logger.info(f"Removed empty generic folder: {folder.name}")
                 except: pass

if __name__ == "__main__":
    asyncio.run(main())
