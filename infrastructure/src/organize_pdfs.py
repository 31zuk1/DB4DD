
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
import yaml
import difflib
import unicodedata

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
        logging.FileHandler(PROJECT_INFRA_ROOT.parent / "logs/organize_pdfs.log")
    ]
)
logger = logging.getLogger(__name__)

# --- Configuration ---
PROJECT_INFRA_ROOT = Path(__file__).resolve().parent.parent
REPO_ROOT = PROJECT_INFRA_ROOT.parent
SOURCE_DIR = REPO_ROOT / "data/raw/crawler_downloads/master_raw"
TARGET_DIR = REPO_ROOT / "data/input/crawled/デジタル庁"
CACHE_DIR = REPO_ROOT / "vaults/.cache"
CSV_LOG_PATH = REPO_ROOT / "logs/rename_map.csv"

# --- Pydantic Model for Extraction ---
class MeetingMetadata(BaseModel):
    # We use a dual-field strategy for classification
    existing_match: Optional[str] = Field(None, description="候補リストの中に、この文書の会議に該当するものがあれば、その『正式名称』をそのまま出力してください。（表記ゆらぎや略称も、意味的に同じなら候補を選んでください）")
    new_proposal: Optional[str] = Field(None, description="候補リストに該当がない場合のみ、文書から抽出した新しい会議名を出力してください。")
    
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

# Global lock for Master List updates
master_list_lock = asyncio.Lock()

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

            # 4. Load Master List for Prompt
            MASTER_LIST_PATH = PROJECT_INFRA_ROOT / "config/master_meetings.yaml"
            candidate_list_str = "（マスタ無し）"
            current_meetings = []
            
            # NOTE: We read blindly here (no lock) for performance, assuming reads are safe enough for prompt generation.
            # Even if slightly stale, the LLM will just use what it sees.
            if MASTER_LIST_PATH.exists():
                try:
                    with open(MASTER_LIST_PATH, 'r') as f:
                        data = yaml.safe_load(f)
                        current_meetings = data.get("meetings", [])
                        candidate_list_str = "\n".join([f"- {m}" for m in current_meetings])
                except Exception as e:
                    logger.warning(f"Failed to load master list: {e}")

            # 5. LLM Classification & Extraction
            system_prompt = f"""
You are a government document classifier.
以下の「候補リスト」から、文書が該当する会議名を選んでください。

# 候補リスト:
{candidate_list_str}

# 指示:
1. 文書内の会議名が「候補リスト」にあるものと意味的に一致する場合（略称、表記ゆらぎ含む）は、リスト内の名称を `existing_match` に出力してください。
   例: 文書「モビリティWG」 -> リスト「モビリティワーキンググループ」を選択。
2. リストに全く該当しない場合のみ、`new_proposal` に抽出した名前を出力してください。
   * 重要: 文書が「別表」「リスト」「添付資料」単体であり、具体的な会議体名（「〇〇検討会」等）が明記されていない場合は、無理に捏造せず、必ず `会議名不明` と出力してください。
"""
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"filename: {filename}\ntext:\n{text}"}
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

            # 6. Determine Final Meeting Name
            import unicodedata
            import difflib
            
            final_meeting_name = None
            is_new_discovery = False

            if result.existing_match:
                # LLM Selected an existing meeting
                # Double check validity (simple exact string match)
                if result.existing_match in current_meetings:
                     final_meeting_name = result.existing_match
                     logger.info(f"LLM MATCH: {filename} -> {final_meeting_name}")
                else:
                     # LLM hallucinated a name not in list? Fallback to fuzzy or raw.
                     logger.warning(f"LLM Hallucination? '{result.existing_match}' not in provided list.")
                     final_meeting_name = result.existing_match 
            
            elif result.new_proposal:
                # New meeting proposal
                normalized = unicodedata.normalize('NFKC', result.new_proposal)
                final_meeting_name = re.sub(r'[\\/:*?"<>|]', '', normalized).strip()
                is_new_discovery = True
                logger.info(f"LLM NEW PROPOSAL: {filename} -> {final_meeting_name}")

            else:
                 logger.warning(f"LLM returned neither match nor proposal: {filename}")
                 return

            safe_meeting = final_meeting_name
            
            # Update Master List if NEW
            # EXCEPTION: Do not add '会議名不明' to the master list.
            if is_new_discovery and not dry_run and MASTER_LIST_PATH.exists() and safe_meeting != "会議名不明":
                # CRITICAL: Use lock to prevent race conditions (duplicates/overwrites)
                async with master_list_lock:
                    try:
                        # 1. Read latest state
                        current_data = {"meetings": []}
                        if MASTER_LIST_PATH.exists():
                            with open(MASTER_LIST_PATH, 'r') as f:
                                current_data = yaml.safe_load(f) or {"meetings": []}
                        
                        # 2. Update memory
                        if safe_meeting not in current_data["meetings"]:
                            current_data["meetings"].append(safe_meeting)
                            current_data["meetings"].sort()
                            
                            # 3. Atomic Write (Write to temp -> Rename)
                            # This prevents file corruption on crash
                            tmp_path = MASTER_LIST_PATH.with_suffix('.tmp')
                            with open(tmp_path, 'w') as f:
                                yaml.dump(current_data, f, allow_unicode=True)
                            
                            tmp_path.replace(MASTER_LIST_PATH)
                            logger.info(f"LEARNED: Added '{safe_meeting}' to Master List.")
                        else:
                            logger.info(f"LEARNED: '{safe_meeting}' was already added by another thread.")
                            
                    except Exception as e:
                        logger.error(f"Failed to update Master List: {e}")

            normalized_doc = unicodedata.normalize('NFKC', result.document_name)
            safe_doc = re.sub(r'[\\/:*?"<>|]', '', normalized_doc).strip()
            if safe_doc.lower().endswith('.pdf'):
                safe_doc = safe_doc[:-4]


            
            round_str = f"第{result.round_number:02d}回" if result.round_number else "回数不明"
            
            new_filename = f"{safe_meeting}_{round_str}_{date}_{safe_doc}.pdf"
            
            logger.info(f"Proposed Name: {filename} -> {new_filename}")
            
            if csv_writer:
                csv_writer.writerow([filename, new_filename, safe_meeting, result.round_number, result.document_name])

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
