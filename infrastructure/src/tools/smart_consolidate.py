import csv
import shutil
import os
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

ROOT_DIR = Path("data/input/crawled/デジタル庁")
REPORT_FILE = Path("infrastructure/keyword_match_report.csv")

# Generic names to SKIP merging (Manual safety list)
BLACKLIST = [
    "有識者会議", 
    "作業部会", 
    "中間とりまとめ", 
    "本検討会", 
    "第9回作業部会",
    "リスト",
    "報告書"
]

def merge_directories(source: Path, target: Path):
    if not source.exists():
        return
    
    if not target.exists():
        logger.info(f"Creating target: {target.name}")
        target.mkdir(parents=True, exist_ok=True)
    
    # Process subdirectories (Round X) first
    for item in source.iterdir():
        dest = target / item.name
        
        if item.is_dir():
            if dest.exists():
                # Merge sub-subdirectory
                merge_directories(item, dest)
                try:
                    item.rmdir() # Remove if empty
                except:
                    pass
            else:
                shutil.move(str(item), str(dest))
        else:
            if dest.exists():
                # File collision
                new_name = f"{item.stem}_merged{item.suffix}"
                dest = target / new_name
            shutil.move(str(item), str(dest))
            
    # Cleanup source
    try:
        source.rmdir()
        logger.info(f"MERGED: {source.name} -> {target.name}")
    except OSError:
        logger.warning(f"Could not remove source (not empty?): {source.name}")

def main():
    if not REPORT_FILE.exists():
        logger.error("Report file not found.")
        return

    count = 0
    with open(REPORT_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            folder_name = row["Folder"]
            potential_matches = row["Potential Matches"]
            
            if not potential_matches:
                continue
                
            # split by |
            candidates = [c.strip() for c in potential_matches.split('|')]
            target_name = candidates[0] # Naive strategy: Pick first one
            
            # Safety checks
            if folder_name in BLACKLIST:
                logger.info(f"SKIPPING (Blacklist): {folder_name}")
                continue
                
            source_path = ROOT_DIR / folder_name
            target_path = ROOT_DIR / target_name
            
            if not source_path.exists():
                continue
                
            if source_path == target_path:
                continue
            
            # Additional Heuristic:
            # If target name is significantly different from source (e.g. wrong keyword match), warn/skip?
            # But we trusted keyword_match.py so let's proceed but log.
            
            logger.info(f"Processing: {folder_name} -> {target_name}")
            merge_directories(source_path, target_path)
            count += 1

    logger.info(f"Smart consolidation complete. Processed {count} folders.")

if __name__ == "__main__":
    main()
