import shutil
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

ROOT_DIR = Path("data/input/crawled/デジタル庁")

# Defined Merges: Source -> Target
# We choose the most "standard" looking name as Target
MERGE_MAP = {
    # WG variations
    "モビリティWG": "モビリティワーキンググループ",
    "モビリティワーキンググループ(モビリティWG)": "モビリティワーキンググループ",
    "デジタル社会推進会議モビリティワーキンググループ": "モビリティワーキンググループ",
    "モビリティワーキンググループ会議": "モビリティワーキンググループ",
    
    # Roadmap Study Group variations
    "「モビリティ・ロードマップ」のありかたに関する研究会": "モビリティ・ロードマップのありかたに関する研究会",
    "デジタル庁モビリティ・ロードマップのありかたに関する研究会": "モビリティ・ロードマップのありかたに関する研究会",
    
    # Roadmap variations
    "モビリティ・ロードマップ会議": "モビリティ・ロードマップ", 
    # Note: We keep 2024/2025 separate as per previous instruction.
    
    # Catch-up Phase 2
    "モビリティワーキンググループ(第11 回)": "モビリティワーキンググループ", # Round info in name
    "モビリティ作業部会": "モビリティワーキンググループ", # Translation
    "モビリティ・イノベーション連携研究機構会議": "モビリティ・イノベーション連携研究機構", # Suffix
    "モビリティ会議": "モビリティワーキンググループ", # Highly likely Generic for WG, but risky. Let's merge to WG based on context.
    "デジタル交通社会に向けたモビリティサービス会議": "デジタル交通社会に向けたモビリティサービス" # Suffix removal guess
}

def merge_directories(source: Path, target: Path):
    if not source.exists():
        return
    
    if not target.exists():
        logger.info(f"Creating target: {target.name}")
        target.mkdir(parents=True, exist_ok=True)
    
    # Process subdirectories (Round X)
    for item in source.iterdir():
        dest = target / item.name
        
        if item.is_dir():
            if dest.exists():
                # Merge sub-subdirectory recursively
                merge_directories(item, dest)
                try:
                    item.rmdir()
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
    count = 0
    for src_name, tgt_name in MERGE_MAP.items():
        source_path = ROOT_DIR / src_name
        target_path = ROOT_DIR / tgt_name
        
        if source_path.exists():
            if source_path == target_path:
                continue
                
            logger.info(f"Processing: {src_name} -> {tgt_name}")
            merge_directories(source_path, target_path)
            count += 1
            
    logger.info(f"Mobility cleanup complete. Processed {count} folders.")

if __name__ == "__main__":
    main()
