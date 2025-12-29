import os
import re
import unicodedata
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("normalization.log")
    ]
)
logger = logging.getLogger(__name__)

TARGET_ROOT = Path("data/input/crawled/デジタル庁")

def normalize_text(text: str) -> str:
    """Normalize text to NFKC (full-width to half-width) and sanitize."""
    # 1. NFKC Normalization
    normalized = unicodedata.normalize('NFKC', text)
    
    # 2. Sanitize (remove illegal chars for filenames)
    # Keep allowed characters: alphanumeric, Japanese, basic symbols
    # Remove filesystem reserved chars: / \ : * ? " < > |
    safe_text = re.sub(r'[\\/:*?"<>|]', '', normalized).strip()
    
    return safe_text

def process_directory(directory: Path):
    """Process a directory: first its children, then rename the directory itself."""
    
    # 1. Process files in this directory
    try:
        # List items to iterate safely (since we might rename them)
        try:
            items = list(directory.iterdir())
        except FileNotFoundError:
            return # Directory renamed or deleted by recursive call?
            
        # Separate files and directories
        files = [x for x in items if x.is_file() and not x.name.startswith('.')]
        subdirs = [x for x in items if x.is_dir()]
        
        # Recurse into subdirectories first (Bottom-up for directories)
        for subdir in subdirs:
            process_directory(subdir)
            
        # Rename files
        for file_path in files:
            original_name = file_path.name
            stem = file_path.stem
            suffix = file_path.suffix
            
            # Normalize stem only, keep extension as is (or lower/normalize extension too?)
            # Usually strict lower for extension is safer, but let's stick to NFKC for everything
            
            new_stem = normalize_text(stem)
            new_name = new_stem + suffix
            
            if original_name != new_name:
                new_path = file_path.parent / new_name
                
                if new_path.exists():
                    logger.warning(f"SKIP (Collision): {original_name} -> {new_name}")
                else:
                    try:
                        file_path.rename(new_path)
                        logger.info(f"FILE: {original_name} -> {new_name}")
                    except OSError as e:
                        logger.error(f"ERROR: {original_name} -> {e}")

        # 2. Rename the directory itself (after children are processed)
        # We don't rename the root target folder, only subfolders
        if directory == TARGET_ROOT:
            return

        dir_name = directory.name
        new_dir_name = normalize_text(dir_name)
        
        if dir_name != new_dir_name:
            new_dir_path = directory.parent / new_dir_name
            
            # Check for collision
            if new_dir_path.exists():
                 if directory.samefile(new_dir_path):
                     pass
                 else:
                     logger.info(f"MERGE (Dir Collision): {dir_name} -> {new_dir_name}")
                     # Move all contents from directory to new_dir_path
                     try:
                         for item in directory.iterdir():
                             dest = new_dir_path / item.name
                             if dest.exists():
                                 # Suffix collision in merge?
                                 dest = new_dir_path / f"merged_{item.name}"
                             
                             item.rename(dest)
                         
                         # Remove empty source dir
                         directory.rmdir() 
                         logger.info(f"MERGED and REMOVED: {dir_name}")
                     except Exception as e:
                         logger.error(f"ERROR Merging {dir_name}: {e}")
                     return

            try:
                directory.rename(new_dir_path)
                logger.info(f"DIR : {dir_name} -> {new_dir_name}")
            except OSError as e:
                logger.error(f"ERROR Dir: {dir_name} -> {e}")

    except Exception as e:
        logger.error(f"Fatal error in {directory}: {e}")

if __name__ == "__main__":
    if not TARGET_ROOT.exists():
        logger.error(f"Target root not found: {TARGET_ROOT}")
    else:
        logger.info(f"Starting normalization on {TARGET_ROOT}...")
        process_directory(TARGET_ROOT)
        logger.info("Normalization complete.")
