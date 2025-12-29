import os
import unicodedata
import re
from pathlib import Path

BASE_DIR = Path("infrastructure/data/input/crawled/デジタル庁")

def simple_sanitize(name):
    # Match organize_pdfs.py: remove illegal chars for filename safety
    # We want to preserve dots for extensions if it's a file, but simple sanitization is safer for now.
    # However, simple re.sub removes dots too, which is bad for files.
    # We'll handle stem/suffix separately in the main loop.
    return re.sub(r'[\\/:*?"<>|]', '', name).strip()

def normalize_recursively(root_dir):
    # Use walk with topdown=False to rename children before parents
    for root, dirs, files in os.walk(root_dir, topdown=False):
        
        # 1. Rename Files
        for filename in files:
            file_path = Path(root) / filename
            
            # Separate stem and suffix
            stem = file_path.stem
            suffix = file_path.suffix
            
            # Normalize stem
            norm_stem = unicodedata.normalize('NFKC', stem)
            # Remove illegal chars from stem
            safe_stem = simple_sanitize(norm_stem)
            
            # Reconstruct
            new_name = safe_stem + suffix
            
            if filename != new_name:
                new_path = Path(root) / new_name
                try:
                    if new_path.exists():
                        print(f"Skipping file (Target exists): {filename} -> {new_name}")
                    else:
                        file_path.rename(new_path)
                        print(f"File: {filename} -> {new_name}")
                except Exception as e:
                    print(f"Error renaming file {filename}: {e}")

        # 2. Rename Directories
        for dirname in dirs:
            dir_path = Path(root) / dirname
            
            norm_dirname = unicodedata.normalize('NFKC', dirname)
            safe_dirname = simple_sanitize(norm_dirname)
            
            if dirname != safe_dirname:
                new_path = Path(root) / safe_dirname
                try:
                    if new_path.exists():
                         # Merge logic could be complex, for now skip or manual merge needed?
                         # If target exists and is a dir, we should probably move contents?
                         # Let's just warn for now to avoid data loss.
                        print(f"WARNING: Directory Merge Required (Target exists): {dirname} -> {safe_dirname}")
                        # Attempt to merge simply?
                        # No, safer to alert user or handle manually if conflicts arise.
                    else:
                        dir_path.rename(new_path)
                        print(f"Dir:  {dirname} -> {safe_dirname}")
                except Exception as e:
                    print(f"Error renaming dir {dirname}: {e}")

if __name__ == "__main__":
    if not BASE_DIR.exists():
        print(f"Directory not found: {BASE_DIR}")
    else:
        print(f"Normalizing contents of: {BASE_DIR}")
        normalize_recursively(BASE_DIR)
