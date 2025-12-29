import os
import unicodedata
from pathlib import Path

import re

TARGET_DIR = Path("data/input/crawled/デジタル庁")

def normalize_files():
    count = 0
    for root, dirs, files in os.walk(TARGET_DIR):
        for filename in files:
            if not filename.lower().endswith(".pdf"):
                continue
                
            # Match organize_pdfs.py logic: remove illegal chars
            # But wait, we want to keep extension .pdf safe
            stem = Path(filename).stem
            suffix = Path(filename).suffix
            
            norm_stem = unicodedata.normalize('NFKC', stem)
            safe_stem = re.sub(r'[\\/:*?"<>|]', '', norm_stem).strip()
            
            normalized_name = safe_stem + suffix
            
            if filename != normalized_name:
                old_path = Path(root) / filename
                new_path = Path(root) / normalized_name
                
                # Handle potential collision
                if new_path.exists():
                    print(f"Skipping (Target exists): {filename} -> {normalized_name}")
                    continue
                    
                print(f"Renaming: {filename} -> {normalized_name}")
                old_path.rename(new_path)
                count += 1
                
    print(f"Total renamed: {count}")

if __name__ == "__main__":
    normalize_files()
