import os
import re
import difflib
import fitz  # PyMuPDF
from pathlib import Path
import csv
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

ROOT_DIR = Path("data/input/crawled/デジタル庁")
REPORT_FILE = Path("infrastructure/consolidation_report.csv")

# Heuristics for "Suspicious" folders (likely document titles)
SUSPICIOUS_SUFFIXES = ["について", "の概要", "報告書", "リスト", "（案）", "(案)", "議事次第", "議事録", "資料"]
GENERIC_NAMES = ["有識者会議", "本検討会", "中間とりまとめ", "ワーキンググループ", "サブワーキンググループ", "作業部会"]

def is_suspicious(folder_name: str) -> bool:
    if any(folder_name.endswith(s) for s in SUSPICIOUS_SUFFIXES):
        return True
    if folder_name in GENERIC_NAMES:
        return True
    if len(folder_name) > 50: # Arbitrary length threshold for "sentence-like" titles
        return True
    return False

def extract_meeting_name_from_pdf(pdf_path: Path) -> str:
    """Read first page and try to find meeting name pattern."""
    try:
        doc = fitz.open(pdf_path)
        text = doc[0].get_text() if len(doc) > 0 else ""
        doc.close()
        
        # Heuristics to find meeting name in text
        # Look for "第X回 〇〇会議"
        match = re.search(r'第\s*\d+\s*回\s*(.+?)(?:\r|\n|　|（|「|開催)', text)
        if match:
             candidate = match.group(1).strip()
             # Clean up
             return candidate
             
        # Look for lines ending in "会議", "検討会", "WG"
        lines = text.split('\n')
        for line in lines[:10]: # Check first 10 lines
            line = line.strip()
            if (line.endswith("会議") or line.endswith("検討会") or line.endswith("ワーキンググループ") or line.endswith("WG")) and len(line) < 40:
                return line
        
        return ""
    except Exception as e:
        return ""

def calculate_similarity(name1: str, name2: str) -> float:
    return difflib.SequenceMatcher(None, name1, name2).ratio()

def main():
    all_folders = [f for f in ROOT_DIR.iterdir() if f.is_dir()]
    targets = []
    candidates = []

    # 1. Classify
    for folder in all_folders:
        if is_suspicious(folder.name):
            candidates.append(folder)
        else:
            targets.append(folder)

    logger.info(f"Targets (Valid-ish): {len(targets)}")
    logger.info(f"Candidates (Suspicious): {len(candidates)}")

    results = []

    # 2. Analyze Candidates
    for folder in candidates:
        # Find a representative PDF (preferably one with '議事次第' or 'main' in name)
        pdfs = list(folder.rglob("*.pdf"))
        if not pdfs:
            continue
            
        # Prioritize Agenda/Minutes
        representative_pdf = pdfs[0]
        for pdf in pdfs:
            if "議事次第" in pdf.name or "議事録" in pdf.name:
                representative_pdf = pdf
                break
        
        extracted_name = extract_meeting_name_from_pdf(representative_pdf)
        
        best_match = None
        highest_score = 0.0
        
        # 3. Match against Targets
        if extracted_name:
            # First, check direct containment
            for target in targets:
                if target.name in extracted_name:
                    score = 0.9  # High confidence if target is substring of extracted
                    if score > highest_score:
                        highest_score = score
                        best_match = target.name
                elif extracted_name in target.name:
                     score = 0.8 # Target contains extracted (extracted might be short)
                     if score > highest_score:
                        highest_score = score
                        best_match = target.name

            # Fuzzy matching
            if highest_score < 0.8:
                for target in targets:
                    score = calculate_similarity(extracted_name, target.name)
                    if score > highest_score:
                        highest_score = score
                        best_match = target.name
        
        results.append({
            "Folder": folder.name,
            "Extracted Name": extracted_name,
            "Best Match": best_match,
            "Score": round(highest_score, 2),
            "Reason": "Similarity" if highest_score > 0 else "No Match"
        })

    # 4. Generate Report
    with open(REPORT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["Folder", "Extracted Name", "Best Match", "Score", "Reason"])
        writer.writeheader()
        writer.writerows(results)
    
    logger.info(f"Analysis complete. Report saved to {REPORT_FILE}")
    
    # Print high confidence matches for preview
    print("\n--- High Confidence Matches (Score > 0.7) ---")
    for r in results:
        if r["Score"] > 0.7:
             print(f"{r['Folder']} -> {r['Best Match']} (Score: {r['Score']}, Extracted: {r['Extracted Name']})")

if __name__ == "__main__":
    main()
