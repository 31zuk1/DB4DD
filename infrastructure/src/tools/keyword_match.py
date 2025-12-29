import csv
from pathlib import Path

ROOT_DIR = Path("data/input/crawled/デジタル庁")
REPORT_FILE = Path("infrastructure/consolidation_report.csv")
NEW_REPORT_FILE = Path("infrastructure/keyword_match_report.csv")

def main():
    # Load previous report to get candidates
    candidates = []
    with open(REPORT_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["Score"] == "0.0": # Only process failed matches
                candidates.append(row["Folder"])

    # Get all potential targets (folders that are NOT candidates)
    # We re-evaluate targets based on directory existence
    all_dirs = [d.name for d in ROOT_DIR.iterdir() if d.is_dir()]
    targets = [d for d in all_dirs if d not in candidates]

    results = []

    for cand in candidates:
        # 1. Clean candidate name to get keyword
        keyword = cand
        for suffix in ["について", "の概要", "報告書", "リスト", "（案）", "(案)", "議事次第", "議事録", "資料", "プレゼン資料"]:
            keyword = keyword.replace(suffix, "")
        
        keyword = keyword.strip()
        if len(keyword) < 2: continue

        matches = []
        for target in targets:
            # Check if keyword is part of target or vice versa
            # But avoid generic matches like "デジタル"
            if keyword in target or target in keyword:
                 matches.append(target)
        
        # Filter matches to be non-trivial (e.g. not just matching "デジタル")
        matches = [m for m in matches if len(m) > 4] # arbitrary length filter
        
        if matches:
            # Pick longest match as best guess? Or listing all?
            # Let's list top 3
            results.append({
                "Folder": cand,
                "Keyword": keyword,
                "Potential Matches": " | ".join(matches[:3])
            })

    with open(NEW_REPORT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["Folder", "Keyword", "Potential Matches"])
        writer.writeheader()
        writer.writerows(results)
    
    # Print preview
    for r in results[:10]:
        print(f"{r['Folder']} -> {r['Potential Matches']}")

if __name__ == "__main__":
    main()
