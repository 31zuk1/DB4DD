import yaml
import re
from pathlib import Path

ROOT_DIR = Path("data/input/crawled/デジタル庁")
OUTPUT_PATH = Path("infrastructure/config/master_meetings.yaml")

def clean_meeting_name(name):
    # Remove round numbers like "第X回", "第2回"
    name = re.sub(r'第\d+回', '', name)
    name = re.sub(r'第[一二三四五六七八九十]+回', '', name)
    # Remove year prefixes if they look like specific instances "令和X年度"
    # But some meetings ARE specific to a year like "Roadmap 2024".
    # User said KEEP 2024/2025.
    # So we remove "fiscal year" but keep "2024"?
    # "令和5年度..." -> Remove?
    name = re.sub(r'^令和\d+年度', '', name)
    name = re.sub(r'\s+', '', name) # Remove spaces
    return name

def main():
    folders = [d.name for d in ROOT_DIR.iterdir() if d.is_dir()]
    unique_meetings = set()
    
    for folder in folders:
        cleaned = clean_meeting_name(folder)
        if len(cleaned) > 4: # Ignore "WG" or short junk
            unique_meetings.add(cleaned)
            
    # Add official known ones manual override
    manual_adds = [
        "デジタル社会推進会議",
        "デジタル社会構想会議",
        "データ戦略推進ワーキンググループ",
        "マイナンバー制度及び国と地方のデジタル基盤抜本改善ワーキンググループ",
        "デジタル関係制度改革検討会"
    ]
    unique_meetings.update(manual_adds)
    
    sorted_meetings = sorted(list(unique_meetings))
    
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        yaml.dump({"meetings": sorted_meetings}, f, allow_unicode=True)
        
    print(f"Generated master list with {len(sorted_meetings)} entries at {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
