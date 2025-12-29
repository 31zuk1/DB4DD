import shutil
import os
from pathlib import Path

BASE_DIR = Path("data/input/crawled/デジタル庁")

# Target -> [List of Source Folders]
CONSOLIDATION_MAP = {
    # 1. Main Ministerial Conference
    "地方公共団体の基幹業務等システムの統一・標準化に関する関係府省会議": [
        "地方公共団体の基幹業務等システムの統一・標準化に関する関係省庁会議",
        "関係省庁会議",
        "関係府省会議",
        "地方公共団体の基幹業務システムの統一・標準化",
        "地方公共団体の基幹業務システムの統一・標準化に関する今後の取組について",
        "地方公共団体の基幹業務システムの標準化のために検討すべき点について",
        "地方公共団体の基幹業務システムの標準仕様",
        "地方公共団体の基幹業務システムの標準仕様における機能要件の標準の定め方について",
        "地方公共団体の基幹業務システムの標準仕様における業務フローについて"
    ],
    # 2. Technical Requirements Study Group
    "地方公共団体の基幹業務システムの統一・標準化に関する共通機能等技術要件検討会": [
        "共通機能等技術要件検討会",
        "地方公共団体基幹業務システムの統一・標準化に関する共通機能等技術要件検討会"
    ],
    # 3. Working Teams
    "地方公共団体の基幹業務システムの統一・標準化に関する共通機能等技術要件検討会 データ連携ワーキングチーム": [
        "共通機能等技術要件検討会 データ連携ワーキングチーム",
        "地方公共団体の基幹業務システムの統一・標準化に関する共通機能等技術要件検討会 データ連携ワーキングチーム", # Self
        "共通機能等技術要件検討会 宛名管理ワーキングチーム（第１回） 兼 データ連携ワーキングチーム（第２回）"
    ],
    "地方公共団体の基幹業務システムの統一・標準化に関する共通機能等技術要件検討会 申請管理ワーキングチーム": [
        "共通機能等技術要件検討会 申請管理ワーキングチーム"
    ],
    "地方公共団体の基幹業務システムの統一・標準化に関する共通機能等技術要件検討会 宛名管理ワーキングチーム": [
        "共通機能等技術要件検討会 宛名管理ワーキングチーム"
    ]
}

def consolidate():
    for target_name, sources in CONSOLIDATION_MAP.items():
        target_path = BASE_DIR / target_name
        
        # Ensure target exists
        if not target_path.exists():
            print(f"Creating target: {target_name}")
            target_path.mkdir(parents=True, exist_ok=True)
            
        for source_name in sources:
            source_path = BASE_DIR / source_name
            if not source_path.exists():
                continue
                
            if source_path == target_path:
                continue

            print(f"Processing source: {source_name}")
            
            # Walk through source directory
            for round_dir in source_path.iterdir():
                if not round_dir.is_dir():
                    continue
                    
                target_round_dir = target_path / round_dir.name
                target_round_dir.mkdir(exist_ok=True)
                
                # Move files
                for file_path in round_dir.iterdir():
                    if not file_path.is_file():
                        continue
                        
                    dest_path = target_round_dir / file_path.name
                    if dest_path.exists():
                        print(f"  Conflict: {file_path.name} -> Rename")
                        dest_path = target_round_dir / f"dup_{file_path.name}"
                        
                    print(f"  Moving: {file_path.name} -> {target_name}/{round_dir.name}/")
                    shutil.move(str(file_path), str(dest_path))
            
            # Cleanup source if empty
            try:
                shutil.rmtree(source_path)
                print(f"Removed source: {source_name}")
            except Exception as e:
                print(f"Error removing {source_name}: {e}")

if __name__ == "__main__":
    consolidate()
