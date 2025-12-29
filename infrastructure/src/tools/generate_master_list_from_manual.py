import yaml
from pathlib import Path

# User explicitly configured this as the Ground Truth path
MANUAL_ROOT = Path("data/input/manual/デジタル庁")
OUTPUT_PATH = Path("infrastructure/config/master_meetings.yaml")

def main():
    if not MANUAL_ROOT.exists():
        print(f"Error: Manual root not found at {MANUAL_ROOT}")
        return

    # Get all folder names in manual directory
    folders = [d.name for d in MANUAL_ROOT.iterdir() if d.is_dir()]
    
    # Sort them for consistency
    sorted_meetings = sorted(folders)
    
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        yaml.dump({"meetings": sorted_meetings}, f, allow_unicode=True)
        
    print(f"Generated MANUAL master list with {len(sorted_meetings)} entries at {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
