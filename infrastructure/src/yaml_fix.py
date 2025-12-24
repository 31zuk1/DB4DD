#!/usr/bin/env python3
import pathlib, re

SCRIPT_DIR = pathlib.Path(__file__).parent
# Use DB_complete by default, or DB if it doesn't exist (backwards compatibility)
DB_BASE = SCRIPT_DIR.parent / "DB"
VAULT = DB_BASE / "DB_complete" if (DB_BASE / "DB_complete").exists() else DB_BASE
pat = re.compile(r'\[\[\\\]\]n?')   # [[\]] と [[\]]n の両方

for md in VAULT.rglob("*.md"):
    txt = md.read_text(encoding="utf-8")
    new = pat.sub("", txt)          # ゴミを削除
    if new != txt:
        md.write_text(new, encoding="utf-8")
        print("clean:", md)
print("✅ YAML ゴミ掃除完了")
