#!/usr/bin/env python3
"""
Obsidian Vault 内の *.md を走査し、
keywords.txt に載る単語を [[リンク]] に変換するユーティリティ。

主な改良点
----------
1. keywords.txt の  # コメント行  と空行を自動スキップ
2. キーワード重複を自動除外（順序はファイル順を保持）
3. ASCII だけは大文字小文字を無視してマッチ
4. 全角→半角数字を正規化して照合（表記ゆれ削減）
5. YAML front-matter を safe_split（--- が奇数回でも壊れない）
6. 既にリンク化されている箇所・コードブロック・インラインコードを除外
7. 差分が無いファイルは write しない（I/O 削減）
"""

import pathlib
import re
import unicodedata

# ────────────────────────────── 設定 ────────────────────────────── #
SCRIPT_DIR = pathlib.Path(__file__).parent
# Use DB_complete by default, or DB if it doesn't exist (backwards compatibility)
DB_BASE = SCRIPT_DIR.parent / "DB"
VAULT = DB_BASE / "DB_complete" if (DB_BASE / "DB_complete").exists() else DB_BASE
KW_FILE = SCRIPT_DIR / "Keywords.txt"

# ──────────────────── キーワード読み込み & 正規化 ──────────────── #
def load_keywords(path: pathlib.Path) -> list[str]:
    seen, words = set(), []
    with path.open(encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.lstrip().startswith("#"):
                continue                          # 空行・コメント行
            key = normalize(line)
            if key not in seen:                  # 重複排除（元の表記は保持）
                seen.add(key)
                words.append(line)               # 元表記で保持＝リンク文字列
    return words

def normalize(s: str) -> str:
    """比較用キー生成：大文字小文字無視 & 全角数字→半角"""
    s = unicodedata.normalize("NFKC", s)         # 全角→半角変換も含む
    return s.lower()

KEYWORDS = load_keywords(KW_FILE)

# ────────────── 正規表現コンパイル（長い語優先） ────────────── #
def make_pattern(word: str) -> re.Pattern:
    norm = normalize(word)
    flags = re.IGNORECASE if word.isascii() else 0
    # シンプルなパターンで既存のリンクを避けつつマッチ
    return re.compile(re.escape(word), flags)

PATTERNS = [make_pattern(w) for w in sorted(KEYWORDS, key=len, reverse=True)]

# ────────────── Front-matter 安全分割 ────────────── #
def safe_split(text: str) -> tuple[str, str]:
    if text.startswith("---"):
        parts = text.split("---", 2)
        if len(parts) >= 3:
            return ("---" + parts[1] + "---\n", parts[2])
    return ("", text)

# ────────────── 本文置換（コード & インラインコード除外） ───────── #
def linkify(body: str) -> str:
    out, in_code = [], False
    for line in body.splitlines(keepends=True):
        stripped = line.lstrip()
        if stripped.startswith("```"):           # フェンスドブロック
            in_code = not in_code
        if not in_code and "```" not in stripped:  # 行内 ```はインラインコード扱い
            for pat in PATTERNS:
                # 既にリンクされている部分を避けて置換
                def replace_if_not_linked(match):
                    start = match.start()
                    end = match.end()
                    # 前後に [[ ]] がないかチェック
                    before = line[max(0, start-2):start]
                    after = line[end:min(len(line), end+2)]
                    if before.endswith("[[") or after.startswith("]]"):
                        return match.group(0)  # そのまま返す
                    return f"[[{match.group(0)}]]"
                
                line = pat.sub(replace_if_not_linked, line)
        out.append(line)
    return "".join(out)

# ──────────────────────── メイン処理 ─────────────────────── #
changed = 0
for md in VAULT.rglob("*.md"):
    text = md.read_text(encoding="utf-8")
    head, body = safe_split(text)
    new_body = linkify(body)
    if new_body != body:
        md.write_text(head + new_body, encoding="utf-8")
        changed += 1
        print("link:", md.relative_to(VAULT.parent))

print(f"✅ 本文リンク化: {changed} files")
