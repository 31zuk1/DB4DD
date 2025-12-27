# DB4DD - Data Base for Digital Democracy

## 概要

DB4DDは、日本の政府会議文書（PDF）を自動処理し、AIによる要約をMarkdown形式で生成するシステムです。OpenAI APIを活用して構造化された情報を抽出し、包括的な要約を作成し、Obsidian vaultとしてデータを整理します。

現在、以下の2つの政府省庁を対象としています：
- **デジタル庁** - 60以上の会議グループ、403以上のファイル
- **こども家庭庁** - 40以上の会議グループ、310以上のファイル

## 主な機能

- **PDFの自動処理**: 政府会議のPDFからテキストを抽出・処理
- **多段階AI要約**:
  - 適応的なテキストチャンキング
  - 固有名詞、数値、アクションアイテムの並列抽出
  - 包括的な分析のための多層要約
- **構造化された出力**: ObsidianのYAMLフロントマターと互換性のあるMarkdownファイルを生成
- **スマートキャッシング**: 効率化のための二層キャッシングシステム（APIレスポンス + テキスト抽出）
- **適応的レート制限**: API使用を最適化するためのインテリジェントな並列制御
- **Obsidian統合**: ナレッジグラフ構築のための自動ウィキリンク生成

## アーキテクチャ

### 処理パイプライン

1. **PDF抽出** - PyMuPDFを使用したテキスト抽出
2. **インテリジェントチャンキング** - ドキュメント長に基づく適応的なチャンクサイズ調整
3. **並列処理** - 固有名詞、数値、アクションアイテムの同時抽出
4. **多層要約**:
   - チャンクごとの詳細なミニ要約
   - 全文の深い分析
   - セクションの統合と合成
   - 構造化された出力を持つ強化された最終要約
5. **Markdown生成** - 重複排除を伴う構造化された出力

### 出力構造

各会議要約には以下が含まれます：
- **概要**: 3-4文の全体概要
- **主要な論点**: 5-8個の主要な議論ポイント
- **議論の流れ**: 時系列のナラティブ
- **決定事項**: 具体的な決定事項と次のステップ
- **未解決の課題**: 未解決の問題
- **重要な固有名詞**: 重要な人物、組織、システム
- **タグ**: 分類キーワード

## インストール

### 前提条件

- Python 3.8以上
- OpenAI APIキー

### セットアップ

1. リポジトリをクローン:
```bash
git clone https://github.com/yourusername/DB4DD.git
cd DB4DD
```

2. 依存関係をインストール:
```bash
cd infrastructure
pip install -r requirements.txt
```

3. 環境を設定:
```bash
cp .env.example .env
# .envファイルをOpenAI APIキーと設定で編集
```

必要な環境変数:
```bash
OPENAI_API_KEY=sk-...                    # OpenAI APIキー
OPENAI_MODEL=gpt-4o-mini                 # 使用するモデル
VAULT_ROOT=./vaults                      # 出力ディレクトリ
MAX_CONCURRENT_REQUESTS=10               # 最大並列リクエスト数
OPENAI_MAX_PARALLEL=20                   # 最大並列ワーカー数
```

## 使い方

すべての処理スクリプトは`infrastructure/`ディレクトリから実行する必要があります。

### 基本的な処理

```bash
cd infrastructure

# 未処理のPDFをすべて処理
python src/main.py

# 特定の会議を処理
python src/main.py --meeting "デジタル"

# 特定の回を処理
python src/main.py --round 5

# 既存のファイルを再処理
python src/main.py --overwrite

# ドライラン（処理内容のプレビュー）
python src/main.py --dry-run
```

# 省庁別フィルタ
python src/main.py --ministry "デジタル庁"
```

### テキストキャッシュからの処理

PDFからではなく、すでに抽出済みのテキストキャッシュから処理を行う場合（高速・再処理用）:

```bash
# 基本的な使用法
python src/main_from_text_cache.py

# Turboモード（AIを使わず高速に骨子を作成、オフライン可）
python src/main_from_text_cache.py --turbo

# スマートモード（巨大なファイルをスキップして安全に処理）
python src/main_from_text_cache.py --max-size-kb 500
```

### パフォーマンスチューニング

```bash
# 最大並列処理モード
python src/main.py --aggressive

# カスタムレート制限
python src/main.py --rate-limit-rpm 3000 --rate-limit-tpm 150000

# ワーカー数の調整
python src/main.py --workers 8
```

### キャッシュ管理

```bash
# APIキャッシングを無効化
python src/main.py --nocache

# 7日より古いキャッシュを削除
python src/main.py --cleanup-cache 7

# vaultと処理済みデータベースをクリア
python src/main.py --clean
```

### Obsidianウィキリンク生成

Obsidian統合のためにキーワードをウィキリンクに変換:

```bash
python src/wikilinkify.py
```

このスクリプトは:
- vault内のすべての`*.md`ファイルをスキャン
- `Keywords.txt`からキーワードを`[[WikiLinks]]`に変換
- YAMLフロントマター、コードブロック、既にリンクされたテキストをスキップ

## ディレクトリ構造

```
DB4DD/
├── infrastructure/
│   ├── src/
│   │   ├── main.py                      # メイン処理（セッション統合機能付き）
│   │   ├── main_from_text_cache.py      # キャッシュされたテキストからの処理
│   │   ├── wikilinkify.py               # Obsidianウィキリンクジェネレーター
│   │   ├── core/                        # コアモジュール
│   │   │   ├── api_client.py            # キャッシング付きOpenAI APIクライアント
│   │   │   ├── rate_limiter.py          # 適応的レート制限
│   │   │   └── models.py                # Pydanticモデル
│   │   ├── processing/                  # 処理モジュール
│   │   │   ├── pdf_processor.py         # PDFテキスト抽出
│   │   │   ├── text_summarizer.py       # 多段階AI要約
│   │   │   └── prompt_manager.py        # プロンプトテンプレート
│   │   ├── output/                      # 出力生成
│   │   │   └── markdown_generator.py    # Markdownファイル生成
│   │   └── utils/                       # ユーティリティ
│   │       ├── file_utils.py            # ファイル解析とデータベース
│   │       └── file_utils_enhanced.py   # 拡張パーサー
│   ├── data/
│   │   ├── raw/                         # ソースPDFファイル
│   │   ├── text_cache/                  # キャッシュされたテキスト抽出
│   │   └── raw_shortened/               # 処理済みPDF
│   ├── vaults/                          # 出力Obsidian vaults
│   │   ├── デジタル庁/                   # デジタル庁vault
│   │   └── こども家庭庁/                 # こども家庭庁vault
│   └── .cache/                          # APIレスポンスキャッシュ
└── CLAUDE.md                            # Claude Code用プロジェクトドキュメント
```

## ファイル命名規則

### 入力PDF

期待される命名パターン:
```
{会議名}_第{N}回_{YYYYMMDD}_{オプション接尾辞}.pdf
```

例: `デジタル社会推進会議_第05回_20230615_資料1.pdf`

### 出力Markdown

生成される命名パターン:
```
{会議名}_第{N}回_{YYYY}-{MM}-{DD}.md
```

## 高度な機能

### 適応的チャンクサイズ調整

- 500kトークン超のドキュメント: 100k文字に切り詰め、500文字チャンクを使用
- 100kトークン超のドキュメント: chunk_size/4を使用（デフォルト: 500文字）
- 通常のドキュメント: 長さに基づいて1000〜chunk_sizeの間で最適化

### スマートバッチ処理

- 50チャンク超のドキュメントの場合、約25グループにバッチ化
- 詳細を維持しながらAPI呼び出しを削減

### 重複排除

- main_arguments、action_items、open_issues、named_entitiesに適用
- 0.8の類似度閾値を使用（単語重複率）
- 最終出力での冗長な情報を防止

## コントリビューション

コントリビューションを歓迎します！プルリクエストをお気軽に提出してください。

## ライセンス

[ライセンスをここに記載]

## 謝辞

このプロジェクトは以下を使用しています：
- OpenAI API - AI要約
- PyMuPDF - PDFテキスト抽出
- Pydantic - データ検証
- Obsidian - ナレッジマネジメント
