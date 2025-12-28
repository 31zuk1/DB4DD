# DB4DD Crawler Module

デジタル庁などの省庁サイトから会議資料（PDF）を自動収集するためのモジュールです。

## 構成
* `engine.py`: クローラーのコアロジック（CrawlerEngineクラス）

## 機能
* 会議一覧ページの巡回（ページネーション自動追跡機能付き）
* 詳細ページ（会議回ごとのページ）の再帰的探索
* PDFリンクの抽出
* 差分検知（`crawler_state.json` による重複防止）
* `master_raw` フォルダへの一元保存（重複排除）

## 使い方（このディレクトリ単体ではなく、親のmain_crawler.pyから呼ぶ）

```python
from src.crawler import CrawlerEngine
# ...
engine = CrawlerEngine(output_base_dir=Path(...))
engine.run()
```
