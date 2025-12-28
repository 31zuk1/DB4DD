# DB4DD Crawler Module

デジタル庁などの省庁サイトから会議資料（PDF）を自動収集するためのモジュールです。

## 構成
* `engine.py`: クローラーのコアロジック（CrawlerEngineクラス）

## 機能
* 会議一覧ページの巡回
* PDFリンクの抽出
* 差分検知（`crawler_state.json` による重複防止）
* 日付別フォルダへの保存

## 使い方（このディレクトリ単体ではなく、親のmain_crawler.pyから呼ぶ）

```python
from src.crawler import CrawlerEngine
# ...
engine = CrawlerEngine(output_base_dir=Path(...))
engine.run()
```
