import yaml
from pathlib import Path

OUTPUT_PATH = Path("infrastructure/config/master_meetings.yaml")

# Manually extracted from https://www.digital.go.jp/councils
# This is the Ground Truth.
OFFICIAL_MEETINGS = [
    # Top Level
    "デジタル社会推進会議",
    "デジタル社会推進会議幹事会",
    "先進的AI利活用アドバイザリーボード",
    "データ戦略推進ワーキンググループ",
    "デジタル関係制度改革検討会",
    "デジタル関係制度改革検討会 デジタル法制ワーキンググループ", # Note: Abolished but likely in PDFs
    "デジタル関係制度改革検討会 テクノロジーベースの規制改革推進委員会", # Nested name
    "ベース・レジストリ推進有識者会合",
    "デジタル社会構想会議",
    "マイナンバー制度及び国と地方のデジタル基盤抜本改善ワーキンググループ",
    "コンプライアンス委員会",
    
    # Others
    "マイナンバー情報総点検本部",
    "こどもに関する情報・データ連携 副大臣プロジェクトチーム",
    "マイナンバーカードと健康保険証の一体化に関する検討会",
    "マイナンバーカードの普及・利用の推進に関する関係省庁連絡会議",
    "マイナンバーカードの普及と健康保険証利用に関する関係府省庁会議",
    "マイナンバーカード、健康保険証、運転免許証の一体化・活用普及に関する検討会",
    "次期個人番号カードタスクフォース",
    "地方公共団体の基幹業務等システムの統一・標準化に関する関係省庁会議",
    "地方公共団体の基幹業務システムの統一・標準化に関する共通機能等課題検討会",
    "地方公共団体の基幹業務システムの統一・標準化に関する共通機能等技術要件検討会",
    "標準仕様の指定都市における課題等検討会",
    "地方公共団体情報システムにおける文字要件の運用に関する検討会",
    "地方公共団体情報システムにおける文字の標準化に関する有識者会議",
    "地方公共団体情報システムにおける標準化にかかる共通基準に関する検討会",
    "国・地方ネットワークの将来像及び実現シナリオに関する検討会",
    "「デジタルの日」検討委員会",
    "次世代セキュリティアーキテクチャ検討会",
    "デジタル庁情報システム調達改革検討会",
    "モビリティワーキンググループ",
    "AI時代における自動運転車の社会的ルールの在り方検討サブワーキンググループ",
    "教育分野の認証基盤の在り方に関する検討会",
    "Web3.0研究会",
    "デジタル改革に向けたマルチステークホルダーモデルの運用（処分通知等のデジタル化）",
    "マイナンバーカードの機能のスマートフォン搭載に関する検討会",
    "地域幸福度（Well-Being）指標の活用促進に関する検討会",
    "電子委任状法施行状況検討会",
    "本人確認ガイドラインの改定：本人確認実務の課題・事例・手法とそのガイドラインに関する有識者会議",
    "事業者のデジタル化等に係る関係省庁等連絡会議",
    "国際データガバナンス検討会",
    "国際データガバナンスアドバイザリー委員会",
    "電子署名法認定基準のモダナイズ検討会",
    "Verifiable Credential (VC/VDC) の活用におけるガバナンスに関する有識者会議",
    "属性証明の課題整理に関する有識者会議",
    "サービスデザイン関連ガイドライン改訂に係る検討会"
]

# Variations mapping (Official -> Common Variations found in PDFs)
# Only add if we want to reverse-map, but organise_pdfs does fuzzy match.
# So having the official name in list is enough (fuzzy match will catch "WG" -> "ワーキンググループ")

def main():
    sorted_meetings = sorted(OFFICIAL_MEETINGS)
    
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        yaml.dump({"meetings": sorted_meetings}, f, allow_unicode=True)
        
    print(f"Generated OFFICIAL master list with {len(sorted_meetings)} entries at {OUTPUT_PATH}")

if __name__ == "__main__":
    main()
