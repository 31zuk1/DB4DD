"""
Prompt management for different analysis tasks.
"""
import textwrap

class PromptManager:
    """Manages prompts for different analysis stages."""
    
    def __init__(self):
        self.prompts = {
            'extract': self._extract_prompt,
            'detailed_mini': self._detailed_mini_prompt,
            'deep_analysis': self._deep_analysis_prompt,
            'enhanced_final': self._enhanced_final_prompt
        }
    
    def get(self, prompt_type: str, **kwargs) -> str:
        """Get a prompt with interpolated variables."""
        if prompt_type not in self.prompts:
            raise ValueError(f"Unknown prompt type: {prompt_type}")
        return self.prompts[prompt_type](**kwargs)
    
    def _extract_prompt(self, text: str) -> str:
        return f"""
この文書から以下の情報を抽出してください：

1. named_entities: 重要な人物名、組織名、システム名、プロジェクト名（10個以内）
2. numbers: 重要な数値・統計・予算・期限（10個以内） 
3. todos: アクションアイテムや決定事項（10個以内）

文書:
{text}

JSON形式で回答してください。空の場合は空配列[]を返してください。
"""

    def _detailed_mini_prompt(self, text: str) -> str:
        return f"""
この文書の内容を詳細に分析し、以下の形式で要約してください：

sections配列に、意味のあるセクションごとに分けて以下を記述：
- title: セクションのタイトル（例：「議題」「決定事項」「課題」「背景」など）
- content: そのセクションの具体的内容

重要：
- 文書の構造と内容を正確に反映する
- 決定事項、議題、課題は必ず別セクションに分ける
- 具体的で詳細な情報を残す

文書:
{text}

JSON形式で回答してください。
"""

    def _deep_analysis_prompt(self, full_text: str) -> str:
        return f"""
この会議文書の冒頭部分を詳細分析し、会議の目的・背景・重要な決定事項を抽出してください：

sections配列で以下を明確に分けて記述：
- 「開催目的」: なぜこの会議が開催されたのか
- 「背景・経緯」: この会議に至る経緯や背景
- 「主要決定事項」: 会議で決定された重要事項
- 「今後の方針」: 今後の方向性や次のステップ

文書冒頭:
{full_text}

JSON形式で回答してください。各セクションは具体的で詳細に記述してください。
"""

    def _enhanced_final_prompt(self, summary_text: str, extraction_text: str, full_text_sample: str) -> str:
        return f"""
以下の情報を統合して、会議の包括的な要約を作成してください：

要求形式：
- summary: 会議全体の3-4文での簡潔な要約
- main_arguments: 会議で議論された主要な論点5-8個（具体的な議論内容）
- discussion_flow: 会議全体の議論の流れを時系列で説明（段落形式）
- action_items: 具体的なアクションアイテム（5個以内）
- open_issues: 未解決の課題や今後の検討事項（5個以内）
- named_entities: 重要な人物・組織・システム名（10個以内）
- tags: この会議を特徴づけるタグ（3-5個）

統合すべき情報：
{summary_text}

原文サンプル：
{full_text_sample}

重要：
- main_argumentsは議論された核心的な論点を記載
- discussion_flowは「まず〜について議論され、次に〜が検討され、最終的に〜が確認された」のような流れを記述
- 各項目は重複を避け、具体的で有用な情報のみを含めてください
JSON形式で回答してください。
"""