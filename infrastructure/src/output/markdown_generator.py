"""
Markdown file generation with YAML frontmatter.
"""
import re
import yaml
import logging
from pathlib import Path
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

class MarkdownGenerator:
    """Generates clean markdown files with proper structure."""
    
    @staticmethod
    def extract_meeting_purpose(outline: str) -> str:
        """Extract meeting purpose from outline."""
        if not outline:
            return ""
        
        # Look for purpose-related sections
        purpose_patterns = [
            r'開催目的[：:]\s*(.+?)(?=\n\n|\n[#*]|$)',
            r'目的[：:]\s*(.+?)(?=\n\n|\n[#*]|$)',
            r'趣旨[：:]\s*(.+?)(?=\n\n|\n[#*]|$)'
        ]
        
        for pattern in purpose_patterns:
            match = re.search(pattern, outline, re.DOTALL | re.IGNORECASE)
            if match:
                purpose_text = match.group(1).strip()
                return f"## 開催目的\n\n{purpose_text}"
        
        return ""
    
    @staticmethod
    def extract_decisions_from_outline(outline: str) -> str:
        """Extract decisions from outline."""
        if not outline:
            return "（記録なし）"
        
        # Look for decision-related sections
        decision_patterns = [
            r'決定事項[：:](.+?)(?=\n\n|\n[#*]|$)',
            r'合意事項[：:](.+?)(?=\n\n|\n[#*]|$)',
            r'承認事項[：:](.+?)(?=\n\n|\n[#*]|$)'
        ]
        
        for pattern in decision_patterns:
            match = re.search(pattern, outline, re.DOTALL | re.IGNORECASE)
            if match:
                decisions_text = match.group(1).strip()
                # Format as bullet points if not already
                lines = [line.strip() for line in decisions_text.split('\n') if line.strip()]
                formatted_lines = []
                for line in lines:
                    if not line.startswith('-') and not line.startswith('*'):
                        formatted_lines.append(f"- {line}")
                    else:
                        formatted_lines.append(line)
                return '\n'.join(formatted_lines) if formatted_lines else "（記録なし）"
        
        return "（記録なし）"
    
    @staticmethod
    def deduplicate_list(items: List[str], similarity_threshold: float = 0.8) -> List[str]:
        """Remove duplicate items based on similarity."""
        if not items:
            return []
        
        unique_items = []
        for item in items:
            item = item.strip()
            if not item:
                continue
                
            is_duplicate = False
            for existing in unique_items:
                # Simple similarity check
                if item.lower() == existing.lower():
                    is_duplicate = True
                    break
                # Check for substantial overlap
                words1 = set(item.lower().split())
                words2 = set(existing.lower().split())
                if len(words1) > 0 and len(words2) > 0:
                    overlap = len(words1 & words2) / len(words1 | words2)
                    if overlap > similarity_threshold:
                        is_duplicate = True
                        break
            
            if not is_duplicate:
                unique_items.append(item)
        
        return unique_items
    
    def generate_markdown(self, summary: Dict[str, Any], meeting: str, round_num: int, 
                         date: str, pdf_name: str) -> str:
        """Generate clean markdown content with proper structure."""
        # Create clean frontmatter without duplicating content
        frontmatter_data = {
            'date': f"{date[:4]}-{date[4:6]}-{date[6:]}",
            'meeting': meeting,
            'round': int(round_num),
            'source_pdf': pdf_name,
            'status': 'completed',
            'tags': summary.get('tags', [])
        }
        
        # Deduplicate and organize content
        main_arguments = self.deduplicate_list(summary.get('main_arguments', []))
        discussion_flow = summary.get('discussion_flow', '')
        action_items = self.deduplicate_list(summary.get('action_items', []))
        open_issues = self.deduplicate_list(summary.get('open_issues', []))
        named_entities = summary.get('named_entities', [])
        
        # Extract structured sections from outline (without duplication)
        outline = summary.get('outline', '')
        meeting_purpose = self.extract_meeting_purpose(outline)
        decisions = self.extract_decisions_from_outline(outline)
        
        # Clean summary - remove any potential title duplication
        clean_summary = summary.get('summary', '').strip()
        # Remove any lines that duplicate the meeting title
        summary_lines = [line.strip() for line in clean_summary.split('\n') if line.strip()]
        filtered_summary_lines = []
        for line in summary_lines:
            # Skip lines that are essentially the meeting title or duplicates
            if not (meeting in line and ('第' in line or '回' in line)):
                filtered_summary_lines.append(line)
        clean_summary = '\n'.join(filtered_summary_lines)
        
        # If no purpose found in outline, create from summary (but avoid duplication)
        if not meeting_purpose and clean_summary:
            meeting_purpose = f"## 開催目的\n\n{clean_summary}"
            clean_summary = ""  # Don't duplicate in summary section
        
        # If no decisions found, check if they're in main_arguments or action_items
        if decisions == "（記録なし）":
            # 決定事項らしい内容を main_arguments から抽出
            potential_decisions = [
                point for point in main_arguments 
                if any(keyword in point for keyword in ["決定", "承認", "合意", "採択", "確認"])
            ]
            if potential_decisions:
                decisions = '\n'.join(f"- {decision}" for decision in potential_decisions)
        
        # Generate YAML frontmatter
        fm_yaml = yaml.safe_dump(frontmatter_data, allow_unicode=True, default_flow_style=False)
        
        # Build content sections
        content_sections = []
        
        if clean_summary:
            content_sections.append(f"## 会議概要\n\n{clean_summary}")
        
        if meeting_purpose:
            content_sections.append(meeting_purpose.strip())
        
        newline = chr(10)
        
        if main_arguments:
            arguments_text = newline.join(f'- {point}' for point in main_arguments)
            content_sections.append(f"## 主要な論点{newline}{newline}{arguments_text}")
        
        if discussion_flow:
            content_sections.append(f"## 議論の流れ{newline}{newline}{discussion_flow}")
        
        if decisions and decisions != "（記録なし）":
            content_sections.append(f"## 決定事項{newline}{newline}{decisions}")
        
        if action_items:
            actions_text = newline.join(f'- {item}' for item in action_items)
            content_sections.append(f"## アクションアイテム{newline}{newline}{actions_text}")
        
        if open_issues:
            issues_text = newline.join(f'- {issue}' for issue in open_issues)
            content_sections.append(f"## 今後の課題{newline}{newline}{issues_text}")
        
        # Reference section with improved formatting
        reference_parts = ["## 参考情報"]
        if named_entities:
            reference_parts.append(f"#### 関連組織・人物")
            reference_parts.append(f"{', '.join(named_entities)}")
            reference_parts.append(f"#### 出典")
            reference_parts.append(f"{pdf_name}")
        else:
            reference_parts.append(f"#### 出典")
            reference_parts.append(f"{pdf_name}")
        
        content_sections.append(newline.join(reference_parts))
        
        # Join sections with double newline for better readability
        content_text = (newline + newline).join(content_sections)
        
        return f"""---
{fm_yaml}---

# {meeting} 第{round_num}回

{content_text}
"""