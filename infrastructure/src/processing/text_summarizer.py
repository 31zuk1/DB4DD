"""
Text summarization and analysis using OpenAI API.
"""
import logging
from typing import Dict, List, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from core.api_client import APIClient
from core.models import MeetingSummary, MiniSummary, ExtractionResult
from processing.prompt_manager import PromptManager

logger = logging.getLogger(__name__)

class TextSummarizer:
    """Handles text summarization with structured outputs."""
    
    def __init__(self, api_client: APIClient, chunk_size: int = 3000):
        self.api_client = api_client
        self.chunk_size = chunk_size
        self.pm = PromptManager()
    
    def power_summary(self, raw_text: str, nocache: bool = False) -> Dict[str, Any]:
        """Generate comprehensive summary with multi-stage processing."""
        # Check if text is too large and needs special handling
        estimated_tokens = len(raw_text) / 4  # Rough estimate: 1 token ≈ 4 chars
        if estimated_tokens > 500000:  # Extremely large document (>2MB)
            logger.warning(f"Extremely large document detected ({len(raw_text)} chars, ~{int(estimated_tokens)} tokens) - using truncation")
            # For massive documents, truncate and use small chunks
            raw_text = raw_text[:100000]  # Keep only first 100k chars
            optimal_chunk_size = 500
        elif estimated_tokens > 100000:  # Very large document
            logger.warning(f"Very large document detected ({len(raw_text)} chars, ~{int(estimated_tokens)} tokens)")
            # Use smaller chunk size for large documents
            optimal_chunk_size = min(500, self.chunk_size // 4)
        else:
            # Intelligent chunking - optimize chunk size based on content
            optimal_chunk_size = min(self.chunk_size, max(1000, len(raw_text) // 20))
        
        chunks = [raw_text[i:i+optimal_chunk_size] for i in range(0, len(raw_text), optimal_chunk_size)]
        
        # Smart batching - group small chunks together
        if len(chunks) > 50:  # If too many small chunks
            batch_size = len(chunks) // 25  # Target ~25 batches
            batched_chunks = []
            for i in range(0, len(chunks), batch_size):
                batch_content = '\n\n---CHUNK_SEPARATOR---\n\n'.join(chunks[i:i+batch_size])
                batched_chunks.append(batch_content)
            chunks = batched_chunks
        
        # Dynamic worker adjustment based on rate limiter
        effective_workers = min(self.api_client.rate_limiter.max_concurrent, len(chunks), 40)
        logger.info(f"Processing {len(chunks)} optimized chunks with {effective_workers} workers")
        
        # 1) Extract information from all chunks (parallel processing)
        extractions = []
        with ThreadPoolExecutor(max_workers=effective_workers) as ex:
            extraction_futures = [
                ex.submit(
                    self.api_client.structured_chat,
                    [{'role': 'system', 'content': self.pm.get('extract', text=chunk)}],
                    ExtractionResult,
                    400,
                    not nocache
                )
                for chunk in chunks
            ]
            for f in as_completed(extraction_futures):
                extractions.append(f.result())
        
        # 2) Detailed mini summaries (all chunks)
        mini_summaries = []
        with ThreadPoolExecutor(max_workers=effective_workers) as ex:
            futures = [
                ex.submit(
                    self.api_client.structured_chat,
                    [{'role': 'system', 'content': self.pm.get('detailed_mini', text=chunk)}],
                    MiniSummary,
                    800,
                    not nocache
                )
                for chunk in chunks
            ]
            for f in as_completed(futures):
                mini_summaries.append(f.result())
        
        # 3) Full text detailed analysis
        full_text_analysis = self.api_client.structured_chat(
            [{'role': 'system', 'content': self.pm.get('deep_analysis', full_text=raw_text[:5000])}],
            MiniSummary,
            1000,
            not nocache
        )
        mini_summaries.append(full_text_analysis)
        
        # 4) Combine sections by category
        combined_sections = {}
        for mini in mini_summaries:
            for section in mini.sections:
                title = section.title.strip()
                if title not in combined_sections:
                    combined_sections[title] = []
                combined_sections[title].append(section.content.strip())
        
        # 5) Generate summary text
        newline = chr(10)
        section_text = newline.join(f"### {title}{newline}{newline.join(contents)}" for title, contents in combined_sections.items())
        
        all_entities = list(dict.fromkeys([entity for ext in extractions for entity in ext.named_entities]))[:20]
        all_numbers = list(dict.fromkeys([num for ext in extractions for num in ext.numbers]))[:15]
        all_todos = list(dict.fromkeys([todo for ext in extractions for todo in ext.todos]))[:15]
        
        summary_text = f"""
主要セクション:
{section_text}

抽出された情報:
人物・組織: {', '.join(all_entities)}
重要な数値: {', '.join(all_numbers)}
TODO/アクション: {', '.join(all_todos)}
"""
        
        # 6) Enhanced final summary
        final_summary = self.api_client.structured_chat(
            [{'role': 'system', 'content': self.pm.get('enhanced_final', 
                                                     summary_text=summary_text,
                                                     extraction_text="",
                                                     full_text_sample=raw_text[:2000])}],
            MeetingSummary,
            1500,
            not nocache
        )
        
        # Convert to dict format
        return {
            'summary': final_summary.summary,
            'main_arguments': final_summary.main_arguments,
            'discussion_flow': final_summary.discussion_flow,
            'action_items': final_summary.action_items,
            'open_issues': final_summary.open_issues,
            'named_entities': final_summary.named_entities,
            'tags': final_summary.tags,
            'outline': summary_text
        }
    
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