"""
Pydantic models for structured data processing.
"""
from typing import List, Optional
from pydantic import BaseModel

class MiniSection(BaseModel):
    title: str
    content: str

class MiniSummary(BaseModel):
    sections: List[MiniSection]

class ExtractionResult(BaseModel):
    named_entities: List[str]
    numbers: List[str]
    todos: List[str]

class MeetingSummary(BaseModel):
    summary: str
    main_arguments: List[str]
    discussion_flow: str
    action_items: List[str]
    open_issues: List[str]
    named_entities: List[str]
    tags: List[str]

class FrontMatter(BaseModel):
    date: str
    meeting: str
    round: int
    source_pdf: str
    status: str
    tags: List[str]