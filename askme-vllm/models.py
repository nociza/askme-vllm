from typing import List, Dict, Optional
from dataclasses import dataclass, field
import hashlib


@dataclass
class Paragraph:
    id: int
    page_name: str
    section_name: str
    subsection_name: Optional[str] = None
    subsubsection_name: Optional[str] = None
    text: str = ""
    section_hierarchy: str = ""
    text_cleaned: str = ""
    word_count: int = 0
    is_bad: bool = False
    within_page_order: int = 0
    processed: bool = False
    original_entry_id: Optional[int] = None


@dataclass
class Author:
    id: int
    model: str
    prompt: Optional[str] = None
    username: Optional[str] = None
    hash: str = field(init=False)

    def __post_init__(self):
        self.hash = self.generate_hash(self.model, self.prompt)

    @staticmethod
    def generate_hash(model: str, prompt: Optional[str]) -> str:
        return hashlib.sha256(f"{model}:{prompt}".encode("utf-8")).hexdigest()


@dataclass
class Question:
    id: int
    paragraph_id: int
    scope: str
    context: str
    text: str
    author_id: int
    timestamp: str
    upvote: int = 0
    downvote: int = 0
    turns: str = "single"
    filtered: bool = False
    is_answerable_zs: bool = True
    is_answerable_ic: bool = True
    rejected: bool = False
    processed: bool = False


@dataclass
class Answer:
    id: int
    question_id: int
    author_id: int
    setting: str
    timestamp: str
    text: str
    processed: bool = False


@dataclass
class Rating:
    id: int
    text: str
    value: int
    answer_id: int
    author_id: int
    timestamp: str


@dataclass
class Dataset:
    paragraphs: List[Paragraph] = field(default_factory=list)
    authors: List[Author] = field(default_factory=list)
    questions: List[Question] = field(default_factory=list)
    answers: List[Answer] = field(default_factory=list)
    ratings: List[Rating] = field(default_factory=list)


dataset = Dataset()
