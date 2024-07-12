from collections import defaultdict
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

    paragraph_dict: Dict[int, Paragraph] = field(default_factory=dict)
    author_dict: Dict[int, Author] = field(default_factory=dict)
    question_dict: Dict[int, Question] = field(default_factory=dict)
    answer_dict: Dict[int, Answer] = field(default_factory=dict)
    rating_dict: Dict[int, Rating] = field(default_factory=dict)

    questions_by_paragraph: Dict[int, List[Question]] = field(
        default_factory=lambda: defaultdict(list)
    )
    answers_by_question: Dict[int, List[Answer]] = field(
        default_factory=lambda: defaultdict(list)
    )
    ratings_by_answer: Dict[int, List[Rating]] = field(
        default_factory=lambda: defaultdict(list)
    )

    def __post_init__(self):
        self.build_lookup_dicts()

    def build_lookup_dicts(self):
        self.paragraph_dict = {p.id: p for p in self.paragraphs}
        self.author_dict = {a.id: a for a in self.authors}
        self.question_dict = {q.id: q for q in self.questions}
        self.answer_dict = {a.id: a for a in self.answers}
        self.rating_dict = {r.id: r for r in self.ratings}

        for question in self.questions:
            self.questions_by_paragraph[question.paragraph_id].append(question)

        for answer in self.answers:
            self.answers_by_question[answer.question_id].append(answer)

        for rating in self.ratings:
            self.ratings_by_answer[rating.answer_id].append(rating)

    def add_paragraph(self, paragraph: Paragraph):
        self.paragraphs.append(paragraph)
        self.paragraph_dict[paragraph.id] = paragraph

    def add_author(self, author: Author):
        self.authors.append(author)
        self.author_dict[author.id] = author

    def add_question(self, question: Question):
        self.questions.append(question)
        self.question_dict[question.id] = question
        self.questions_by_paragraph[question.paragraph_id].append(question)

    def add_answer(self, answer: Answer):
        self.answers.append(answer)
        self.answer_dict[answer.id] = answer
        self.answers_by_question[answer.question_id].append(answer)

    def add_rating(self, rating: Rating):
        self.ratings.append(rating)
        self.rating_dict[rating.id] = rating
        self.ratings_by_answer[rating.answer_id].append(rating)

    def get_paragraph(self, paragraph_id: int) -> Optional[Paragraph]:
        return self.paragraph_dict.get(paragraph_id)

    def get_author(self, author_id: int) -> Optional[Author]:
        return self.author_dict.get(author_id)

    def get_question(self, question_id: int) -> Optional[Question]:
        return self.question_dict.get(question_id)

    def get_answer(self, answer_id: int) -> Optional[Answer]:
        return self.answer_dict.get(answer_id)

    def get_rating(self, rating_id: int) -> Optional[Rating]:
        return self.rating_dict.get(rating_id)

    def get_questions_for_paragraph(self, paragraph_id: int) -> List[Question]:
        return self.questions_by_paragraph.get(paragraph_id, [])

    def get_answers_for_question(self, question_id: int) -> List[Answer]:
        return self.answers_by_question.get(question_id, [])

    def get_ratings_for_answer(self, answer_id: int) -> List[Rating]:
        return self.ratings_by_answer.get(answer_id, [])


dataset = Dataset()
