from sqlalchemy import Column, Integer, String, Text, Boolean, Enum
from fleecekmbackend.db.ctl import Base
import hashlib


class WikiTextStructured(Base): # change to paragraph
    __tablename__ = "wiki_text_structured"
    id = Column(Integer, primary_key=True, index=True)
    page_name = Column(String)
    section_name = Column(String)
    subsection_name = Column(String)
    subsubsection_name = Column(String)
    text = Column(Text)
    section_hierarchy = Column(String)
    text_cleaned = Column(Text)
    word_count = Column(Integer) # use character count to estimate
    is_bad = Column(Boolean) # change to llm_quality_check or similar
    within_page_order = Column(Integer)

class Author(Base):
    id = Column(Integer, primary_key=True, index=True)
    model = Column(String) # can be human
    prompt = Column(String, optional=True) 
    username = Column(String, optional=True)

class Question(Base):
    id = Column(Integer, primary_key=True, index=True)
    paragraph_id = Column(Integer) 
    scope = Column(String)
    text = Column(Text)
    author_id = Column(Integer) 
    timestamp = Column(String) 
    upvote = Column(Integer)
    downvote = Column(Integer)

class Answer(Base):
    id = Column(Integer, primary_key=True, index=True)
    question_id = Column(Integer)
    author_id = Column(Integer)
    setting = Column(Enum("zs", "ic", "human"))

    timestamp = Column(String)
    text = Column(Text)

class Rating(Base):
    id = Column(Integer, primary_key=True, index=True)
    text = Column(Text)
    value = Column(Integer)
    answer_id = Column(Integer)
    author_id = Column(Integer)
    timestamp = Column(String)

class WikiTextQA(Base):
    __tablename__ = "wiki_text_qa"
    id = Column(Integer, primary_key=True, index=True)
    paragraph = Column(Text) # change to paragraph_hash
    paragraph_hash = Column(Integer)
    question = Column(Text) 
    ans_zs = Column(Text)
    ans_ic = Column(Text)
    rating_zs_score = Column(Integer)
    rating_zs_rationale = Column(Text)
    rating_ic_score = Column(Integer)
    rating_ic_rationale = Column(Text)
    scope = Column(Text, default="single-paragraph")
    hash = Column(String)

    def __hash__(self):
        data = f"{self.paragraph}{self.question}{self.ans_zs}{self.ans_ic}".encode('utf-8')
        return hashlib.sha256(data).hexdigest()

    def __eq__(self, other):
        if isinstance(other, WikiTextQA):
            return self.hash == other.hash
        return False