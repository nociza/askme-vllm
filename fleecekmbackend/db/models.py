from sqlalchemy import Column, Integer, String, Text, Boolean
from fleecekmbackend.db.ctl import Base
import hashlib


class WikiTextStructured(Base):
    __tablename__ = "wiki_text_structured"
    id = Column(Integer, primary_key=True, index=True)
    page_name = Column(String)
    section_name = Column(String)
    subsection_name = Column(String)
    subsubsection_name = Column(String)
    text = Column(Text)
    section_hierarchy = Column(String)
    text_cleaned = Column(Text)
    word_count = Column(Integer)
    is_bad = Column(Boolean)

class WikiTextQA(Base):
    __tablename__ = "wiki_text_qa"
    id = Column(Integer, primary_key=True, index=True)
    paragraph = Column(Text)
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
