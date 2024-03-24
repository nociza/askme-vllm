from sqlalchemy import Column, Integer, String, Text, Boolean, Enum
from fleecekmbackend.db.ctl import Base


class Paragraph(Base): # change to paragraph
    __tablename__ = "paragraph"
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

    processed = Column(Integer, default=-1) # -1 for not processed, > 0 for processed and order

class Author(Base):
    __tablename__ = "author"
    id = Column(Integer, primary_key=True, index=True)
    model = Column(String) # can be human
    prompt = Column(String, nullable=True) 
    username = Column(String, nullable=True)

class Question(Base):
    __tablename__ = "question"
    id = Column(Integer, primary_key=True, index=True)
    paragraph_id = Column(Integer) 
    scope = Column(String) # the scope of the question, e.g. "single-paragraph"
    context = Column(Text) # the context of the question for a fair zeroshot evaluation
    text = Column(Text)
    author_id = Column(Integer) 
    timestamp = Column(String) 
    upvote = Column(Integer)
    downvote = Column(Integer)

class Answer(Base):
    __tablename__ = "answer"
    id = Column(Integer, primary_key=True, index=True)
    question_id = Column(Integer)
    author_id = Column(Integer)
    setting = Column(Enum("zs", "ic", "human"))
    timestamp = Column(String)
    text = Column(Text)

class Rating(Base):
    __tablename__ = "rating"
    id = Column(Integer, primary_key=True, index=True)
    text = Column(Text) # rationale for the rating
    value = Column(Integer) # score from 1 to 5
    answer_id = Column(Integer)
    author_id = Column(Integer)
    timestamp = Column(String)
