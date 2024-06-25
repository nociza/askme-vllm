from sqlalchemy import Column, Integer, String, Text, Boolean, Enum, UniqueConstraint
from fleecekmbackend.db.ctl import Base


class Paragraph(Base):
    __tablename__ = "paragraph"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    page_name = Column(String(1023))
    section_name = Column(String(1023))
    subsection_name = Column(String(1023))
    subsubsection_name = Column(String(1023))
    text = Column(Text)
    section_hierarchy = Column(String(8191))
    text_cleaned = Column(Text)
    word_count = Column(Integer)  # use character count to estimate
    is_bad = Column(Boolean)  # change to llm_quality_check or similar
    within_page_order = Column(Integer)

    processed = Column(Boolean, default=False)


class Author(Base):
    __tablename__ = "author"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    model = Column(String(1023))  # can be human
    prompt = Column(Text, nullable=True)
    username = Column(String(1023), nullable=True)

    __table_args__ = (UniqueConstraint("model", "prompt", name="_model_prompt_uc"),)


class Question(Base):
    __tablename__ = "question"
    id = Column(Integer, primary_key=True, index=True)
    paragraph_id = Column(Integer, index=True)
    scope = Column(String(1023))  # the scope of the question, e.g. "single-paragraph"
    context = Column(Text)  # the context of the question for a fair zeroshot evaluation
    text = Column(Text)
    author_id = Column(Integer)
    timestamp = Column(String(255))
    upvote = Column(Integer)
    downvote = Column(Integer)
    turns = Column(
        String(63), default="multi", index=True
    )  # multi, single, or followup

    filtered = Column(Boolean, default=False)
    is_answerable_zs = Column(Boolean, default=True)
    is_answerable_ic = Column(Boolean, default=True)
    rejected = Column(Boolean, default=False)

    processed = Column(Boolean, default=False)


class Answer(Base):
    __tablename__ = "answer"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    question_id = Column(Integer)
    author_id = Column(Integer)
    setting = Column(Enum("zs", "ic", "human"))
    timestamp = Column(String(255))
    text = Column(Text)

    processed = Column(Boolean, default=False)


class Rating(Base):
    __tablename__ = "rating"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    text = Column(Text)  # rationale for the rating
    value = Column(Integer)  # score from 1 to 5
    answer_id = Column(Integer)
    author_id = Column(Integer)
    timestamp = Column(String(255))


class Metadata(Base):
    __tablename__ = "metadata"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    key = Column(String(1023))
    value = Column(Text)


class RejectedQuestion(Base):
    __tablename__ = "rejected_question"
    id = Column(Integer, primary_key=True, index=True)
    paragraph_id = Column(Integer, index=True)
    scope = Column(String(1023))  # the scope of the question, e.g. "single-paragraph"
    context = Column(Text)  # the context of the question for a fair zeroshot evaluation
    text = Column(Text)
    author_id = Column(Integer)
    timestamp = Column(String(255))
    turns = Column(
        String(63), default="multi", index=True
    )  # multi, single, or followup
    is_answerable_zs = Column(
        Boolean, default=False
    )  # is the question answerable in a zero-shot setting
    is_answerable_ic = Column(
        Boolean, default=False
    )  # is the question answerable in an in-context setting


class Feedback(Base):
    __tablename__ = "feedback"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    text = Column(Text)
    question_id = Column(Integer)
    author_id = Column(Integer)
    timestamp = Column(String(255))
