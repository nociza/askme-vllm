from sqlalchemy import Column, Integer, String, Text
from app.db.database import Base


class WikiTextStructured(Base):
    __tablename__ = "wiki_text_structured"
    id = Column(Integer, primary_key=True, index=True)
    page_name = Column(String)
    section_name = Column(String)
    subsection_name = Column(String)
    subsubsection_name = Column(String)
    text = Column(Text)
