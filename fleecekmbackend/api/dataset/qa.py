from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from fleecekmbackend.db.helpers import get_random_samples_raw_as_df
from fleecekmbackend.db.ctl import get_db
from fleecekmbackend.services.dataset.fleece_qa import process_paragraphs
from fleecekmbackend.db.models import Paragraph, Author, Question, Answer, Rating
from sqlalchemy import func, select
import logging

logging.getLogger().addHandler(logging.StreamHandler())
router = APIRouter()

@router.get("/rand-sample-create")
async def random_samples_create(n: int, db: Session = Depends(get_db)):
    try:
        conn = await db.connection()
        
        # Create the tables if they don't exist
        tables = [Paragraph, Author, Question, Answer, Rating]
        for table in tables:
            has_table = await conn.run_sync(
                lambda conn: conn.dialect.has_table(conn, table.__tablename__)
            )
            if not has_table:
                await conn.run_sync(table.__table__.create)
        await conn.commit()
        
        samples_df = await get_random_samples_raw_as_df(n, db)
        
        try:
            processed_qa_objs = []
            for _, sample in samples_df.iterrows():
                processed_qa_obj = await process_paragraphs(sample)
                
                # Check if the paragraph already exists
                existing_paragraph = db.query(Paragraph).filter_by(id=processed_qa_obj.paragraph_id).first()
                
                # Check if the author already exists
                existing_author = db.query(Author).filter_by(id=processed_qa_obj.author_id).first()
                if existing_author is None:
                    author = Author(
                        id=processed_qa_obj.author_id,
                        model=processed_qa_obj.model,
                        prompt=processed_qa_obj.prompt,
                        username=processed_qa_obj.username
                    )
                    db.add(author)
                
                # Check if the question already exists
                existing_question = db.query(Question).filter_by(id=processed_qa_obj.question_id).first()
                if existing_question is None:
                    question = Question(
                        id=processed_qa_obj.question_id,
                        paragraph_id=processed_qa_obj.paragraph_id,
                        scope=processed_qa_obj.scope,
                        text=processed_qa_obj.question_text,
                        author_id=processed_qa_obj.author_id,
                        timestamp=processed_qa_obj.timestamp,
                        upvote=processed_qa_obj.upvote,
                        downvote=processed_qa_obj.downvote
                    )
                    db.add(question)
                
                # Check if the answer already exists
                existing_answer = db.query(Answer).filter_by(id=processed_qa_obj.answer_id).first()
                if existing_answer is None:
                    answer = Answer(
                        id=processed_qa_obj.answer_id,
                        question_id=processed_qa_obj.question_id,
                        author_id=processed_qa_obj.author_id,
                        setting=processed_qa_obj.setting,
                        timestamp=processed_qa_obj.timestamp,
                        text=processed_qa_obj.answer_text
                    )
                    db.add(answer)
                
                # Check if the rating already exists
                existing_rating = db.query(Rating).filter_by(id=processed_qa_obj.rating_id).first()
                if existing_rating is None:
                    rating = Rating(
                        id=processed_qa_obj.rating_id,
                        text=processed_qa_obj.rating_text,
                        value=processed_qa_obj.rating_value,
                        answer_id=processed_qa_obj.answer_id,
                        author_id=processed_qa_obj.author_id,
                        timestamp=processed_qa_obj.timestamp
                    )
                    db.add(rating)
                
                processed_qa_objs.append(processed_qa_obj)
            
            db.commit()
            return processed_qa_objs
        
        except Exception as e:
            db.rollback()  # Rollback the data insertion transaction if an error occurs
            raise e
    
    except Exception as e:
        logging.error(f"Error loading random samples: {str(e)}")
        raise e

# only samples from the existing database, no new samples are created
@router.get("/rand-sample-existing")
async def random_samples_existing(n: int, db: Session = Depends(get_db)):
    query = select(Question).order_by(func.random()).limit(n)
    result = db.execute(query)
    samples = result.scalars().all()
    return samples