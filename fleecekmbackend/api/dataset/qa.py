from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from fleecekmbackend.db.ctl import get_db, async_session
from fleecekmbackend.db.models import Paragraph, Question, Answer, Author, Rating
from fleecekmbackend.services.dataset.fleece_qa import generate_answer_rating
from sqlalchemy import func, select
import logging

logging.getLogger().addHandler(logging.StreamHandler())
logging.getLogger().setLevel(logging.INFO)
router = APIRouter()

@router.get("/random-samples")
async def random_samples_existing(n: int, db: Session = Depends(get_db)):
    async with async_session() as session:
        query = select(Question).order_by(func.random()).limit(n)
        result = await session.execute(query)
        samples = result.scalars().all()
        return samples

# rate an answer to a question using llm 
@router.post("/rate-answer")
async def rate_answer(user_name: str, answer: str, question_id: str, db: Session = Depends(get_db)):
    async with async_session() as session:
        existing_author = (await session.execute(select(Author).where(Author.username == user_name))).scalar()
        if existing_author is None:
            author = Author(username=user_name, model="human")
            session.add(author)
            await session.commit()
            await session.refresh(author, ["id"])
            author_id = author.id
        else:
            author_id = existing_author.id
        
        existing_question = (await session.execute(select(Question).where(Question.id == question_id))).scalar()

        if existing_question is None:
            return {"error": "question not found"}
        
        existing_answer = (await session.execute(select(Answer).where(Answer.question_id == question_id and Answer.author_id == author_id))).scalar()
        if existing_answer is not None:
            return {"error": "answer already rated"}
        
        answer = Answer(question_id=question_id, author_id=author_id, setting="human", text=answer)
        session.add(answer)
        await session.commit()
        await session.refresh(answer, ["id"])
        answer_id = answer.id

        rating_id = generate_answer_rating(answer_id, question_id)
        rating = await session.execute(select(Rating).where(Rating.id == rating_id)).scalar()
        return {
            "id": answer_id,
            "value": rating.value,
            "text": rating.text
        }

# store the user rating the quality of a question
@router.post("/user-rate-question")
async def user_rate_question(user_name: str, question_id: str, value: int, text: str, db: Session = Depends(get_db)):
    async with async_session() as session:
        existing_author = (await session.execute(select(Author).where(Author.username == user_name))).scalar()
        if existing_author is None:
            author = Author(username=user_name, model="human")
            session.add(author)
            await session.commit()
            await session.refresh(author, ["id"])
            author_id = author.id
        else:
            author_id = existing_author.id
        
        existing_question = (await session.execute(select(Question).where(Question.id == question_id))).scalar()

        if existing_question is None:
            return {"error": "question not found"}
        
        existing_rating = (await session.execute(select(Rating).where(Rating.answer_id == question_id and Rating.author_id == author_id))).scalar()
        if existing_rating is not None:
            return {"error": "question already rated"}
        
        rating = Rating(answer_id=question_id, author_id=author_id, value=value, text=text)
        session.add(rating)
        await session.commit()
        await session.refresh(rating, ["id"])
        return {
            "id": rating.id,
            "value": rating.value,
            "text": rating.text
        }

    
@router.get("/progress") # get the progress of the qa generation
async def get_progress(db: Session = Depends(get_db)):
    async with async_session() as session:
        largest_processed = (await session.execute(select(func.max(Paragraph.processed)))).scalar()
        if largest_processed is None:
            largest_processed = 0
        total = (await session.execute(select(func.count(Paragraph.id)))).scalar()
        return {"progress": largest_processed, "total": total, "percentage": largest_processed/total*100}
