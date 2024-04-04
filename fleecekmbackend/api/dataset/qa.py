from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from fleecekmbackend.db.ctl import get_db, async_session
from fleecekmbackend.db.models import Paragraph, Question, Answer, Author, Rating
from fleecekmbackend.services.dataset.fleece_qa import generate_answer_rating, generate_fact_with_context
from sqlalchemy import func, select
import logging

logging.getLogger().addHandler(logging.StreamHandler())
logging.getLogger().setLevel(logging.INFO)
router = APIRouter()

@router.get("/random-sample-r2l")
async def random_sample_r2l(n: int):
    async with async_session() as session:
        query = select(Paragraph).where(Paragraph.processed != -1).order_by(func.random()).limit(n)
        paragraph = (await session.execute(query)).scalars().all()
        _, fact_with_context = generate_fact_with_context(paragraph[0])
        # get one random question that's related to the paragraph
        question = (await session.execute(select(Question).where(Question.paragraph_id == paragraph[0].id))).scalar()
        return {
            "paragraph": paragraph[0].text,
            "paragraph_id": paragraph[0].id,
            "fact_with_context": fact_with_context,
            "question": question.text,
            "question_id": question.id
        }

@router.get("/answer-generated")
async def answer_generated(question_id: str):
    async with async_session() as session:
        question = (await session.execute(select(Question).where(Question.id == question_id))).scalar()
        if question is None:
            return {"error": "question not found"}
        answer = (await session.execute(select(Answer).where(Answer.question_id == question_id))).scalar()
        if answer is None:
            return {"error": "answer not found"}
        rating = (await session.execute(select(Rating).where(Rating.answer_id == answer.id))).scalar()
        return {
            "answer": answer.text,
            "answer_id": answer.id,
            "rating": {
                "value": rating.value,
                "text": rating.text
            }
        }

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
        
        existing_answer = (await session.execute(select(Answer).where(Answer.question_id == question_id, Answer.author_id == author_id, Answer.text == answer))).scalar()
        if existing_answer is not None:
            return {"error": "answer already rated"}
        
        answer = Answer(question_id=question_id, author_id=author_id, setting="human", text=answer)
        session.add(answer)
        await session.commit()
        await session.refresh(answer, ["id"])
        answer_id = answer.id

        rating_id = await generate_answer_rating(db, question_id, answer_id)
        print(rating_id)
        rating = (await session.execute(select(Rating).where(Rating.id == rating_id))).scalar()
        return {
            "id": rating.id,
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
