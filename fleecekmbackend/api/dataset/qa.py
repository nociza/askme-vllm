from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from fleecekmbackend.db.ctl import get_db, async_session
from fleecekmbackend.db.models import Paragraph, Question, Answer, Author, Rating, Metadata
from fleecekmbackend.services.dataset.fleece_qa import generate_answer_rating, generate_fact_with_context
from sqlalchemy import func, select
import logging
import sys
import random


root = logging.getLogger()
root.setLevel(logging.ERROR)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.ERROR)
FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
formatter = logging.Formatter(FORMAT)
ch.setFormatter(formatter)
root.addHandler(ch)

router = APIRouter()

@router.get("/random-sample-r2l")
async def random_sample_r2l(n: int):
    async with async_session() as session:
        question = None
        while not question:
            max_offset = (await session.execute(select(Metadata.value).where(Metadata.key == "largest_processed"))).scalar()
            if max_offset is None:
                max_offset = (await session.execute(select(func.max(Paragraph.processed)))).scalar()
                metadata = Metadata(key="largest_processed", value=max_offset)
                session.add(metadata)
                await session.commit()

            offset = random.randint(0, int(max_offset))
            # get a random paragraph that has been processed
            paragraph = (await session.execute(select(Paragraph).where(Paragraph.processed != -1).offset(offset).limit(1))).scalar()
            if paragraph is None:
                continue
            _, fact_with_context = generate_fact_with_context(paragraph)
            # get one random question that's related to the paragraph
            question = (await session.execute(select(Question).where(Question.paragraph_id == paragraph.id))).scalar()
            
        return {
            "paragraph": paragraph.text_cleaned,
            "paragraph_id": paragraph.id, 
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
async def rate_answer(user_name: str, answer: str, question_id: str):
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
        await session.flush()
        await session.refresh(answer, ["id"])
        answer_id = answer.id

        rating_id = await generate_answer_rating(session, question_id, answer_id)
        await session.commit()
        rating = (await session.execute(select(Rating).where(Rating.id == rating_id))).scalar()
        return {
            "id": rating_id,
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
        largest_processed = (await session.execute(select(Metadata.value).where(Metadata.key == "largest_processed"))).scalar()
        if largest_processed is None:
            largest_processed = (await session.execute(select(func.max(Paragraph.processed))).scalar())
            metadata = Metadata(key="largest_processed", value=largest_processed)
            session.add(metadata)
            await session.commit()

        total = (await session.execute(select(Metadata.value).where(Metadata.key == "num_paragraphs"))).scalar()
        if total is None:
            total = (await session.execute(select(func.max(Paragraph.id)))).scalar()
            metadata = Metadata(key="num_paragraphs", value=total)
            session.add(metadata)
            await session.commit()

        return {"progress": int(largest_processed), "total": int(total), "percentage": int(largest_processed) / int(total) * 100}

@router.get("/progress-accurate")
async def get_progress_accurate(db: Session = Depends(get_db)):
    async with async_session() as session:
        largest_processed = (await session.execute(select(func.max(Paragraph.processed)))).scalar()
        total = (await session.execute(select(func.max(Paragraph.id)))).scalar()
        
        metadata = Metadata(key="largest_processed", value=largest_processed)
        session.add(metadata)
        await session.commit()

        return {"progress": int(largest_processed), "total": int(total), "percentage": int(largest_processed) / int(total) * 100}