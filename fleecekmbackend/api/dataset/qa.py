from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from fleecekmbackend.db.helpers import get_random_samples_raw_as_df
from fleecekmbackend.db.ctl import get_db
from fleecekmbackend.services.dataset.fleece_qa import process_row
from fleecekmbackend.db.models import WikiTextQA
from sqlalchemy import func, select
import logging

logging.getLogger().addHandler(logging.StreamHandler())

router = APIRouter()


@router.get("/rand-sample-create")
async def random_samples_create(n: int, db: Session = Depends(get_db)):
    try:
        conn = await db.connection()

        # create the table if it doesn't exist
        has_table = await conn.run_sync(
            lambda conn: conn.dialect.has_table(conn, WikiTextQA.__tablename__)
        )
        if not has_table:
            await conn.run_sync(WikiTextQA.__table__.create)
            await conn.commit()

        samples_df = await get_random_samples_raw_as_df(n, db)
        try:
            processed_qa_objs = []
            for _, sample in samples_df.iterrows():
                processed_qa_obj = await process_row(sample)
                existing_qa_obj = db.query(WikiTextQA).filter(WikiTextQA.hash == processed_qa_obj.hash).first()
                if existing_qa_obj is None:
                    db.add(processed_qa_obj)
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
    query = select(WikiTextQA).order_by(func.random()).limit(n)
    result = db.execute(query)
    samples = result.scalars().all()
    return samples
