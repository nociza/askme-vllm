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
        # Check if the table exists and has data
        conn = db.connection()
        has_table = db.bind.has_table(conn, WikiTextQA.__tablename__)

        if not has_table:
            # Create the table if it doesn't exist
            WikiTextQA.__table__.create(bind=db.bind)
            db.commit()  # Commit the table creation transaction

        samples = await get_random_samples_raw_as_df(n, db)
        try:
            for sample in samples:
                processed_qa_obj = await process_row(sample, db)
                # Check if the QA object already exists in the database based on hash comparison
                existing_qa_obj = db.query(WikiTextQA).filter(WikiTextQA.hash == processed_qa_obj.hash).first()
                if existing_qa_obj is None:
                    # QA object does not exist, append it to the database
                    db.add(processed_qa_obj)
            db.commit()  # Commit the data insertion transaction
        except Exception as e:
            db.rollback()  # Rollback the data insertion transaction if an error occurs
            raise e

    except Exception as e:
        logging.error(f"Error loading random samples: {str(e)}")
    finally:
        logging.info("Random samples loaded successfully")

# only samples from the existing database, no new samples are created
@router.get("/rand-sample-existing")
async def random_samples_existing(n: int, db: Session = Depends(get_db)):
    query = db.query(WikiTextQA).order_by(func.random()).limit(n)
    samples = query.all()
    return samples
