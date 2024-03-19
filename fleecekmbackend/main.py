from fastapi import FastAPI
from fleecekmbackend.api.dataset.raw import router as raw_dataset_router
from fleecekmbackend.api.dataset.qa import router as qa_dataset_router
from fleecekmbackend.db.ctl import create_tables_if_not_exist
from fleecekmbackend.db.helpers import load_csv_data
from fleecekmbackend.core.config import DATASET_PATH
import logging

app = FastAPI()

# Include sub-routers
app.include_router(raw_dataset_router, prefix="/raw", tags=["raw"])
app.include_router(qa_dataset_router, prefix="/qa", tags=["qa"])


@app.get("/")
async def read_root():
    return {"message": "Welcome to the WikiText API!"}


@app.on_event("startup")
async def startup_event():
    await create_tables_if_not_exist()
    try:
        with open(DATASET_PATH, "r") as file:
            await load_csv_data(file)
    except FileNotFoundError:
        logging.error("CSV file not found. Skipping data loading.")
    except Exception as e:
        logging.error(f"Error loading CSV data: {str(e)}")
