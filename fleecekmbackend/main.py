from fastapi import FastAPI
from fleecekmbackend.api.dataset.sample import router as sample_router
from fleecekmbackend.db.database import create_tables_if_not_exist
from fleecekmbackend.core.utils.io import load_csv_data
from fleecekmbackend.core.config import DATASET_PATH
import logging

app = FastAPI()

# Include sub-routers
app.include_router(sample_router, prefix="/sample", tags=["sample"])


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
