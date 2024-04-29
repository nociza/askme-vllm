import logging
import asyncio

from fastapi import FastAPI, Request
from contextlib import asynccontextmanager

from fleecekmbackend.api.dataset.raw import router as raw_dataset_router
from fleecekmbackend.api.dataset.qa import router as qa_dataset_router
from fleecekmbackend.db.ctl import create_tables_if_not_exist
from fleecekmbackend.db.helpers import load_csv_data
from fleecekmbackend.core.config import DATASET_PATH
from fleecekmbackend.services.dataset.async_generate_qa import start_background_process

load_csv_lock = asyncio.Lock()
background_process_lock = asyncio.Lock()

@asynccontextmanager
async def lifespan(app: FastAPI):
    await create_tables_if_not_exist()

    async with load_csv_lock:
        try:
            with open(DATASET_PATH, "r") as file:
                await load_csv_data(file)
        except FileNotFoundError:
            logging.error("CSV file not found. Skipping data loading.")
        except Exception as e:
            logging.error(f"Error loading CSV data: {str(e)}")

    # async with background_process_lock:
    #     asyncio.create_task(start_background_process())

    yield

app = FastAPI(lifespan=lifespan)

# Include sub-routers
app.include_router(raw_dataset_router, prefix="/raw", tags=["raw"])
app.include_router(qa_dataset_router, prefix="/qa", tags=["qa"])

@app.get("/")
async def read_root():
    return {"message": "Welcome to the WikiText API!"}