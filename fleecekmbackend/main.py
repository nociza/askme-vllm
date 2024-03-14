from fastapi import FastAPI
from fleecekmbackend.api.dataset.sample import router as sample_router
from fleecekmbackend.db.database import create_tables, delete_tables, engine
from fleecekmbackend.core.utils.io import load_csv_data
from fleecekmbackend.core.config import DATASET_PATH
from fastapi import FastAPI

app = FastAPI()

# Include sub-routers
app.include_router(sample_router, prefix="/sample", tags=["sample"])


@app.get("/")
async def read_root():
    return {"message": "Welcome to the WikiText API!"}


@app.on_event("startup")
async def startup_event():
    await create_tables()
    try:
        with open(DATASET_PATH, "r") as file:
            await load_csv_data(file)
    except FileNotFoundError:
        print("CSV file not found. Skipping data loading.")
    except Exception as e:
        print(f"Error loading CSV data: {str(e)}")


@app.on_event("shutdown")
async def shutdown_event():
    try:
        await delete_tables()
    except Exception as e:
        print(f"Error deleting tables: {str(e)}")
    finally:
        await engine.dispose()
