from fastapi import FastAPI
from app.api.dataset import sample
from app.db.database import create_tables, delete_tables, engine
from app.core.utils import load_csv_data
from fastapi import FastAPI, BackgroundTasks

app = FastAPI()

app.include_router(sample.router)


@app.on_event("startup")
async def startup_event():
    await create_tables()
    print("Tables created")
    try:
        with open("data/wiki_text_structured.csv", "r") as file:
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
