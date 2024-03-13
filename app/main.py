from fastapi import FastAPI
from app.api.dataset import csv_loader, random_samples
from app.db.database import create_tables

app = FastAPI()

app.include_router(csv_loader.router)
app.include_router(random_samples.router)


@app.on_event("startup")
async def startup_event():
    await create_tables()
