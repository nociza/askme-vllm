import asyncio
import logging

from fleecekmbackend.db.ctl import create_tables_if_not_exist
from fleecekmbackend.db.helpers import load_csv_data
from fleecekmbackend.core.config import DATASET_PATH
from fleecekmbackend.services.dataset.async_generate_qa import start_background_process

load_csv_lock = asyncio.Lock()
background_process_lock = asyncio.Lock()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


async def main():
    await create_tables_if_not_exist()

    async with load_csv_lock:
        try:
            with open(DATASET_PATH, "r") as file:
                await load_csv_data(file)
        except FileNotFoundError:
            logging.error("CSV file not found. Skipping data loading.")
        except Exception as e:
            logging.error(f"Error loading CSV data: {str(e)}")

    async with background_process_lock:
        print("Starting background process")
        await start_background_process()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logging.error(f"Exception in main: {str(e)}")
    finally:
        logging.shutdown()
