import asyncio
import logging
import time

from fleecekmbackend.db.ctl import create_tables_if_not_exist
from fleecekmbackend.db.helpers import load_csv_data, load_csv_data_top_n
from fleecekmbackend.core.config import DATASET_PATH, LOGGING_LEVEL
from fleecekmbackend.services.generation.end2end import start_background_process_e2e
from fleecekmbackend.services.generation.stage2stage import start_background_process_s2s

load_csv_lock = asyncio.Lock()
background_process_lock = asyncio.Lock()

logging.basicConfig(
    level=LOGGING_LEVEL, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


async def main():
    await create_tables_if_not_exist()

    async with load_csv_lock:
        try:
            with open(DATASET_PATH, "r") as file:
                # await load_csv_data(file)
                await load_csv_data_top_n(file, 100)
        except FileNotFoundError:
            logging.error("CSV file not found. Skipping data loading.")
        except Exception as e:
            logging.error(f"Error loading CSV data: {str(e)}")

    async with background_process_lock:
        print("Starting background process")
        await start_background_process_e2e()


if __name__ == "__main__":
    try:
        # time the execution of the main function
        start_time = time.time()
        asyncio.run(main())
        end_time = time.time()
        print(f"Execution time: {end_time - start_time}")
    except Exception as e:
        logging.error(f"Exception in main: {str(e)}")
    finally:
        logging.shutdown()
