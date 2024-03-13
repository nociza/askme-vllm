import pandas as pd
from io import StringIO
from fleecekmbackend.db.database import SessionLocal
from fleecekmbackend.db.models import WikiTextStructured


async def load_csv_data(file):
    db = SessionLocal()
    try:
        df = pd.read_csv(file)
        # Add more data processing here

        # Insert the data into the database
        df.to_sql(
            name=WikiTextStructured.__tablename__,
            con=db.bind,
            if_exists="append",
            index=False,
        )
        db.commit()
    except Exception as e:
        print(f"Error loading CSV data helper: {str(e)}")
        db.rollback()
    finally:
        print("Data loaded successfully")
        db.close()
