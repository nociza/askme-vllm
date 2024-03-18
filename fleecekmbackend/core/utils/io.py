from fleecekmbackend.db.database import SessionLocal
from fleecekmbackend.db.models import WikiTextStructured
import pandas as pd

async def load_csv_data(file):
    db = SessionLocal()
    try:
        df = pd.read_csv(file)
        # Add more data processing here

        # Check if the table exists
        if not await db.connection().dialect.has_table(db.connection(), WikiTextStructured.__tablename__):
            # Create the table
            await WikiTextStructured.__table__.create(db.bind)

        # Lock the table
        await db.execute(
            f"LOCK TABLE {WikiTextStructured.__tablename__} IN ACCESS EXCLUSIVE MODE"
        )

        # Insert the data into the database
        await df.to_sql(
            name=WikiTextStructured.__tablename__,
            con=db.bind,
            if_exists="append",
            index=False,
        )
        await db.commit()
    except Exception as e:
        print(f"Error loading CSV data helper: {str(e)}")
        await db.rollback()
    finally:
        print("Data loaded successfully")
        await db.close()
