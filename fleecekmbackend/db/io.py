from fleecekmbackend.db.database import async_session
from fleecekmbackend.db.models import WikiTextStructured
from fleecekmbackend.db.database import async_session
from sqlalchemy import func, select
import pandas as pd
import logging

logging.getLogger().addHandler(logging.StreamHandler())

async def load_csv_data(file):
    async with async_session() as db:
        try:
            # Check if the table exists and has data
            conn = await db.connection()
            has_table = await conn.run_sync(
                lambda conn: conn.dialect.has_table(conn, WikiTextStructured.__tablename__)
            )
            
            if has_table:
                # Check if the table has any data
                result = await conn.execute(select(func.count()).select_from(WikiTextStructured))
                count = result.scalar()
                if count > 0:
                    print(f"Dataset is already loaded with {count} entries. Skipping loading process.")
                    return
            
            # Load the dataset if the table doesn't exist or is empty
            df = pd.read_csv(file)
            # Add more data processing here
            
            if not has_table:
                # Create the table if it doesn't exist
                await conn.run_sync(WikiTextStructured.__table__.create)
                await conn.commit()  # Commit the table creation transaction
                
            try:
                # Insert the data into the database
                await conn.run_sync(
                    lambda conn: df.to_sql(
                        name=WikiTextStructured.__tablename__,
                        con=conn,
                        if_exists="append",
                        index=False,
                    )
                )
                
                # Commit the data insertion transaction
                await conn.commit()
                
            except Exception as e:
                # Rollback the data insertion transaction if an error occurs
                await conn.rollback()
                raise e
            
        except Exception as e:
            logging.error(f"Error loading CSV data helper: {str(e)}")
            
        finally:
            logging.info("Data loaded successfully")