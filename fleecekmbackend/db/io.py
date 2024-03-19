from fleecekmbackend.db.database import async_session
from fleecekmbackend.db.models import WikiTextStructured
from fleecekmbackend.db.database import async_session
import pandas as pd

async def load_csv_data(file):
   async with async_session() as db:
       try:
           df = pd.read_csv(file)
           # Add more data processing here
           
           # Check if the table exists
           conn = await db.connection()
           has_table = await conn.run_sync(
               lambda conn: conn.dialect.has_table(conn, WikiTextStructured.__tablename__)
           )
           
           if not has_table:
               # Create the table
               await conn.run_sync(WikiTextStructured.__table__.create)
               
           # Lock the table
           await conn.execute(
               f"LOCK TABLE {WikiTextStructured.__tablename__} IN ACCESS EXCLUSIVE MODE"
           )
           
           # Insert the data into the database
           await conn.run_sync(
               lambda conn: df.to_sql(
                   name=WikiTextStructured.__tablename__,
                   con=conn,
                   if_exists="append",
                   index=False,
               )
           )
           
           await db.commit()
           
       except Exception as e:
           print(f"Error loading CSV data helper: {str(e)}")
           await db.rollback()
           
       finally:
           print("Data loaded successfully")