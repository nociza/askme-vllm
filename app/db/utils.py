from sqlalchemy import func
from app.db.models import WikiTextStructured


async def get_random_samples(n: int, db):
    query = db.query(WikiTextStructured).order_by(func.random()).limit(n)
    samples = query.all()
    return samples
