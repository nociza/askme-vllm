from sqlalchemy.orm import DeclarativeBase

from FleeceKMBackend.db.meta import meta


class Base(DeclarativeBase):
    """Base for all models."""

    metadata = meta
