from sqlalchemy.orm import DeclarativeBase

from fleecekm_server.db.meta import meta


class Base(DeclarativeBase):
    """Base for all models."""

    metadata = meta
