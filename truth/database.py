from sqlalchemy import create_engine
from sqlmodel import Session

DATABASE_URL = "duckdb:///kg.db"

engine = create_engine(DATABASE_URL, echo=True)


class Database:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Database, cls).__new__(cls)
            cls._instance.db = Session(engine)
        return cls._instance
