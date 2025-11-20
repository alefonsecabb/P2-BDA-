# connection.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def get_engine():
    """
    Cria e retorna uma engine SQLAlchemy para conexão com o banco PostgreSQL.
    """
    DB_USER = ""
    DB_NAME = ""
    DB_HOST = ""
    DB_PORT = 
    DB_PASSWORD = ""

    DATABASE_URL = (
        f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    return create_engine(DATABASE_URL)


def get_session():
    """
    Cria e retorna uma sessão SQLAlchemy.
    """
    engine = get_engine()
    SessionLocal = sessionmaker(bind=engine)
    return SessionLocal()
