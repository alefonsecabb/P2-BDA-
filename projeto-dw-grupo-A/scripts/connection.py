# connection.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def get_engine():
    """
    Cria e retorna uma engine SQLAlchemy para conexão com o banco PostgreSQL.
    """
    DB_USER = "avnadmin"
    DB_NAME = "defaultdb"
    DB_HOST = "pg-602cf04-teusmath89-cc57.h.aivencloud.com"
    DB_PORT = 23430
    DB_PASSWORD = "AVNS_Q2o5_cku1dndcK4X0nt"

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