# connection.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

DB_USER = "avnadmin"
DB_NAME = "defaultdb"
DB_HOST = "pg-602cf04-teusmath89-cc57.h.aivencloud.com"
DB_PORT = 23430
DB_PASSWORD = "AVNS_Q2o5_cku1dndcK4X0nt" 

DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

def get_session():
    return SessionLocal()