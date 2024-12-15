from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import URL

DATABASE_URL = URL.create(
    drivername='postgresql'
    , username='globant'
    , password='globant'
    , host='localhost'
    , database='globant_challenge'
    , port=5432
)

engine = create_engine(DATABASE_URL)
metadata = MetaData()

Session = sessionmaker(bind=engine)

def init_db():
    metadata.create_all(engine)