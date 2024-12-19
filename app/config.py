from sqlalchemy import create_engine, MetaData, insert, delete
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.engine import URL

DATABASE = 'globant_challenge'


DATABASE_URL = URL.create(
    drivername='postgresql'
    , username='globant'
    , password='globant'
    , host='localhost'
    , database=DATABASE
    , port=5432
)

engine = create_engine(DATABASE_URL)
metadata = MetaData()

Session = sessionmaker(bind=engine)

def init_db():
    metadata.create_all(engine, checkfirst=True)

    data_catalog = [
        [1, 'departments', 'departments__1___1_'],
        [2, 'jobs', 'jobs'],
        [3, 'hired_employees', 'hired_employees__1___1_']
    ]

    table = metadata.tables['catalog_tables']

    for data in data_catalog:
        stmt_delete = delete(table).where(
            table.columns.id.in_([data[0]])
        )
        
        stmt_insert = insert(table).values(
            id=data[0],
            table_name=data[1],
            file_name=data[2]
        )

        
        with engine.connect() as conn:
            conn.execute(stmt_delete)
            conn.execute(stmt_insert)
            conn.commit()
