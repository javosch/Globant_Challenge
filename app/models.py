from sqlalchemy import Table, Column, Integer, String, DateTime
from app.config import metadata

catalog_tables = Table(
    'catalog_tables', metadata,
    Column('id', Integer, primary_key=True),
    Column('table_name', String, nullable=False),
    Column('file_name', String, nullable=False)
)

departments = Table(
    'departments', metadata,
    Column('id', Integer, primary_key=True),
    Column('department', String, nullable=False)
)

jobs = Table(
    'jobs', metadata,
    Column('id', Integer, primary_key=True),
    Column('job', String, nullable=False)
)

hired_employees = Table(
    'hired_employees', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String, nullable=True),
    Column('datetime', DateTime, nullable=True),
    Column('department_id', Integer, nullable=True),
    Column('job_id', Integer, nullable=True)
)
