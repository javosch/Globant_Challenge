from sqlalchemy import Table, Column, Integer, String, DateTime
from app.config import metadata

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
    Column('name', String, nullable=False),
    Column('datetime', DateTime, nullable=False),
    Column('department_id', Integer, nullable=False),
    Column('job_id', Integer, nullable=False)
)