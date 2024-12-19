from sqlalchemy import Table, Integer, String, DateTime, or_
from psycopg2.extensions import register_adapter, AsIs
import pandas as pd
import numpy as np

import os

from app.models import metadata
from app.config import engine

register_adapter(np.int64, AsIs)
register_adapter(np.float64, AsIs)

# Connect metadata with engine, to reflect tables on DataBase
metadata.reflect(bind=engine)

# Message to send when functions end
message_to_return = {
    'status':'',
    'status_code': 102,
    'affected_rows': 0,
    'operation': '',
    'message': '',
    'data': ''
}

def employees_hired_q_2021(year: int) -> str:
    """
    Retrieves the number of employees hired for each job and department in the selected year,
    divided by quarters, and returns the result as a JSON string.

    Args:
        year (int): The year for which the data is requested.

    Returns:
        str: A JSON string representing the number of employees hired, divided by quarter.
    """

    sql = f"""
    SELECT 
        d.department AS department_name,
        j.job AS job_name,
        SUM(CASE WHEN DATE_PART('quarter', h.datetime::timestamp) = 1 THEN 1 ELSE 0 END) AS Q1,
        SUM(CASE WHEN DATE_PART('quarter', h.datetime::timestamp) = 2 THEN 1 ELSE 0 END) AS Q2,
        SUM(CASE WHEN DATE_PART('quarter', h.datetime::timestamp) = 3 THEN 1 ELSE 0 END) AS Q3,
        SUM(CASE WHEN DATE_PART('quarter', h.datetime::timestamp) = 4 THEN 1 ELSE 0 END) AS Q4
    FROM
        hired_employees h
    JOIN
        departments d
        ON h.department_id = d.id
    JOIN
        jobs j
        ON h.job_id = j.id
    WHERE
        DATE_PART('year', h.datetime::timestamp) = {year}
    GROUP BY
        d.department,
        j.job
    ORDER BY
        d.department,
        j.job
    """
    
    with engine.begin() as conn:
        df = pd.read_sql(sql, conn)
        conn.close()

    result = df.to_json()
    
    return result


def total_employees_by_department(year:int) -> str:
    """
    Retrieves list of ids, name and number of employees hired of each department that hired more employees
    than the mean of employees hired in selected year for all the departments, and returns
    the result as a JSON string.

    Args:
        year (int): The year for which the data is requested.

    Returns:
        str: A JSON string representing the number of employees hired, divided by quarter.
    """

    sql = f"""
    WITH department_hires AS (
        SELECT 
            d.id AS department_id,
            d.department AS department_name,
            COUNT(*) AS hired
        FROM
            hired_employees h
        JOIN
            departments d
            ON h.department_id = d.id
        WHERE
            DATE_PART('year', h.datetime::timestamp) = {year}
        GROUP BY
            d.id,
            d.department
    ),
    average_hires AS (
        SELECT
            AVG(hired) AS mean_hires
        FROM
            department_hires
    )
    SELECT 
        dh.department_id,
        dh.department_name,
        dh.hired
    FROM
        department_hires dh
    JOIN
        average_hires ah
        ON dh.hired > ah.mean_hires
    ORDER BY
        dh.hired DESC
    """
    
    with engine.begin() as conn:
        df = pd.read_sql(sql, conn)
        conn.close()

    result = df.to_json()
    
    return result
