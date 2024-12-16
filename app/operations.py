from fastapi import FastAPI, UploadFile, BackgroundTasks, HTTPException
from app.models import metadata
from app.config import engine
from sqlalchemy import select, delete
import numpy as np
import pandas as pd
import os


# Process CSV and upload to DB
def process_csv(file_path: str):
    try:
        # Infer table name from file name
        table_name = os.path.splitext(os.path.basename(file_path))[0]

        catalog_table_sql = 'SELECT table_name, file_name FROM catalog_tables'
        catalog_table_data = pd.read_sql(catalog_table_sql, con=engine).set_index('file_name')
        map_names_tables = catalog_table_data.to_dict()['table_name']

        try:
            table_name = map_names_tables[table_name]
            table = metadata.tables.get(table_name)
        except KeyError:
            table = metadata.tables.get(table_name)

        if table is None:
            raise ValueError(f"Table {table_name} does not exist in the database.")

        # Get columns names
        cols = [col.name for col in table.columns.values()]

        # Load CSV data
        df = pd.read_csv(
            file_path
            , sep=','
            , names=cols
        )

        # Change column datetime to datetime format
        if table_name == "hired_employees":
            df["datetime"] = pd.to_datetime(df["datetime"]).replace({np.nan: None})

        # Replace null values with None, the engine works with None
        df = df.replace({np.nan: None})

        with engine.begin() as conn:
            conn.execute(table.insert(), df.to_dict(orient="records"))
        
        return True

    except Exception as e:
        error = f"Error processing file {file_path}: {str(e)}"
        print(error)
        return error
    finally:
        os.remove(file_path)


def init_operations(app: FastAPI):
    @app.get('/get_tables/')
    async def available_tables():
        # Por implementar lectura de tablas disponibles
        pass


    @app.get('/get_info/{table_name}/{id}')
    async def get_info(table_name: str, id: int):
        # Por implementar lectura de datos
        pass


    @app.post('/upload_csv/')
    async def upload_csv(file: UploadFile): #, background_tasks: BackgroundTasks):
        # Por implementar el upload
        try:
            os.makedirs('./tmp_data/', exist_ok=True)
            file_path = f"./tmp_data/{file.filename}"
            with open(file_path, "wb") as buffer:
                buffer.write(await file.read())

            # background_tasks.add_task(process_csv, file_path)

            result = process_csv(file_path=file_path)
            print(result)

            if result == True:
                return {"message": f"File {file.filename} uploaded and processing started."}
            else:
                raise Exception(result)

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error uploading file: {str(e)}")
        

    @app.post('/batch_insert/')
    async def batch_insert(request: dict):
        # Por implementar insertado en batch
        pass


    @app.put('/update/{table_name}/{id}')
    async def update_record(table_name: str, id: int, request: dict):
        # Por implementar el update
        pass


    @app.delete('/delete/{table_name}/{id}')
    async def delete_record(table_name: str, id: int):
        # Por implementar el delete
        pass

