from fastapi import FastAPI, UploadFile, HTTPException, Query
import json
import os

from app.config import metadata
from app.config import engine
from app.utils import process_csv, update_records, select_records
from app.business import employees_hired_q_2021, total_employees_by_department


def init_operations(app: FastAPI):
    @app.get('/get_tables/')
    async def available_tables():
        # Por implementar lectura de tablas disponibles
        metadata.reflect(bind=engine)
        tables = list(metadata.tables.keys())
        return json.dumps({'tables_names': tables})


    @app.get('/get_info/{table_name}/{id_columns}/{id}')
    async def get_info(table_name: str, id_columns: str, id: str):
        # TODO: implementar manejo para poder pasar listas como input
        result = select_records(table_name, id_columns, id)

        return result


    @app.post('/upload_csv/')
    async def upload_csv(file: UploadFile):
        try:
            os.makedirs('./tmp_data/', exist_ok=True)
            file_path = f'./tmp_data/{file.filename}'
            with open(file_path, "wb") as buffer:
                buffer.write(await file.read())

            # background_tasks.add_task(process_csv, file_path)
            print(f'File {file.filename} uploaded.')

            print(f'File {file.filename} processing started.')
            result = process_csv(file_path=file_path)
            
            if result['status_code'] == 200:
                # TODO  Debo poner un return acá, la API devolverá como mensaje el dict que ponga
                # TODO  Dejarlo igual que el resto de funciones de utils.py
                df = result['data']['dataframe']
                table_name = result['data']['table_name']

                print(f'File {file.filename} inserting started.')
                result = update_records(table_name, df, id_columns=['id'])

                return json.dumps(result)
                # return str(result)
                # print({"message": f"File {file.filename} uploaded and processing started."})

            else:
                raise Exception(result)

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error uploading file: {str(e)}")
        

    @app.post('/batch_insert/')
    async def batch_insert(request: dict):
        # Por implementar insertado en batch
        return pass_func


    @app.put('/update/{table_name}/{id}')
    async def api_update_record(table_name: str, id: str):
        # Por implementar el update
        return pass_func


    @app.delete('/delete/{table_name}/{id}')
    async def delete_record(table_name: str, id: str):
        # Por implementar el delete
        return pass_func
    
    """
    ------------------
        Business End Points
    ------------------
    
    """

    desc_temp = 'Number of employees hired for each job and department in selected year divided by quarter.'
    @app.get('/business/employees_hired_by_q', description=desc_temp)
    async def employees_hired_by_q(year: int = Query(default=2021)):
        try:
            result = employees_hired_q_2021(year)
            return result
        
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error getting records: {str(e)}")
    

    desc_temp = 'List of ids, name and number of employees hired of each department that hired more employees than the mean of employees hired in selected year for all the departments'
    @app.get('/business/total_employees_by_department', description=desc_temp)
    async def employees_by_department(year: int = Query(default=2021)):
        try:
            result = total_employees_by_department(year)
            return result
        
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error getting records: {str(e)}")

    

pass_func = 'Not implemented yet.'