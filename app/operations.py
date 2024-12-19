from fastapi import FastAPI, UploadFile, HTTPException
import json
import os

from app.config import metadata
from app.config import engine
from app.utils import process_csv, update_records


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
        pass


    @app.put('/update/{table_name}/{id}')
    async def api_update_record(table_name: str, id: int, request: dict):
        # Por implementar el update
        pass


    @app.delete('/delete/{table_name}/{id}')
    async def delete_record(table_name: str, id: int):
        # Por implementar el delete
        pass
    





#     TABLE_NAME = 'hired_employees'
#     COLUMNS_IDS = ['id']
#     import pandas as pd
#     new_data_path = r'C:\Users\JavierSchmitt\Downloads\hired_employees__1___1_.csv'
#     df = pd.read_csv(
#         new_data_path
#         , sep=','
#         , names=['id', 'name', 'datetime', 'department_id', 'job_id']
#     )[:2]

#     df.loc[1, 'name'] = 'PRUEBA PRUEBA'

#     # result = insert_records(TABLE_NAME, df, COLUMNS_IDS)
#     # result = delete_records(TABLE_NAME, df, COLUMNS_IDS)
#     import asyncio

#     async def test_upload_csv(file_path):
#         return await upload_csv(file_path)
    
#     with open(new_data_path, "rb") as f:
#         upload_file = UploadFile(filename="hired_employees.csv", file=f)

#         result = asyncio.run(test_upload_csv(upload_file))
#     # result = upload_csv(new_data_path)
#     # df = (await result)['data']

#     print('ok')

# app = FastAPI()
# init_operations(app)
# print('ok')