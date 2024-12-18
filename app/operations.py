from fastapi import FastAPI, UploadFile, BackgroundTasks, HTTPException
import os

from app.models import metadata
from app.config import engine
from app.utils import process_csv, insert_records


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

            if result.status_code == 200:
                # TODO  Debo poner un return acá, la API devolverá como mensaje el dict que ponga
                # TODO  Dejarlo igual que el resto de funciones de utils.py
                print({"message": f"File {file.filename} uploaded and processing started."})

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

