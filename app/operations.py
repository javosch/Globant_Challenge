from fastapi import FastAPI, UploadFile, BackgroundTasks

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
    async def upload_csv(file: UploadFile, background_tasks: BackgroundTasks):
        # Por implementar el upload
        pass

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