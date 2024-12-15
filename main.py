from fastapi import FastAPI
from app.operations import init_operations
from app.config import init_db

# FastAPI initialization
app = FastAPI()

# Initialize database and routes
init_db()
init_operations(app)

@app.get('/')
def root():
    return {'apiStatus':'API is running'}