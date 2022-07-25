from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import sqlalchemy

model_path = "./finalized_regtree.sav"

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


HOST="localhost"
HOST="192.53.165.168" 
DBNAME="taxisdb"
USER="airflow"
PASS="airflow"
DATABASE_LOCATION=f'postgresql+psycopg2://{USER}:{PASS}@{HOST}:5432/{DBNAME}'


@app.get("/query/")
def get_query(*, consulta: str = Query(description="Consulta con letras mayusculas y ; al final")):
    engine = sqlalchemy.create_engine(DATABASE_LOCATION,echo=True)
    with engine.connect() as cursor:
        sql_query = consulta
        resultado = cursor.execute(sql_query)

    return resultado.fetchall()

@app.get("/model")
def get_modelo():
    return FileResponse(path=model_path, filename=model_path, media_type='application/x-spss-sav')
