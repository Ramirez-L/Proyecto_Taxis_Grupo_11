from fastapi import FastAPI, Query
import sqlalchemy

app = FastAPI()


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
