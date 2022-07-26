import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.tree import DecisionTreeRegressor
import pickle


from sqlalchemy import create_engine
import pandas as pd


HOST="192.53.165.168"
DBNAME="taxisdb"
USER="airflow"
PASS="airflow"
BATCH_SIZE_TRIP=100000
DATABASE_LOCATION=f'postgresql+psycopg2://{USER}:{PASS}@{HOST}:5432/{DBNAME}'

query="""
SELECT
    id_rate ,
    trip_distance,
    tiempo_viaje,
    pu_b.id_borough as pu_borough,
    do_b.id_borough as do_borough,
    fare_amount
FROM trip
JOIN location pu_l on pu_l.id_location = trip.pu_location
JOIN location do_l on do_l.id_location = trip.do_location
JOIN borough pu_b on pu_b.id_borough = pu_l.id_borough
JOIN borough do_b on do_b.id_borough = do_l.id_borough
"""

print("Realizando la consulta SQL")
print("--------------------------")

engine= create_engine(DATABASE_LOCATION)
with engine.connect()  as conn:
    df_trip=pd.read_sql(query,conn)
df_trip.head()


x_train, x_test, y_train, y_test= train_test_split(df_trip[["id_rate","trip_distance","pu_borough","do_borough"]],df_trip["fare_amount"],test_size=0.2)
x_train

print("Entrenando el modelo")
print("--------------------------")

regtree = DecisionTreeRegressor(min_samples_split=200, min_samples_leaf=1000, max_depth=20)
regtree.fit(x_train,y_train)
print(f"Score del modelo: {regtree.score(x_test,y_test)}")

filename = 'finalized_regtree.sav'
pickle.dump(regtree, open(filename, 'wb'))
 
print("Modelo Exportado")
print("--------------------------")

from paramiko import SSHClient
from scp import SCPClient

ssh = SSHClient()
ssh.load_system_host_keys()
ssh.connect(hostname='192.53.165.168', 
            username='root',
            password='testpy1234.',
)

# SCPCLient takes a paramiko transport as its only argument
scp = SCPClient(ssh.get_transport())

scp.put('finalized_regtree.sav', '/root/Proyecto_Taxis_Grupo_11/Machine_Learning/.')
# scp.get('file_path_on_remote_machine', 'file_path_on_local_machine')

scp.close()


print("Modelo Enviado a la nube")
print("--------------------------")