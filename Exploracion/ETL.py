from fileinput import filename
import pandas as pd
import sqlalchemy
from datetime import timedelta, time
import weather 
import os

HOST="localhost"
HOST="192.53.165.168" 
DBNAME="taxisdb"
USER="airflow"
PASS="airflow"
DATABASE_LOCATION=f'postgresql+psycopg2://{USER}:{PASS}@{HOST}:5432/{DBNAME}'
BASE_URL_TAXIS="https://d37ci6vzurychx.cloudfront.net/trip-data/"

"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet"


def create_schema():
    engine = sqlalchemy.create_engine(DATABASE_LOCATION,echo=True)

    with engine.connect() as cursor:
        sql_query="""
        CREATE TABLE IF NOT EXISTS vendor (
            id_vendor INT UNIQUE PRIMARY KEY,
            vendor_name VARCHAR (100) NOT NULL
        );
        """
        cursor.execute(sql_query)

        sql_query="""
        CREATE TABLE IF NOT EXISTS rate (
            id_rate INT UNIQUE PRIMARY KEY,
            rate_name VARCHAR (100) NOT NULL
        );
        """
        cursor.execute(sql_query)

        sql_query="""
        CREATE TABLE IF NOT EXISTS payment (
            id_payment INT UNIQUE PRIMARY KEY,
            payment_type VARCHAR (100) NOT NULL
        );
        """
        cursor.execute(sql_query)
        
        sql_query="""
        CREATE TABLE IF NOT EXISTS borough (
            id_borough INT UNIQUE PRIMARY KEY,
            borough VARCHAR (100) NOT NULL,
            latitud FLOAT4 ,
            longitud FLOAT4
        );
        """
        cursor.execute(sql_query)

        sql_query="""
        CREATE TABLE IF NOT EXISTS location (
            id_location INT UNIQUE PRIMARY KEY,
            id_borough INT,
            zone VARCHAR (100) NOT NULL,
            service_zone VARCHAR (100) NOT NULL,

            CONSTRAINT fk_borough
                FOREIGN KEY(id_borough) 
                REFERENCES borough(id_borough)
        );
        """
        cursor.execute(sql_query)

        sql_query="""
        CREATE TABLE IF NOT EXISTS weather (
            id_weather SERIAL UNIQUE PRIMARY KEY,
            id_borough INT NOT NULL,
            date_time TIMESTAMP NOT NULL,
            minimum_temperature FLOAT4 NOT NULL,
            maximum_temperature FLOAT4 NOT NULL,
            temperature FLOAT4 NOT NULL,
            relative_humidity FLOAT4 NOT NULL,
            wind_speed FLOAT4 NOT NULL,
            conditions VARCHAR (250) NOT NULL, 

            CONSTRAINT fk_borough
                FOREIGN KEY(id_borough) 
                REFERENCES borough(id_borough)
        );
        """
        cursor.execute(sql_query)

        # sql_query="""
        # CREATE TABLE IF NOT EXISTS calendario (
        #     date DATE UNIQUE PRIMARY KEY,
        #     year INT,
        #     month INT,
        #     day INT,
        #     week INT,
        #     quarter INT,
        #     year_half INT,
        #     weekday INT
        # );
        # """
        # cursor.execute(sql_query)

        sql_query="""
        CREATE TABLE IF NOT EXISTS trip (

            id_trip SERIAL NOT NULL PRIMARY KEY,

            id_vendor INT NOT NULL,
            pickup_datetime TIMESTAMP NOT NULL,
            dropoff_datetime TIMESTAMP NOT NULL,
            passager_count INT NOT NULL,
            trip_distance DOUBLE PRECISION  NOT NULL,
            id_rate INT NOT NULL,
            store_fwd_flag VARCHAR(4) NOT NULL,
            pu_location INT NOT NULL,
            do_location INT NOT NULL,
            id_payment INT NOT NULL,
            fare_amount DOUBLE PRECISION NOT NULL,
            extra DOUBLE PRECISION NOT NULL,
            mta_tax DOUBLE PRECISION NOT NULL,
            tip_amount DOUBLE PRECISION NOT NULL,
            tolls_amount DOUBLE PRECISION NOT NULL,
            improve_surcharge DOUBLE PRECISION NOT NULL,
            total_amount DOUBLE PRECISION NOT NULL,
            congestion_surcharge DOUBLE PRECISION NOT NULL,
            tiempo_viaje DOUBLE PRECISION NOT NULL,

            CONSTRAINT fk_vendor
            FOREIGN KEY(id_vendor) 
            REFERENCES vendor(id_vendor),

            CONSTRAINT fk_rate
            FOREIGN KEY(id_rate) 
            REFERENCES rate(id_rate),

            CONSTRAINT fk_pu_location
            FOREIGN KEY(pu_location) 
            REFERENCES location(id_location),

            CONSTRAINT fk_do_location
            FOREIGN KEY(do_location)  
            REFERENCES location(id_location), 

            CONSTRAINT fk_payment 
            FOREIGN KEY(id_payment) 
            REFERENCES payment(id_payment) 
        );
        """
        cursor.execute(sql_query)

def extract_transform_trip(file):
    ruta = file
    # Leer dataset
    # data = pd.read_parquet('../Data/yellow_tripdata_2018-01.parquet')
    print("Leyendo Archivo")
    data = pd.read_parquet(ruta)
    print("lo leyo")
    print("_____________________________")

    # Agregar nueva columna - Tiempo de Viaje 
    print("crear columna")
    data["Tiempo_Viaje"]=data["tpep_dropoff_datetime"]-data["tpep_pickup_datetime"]
    print("_____________________________")


    # Completar valores vacios
    print("completar valores faltantes")
    data["congestion_surcharge"].fillna(0, inplace=True)
    data["airport_fee"].fillna(0, inplace=True)
    print("_____________________________")

    # Convertir valores negativos en positivos
    print("valores absolutos")
    columnas=['VendorID','passenger_count', 'trip_distance', 'RatecodeID','PULocationID', 'DOLocationID', 'payment_type', 'fare_amount', 'extra','mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge','total_amount', 'congestion_surcharge', 'airport_fee']
    data[columnas]=data[columnas].abs()
    print("_____________________________")

    # Convertir viajes negativos en Cero
    print("Convertir viajes negativos en Cero")
    data.loc[data["Tiempo_Viaje"]<timedelta(0),"Tiempo_Viaje"]=timedelta(0)
    print("_____________________________")

    # Convertir valores muy lejanos en 0 para mantener el registro
    print("# Convertir valores muy lejanos en 0 para mantener el registro")
    data.loc[data["trip_distance"]>25000,"trip_distance"]=0
    print("_____________________________")

    # Convertir el tiempo de Viaje a segundos
    print("Convertir el tiempo de Viaje a segundos")
    data["Tiempo_Viaje_s"]=[x.total_seconds() for x in data["Tiempo_Viaje"]]
    print("_____________________________")

    # Remover valores de Segundos muy altos a cero
    print("Remover valores de Segundos muy altos a cero")
    data.loc[data["Tiempo_Viaje_s"]>100000,"Tiempo_Viaje_s"]=0
    print("_____________________________")

    # Borrar columnas no usadas
    print("Borrar columnas no usadas")
    data.drop("Tiempo_Viaje", axis=1, inplace=True)
    data.drop("airport_fee", axis=1, inplace=True)
    print("_____________________________")

    data.drop(data[(data["DOLocationID"].isin([1,264,265])) | (data["PULocationID"].isin([1,264,265]))].index,axis=0, inplace=True)
    data.drop(data[data["RatecodeID"]==99].index,axis=0, inplace=True)
    
    return data

def load_trip(file):
    engine=sqlalchemy.create_engine(DATABASE_LOCATION,echo=True)
    
    df_taxis = pd.read_csv(file, iterator=True, chunksize=100000)
    df_taxis.columns=["id_vendor","pickup_datetime","dropoff_datetime","passager_count","trip_distance","id_rate",
    "store_fwd_flag","pu_location","do_location","id_payment","fare_amount","extra","mta_tax","tip_amount","tolls_amount",
    "improve_surcharge","total_amount","congestion_surcharge","tiempo_viaje"]
    
    print(f"loading {file} to taxisdb.trip"+"."*50)
    try:
        df = next(df_taxis)        
        df.to_sql(name="trip", con=engine,index=False, if_exists='append')

        while True:
                t_start = time()
                df = next(df_taxis)
                df.to_sql(name="trip", con=engine,index=False, if_exists='append')
                t_end = time()
                print('insertar otro parte del df... tomo %.3f segundos' %(t_end - t_start))

    except Exception as e:  #Want to capture all errors
        print("Shiat broke down ..."+"*"*100)
        print(e)

def create_load_vendor():
    engine=sqlalchemy.create_engine(DATABASE_LOCATION,echo=True)
    df_vendor = pd.DataFrame({
        "id_vendor":[1,2],
        "vendor_name":["Creative Mobiles Technologies","VeriFone Inc."]
    })
    try:
        df_vendor.to_sql("vendor", con=engine, index=False,if_exists="append")
    except Exception as e:  #Want to capture all errors
        print("Shiat broke down ..."+"*"*50)
        print(e)

def create_load_rate():
    engine=sqlalchemy.create_engine(DATABASE_LOCATION,echo=True)
    df_rate = pd.DataFrame({
        "id_rate":[1,2,3,4,5,6],
        "rate_name":["Standard rate","JFK","Newark","Nassau or Westchester","Negotiated fare","Group ride"]
    })
    try:
        df_rate.to_sql("rate", con=engine, index=False,if_exists="append")
    except Exception as e:  #Want to capture all errors
        print("Shiat broke down ..."+"*"*50)
        print(e)

def create_load_payment():
    engine=sqlalchemy.create_engine(DATABASE_LOCATION,echo=True)
    df_payment = pd.DataFrame({
        "id_rate":[1,2,3,4,5,6],
        "rate_name":["Credit card","Cash","No charge","Dispute","Unknown","Voided trip"]
    })
    try:
        df_payment.to_sql("payment", con=engine, index=False,if_exists="append")
    except Exception as e:  #Want to capture all errors
        print("Shiat broke down ..."+"*"*50)
        print(e)

def create_load_borough():
    engine=sqlalchemy.create_engine(DATABASE_LOCATION,echo=True)
    df_borough = pd.DataFrame({
        "id_borough": [1, 2, 3, 4, 5],
        "borough": ["Manhattan", "Brooklyn", "Bronx", "Queens", "Staten Island"],
        "latitud": [40.776676, 40.650002, 40.837048, 40.742054, 40.579021],
        "longitud": [-73.971321, -73.949997, -73.865433, -73.769417, -74.151535]
    })
    try:
        df_borough.to_sql("borough", con=engine, index=False,if_exists="append")
    except Exception as e:  #Want to capture all errors
        print("Shiat broke down ..."+"*"*50)
        print(e)

def create_load_location():
    engine=sqlalchemy.create_engine(DATABASE_LOCATION,echo=True)
    df_location = pd.read_csv("../taxi+_zone_lookup.csv")
    df_location.columns=["id_location","borough","zone","service_zone"]

    try:
        df_location.to_sql("location", con=engine, index=False,if_exists="append")
    except Exception as e:  #Want to capture all errors
        print("Shiat broke down ..."+"*"*50)
        print(e)

def extract_transform_weather(year,month,out_dir="../Data/"):
    weathers=[]
    for borough in weather.BOROUGHS.columns:    
        filename=out_dir+f"weather_{borough}_{year}-{month}.csv"
        if not os.path.exists(filename):
            weather.get_weather_Month(weather.API_KEY_LIST,borough,year,month,out_dir=out_dir)
        df_weather=pd.read_csv(filename)
        df_weather["id_borough"]=weather.BOROUGHS[borough]["id_borough"]
        weathers.append(df_weather)
        # if os.path.exists(filename):
        #     os.remove(filename)

    df_weather=pd.concat(weathers, axis=0)
    print("success: concatenated Weathers csv files!")
    df_weather=df_weather[["id_borough","Date time","Minimum Temperature","Maximum Temperature","Temperature","Relative Humidity","Wind Speed","Conditions"]]
    
    df_weather.columns=[col.replace(" ","_").lower() for col in df_weather.columns]
    weather_concat_file_name=out_dir+f"Weather_{year}_{month}.csv"
    df_weather.to_csv(weather_concat_file_name, index=False)
    print(f"success: {weather_concat_file_name} Created successfully!!!")

    return out_dir+f"Weather_{year}_{month}.csv"

def load_weather(file):
    engine=sqlalchemy.create_engine(DATABASE_LOCATION,echo=True)
    df_weather = pd.read_csv(file)
    try:
        df_weather.to_sql("weather", con=engine, index=False,if_exists="append")
    except Exception as e:  #Want to capture all errors
        print("Shiat broke down ..."+"*"*50)
        print(e)

if __name__ == "__main__":
    
    create_schema()

    create_load_vendor()
    create_load_rate()
    create_load_payment()
    create_load_borough()
    create_load_location()

    weather_filename=extract_transform_weather(2018,1,out_dir="../Data/")
    load_weather(weather_filename)

    data = extract_transform_trip("../Data/yellow_tripdata_2018-01.parquet")
    data.to_csv("..\Data\Taxis.csv", index=False)
    load_trip("..\Data\Taxis.csv")