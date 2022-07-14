import pandas as pd
from datetime import timedelta

def read_data(file):

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
    data["Tiempo_Viaje"][data["Tiempo_Viaje"]<timedelta(0)]=timedelta(0)
    print("_____________________________")

    # Convertir valores muy lejanos en 0 para mantener el registro
    print("# Convertir valores muy lejanos en 0 para mantener el registro")
    data["trip_distance"][data["trip_distance"]>25000]=0
    print("_____________________________")

    # Convertir el tiempo de Viaje a segundos
    print("Convertir el tiempo de Viaje a segundos")
    data["Tiempo_Viaje_s"]=[x.total_seconds() for x in data["Tiempo_Viaje"]]
    print("_____________________________")

    # Remover valores de Segundos muy altos a cero
    print("Remover valores de Segundos muy altos a cero")
    data["Tiempo_Viaje_s"][data["Tiempo_Viaje_s"]>100000]=0
    print("_____________________________")

    # Borrar columnas no usadas
    print("Borrar columnas no usadas")
    data.drop("Tiempo_Viaje", axis=1, inplace=True)
    data.drop("airport_fee", axis=1, inplace=True)
    print("_____________________________")

    data.drop(data[(data["DOLocationID"].isin([1,264,265])) | (data["PULocationID"].isin([1,264,265]))].index,axis=0, inplace=True)
    data.drop(data[data["RatecodeID"]==99].index,axis=0, inplace=True)

    return data
