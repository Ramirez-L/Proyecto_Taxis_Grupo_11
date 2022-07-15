import pandas as pd
from funciones import read_data

def createDummies(df, var_name):
    dummy = pd.get_dummies(df[var_name], prefix=var_name)
    df = df.drop(var_name, axis = 12)
    df = pd.concat([df, dummy ], axis = 1)
    return df

data1=pd.read_csv("../Data/weather_Manhattan_2018-01.csv")
data2=pd.read_csv("../Data/weather_Brooklyn_2018-01.csv")
data3=pd.read_csv("../Data/weather_Bronx_2018-01.csv")
data4=pd.read_csv("../Data/weather_Queens_2018-01.csv")
data5=pd.read_csv("../Data/weather_Staten_Island_2018-01.csv")

data1["id_borough"]=1
data2["id_borough"]=2
data3["id_borough"]=3
data4["id_borough"]=4
data5["id_borough"]=5

data=pd.concat([data1,data2,data3,data4,data5], axis=0)
data=data[["id_borough","Date time","Minimum Temperature","Maximum Temperature","Temperature","Relative Humidity","Wind Speed","Conditions"]]


data=createDummies(data,"Conditions")

data.columns=[col.replace(" ","_").lower() for col in data.columns]

data.to_csv("../Data/Weather.csv", index=False)

data=read_data("../Data/yellow_tripdata_2018-01.parquet")
data.to_csv("../Data/Taxis.csv", index=False)