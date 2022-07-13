import pandas as pd
from funciones import read_data

data1= pd.read_csv("..\Data\weather_Bronx_2018-01.csv")
data2= pd.read_csv("..\Data\weather_Brooklyn_2018-01.csv")
data3= pd.read_csv("..\Data\weather_Manhattan_2018-01.csv")
data4= pd.read_csv("..\Data\weather_Queens_2018-01.csv")
data5= pd.read_csv("..\Data\weather_Staten_Island_2018-01.csv")

data=pd.concat([data1,data2,data3,data4,data5], axis=0)

data["Conditions"].unique()
data=data[["Date time","Latitude","Longitude","Minimum Temperature","Maximum Temperature","Temperature","Wind Speed","Conditions"]]

def createDummies(df, var_name):
    dummy = pd.get_dummies(df[var_name], prefix=var_name)
    df = df.drop(var_name, axis = 1)
    df = pd.concat([df, dummy ], axis = 1)
    return df

data=createDummies(data,"Conditions")

data.to_csv("..\Data\Weather.csv", index=True)

data=read_data("..\Data\yellow_tripdata_2018-01.parquet")
data.to_csv("..\Data\Taxis.csv", index=True)