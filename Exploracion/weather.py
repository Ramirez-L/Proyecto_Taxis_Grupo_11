# pip install pyowm
from datetime import datetime, timedelta, timezone
import pandas as pd
import requests

API_KEY_LIST= [
    "VBF8WCN29MQP5BNCBQ5QG5U7E",
    "EPVYJQV9TTNYYN52QNYRLEL58",
    "P3KPQJVC4HWS6HZ9A8KR94TV3",
    "E3DLF4LEV66P6WCEDPK2HTA5Z",
    "99LZHCW823HQ5P5Y44XPVR2F5",
    "WPDFT7FX9KGMSAZG9YMQMRSLB",
]

BOROUGHS=pd.DataFrame({
    "Manhattan":{
        "id_borough":1,
        "lat":40.776676,
        "lon":-73.971321,},

    "Brooklyn":{
        "id_borough":2,
        "lat":40.650002,
        "lon":-73.949997,},

    "Bronx":{
        "id_borough":3,
        "lat":40.837048,
        "lon":-73.865433,},
        
    "Queens":{
        "id_borough":4,
        "lat":40.742054,
        "lon":-73.769417,},

    "Staten_Island":{
        "id_borough":5,
        "lat":40.579021,
        "lon":-74.151535,},    
})


def date_range(start, end,freq="D"):
    delta = end - start  # as timedelta
    if freq == "D":
        time_range = [start + timedelta(days=i) for i in range(delta.days)]
    elif freq == "H":
        time_range = [start + timedelta(hours=i) for i in range(delta.days*24)]
    elif freq == "M":
        time_range = [start + timedelta(minutes=i) for i in range(delta.days*24*60)]
    return time_range


def get_weather(API_KEY,lat,lon,start,end,freq=24):        
    URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/weatherdata/history"
    params={
        "aggregateHours":freq,
        "startDateTime":start,
        "endDateTime":end,
        "contentType":"csv",
        "location":f"{lat},{lon}",
        "key":API_KEY,
    }
    try:
        response=requests.request("GET",URL,params=params)
        return response
    except:
        print("Error")


def get_weather_Day(API_KEY,borough,date,freq=1):
    start=date
    end=date+timedelta(hours=23)
    formatDate="%Y-%m-%dT%H:%M:%S"
    response=get_weather(
        API_KEY,
        BOROUGHS[borough].lat,
        BOROUGHS[borough].lon,
        start=start.strftime(formatDate),
        end=end.strftime(formatDate),
        freq=freq)
    return response


def get_weather_Month(API_KEY_LIST,borough,year,mont,freq=1,out_dir="./"):
    start = datetime(year,mont,1)
    end = datetime(year,mont+1,1)-timedelta(days=1)

    formatDate="%Y-%m-%dT%H:%M:%S"

    for API_KEY in API_KEY_LIST:
        response=get_weather(
            API_KEY,
            BOROUGHS[borough].lat,
            BOROUGHS[borough].lon,
            start=start.strftime(formatDate),
            end=end.strftime(formatDate),
            freq=freq)
        if response.text.count("\n")>1:
            file_name=f"weather_{borough}_{year}-{mont}.csv".replace(" ","_")
            header,body=response.text.split('\n',1)
            
            with open(out_dir+file_name,"a+") as f:
                f.seek(0)
                if f.readline()!=header+'\n':
                    f.write(header+'\n')
                f.write(body+"\n")
                print(f"success: {file_name} Created successfully!!")
            break
        else:
            print("Burned API_KEY:",API_KEY,response.text)

    return response

    
def get_weather_Month_daybyday(API_KEY_LIST,borough,year,mont,out_dir="./"):
    start = datetime(year,mont,1)
    end = datetime(year,mont+1,1)
    days_list=date_range(start,end,freq="D")

    for day in days_list:
        BURNEDS_APIS=[]
        for API_KEY in API_KEY_LIST:
            response = get_weather_Day(API_KEY,borough,day)

            if response.text.count("\n")>1:
                file_name=f"weather_{borough}_{day.strftime('%Y-%m')}.csv".replace(" ","_")
                header,body=response.text.split('\n',1)
                
                with open(out_dir+file_name,"a+") as f:
                    f.seek(0)
                    if f.readline()!=header+'\n':
                        f.write(header+'\n')
                    f.write(body+"\n")
                    print(f"success: Add weather_{borough}_{day.strftime('%Y-%m-%d')} to {file_name}")
                break
            else:
                print("Burned API_KEY:",API_KEY,response.text)
                BURNEDS_APIS.append(API_KEY)

        for BURNED_API in BURNEDS_APIS:
            API_KEY_LIST.remove(BURNED_API)




if __name__=="__main__":

    for borough in BOROUGHS.columns:
        get_weather_Month(API_KEY_LIST,borough,2018,3,out_dir="../Data")


