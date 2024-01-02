import requests
from datetime import datetime,timedelta
import numpy as np
import pandas as pd
import json
import time
import warnings
from pandas.errors import SettingWithCopyWarning

# Ignore the specific warning about setting values on a copy of a slice
warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)

def getpremiumindex():
    df = pd.DataFrame()
    api= 'v1/premiumIndexKlines'
    ticker = 'BTCUSDT'
    time_interval = '1m'
    start_time = datetime(2022,1,1) 
    end_time = datetime.now() 

    data = []
    while start_time < end_time:
        print(start_time)
        start_time_2 = int(start_time.timestamp() * 1000)
        url = 'https://fapi.binance.com/fapi/'+str(api)+'?symbol='+str(ticker)+'&interval='+str(time_interval)+'&limit=1500&startTime='+str(start_time_2)
        resp = requests.get(url)
        resp = json.loads(resp.content.decode())  

        data.append(resp)
            
        start_time = start_time + timedelta(minutes=1500)
        
    data = pd.DataFrame(data)

    df = pd.DataFrame(data)

    combined_rows = []
    for _, row in df.iterrows():
        combined_row = []
        for cell in row:
            combined_row.extend(cell if cell is not None else [np.nan, np.nan, np.nan])
        combined_rows.append(combined_row)

    split_rows = [row[i:i+12] for row in combined_rows for i in range(0, len(row), 12)]


    new_df = pd.DataFrame(split_rows)
    return new_df

def gethistoricalprice():
    df = pd.DataFrame()
    api= 'v1/klines'
    ticker = 'BTCUSDT'
    time_interval = '1m'
    start_time = datetime(2022,1,1) 
    end_time = datetime.now() 

    data = []
    while start_time < end_time:
        print(start_time)   
        start_time_2 = int(start_time.timestamp() * 1000)
        url = 'https://fapi.binance.com/fapi/'+str(api)+'?symbol='+str(ticker)+'&interval='+str(time_interval)+'&limit=1500&startTime='+str(start_time_2)
        resp = requests.get(url)
        resp = json.loads(resp.content.decode())  

        data.append(resp)
            
        start_time = start_time + timedelta(minutes=1500)
        
    data = pd.DataFrame(data)

    df = pd.DataFrame(data)

    combined_rows = []
    for _, row in df.iterrows():
        combined_row = []
        for cell in row:
            combined_row.extend(cell if cell is not None else [np.nan, np.nan, np.nan])
        combined_rows.append(combined_row)

    split_rows = [row[i:i+12] for row in combined_rows for i in range(0, len(row), 12)]

    new_df = pd.DataFrame(split_rows)
    new_df[0] = pd.to_datetime(new_df[0], unit='ms')
    new_df[6] = pd.to_datetime(new_df[6], unit ='ms')
    new_df.columns = ['Time', 'Open', 'High', 'Low','Close', 'Ignore', 'Close Time','Ignore', 'Ignore', 'Ignore','Ignore', 'Ignore']
    return new_df


def processrawdata(dataa):
    dataa[0] = pd.to_datetime(dataa[0], unit='ms')
    dataa[6] = pd.to_datetime(dataa[6], unit ='ms')

    for l in range(1,5):
        x=0
        print(l)
        for i in dataa[l]:
            dataa[l][x] = float(i)*100
            x = x +1 
            print(x)

    dataa.columns = ['Time', 'Open', 'High', 'Low','Close', 'Ignore', 'Close Time','Ignore', 'Ignore', 'Ignore','Ignore', 'Ignore']
    return dataa



premiumindex = getpremiumindex()
y= processrawdata(dataa=premiumindex)
y

pricedata = gethistoricalprice()
pricedata[['Time','Close']]

y['Close Price'] = pricedata['Close']

finalizeddata = y[['Time', 'Open', 'High', 'Low' ,'Close Time','Close Price' ]]

finalizeddata.to_csv('btcpremiumindexdata.csv')

# while True:
#     time.sleep(2)
#     premiumindex = getlatestpremiumindex()
#     y = processrawdata(dataa=premiumindex)
#     print(y['Close'].tail(1).values[0])



