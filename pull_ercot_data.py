from bs4 import BeautifulSoup as bs
from sqlalchemy import create_engine
import requests, zipfile, io
import requests
import pandas as pd
import datetime
import os

#Seven-Day Load Forecast by Weather Zone URL
load_url = 'http://mis.ercot.com/misapp/GetReports.do?reportTypeId=12312&reportTitle=Seven-Day%20Load%20Forecast%20by%20Weather%20Zone&showHTMLView=&mimicKey'
#Wind Power Production - Hourly Averaged Actual and Forecasted Values
wind_url = 'http://mis.ercot.com/misapp/GetReports.do?reportTypeId=13028&reportTitle=Wind%20Power%20Production%20-%20Hourly%20Averaged%20Actual%20and%20Forecasted%20Values&showHTMLView=&mimicKey'
#Solar Power Production - Hourly Averaged Actual and Forecasted Values
solar_url = 'http://mis.ercot.com/misapp/GetReports.do?reportTypeId=13483&reportTitle=Solar%20Power%20Production%20-%20Hourly%20Averaged%20Actual%20and%20Forecasted%20Values&showHTMLView=&mimicKey'
#Hourly Resource Outage Capacity
outage_url = 'http://mis.ercot.com/misapp/GetReports.do?reportTypeId=13103&reportTitle=Hourly%20Resource%20Outage%20Capacity&showHTMLView=&mimicKey'


#create list to store ercot urls
ercot_urls = [load_url, wind_url, solar_url, outage_url]

# 1) ***********PULL LOAD DATA******************
response = requests.get(load_url)

lsoup = bs(response.text, 'html.parser')

l_url_results = lsoup.find_all('td', class_='labelOptional', text=True)

base_url = 'http://mis.ercot.com'

data_urls = []

for result in l_url_results:
    try:
        end_url = result.find('div').a['href']
        full_url = base_url + end_url
        data_urls.append(full_url)
    except:
        pass

ldata = requests.get(data_urls[0], stream=True)

lz = zipfile.ZipFile(io.BytesIO(ldata.content))

lz.extractall()

file = lz.namelist()[0]

print(f'Load File Name: {file}')

#load zip file to df
load_fcst = pd.read_csv(file)

#delete zip file after reading
os.remove(file)

#clean existing columns
load_fcst['DeliveryDate'] = pd.to_datetime(load_fcst.DeliveryDate).dt.normalize()
load_fcst['HourEnding'] = load_fcst.HourEnding.str.split(':').str[0]

#add file name and process info to df
load_fcst['file_name'] = lz.namelist()[0]
load_fcst['process_date'] = pd.to_datetime(load_fcst.file_name.str.split('.').str[3]).dt.normalize()
load_fcst['process_hour'] = load_fcst.file_name.str.split('.').str[4]
load_fcst['process_dt'] = pd.to_datetime(load_fcst.file_name.str.split('.').str[3] + load_fcst.file_name.str.split('.').str[4])
load_fcst['process_hour'] = load_fcst.process_hour.str[0:2]

load_fcst.HourEnding = load_fcst.HourEnding.astype(str)

#add delivery date time
load_fcst['delivery_dt'] = pd.to_datetime(load_fcst.DeliveryDate) + load_fcst.HourEnding.astype('timedelta64[h]')

#add weekday columns
load_fcst['weekday'] = load_fcst.delivery_dt.dt.day_name().str.upper()

#use pd.melt to unpivot columns
long_load_fcst = pd.melt(load_fcst, id_vars=['DeliveryDate', 'weekday', 'HourEnding', 'DSTFlag', 'file_name', 'process_date', 'process_hour', 'process_dt', 'delivery_dt'],var_name='weather_zone', value_name ='mw')

#rename columns
long_load_fcst = long_load_fcst.rename(columns={'DeliveryDate':'delivery_date','HourEnding':'delivery_hour','DSTFlag':'dst_flag'})

#reorganize columns
arranged_columns = ['delivery_dt', 'delivery_date', 'delivery_hour','dst_flag', 'weekday', 'weather_zone', 'mw', 'process_date', 'process_hour', 'process_dt', 'file_name']

long_load_fcst = long_load_fcst[arranged_columns]

# 2) ***********PULL OUTAGE DATA******************

response = requests.get(outage_url)
osoup = bs(response.text, 'html.parser')
outage_url_results = osoup.find_all('td', class_='labelOptional', text=True)

data_urls = []

for result in outage_url_results:
    try:
        end_url = result.find('div').a['href']
        full_url = base_url + end_url
        data_urls.append(full_url)
        #print(full_url)
    except:
        pass

odata = requests.get(data_urls[0], stream=True)

oz = zipfile.ZipFile(io.BytesIO(odata.content))
oz.extractall()
file = oz.namelist()[0]

print(f'Outage File Name: {file}')

#load zip file to df
outage_fcst = pd.read_csv(file)

#delete zip file after reading
os.remove(file)

#clean existing columns
outage_fcst['Date'] = pd.to_datetime(outage_fcst.Date).dt.normalize()

#add file name and process info to df
outage_fcst['FileName'] = oz.namelist()[0]
outage_fcst['ProcessDate'] = pd.to_datetime(outage_fcst.FileName.str.split('.').str[3]).dt.normalize()
outage_fcst['ProcessHour'] = outage_fcst.FileName.str.split('.').str[4]
outage_fcst['ProcessDT'] = pd.to_datetime(outage_fcst.FileName.str.split('.').str[3] + outage_fcst.FileName.str.split('.').str[4])
outage_fcst['ProcessHour'] = outage_fcst.ProcessHour.str[0:2]

#change hour ending data type to string
outage_fcst.HourEnding = outage_fcst.HourEnding.astype(str)

#add delivery date time
outage_fcst['DeliveryDT'] = pd.to_datetime(outage_fcst.Date) + outage_fcst.HourEnding.astype('timedelta64[h]')

#add weekday columns
outage_fcst['Weekday'] = outage_fcst.DeliveryDT.dt.day_name().str.upper()

#use pd.melt to unpivot columns
long_outage_fcst = pd.melt(outage_fcst, id_vars=['Date', 'HourEnding', 'FileName', 'ProcessDate', 'ProcessHour', 'ProcessDT', 'DeliveryDT', 'Weekday'],var_name='type', value_name ='mw')

#modify type columns
long_outage_fcst['type'] = long_outage_fcst.type.str.replace('Total','')
long_outage_fcst['type'] = long_outage_fcst.type.str.replace('MW','')

long_outage_fcst['type'] = long_outage_fcst.type.str.upper()

#rename columns
long_outage_fcst = long_outage_fcst.rename(columns = {'Date':'delivery_date', 'DeliveryDT':'delivery_dt','HourEnding':'delivery_hour', 
                                                  'Weekday':'weekday', 'ProcessDate':'process_date', 'ProcessHour':'process_hour', 'ProcessDT':'process_dt', 'FileName':'file_name'}, inplace=False)
#reorder columns
arranged_columns = ['delivery_dt', 'delivery_date', 'delivery_hour', 'weekday', 'type', 'mw', 'process_date', 'process_hour',
                    'process_dt', 'file_name']

long_outage_fcst = long_outage_fcst[arranged_columns]

# 3) ***********PULL WIND DATA******************

response = requests.get(wind_url)
wsoup = bs(response.text, 'html.parser')
wind_url_results = wsoup.find_all('td', class_='labelOptional', text=True)

data_urls = []

for result in wind_url_results:
    try:
        end_url = result.find('div').a['href']
        full_url = base_url + end_url
        data_urls.append(full_url)
        #print(full_url)
    except:
        pass
wdata = requests.get(data_urls[0], stream=True)

wz = zipfile.ZipFile(io.BytesIO(wdata.content))
wz.extractall()
file = wz.namelist()[0]

print(f'Wind File Name: {file}')

#load zip file to df
wind_fcst = pd.read_csv(file)

#delete zip file after reading
os.remove(file)

#clean existing columns
wind_fcst['DELIVERY_DATE'] = pd.to_datetime(wind_fcst.DELIVERY_DATE).dt.normalize()

#add file name and process info to df
wind_fcst['FileName'] = wz.namelist()[0]
wind_fcst['ProcessDate'] = pd.to_datetime(wind_fcst.FileName.str.split('.').str[3]).dt.normalize()
wind_fcst['ProcessHour'] = wind_fcst.FileName.str.split('.').str[4]
wind_fcst['ProcessDT'] = pd.to_datetime(wind_fcst.FileName.str.split('.').str[3] + wind_fcst.FileName.str.split('.').str[4])
wind_fcst['ProcessHour'] = wind_fcst.ProcessHour.str[0:2]

#change hour ending data type to string
wind_fcst.HOUR_ENDING = wind_fcst.HOUR_ENDING.astype(str)

#add delivery date time
wind_fcst['DeliveryDT'] = pd.to_datetime(wind_fcst.DELIVERY_DATE) + wind_fcst.HOUR_ENDING.astype('timedelta64[h]')

#add weekday columns
wind_fcst['Weekday'] = wind_fcst.DeliveryDT.dt.day_name().str.upper()

#use pd.melt to unpivot columns
long_wind_fcst = pd.melt(wind_fcst, id_vars=['DELIVERY_DATE', 'HOUR_ENDING', 'DSTFlag', 'FileName', 'ProcessDate', 'ProcessHour', 'ProcessDT', 'DeliveryDT', 'Weekday'],var_name='WindZone', value_name ='MW')

#extract type and zone from WindZone column
long_wind_fcst['Type'] = long_wind_fcst.WindZone.str.split('_').str[0]
long_wind_fcst['Zone'] = long_wind_fcst.WindZone.str.split('_', n=1).str[1]


#rename columns
long_wind_fcst = long_wind_fcst.rename(columns = {'DeliveryDT':'delivery_dt', 'DELIVERY_DATE':'delivery_date', 'HOUR_ENDING':'delivery_hour', 
                                                  'DSTFlag':'dst_flag', 'Weekday':'weekday', 'Type':'type', 'Zone':'zone', 'MW':'mw', 
                                                  'ProcessDate':'process_date', 'ProcessHour':'process_hour', 'ProcessDT':'process_dt', 'FileName':'file_name'}, inplace=False)
#reorder columns
arranged_columns = ['delivery_dt', 'delivery_date', 'delivery_hour', 'dst_flag', 'weekday', 'type', 'zone', 'mw', 'process_date', 'process_hour',
                    'process_dt', 'file_name']

long_wind_fcst = long_wind_fcst[arranged_columns]

# 4) ***********PULL SOLAR DATA******************

response = requests.get(solar_url)
ssoup = bs(response.text, 'html.parser')
solar_url_results = ssoup.find_all('td', class_='labelOptional', text=True)

data_urls = []

for result in solar_url_results:
    try:
        end_url = result.find('div').a['href']
        full_url = base_url + end_url
        data_urls.append(full_url)
        #print(full_url)
    except:
        pass

sdata = requests.get(data_urls[0], stream=True)

sz = zipfile.ZipFile(io.BytesIO(sdata.content))
sz.extractall()
file = sz.namelist()[0]

print(f'Solar File Name: {file}')

#load zip file to df
solar_fcst = pd.read_csv(file)

#delete zip file after reading
os.remove(file)

#clean existing columns
solar_fcst['DELIVERY_DATE'] = pd.to_datetime(solar_fcst.DELIVERY_DATE).dt.normalize()

#add file name and process info to df
solar_fcst['FileName'] = sz.namelist()[0]
solar_fcst['ProcessDate'] = pd.to_datetime(solar_fcst.FileName.str.split('.').str[3]).dt.normalize()
solar_fcst['ProcessHour'] = solar_fcst.FileName.str.split('.').str[4]
solar_fcst['ProcessDT'] = pd.to_datetime(solar_fcst.FileName.str.split('.').str[3] + solar_fcst.FileName.str.split('.').str[4])
solar_fcst['ProcessHour'] = solar_fcst.ProcessHour.str[0:2]

#change hour ending data type to string
solar_fcst.HOUR_ENDING = solar_fcst.HOUR_ENDING.astype(str)

#add delivery date time
solar_fcst['DeliveryDT'] = pd.to_datetime(solar_fcst.DELIVERY_DATE) + solar_fcst.HOUR_ENDING.astype('timedelta64[h]')

#add weekday columns
solar_fcst['Weekday'] = solar_fcst.DeliveryDT.dt.day_name().str.upper()

#use pd.melt to unpivot columns
long_solar_fcst = pd.melt(solar_fcst, id_vars=['DELIVERY_DATE', 'HOUR_ENDING', 'DSTFlag', 'FileName', 'ProcessDate', 'ProcessHour', 'ProcessDT', 'DeliveryDT', 'Weekday'],var_name='solar_zone', value_name ='mw')

#extract type and zone from WindZone column
long_solar_fcst['Type'] = long_solar_fcst.solar_zone.str.split('_').str[0]
long_solar_fcst['Zone'] = long_solar_fcst.solar_zone.str.split('_', n=1).str[1]


#rename columns
long_solar_fcst = long_solar_fcst.rename(columns = {'DeliveryDT':'delivery_dt', 'DELIVERY_DATE':'delivery_date', 'HOUR_ENDING':'delivery_hour', 
                                                  'DSTFlag':'dst_flag', 'Weekday':'weekday', 'Type':'type', 'Zone':'zone', 'MW':'mw', 
                                                  'ProcessDate':'process_date', 'ProcessHour':'process_hour', 'ProcessDT':'process_dt', 'FileName':'file_name'}, inplace=False)
#reorder columns
arranged_columns = ['delivery_dt', 'delivery_date', 'delivery_hour', 'dst_flag', 'weekday', 'type', 'zone', 'mw', 'process_date', 'process_hour',
                    'process_dt', 'file_name']

long_solar_fcst = long_solar_fcst[arranged_columns]


#5)*******Connect and load data to PostgreSQL****************
connection_string = f"postgres:trumpet5@localhost:5432/ercot_db"
engine = create_engine(f'postgresql://{connection_string}')

#fill tables
long_load_fcst.to_sql(name='da_load_fcst', con=engine, if_exists='append', index=False)
long_outage_fcst.to_sql(name='da_outage_fcst', con=engine, if_exists='append', index=False)
long_wind_fcst.to_sql(name='da_wind_fcst', con=engine, if_exists='append', index=False)
long_solar_fcst.to_sql(name='da_solar_fcst', con=engine, if_exists='append', index=False)
