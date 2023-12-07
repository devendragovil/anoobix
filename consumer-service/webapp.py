# Importing all libraries
# First importing the fastapi related methods
from fastapi import FastAPI, Request, Form
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse

# dotenv requried for loading environment variables
import dotenv

# boto3 required for interacting with our dynamodb backend
import boto3

import json, os

# pandas for data manipulation
import pandas as pd
from typing import Annotated
import time

import pickle

from keplergl import KeplerGl
map = KeplerGl()


# Data Preprocessing Pipeline in pandas

# First loading all static files provided by Bart as static GTFS
df_trips = pd.read_csv('../google_transit_20230911-20240101_v3/trips.txt')
df_stops = pd.read_csv('../google_transit_20230911-20240101_v3/stops.txt')
df_stop_times = pd.read_csv('../google_transit_20230911-20240101_v3/stop_times.txt')
df_routes = pd.read_csv('../google_transit_20230911-20240101_v3/routes.txt')

# Selecting relevant columns from each
df_trips_rel = df_trips[['route_id', 'trip_id', 'direction_id']].copy()
df_stop_times_rel = df_stop_times[['trip_id', 'arrival_time', 'stop_id']].copy()
df_routes_rel = df_routes[['route_id', 'route_short_name', 'route_long_name']].copy()
df_stops_rel = df_stops[['stop_id', 'stop_name', 'stop_lat', 'stop_lon']].copy()

# Consolidating all the static files
df_consolidated = df_trips_rel.merge(df_stop_times_rel, how='left', on='trip_id')
df_consolidated = df_consolidated.merge(df_routes_rel, how='left', on='route_id')
df_consolidated = df_consolidated.merge(df_stops_rel, how='left', on='stop_id')

# direction_id of 1 means 'South' while 0 means 'North' --> related to domain understanding
df_consolidated['direction_name'] = df_consolidated['direction_id'].apply(lambda x: 'South' if x ==1 else 'North')

# Removing duplicate and unnecessary stops from the list. These are based on some cases
# related to transfers and accessibility etc. which are currently not supported in our app
df_stops_rel2 = df_stops_rel[
    (~df_stops_rel['stop_name'].str.contains('Enter/Exit'))
    &
    (~df_stops_rel['stop_id'].str.contains('place'))
].copy()

map.add_data(data=df_stops_rel2, name='stops')

with open('./consumer-service/config_to_use_kepler', 'rb') as f:
    config_map = pickle.load(f)

map.config = config_map



# Initializing a session for boto3 and a client for dynamodb
bt3s = boto3.Session(region_name='us-west-2',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    profile_name='dgovil-cli'
)
ddb = bt3s.client('dynamodb')

# loading environment variabls
dotenv.load_dotenv()

# Defining FastAPI app
app = FastAPI()

# Loading Jinja Templates for serving with FastAPI.
templates = Jinja2Templates(directory='consumer-service/templates/')

def convert_to_pst(timeepoch: int) -> str:
    utc_time = time.gmtime(timeepoch)
    return f"{utc_time.tm_hour - 8:02}:{utc_time.tm_min:02}:{utc_time.tm_sec:02}"


@app.get('/')
async def index(request: Request):
    return templates.TemplateResponse(
        'index.html',
        {
            "request": request,
            "input_html": df_stops_rel2[['stop_id', 'stop_name']].to_html(
                index=False,
                classes=['table', 'table-striped', 'table-hover', 'table-sm'],
                justify='left'
            )
        }
    )

@app.post('/status')
async def status_resp(
    stop_id: Annotated[str, Form()],
    request: Request
):
    if stop_id not in df_stops_rel2['stop_id'].values:
        return templates.TemplateResponse('failure.html', {"request": request})
    
    stop_id_user = stop_id
    
    # Getting all items from DynamoDB
    all_items = ddb.scan(TableName='trip-stream')

    # Finding the most recent timestamp
    all_timestamp_id = []
    for item in all_items['Items']:
        timestamp_id = int(item['timestamp_id']['S'])
        all_timestamp_id.append(timestamp_id)
    
    max_timestamp_id = max(all_timestamp_id)
        

    # Parsing the Json Structure into a Table
    values = {
        'timestamp_id': [],
        'time_expected': [],
        'trip_id': [],
        'stop_id': [],
        'delay': []
    }
    for item in all_items['Items']:
        timestamp_id = int(item['timestamp_id']['S'])
        if timestamp_id != max_timestamp_id:
            continue
        trip_id = int(item['trip_id']['S'])

        stop_time_update_dict = json.loads(item['stop_time_update_dict']['S'])

        # number of stop_time_updates in each item
        number_of_updates = len(stop_time_update_dict['delay'])

        for i in range(number_of_updates):
            values['timestamp_id'].append(timestamp_id)
            values['trip_id'].append(trip_id)

            delay = int(stop_time_update_dict['delay'][i])
            stop_id = str(stop_time_update_dict['stop_id'][i])
            time_expected = int(stop_time_update_dict['time_expected'][i])

            values['delay'].append(delay)
            values['time_expected'].append(time_expected)
            values['stop_id'].append(stop_id)

    df_trips_data = pd.DataFrame(values)

    df_trips_data['Last Updated'] = df_trips_data['timestamp_id'].apply(convert_to_pst)
    df_trips_data['Arriving At'] = df_trips_data['time_expected'].apply(convert_to_pst)

    df_trips_data_rel = df_trips_data[df_trips_data['stop_id'] == stop_id_user].copy()

    df_trips_data_rel = df_trips_data_rel.merge(df_consolidated, how='left', on=['trip_id', 'stop_id'])

    df_trips_data_rel['Scheduled Arrival Time'] = df_trips_data_rel['arrival_time']
    df_trips_data_rel['Delay (min)'] = df_trips_data_rel['delay']/60
    df_trips_data_rel['Delay (sec)'] = df_trips_data_rel['delay']
    df_trips_data_rel.sort_values(by='time_expected', ascending=True, inplace=True)

    relevant_columns = [
        'Last Updated',
        'Arriving At',
        'trip_id',
        'stop_id',
        'stop_name',
        'Delay (min)',
        'Delay (sec)',
        'Scheduled Arrival Time',
        'route_short_name',
        'route_long_name',
        'direction_name'
    ]

    df_trips_data_rel = df_trips_data_rel[relevant_columns].copy()

    return templates.TemplateResponse(
        'success.html',
        {
            "request": request,
            "input_html": df_trips_data_rel.to_html(
                index=False,
                classes=['table', 'table-striped', 'table-hover', 'table-sm'],
                justify='left'
            ),
            "map_html": map._repr_html_()
        }
    )

    
