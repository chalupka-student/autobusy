import pandas as pd
import numpy as np
from google.transit import gtfs_realtime_pb2
import requests
import datetime
import os

def opoznienie(positions_df):
    rozklad=pd.read_csv('data/rozklad/stop_times.txt',delimiter=',')
    merged = pd.merge(positions_df,rozklad[['trip_id', 'stop_id', 'departure_time','stop_sequence']],on=['trip_id', 'stop_id','stop_sequence'],how='left')
    merged['timestamp'] = pd.to_datetime(merged['timestamp'])
    merged['planned_departure'] = merged.apply(lambda row: parse_departure_time(row['departure_time'], row['timestamp']) 
                                               if pd.notnull(row['departure_time']) else pd.NaT,axis=1)
    merged['delay_sec'] = (merged['timestamp'] - merged['planned_departure']).dt.total_seconds()
    return merged


def parse_departure_time(departure_time_str, timestamp):
    h, m, s = map(int, departure_time_str.split(':'))
    departure_time = datetime.timedelta(hours=h, minutes=m, seconds=s)
    day_start = datetime.datetime.combine(timestamp.date(), datetime.datetime.min.time())
    planned_time = day_start + departure_time
    if abs((timestamp - planned_time).total_seconds()) > 12 * 3600:
        planned_time -= datetime.timedelta(days=1)
    
    return planned_time


