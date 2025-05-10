import pandas as pd
import numpy as np
from google.transit import gtfs_realtime_pb2
import requests
import datetime
import os

def opoznienie(positions_df):
    rozklad=pd.read_csv('data/rozklad/stop_times.txt',delimiter=',')
    if positions_df.empty:
        print('puste dane')
    try:
        merged = pd.merge(positions_df,rozklad[['trip_id', 'stop_id', 'departure_time','stop_sequence']],on=['trip_id', 'stop_id','stop_sequence'],how='left')
    except(KeyError):
        print('proba obliczenia z pustych danych')
        return pd.DataFrame()
    merged['timestamp'] = pd.to_datetime(merged['timestamp'])
    merged['planned_departure'] = merged.apply(lambda row: parse_departure_time(row['departure_time'], row['timestamp']) 
                                               if pd.notnull(row['departure_time']) else pd.NaT,axis=1)
    merged['delay_sec'] = (merged['timestamp'] - merged['planned_departure']).dt.total_seconds()
    return merged




# def parse_departure_time(departure_time_str, timestamp):
#     h, m, s = map(int, departure_time_str.split(':'))
#     departure_time = datetime.timedelta(hours=h, minutes=m, seconds=s)
#     day_start = datetime.datetime.combine(timestamp.date(), datetime.datetime.min.time())
#     planned_time = day_start + departure_time
#     if abs((timestamp - planned_time).total_seconds()) > 12 * 3600:
#         planned_time -= datetime.timedelta(days=1)
    
#     return planned_time


def parse_departure_time(departure_time_str, timestamp):
    # Dodaj 2 godziny do timestampu, by wyrównać do lokalnego czasu (np. UTC+2)
    timestamp_local = timestamp + datetime.timedelta(hours=2)

    # Parsowanie rozkładowego czasu
    h, m, s = map(int, departure_time_str.split(':'))
    departure_time = datetime.timedelta(hours=h, minutes=m, seconds=s)

    # Początek dnia według timestampu (już przesuniętego)
    day_start = datetime.datetime.combine(timestamp_local.date(), datetime.datetime.min.time())

    # Obliczenie planowanego czasu odjazdu
    planned_time = day_start + departure_time

    # Korekta dnia jeśli potrzeba (np. 25:00 to kolejny dzień)
    if abs((timestamp_local - planned_time).total_seconds()) > 12 * 3600:
        planned_time -= datetime.timedelta(days=1)

    return planned_time
