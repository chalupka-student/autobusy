import pandas as pd
from google.transit import gtfs_realtime_pb2
import requests
import datetime
import os

def opoznienie(positions_df):
    ref_df=pd.read_csv('data/rozklad/stop_times.txt',delimiter=',')

    ref_df['arrival_seconds'] = ref_df['arrival_time'].apply(lambda x: 
        sum(int(a) * b for a, b in zip(x.split(':'), [3600, 60, 1])))

    # Klucz do połączenia: trip_id + stop_id
    merged = pd.merge(
        positions_df,
        ref_df[['trip_id', 'stop_id', 'arrival_seconds']],
        on=['trip_id', 'stop_id'],
        how='left'
    )
    print(merged.head())

    # Konwersja timestamp z pozycji na datetime + sekundy od północy
    merged['timestamp'] = pd.to_datetime(merged['timestamp'])
    merged['actual_seconds'] = merged['timestamp'].dt.hour * 3600 + merged['timestamp'].dt.minute * 60 + merged['timestamp'].dt.second

    # Obliczanie opóźnienia w sekundach
    merged['delay_seconds'] = merged['actual_seconds'] - merged['arrival_seconds']

    