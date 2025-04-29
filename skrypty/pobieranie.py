import pandas as pd
from google.transit import gtfs_realtime_pb2
import requests
import datetime
import zipfile
import io
import os
import opoznienie as opoz


def pobranie_danych(old_frame_positions=pd.DataFrame(),old_frame_updates=pd.DataFrame()):
    feed = gtfs_realtime_pb2.FeedMessage()
    response = requests.get('https://gtfs.ztp.krakow.pl/VehiclePositions_A.pb')

    feed.ParseFromString(response.content)

    vehicles = []
    for entity in feed.entity:
        if entity.HasField('vehicle'):
            vehicle = entity.vehicle
            if vehicle.current_status==1:
                vehicles.append({
                    'vehicle_id': vehicle.vehicle.id,
                    'trip_id': vehicle.trip.trip_id,
                    'route_id': vehicle.trip.route_id,
                    'latitude': vehicle.position.latitude,
                    'longitude': vehicle.position.longitude,
                    'speed': vehicle.position.speed,
                    'timestamp': datetime.datetime.fromtimestamp(vehicle.timestamp),
                    'stop_id':vehicle.stop_id,
                    'current_stop_sequence':vehicle.current_stop_sequence,
                    'current_status': vehicle.current_status,
                    'current_date': datetime.date.today(),
                    'day': datetime.date.today().isoweekday()
                })

    df_positions = pd.DataFrame(vehicles)
    previous_df_positions=old_frame_positions.copy()

    try:
        pd.testing.assert_frame_equal(df_positions,previous_df_positions)
    except(AssertionError):
        os.makedirs('data/positions',exist_ok=True)
        print(f'nowe dane autobusow {datetime.datetime.now()}')
        name=f'data/positions/vehicle_positions_{datetime.datetime.now().strftime('%Y-%m-%d_%H%M%S')}.csv'
        df_positions.to_csv(name, index=False)


    previous_df_positions=df_positions.copy()

    opoz.opoznienie(previous_df_positions)

    feed = gtfs_realtime_pb2.FeedMessage()
    response = requests.get('https://gtfs.ztp.krakow.pl/TripUpdates_A.pb')
    feed.ParseFromString(response.content)
    updates=[]
    for entity in feed.entity:
        if entity.HasField('trip_update'):
            trip_update = entity.trip_update
            trip_id = trip_update.trip.trip_id

            for stu in trip_update.stop_time_update:
                update = {
                    'trip_id': trip_id,
                    'stop_id': stu.stop_id,
                    'arrival_time': None,
                    'departure_time': None,
                    'timestamp': datetime.datetime.now()
                }
                if stu.HasField('arrival'):
                    update['arrival_time'] = datetime.datetime.fromtimestamp(stu.arrival.time)
                if stu.HasField('departure'):
                    update['departure_time'] = datetime.datetime.fromtimestamp(stu.departure.time)

                updates.append(update)

    df_updates = pd.DataFrame(updates)
    previous_df_updates=old_frame_updates.copy()

    try:
        pd.testing.assert_frame_equal(df_updates,previous_df_updates)
    except(AssertionError):
        os.makedirs('data/updates',exist_ok=True)
        print(f'nowe dane opoznien {datetime.datetime.now()}')
        name=f'data/updates/trip_updates_{datetime.datetime.now().strftime('%Y-%m-%d_%H%M%S')}.csv'
        df_updates.to_csv(name, index=False)
    
    previous_df_updates=df_updates.copy()




    return (previous_df_positions,previous_df_updates)

def pobranie_rozkladu():
    r=requests.get('https://gtfs.ztp.krakow.pl/GTFS_KRK_A.zip',stream=True)
    z=zipfile.ZipFile(io.BytesIO(r.content))
    os.makedirs('data/rozklad',exist_ok=True)
    print('pobrano rozklad')
    z.extractall('data/rozklad')
