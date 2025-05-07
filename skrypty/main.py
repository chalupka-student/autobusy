import pobieranie as pob
import time
import sched
import pandas as pd
import datetime
import os

HOURLY_FLAG = 'last_hourly_run.txt'

schedule=sched.scheduler(time.time,time.sleep)


def petla_15_sekund(schedule,old_positions,old_updates):
    (positions_old,updates_old)=pob.pobranie_danych(old_positions,old_updates)
    check_hourly_trigger()
    schedule.enter(10,1,petla_15_sekund,(schedule,positions_old,updates_old))


def hourly_tasks():
    pob.pobranie_rozkladu()
    pob.remove_duplicates_from_file('data/opoznienia/delays.csv')
    pob.remove_duplicates_from_file('data/opoznienia/delays_short.csv')
    print(f"[HOURLY] Tasks done at {datetime.datetime.now()}")


def check_hourly_trigger():
    now = datetime.datetime.now()
    current_hour = now.replace(minute=0, second=0, microsecond=0)

    if os.path.exists(HOURLY_FLAG):
        with open(HOURLY_FLAG, 'r') as f:
            last_run = datetime.datetime.fromisoformat(f.read().strip())
    else:
        last_run = datetime.datetime.min

    if current_hour > last_run:
        hourly_tasks()
        with open(HOURLY_FLAG, 'w') as f:
            f.write(current_hour.isoformat())

empty=pd.DataFrame()

petla_15_sekund(schedule,empty,empty)
schedule.run()