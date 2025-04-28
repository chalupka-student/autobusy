import pobieranie as pob
import time
import sched
import pandas as pd

cnt=0

schedule=sched.scheduler(time.time,time.sleep)

schedule_1day=sched.scheduler(time.time,time.sleep)

def petla_15_sekund(schedule,old_positions,old_updates,cnt):
    (positions_old,updates_old)=pob.pobranie_danych(old_positions,old_updates)
    cnt+=1
    if cnt==3:
        pob.pobranie_rozkladu()
        cnt=0
    schedule.enter(15,1,petla_15_sekund,(schedule,positions_old,updates_old,cnt))
    




test=pd.DataFrame()

petla_15_sekund(schedule,test,test,cnt)
schedule.run()