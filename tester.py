'''
Tests different schedulers for performance.

Returns for each scheduler:
Number of times a zip was unavailable
Mean wait time for emergency (seconds)
Mean wait time for resupply (seconds)
Mean flight time for delivery (seconds)

'''
import pandas as pd
import numpy as np
from zipline import Zip, ZipScheduler
from schedulers import ZipScheduler_Greedy, ZipScheduler_SP, ZipScheduler_NextOrd

def test_scheduler(scheduler, start_time = 25640, end_time = 71840, interval = 60):

    count_unavailable = 0
    wait_time_em = []
    wait_time_re = []
    flight_time = []

    for i in range(start_time, end_time, interval):

        for _, order in orders_df[orders_df['received_time'].between(i-interval, i-1, inclusive=True)].iterrows():
            scheduler.queue_order(order["received_time"], order["hospital_name"], order["priority"])

        ans = scheduler.schedule_next_flight(i)

        if ans:
            if 'available' in ans:
                count_unavailable += 1
            else:
                for order in ans[1]:
                    if order["priority"] == 'Emergency':
                        wait_time_em.append(order["wait"])
                    if order["priority"] == 'Resupply':
                        wait_time_re.append(order["wait"])
                    flight_time.append(ans[2])

    return {"count_unavailable": count_unavailable,
             "wait_emergency": np.mean(wait_time_em),
             "wait_resupply": np.mean(wait_time_re),
             "flight_time": np.mean(flight_time)}


if __name__ == '__main__':
    hosp_df = pd.read_csv("hospitals.csv", names=["hospital_name", "north", "east"])
    # add base coordinates (0,0) to dataframe
    hosp_df.loc[len(hosp_df)] = ("base", 0, 0)
    orders_df = pd.read_csv("orders.csv", names=["received_time", "hospital_name", "priority"])

    zs = ZipScheduler(hosp_df=hosp_df)
    zs_results = test_scheduler(zs)


    zs_nextord = ZipScheduler_NextOrd(hosp_df=hosp_df)
    zs_nextord_results = test_scheduler(zs_nextord)


    zs_greedy = ZipScheduler_Greedy(hosp_df=hosp_df)
    zs_greedy_results = test_scheduler(zs_greedy)


    zs_sp = ZipScheduler_SP(hosp_df=hosp_df)
    zs_sp_results = test_scheduler(zs_sp)

    scheduler_types = ["Regular Scheduler (prioritizes delivery to same place)",
                       "Scheduler that just takes next in range",
                       "Greedy Scheduler",
                       "Scheduler with Shortest Path"]

    results = [zs_results, zs_nextord_results, zs_greedy_results, zs_sp_results]

    for i in range(len(results)):
        print("\n Results for", scheduler_types[i])
        print("Number of times a zip was unavailable: ", results[i]["count_unavailable"])
        print("Mean wait time for emergency (seconds): ", results[i]["wait_emergency"])
        print("Mean wait time for resupply (seconds): ", results[i]["wait_resupply"])
        print("Mean flight time for delivery (seconds): ", results[i]["flight_time"])
