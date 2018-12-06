'''

Implements the default classes, Zip and ZipScheduler.
(Modified schedulers in schedulers.py)

'''

import pandas as pd
import numpy as np

hosp_df = pd.read_csv("hospitals.csv", names=["hospital_name", "north", "east"])
# add base coordinates (0,0) to dataframe
hosp_df.loc[len(hosp_df)] = ("base", 0, 0)
orders_df = pd.read_csv("orders.csv", names=["received_time", "hospital_name", "priority"])

class Zip:
    """
    A class that represents each Zip that delivers orders.

    Methods
    ------
    dispatch - dispatches an order
    update_status - updates whether zip is at base at the current time
    """

    def __init__(self, flight_speed = 30):
        self.flight_speed = flight_speed
        self.at_base = True
        self.leaving_time = 0
        self.return_time = 0
        self.trips_made = 0

    def dispatch(self, current_time, dist_to_travel):
        self.at_base = False
        flight_time = dist_to_travel/self.flight_speed
        self.return_time = current_time + flight_time
        self.trips_made += 1

        return self.return_time

    def update_status(self, current_time):
        if current_time >= self.return_time:
            self.at_base = True
        else:
            self.at_base = False

class ZipScheduler:
    """
    A class that schedules orders.

    Methods
    ------
    makedict - makes dict of hospital distances
    send_zip - sends next zip (if available)
    zips_available - returns number of zips available
    queue_order - queues orders into 2 queues
                  (emergency and resupply) in order of reception
    q_remove - returns queue without that order (no side effects)
    hosp_distance - finds dist between two hospitals, used in makedict
    route_distance - given ordered hospitals, return distance of route
    find_next - helper function for schedule_next_flight:
                finds next order to fulfill
    find_orders - helper function for schedule_next_flight:
                  iterates through both queues
    schedule_next_flight - schedules flight. Returns orders,
                           wait_times, flight_time if scheduled.
                           If no available zips, prints out string telling us
                           this. Else returns None.

    """

    def __init__(self, hosp_df, total_zips=10, max_load=3, flight_speed=30, max_range=160000):
        self.total_zips = total_zips
        self.max_load = max_load
        self.flight_speed = flight_speed
        self.max_range = max_range
        self.hosp_df = hosp_df
        self.emergency = []
        self.resupply = []
        self.dists = self.makedict(self.hosp_df)

        # create a list of our 10 zips
        self.ZipList = [Zip() for i in range(total_zips)]

    def makedict(self, hosp_df):
        d = {}
        for i in range(len(hosp_df)):
            hi = hosp_df.loc[i]
            d[hi["hospital_name"]] = {}
            for j in range(len(hosp_df)):
                hj = hosp_df.loc[j]
                d[hi["hospital_name"]][hj["hospital_name"]] = self.hosp_distance(hi["hospital_name"], hj["hospital_name"])
        return d


    def send_zip(self, current_time, dist_to_travel):
        for z in self.ZipList:
            z.update_status(current_time)
        available_zip = next((z for z in self.ZipList if z.at_base), None)
        if available_zip:
            return available_zip.dispatch(current_time, dist_to_travel)
        else:
            return None

    def zips_available(self, current_time):
        for z in self.ZipList:
            z.update_status(current_time)
        return len([z for z in self.ZipList if z.at_base])

    def queue_order(self, received_time, hospital, priority):
        priority = priority.strip()
        if priority == 'Emergency':
            self.emergency.append({"received_time": received_time,
                                       "hospital": hospital.strip(),
                                       "priority": priority})
        elif priority == 'Resupply':
            self.resupply.append({"received_time": received_time,
                                      "hospital": hospital.strip(),
                                      "priority": priority})

    def q_remove(self, queue, *args):
        return [x for x in queue if x not in args]

    def hosp_distance(self, x, y):
        # distance between hospitals named x and y

        def find_coord(coord, direction):
            return hosp_df.loc[hosp_df["hospital_name"] == coord, direction]

        north_x = find_coord(x, "north")
        east_x = find_coord(x, "east")
        north_y = find_coord(y, "north")
        east_y = find_coord(y, "east")

        return np.sqrt((int(north_x) - int(north_y))**2 + (int(east_x) - int(east_y))**2)


    def route_distance(self, *args):
        # find distance to go back to base from either end
        dist = self.dists["base"][args[0]["hospital"]] + self.dists["base"][args[-1]["hospital"]]

        # find distance between points
        for i in range(len(args) - 1):
            dist += self.dists[args[i]["hospital"]][args[i+1]["hospital"]]
        return dist

    def find_next(self, queue, *args):
        if args:
            # try to find a next order that goes to the same hospital as the last order
            same_hosp = next((order for order in queue
                              if order["hospital"] == args[-1]["hospital"]), None)
            if same_hosp:
                new_queue = self.q_remove(queue, same_hosp)
                return same_hosp, new_queue

        # else find next order in given queue that is within flight range, else returns None
        close_hosp = next((order for order in queue
                          if self.route_distance(*args, order) < self.max_range), None)
        if close_hosp:
            new_queue = self.q_remove(queue, close_hosp)
            return close_hosp, new_queue
        return None, queue


    def find_orders(self):
        orders = []
        if self.emergency:
            while len(orders) < self.max_load:
                order, new_queue = self.find_next(self.emergency, *orders)
                if order:
                    self.emergency = new_queue
                    orders.append(order)
                else:
                    break

            while len(orders) < self.max_load:
                r_order, new_queue = self.find_next(self.resupply, *orders)
                if r_order:
                    self.resupply = new_queue
                    orders.append(r_order)
                else:
                    break

        elif self.resupply:
            while len(orders) < self.max_load:
                r_order, new_queue = self.find_next(self.resupply, *orders)
                if r_order:
                    self.resupply = new_queue
                    orders.append(r_order)
                else:
                    break

        else:
            return None
        return orders

    def schedule_next_flight(self, current_time):
        # first order will be first in line by default
        if self.zips_available(current_time):

            orders = self.find_orders()

            if orders:
                dist_to_travel = self.route_distance(*orders)
                wait_times = [{"priority": order["priority"],
                               "wait": (current_time - order["received_time"])}
                               for order in orders]

                # find next available zip, deploy if available
                return_time = self.send_zip(current_time, dist_to_travel)
                if return_time:
                    flight_time = return_time - current_time
                    return orders, wait_times, flight_time
                else:
                    return "No available zips right now, please wait"
            else:
                return None

        else:
            return "No available zips right now, please wait"
