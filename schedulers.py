'''

Implements modified/enhanced schedulers, ZipScheduler_NextOrd, ZipScheduler_Greedy, and ZipScheduler_SP.
(Modified schedulers in schedulers.py)

'''
from zipline import Zip, ZipScheduler
import pandas as pd
import numpy as np


class ZipScheduler_NextOrd(ZipScheduler):
    """
    A class that just takes the next order that's within range
    """

    def __init__(self, hosp_df, total_zips=10, max_load=3, flight_speed=30, max_range=160000):
        ZipScheduler.__init__(self, hosp_df, total_zips, max_load, flight_speed, max_range)

    def find_next(self, queue, *args):
        # find next order in given queue that is within flight range, else returns None
        close_hosp = next((order for order in queue
                           if self.route_distance(*args, order) < self.max_range), None)
        if close_hosp:
            new_queue = self.q_remove(queue, close_hosp)
            return close_hosp, new_queue
        return None, queue

class ZipScheduler_Greedy(ZipScheduler):
    '''
    A class that finds the closest hospital in queue for each next order
    '''

    def __init__(self, hosp_df, total_zips=10, max_load=3, flight_speed=30, max_range=160000):
        ZipScheduler.__init__(self, hosp_df, total_zips, max_load, flight_speed, max_range)

    def find_next(self, queue, *args):
        if queue:
            # sort by distance to last order
            closest_hosp = sorted(queue, key=lambda x: self.route_distance(*args, x))[0]
            if closest_hosp and self.route_distance(*args, closest_hosp) < self.max_range:
                new_queue = self.q_remove(queue, closest_hosp)
                return closest_hosp, new_queue
        return None, queue


class ZipScheduler_SP(ZipScheduler):
    '''
    A class that implements a shortest path algorithm for order length > 2
    '''

    def __init__(self, hosp_df, total_zips=10, max_load=3, flight_speed=30, max_range=160000):
        ZipScheduler.__init__(self, hosp_df, total_zips, max_load, flight_speed, max_range)

    def shortest_path(self, *args):
        best_route = []
        best_length = float('inf')
        orders = [arg for arg in args]

        for i_order, order in enumerate(orders):
            route = [i_order]
            length = self.dists["base"][order["hospital"]]

            while len(route) < len(orders):
                i_order, nextord, dist = self.get_closest(i_order, orders, route)
                length += dist
                route.append(i_order)

            try:
                length += self.dists["base"][nextord["hospital"]]
            except:
                length += self.dists["base"][order["hospital"]]

            if length < best_length:
                best_length = length
                best_route = route

        best_route_orders = [orders[i] for i in best_route]

        return best_route_orders, best_length

    def get_closest(self, i_order, orders, visited):
        best_distance = float('inf')

        for i, o in enumerate(orders):
            if i not in visited:
                distance = self.dists[o["hospital"]][orders[i_order]["hospital"]]

                if distance < best_distance:
                    closest = o
                    i_closest = i
                    best_distance = distance

        return i_closest, closest, best_distance

    def schedule_next_flight(self, current_time):
        # first order will be first in line by default
        if self.zips_available(current_time):

            orders = self.find_orders()

            if orders:
                if len(orders) > 2:
                    best_route, dist_to_travel = self.shortest_path(*orders)
                else:
                    best_route, dist_to_travel = orders, self.route_distance(*orders)

                wait_times = [{"priority": order["priority"],
                               "wait": (current_time - order["received_time"])}
                               for order in best_route]

                # find next available zip, deploy if available
                return_time = self.send_zip(current_time, dist_to_travel)
                if return_time:
                    flight_time = return_time - current_time
                    return best_route, wait_times, flight_time
                else:
                    return "No available zips right now, please wait"
            else:
                return None

        else:
            return "No available zips right now, please wait"
