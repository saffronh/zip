'''
Unit tests for various methods in different classes.

'''
import unittest
import numpy as np
import pandas as pd
from itertools import permutations
from zipline import Zip, ZipScheduler
from schedulers import ZipScheduler_Greedy, ZipScheduler_SP, ZipScheduler_NextOrd

def queue_test_orders(scheduler, num):
    for i in range(num):
        order = orders_df.loc[i]
        scheduler.queue_order(order["received_time"],
                              order["hospital_name"],
                              order["priority"])

class TestTraveling(unittest.TestCase):

    def test_hosp_distance(self):
        self.assertEqual(int(zs.hosp_distance("Kaduha", "base")), 39789)
        self.assertEqual(int(zs.hosp_distance("Gitwe", "Kabaya")), 57882)
        self.assertEqual(int(zs.hosp_distance("Gitwe", "base")),
                         int(np.sqrt((-18412)**2+(-10076)**2)))

    def test_zip_dispatch(self):
        self.assertEqual(zs.send_zip(30000, 300), 30010)
        self.assertEqual(zs.zips_available(30002), 9)
        self.assertEqual(zs.zips_available(30030), 10)

    def test_queue(self):
        zs.queue_order(20000, "Gitwe", "Resupply")
        zs.queue_order(30000, "Kaduha", "Emergency")
        self.assertEqual(zs.resupply.pop(), {"received_time": 20000,
                                             "hospital": "Gitwe",
                                             "priority": "Resupply"})
        self.assertEqual(zs.emergency.pop(), {"received_time": 30000,
                                              "hospital": "Kaduha",
                                              "priority": "Emergency"})
        self.assertFalse(zs.resupply)
        self.assertFalse(zs.emergency)

    def test_scheduler_greedy(self):
        zsg = ZipScheduler_Greedy(hosp_df = hosp_df)
        # queue orders - we know that it will take Gitwe first b.c. its closest
        # to base, and then Kaduha because it's the next closest to Gitwe, then
        # Kigeme because it's the next closest to Kaduha.
        zsg.queue_order(1, "Kigeme", "Emergency")
        zsg.queue_order(1, "Kabaya", "Emergency")
        zsg.queue_order(1, "Kaduha", "Emergency")
        zsg.queue_order(1, "Gitwe", "Emergency")
        hospdists = [zsg.hosp_distance("base", h) for h in
                     ["Kigeme", "Kabaya", "Kaduha", "Gitwe"]]
        self.assertTrue(min(hospdists) == zsg.hosp_distance("base", "Gitwe"))
        self.assertEqual([h["hospital"] for h in zsg.schedule_next_flight(5)[0]],
                        ["Gitwe", "Kaduha", "Kigeme"])

    def test_scheduler_sp(self):
        zs_sp = ZipScheduler_SP(hosp_df = hosp_df)
        zs_sp.queue_order(1, "Kigeme", "Emergency")
        zs_sp.queue_order(1, "Kabaya", "Emergency")
        zs_sp.queue_order(1, "Kaduha", "Emergency")
        zs_sp.queue_order(1, "Kabaya", "Emergency")
        zs_sp.queue_order(1, "Gitwe", "Emergency")
        zs_sp.queue_order(1, "Kabgayi", "Emergency")
        zs_sp.queue_order(1, "Ruhango", "Emergency")
        self.assertEqual(zs_sp.schedule_next_flight(5)[2]*30,
                         zs_sp.shortest_path({"hospital": "Kaduha"},
                                             {"hospital": "Kigeme"},
                                             {"hospital": "Gitwe"})[1])
    def test_scheduler_regular(self):
        zsr = ZipScheduler(hosp_df = hosp_df)
        queue_test_orders(zsr, 10)
        # takes first
        self.assertEqual(zsr.schedule_next_flight(50000)[0][0]["hospital"], "Gitwe")
        # if we empty the emergency list, resupplies are considered
        zsr.emergency = []
        self.assertEqual(zsr.schedule_next_flight(50000)[0][0]["priority"], "Resupply")
        # queue 1 emergency; order will be emergency/resupply/resupply
        zsr.queue_order(1, "Kigeme", "Emergency")
        self.assertEqual([h["priority"] for h in zsr.schedule_next_flight(50000)[0]],
                        ["Emergency", "Resupply", "Resupply"])
        zsr.emergency = []
        zsr.resupply = []
        zsr.queue_order(70000, "Kigeme", "Emergency")
        self.assertEqual(zs.find_next(zs.emergency, []), (None, []))
        self.assertEqual(zs.find_orders(), None)
        self.assertEqual(len(zsr.schedule_next_flight(75000)[0]), 1)

        # test zips available function
        self.assertEqual(zsr.zips_available(75001), 9)
        self.assertEqual(zsr.zips_available(99001), 10)

    def test_scheduler_nextord(self):
        zs_nextord = ZipScheduler_NextOrd(hosp_df = hosp_df)
        zs_nextord.queue_order(1, "Kigeme", "Emergency")
        zs_nextord.queue_order(1, "Kabaya", "Emergency")
        zs_nextord.queue_order(1, "Kaduha", "Emergency")
        zs_nextord.queue_order(1, "Gitwe", "Emergency")
        # not within range if next 3 all taken
        self.assertTrue(zs_nextord.route_distance(*zs_nextord.emergency[0:3]) > zs_nextord.max_range)
        # within range if Kabaya skipped
        self.assertTrue(zs_nextord.route_distance(zs_nextord.emergency[0],
                        zs_nextord.emergency[2], zs_nextord.emergency[3])
                        < zs_nextord.max_range)
        self.assertEqual([h["hospital"] for h in zs_nextord.schedule_next_flight(5)[0]],
                        ["Kigeme", "Kaduha", "Gitwe"])

if __name__ == '__main__':
        # read in hospitals
    hosp_df = pd.read_csv("hospitals.csv",
                          names=["hospital_name", "north", "east"])
    # add base coordinates (0,0) to dataframe
    hosp_df.loc[len(hosp_df)] = ("base", 0, 0)
    orders_df = pd.read_csv("orders.csv",
                            names=["received_time", "hospital_name", "priority"])

    zs = ZipScheduler(hosp_df = hosp_df)
    unittest.main()
