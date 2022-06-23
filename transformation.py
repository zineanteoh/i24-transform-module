"""
Created on Thu Jun 23, 2022

@author: teohz
"""

import batch_update

class Transformation:
    def __init__(self, batch_update):
        # cache for storing the trajectories
        self.cache = {}
        # sample rate per second (<= 30)
        self.sample_rate = 30
        # batch update object
        self.batch_update = batch_update

    def transform_trajectory(self, traj):
        vehicle_id = traj["id"]

        print("transformed!")
        return 