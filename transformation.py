"""
Created on Thu Jun 23, 2022

@author: teohz
"""

class Transformation:
    def __init__(self, sample_rate = 30):
        # sample rate per second (must be a positive factor of 30)
        self.SAMPLE_RATE = sample_rate
        self.valid_sample_rate = 30 % self.SAMPLE_RATE == 0 and self.SAMPLE_RATE > 0
        if (not self.valid_sample_rate):
            print("[Transformation] Init ERROR: Sample rate is not valid. Needs to be a positive factor of 30")

    def transform_trajectory(self, traj):
        """
        Accepts a trajectory document as parameter and return a dictionary of 
        timestamps with the following schema: 
            {
                t1: [id1, x1, y1],
                t2: [id2, x2, y2],
                ...
            }
        where 
        - id is the vehicle's ObjectID from mongoDB, 
        - x, y are the positions of the vehicle at time t1
        - t1 is the timestamp
        """
        if not self.valid_sample_rate:
            print("[Transformation] Transform ERROR: Sample rate is not valid. Needs to be a positive factor of 30")
            return

        vehicle_id = traj["_id"]
        batch_operations = {}
        c1 = 0
        c2 = 0
        for i in range(len(traj["timestamp"])):
            # TODO: need to normalize time and skip objects based on frame rate
            time = traj["timestamp"][i]
            if time % (1 / self.SAMPLE_RATE) != 0:
                print("NOT ZERO (time: {})".format(time))
                c1 += 1
            else:
                print("ZERO (time: {})".format(time))
                c2 += 1
            x = traj["x_position"][i]
            y = traj["y_position"][i]
            batch_operations[time] = [vehicle_id, x, y]
        print("score: {} vs {}".format(c1, c2))
        # print("\ntransformation complete")
        # for op in batch_operations:
        #     print("dict[{}]: {}".format(op, batch_operations[op]))
        return batch_operations

    def main_loop(self, change_stream_connection, batch_update_connection):
        """
        A child process for transformation. 
        1. Listens to change_stream_connection for trajectory documents. 
        2. Transforms the received trajectory into a dictionary of timestamps: 
            {
                t1: [id1, x1, y1],
                t2: [id2, x2, y2],
                ...
            }
        3. Sends the dictionary of timestamp to batch_update
        """
        if not self.valid_sample_rate:
            print("[Transformation] Main Loop ERROR: Sample rate is not valid. Needs to be a positive factor of 30")
            return

        while True:
            traj_doc = change_stream_connection.get()
            batch_operations = self.transform_trajectory(traj_doc)
            batch_update_connection.send(batch_operations)