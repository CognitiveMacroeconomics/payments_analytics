from keras.utils import Sequence
from dnb import TargetHandler
import numpy as np
import math


class RTGSSequence(Sequence):
    """
        Generator which takes a database handler object.
        Intended to train models with model.fit_generator.
        Takes additional parameter scenario which is a class
        to generate several scenarios with the existing data.

    """
    def __init__(self,db_handler, batch_size, window_size, scenario=False):
        self.batch_size = batch_size
        self.scenario = scenario
        self.x_size = db_handler.count()

    def __len__(self):
        return math.ceil(len(self.x) / self.batch_size)

    def __getitem__(self, idx):
        batch_x = self.x[idx * self.batch_size:(idx + 1) *
        self.batch_size]
        batch_y = self.y[idx * self.batch_size:(idx + 1) *
        self.batch_size]

        return np.array([
            resize(imread(file_name), (200, 200))
               for file_name in batch_x]), np.array(batch_y)

    def __repr__(self):
        return f"<RTGS Sequence generator with settings: batch_size: {self.batch_size}, scenarios: {self.scenario}>"


if __name__=="__main__":
    db_handler = TargetHandler("test", "test", "test", "test", db_type="postgres")
    generator = RTGSSequence(db_handler, 16)
    print(generator)