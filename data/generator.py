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
        self.x = db_handler.get_all()

    def __len__(self):
        return math.ceil(len(self.x) / self.batch_size)

    def __getitem__(self, idx):
        batch_x = self.x[idx * self.batch_size:(idx + 1) *
        self.batch_size]
    
        return np.array([batch_x, batch_x])

    def __repr__(self):
        return f"<RTGS Sequence generator with settings: batch_size: {self.batch_size}, scenarios: {self.scenario}>"


if __name__=="__main__":
    db_handler = TargetHandler("localhost","tempdb","sa","123456QWERD!")
    generator = RTGSSequence(db_handler, 16, 4)
    print(generator)
    print(generator.__getitem__(1))
    
     