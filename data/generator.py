from keras.utils import Sequence
#import TargetHandler
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
        self.window_size = window_size

    def __len__(self):
        return math.ceil(len(self.x) / self.batch_size)

    def __getitem__(self, idx):
        batch_x = self.x[idx * self.batch_size:(idx + 1) *
        self.batch_size]
        if self.window_size == 1:
            batch_x = [np.expand_dims(x, axis=0) for x in batch_x]
    
        return np.array([batch_x, batch_x])

    def __repr__(self):
        return f"<RTGS Sequence generator with settings: batch_size: {self.batch_size}, scenarios: {self.scenario}>"
    
     