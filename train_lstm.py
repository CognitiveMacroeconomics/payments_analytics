from pandas.io import gbq
import google.auth
import os
import pandas as pd
import numpy as np
import seaborn as sns
from data import MemoryPreparer
from models.lstm import make_lstm


#global params
WINDOW_SIZE = 5
HIDDEN_LAYER_SIZE = 32
EPOCHS = 500
BATCH_SIZE = 200

class BigQueryHandler:

    def __init__(self,query, prj_id):

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'data.credentials.json'
        self.data_df = gbq.read_gbq(query, project_id=prj_id)

    def get_dataframe(self):
        return self.data_df

if __name__ == "__main__":

    query_1 = "SELECT * FROM\
            acs-research-prj.deeplearning.payment_transaction_train_set6\
                order by YEAR, MONTH, WEEKNUMBER, DAY, HOURS, MINUTES"

    
    
    prj_id = "acs-research-prj"
    lvts_prased_train = BigQueryHandler(query_1, prj_id)
    lvts_parsed_train_df = lvts_prased_train.get_dataframe()

    print(lvts_parsed_train_df.head())

    query_2 = "SELECT * FROM\
            acs-research-prj.deeplearning.payment_transaction_validate_set6\
                order by YEAR, MONTH, WEEKNUMBER, DAY, HOURS, MINUTES"

    lvts_prased_validate = BigQueryHandler(query_2, prj_id)
    lvts_parsed_validate_df = lvts_prased_validate.get_dataframe()

    print(lvts_parsed_validate_df.head())

    mp_train = MemoryPreparer(window = True, batch_size = BATCH_SIZE,\
                            window_size = WINDOW_SIZE)
    lvts_windowed_train_gen = mp_train.prepare(lvts_parsed_train_df)
    mp_val = MemoryPreparer(window = True, batch_size = BATCH_SIZE,\
                            window_size = WINDOW_SIZE)
    lvts_windowed_val_gen = mp_val.prepare(lvts_parsed_validate_df)

    #print("here1")
    #print(type(lvts_windowed_train_gen))
    #print(lvts_windowed_train_gen.shape)
    #print(type(lvts_windowed_val_gen))
    #print(lvts_windowed_val_gen.shape)

   #Make model
    inp_shape = (WINDOW_SIZE ,lvts_windowed_train_gen.shape[2])
    model = make_lstm(input_shape = inp_shape,\
                    hidden_layer_size=HIDDEN_LAYER_SIZE)
    model.compile(optimizer="adam",loss='mse')

    #Fit model
    history = model.fit(lvts_windowed_train_gen, lvts_windowed_train_gen,\
            validation_data = (lvts_windowed_val_gen, lvts_windowed_val_gen),\
            epochs = EPOCHS, batch_size = 100)

    hist_df = pd.DataFrame(history.history)

    # save
    hist_df.to_csv(("model_results\\lstm_autoencoder_results_exp14.csv"),\
                     mode="a", header=False)

    model.save("lstm_autoencoder_exp14") 