import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pandas.io import gbq
import google.auth
import os
from data import MemoryPreparer
from models.lstm import make_lstm

from tensorflow.keras.models import load_model

# losses_data = pd.read_csv("model_results\\lstm_autoencoder_results_exp11.csv",
#                     header=None,index_col=0, names=["loss","val_loss"] )


# print(losses_data)

# sns.set()
# losses_data.plot()
# plt.show()


##############################################################################

#global params
WINDOW_SIZE = 3
HIDDEN_LAYER_SIZE = 32
EPOCHS = 500
BATCH_SIZE = 200

class BigQueryHandler:

    def __init__(self,query, prj_id):

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'data.credentials.json'
        self.data_df = gbq.read_gbq(query, project_id=prj_id)

    def get_dataframe(self):
        return self.data_df


query_1 = "SELECT * FROM\
            acs-research-prj.deeplearning.payment_transaction_test_set1\
            order by YEAR, MONTH, WEEKNUMBER, DAY, HOURS, MINUTES LIMIT 2000"

        
prj_id = "acs-research-prj"
lvts_prased_test = BigQueryHandler(query_1, prj_id)
lvts_parsed_test_df = lvts_prased_test.get_dataframe()

print(lvts_parsed_test_df.head())

mp_test = MemoryPreparer(window = True, batch_size = BATCH_SIZE,\
                            window_size = WINDOW_SIZE)
lvts_windowed_test_gen = mp_test.prepare(lvts_parsed_test_df)

#print(lvts_windowed_test_gen)

model = load_model('lstm_autoencoder_exp3')

print("Model loaded")

evaluate_model = model.evaluate(lvts_windowed_test_gen,lvts_windowed_test_gen)

test_prediction = model.predict(lvts_windowed_test_gen)

print(test_prediction)

f = open(".\prediction_analysis\prediciton_results_exp3.txt",'w')


for i in range(lvts_windowed_test_gen.shape[0]):
    f.write("Input:\n")
    for j in lvts_windowed_test_gen[i]:
        for k in j:
            f.write(str(k)+" ")
        f.write("\n")
    f.write("\n")

    f.write("Prediction:\n")
    for a in test_prediction[i]:
        for b in a:
            f.write(str(b)+" ")
        f.write("\n")
    f.write("\n")

    print(i)

f.close()