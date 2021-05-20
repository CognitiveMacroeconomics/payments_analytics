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
from keras.models import Model
from sklearn.manifold import TSNE
from sklearn.decomposition import PCA

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
            acs-research-prj.deeplearning.payment_transaction_test_set5\
            order by YEAR, MONTH, WEEKNUMBER, DAY, HOURS, MINUTES"

        
prj_id = "acs-research-prj"
lvts_prased_test = BigQueryHandler(query_1, prj_id)
lvts_parsed_test_df = lvts_prased_test.get_dataframe()

print(lvts_parsed_test_df.head())

# mp_test = MemoryPreparer(window = True, batch_size = BATCH_SIZE,\
#                             window_size = WINDOW_SIZE)
# lvts_windowed_test_gen = mp_test.prepare(lvts_parsed_test_df)

#print(lvts_windowed_test_gen)

model = load_model('autoencoder_exp15')

print("Model loaded")



#############################################################################

# evaluate_model = model.evaluate(lvts_windowed_test_gen,lvts_windowed_test_gen)

# test_prediction = model.predict(lvts_windowed_test_gen)

# print(test_prediction)

# f = open(".\prediction_analysis\prediciton_results_exp11.txt",'w')


# for i in range(lvts_windowed_test_gen.shape[0]):
#     f.write("Input:\n")
#     for j in lvts_windowed_test_gen[i]:
#         for k in j:
#             f.write(str(k)+" ")
#         f.write("\n")
#     f.write("\n")

#     f.write("Prediction:\n")
#     for a in test_prediction[i]:
#         for b in a:
#             f.write(str(b)+" ")
#         f.write("\n")
#     f.write("\n")

#     print(i)

# f.close()

###############################################################################



print("="*20, "Original Model", "="*20)
print(model.summary())

encoding_model = Model(inputs=[model.get_layer("input_1").input],\
                        outputs=[model.get_layer("dense").output])

print("="*20, "Encoder", "="*20)
print(encoding_model.summary())

mp_test = MemoryPreparer(window = False, window_size = WINDOW_SIZE)
lvts_windowed_test_gen = mp_test.prepare(lvts_parsed_test_df)

print("Shape of lvts_windowed_test_gen")
print(lvts_windowed_test_gen.shape)
first = lvts_windowed_test_gen[0]
#days = lvts_windowed_test_gen[:,3]*31
year = lvts_windowed_test_gen[:,3]*20+2010

# for x in lvts_windowed_test_gen[:]:
#     print("Here x")
#     print(x.shape)
#     print(x[:,2])


print(year)
print("Shape of year")
print(len(year))

encoded_test_records = encoding_model.predict(lvts_windowed_test_gen)
print("test shape: {}".format(encoded_test_records.shape))
dim_reduced_test_records = TSNE().fit_transform(encoded_test_records)
print("TSNE SHAPE: {}".format(dim_reduced_test_records.shape))

dim_reduced_test_df = pd.DataFrame(dim_reduced_test_records)
dim_reduced_test_df["label"] = year.round(decimals=0)

# print(dim_reduced_test_df)

##sns.set_palette(sns.color_palette("PRGn", 31))
fig = sns.pairplot(x_vars=[0], y_vars=[1], data=dim_reduced_test_df, hue="label",palette=sns.color_palette("Spectral", 5))
fig._legend.remove()
fig.fig.suptitle('TSNE reduced latent space')
fig.fig.legend(ncol=2, title="year")

# del lvts_parsed_test_df['SENDER_BANK']
# del lvts_parsed_test_df['RECEIVER_BANK']
# lvts_parsed_test_df = lvts_parsed_test_df[["YEAR", "MONTH", "WEEKNUMBER", "DAY", "HOURS", "MINUTES"]]
# tsne = TSNE()
# tsne_reduced = pd.DataFrame(tsne.fit_transform(lvts_parsed_test_df))
# tsne_reduced['label'] =  lvts_parsed_test_df["MONTH"]*12
# tsne_fig = sns.pairplot(x_vars=[0], y_vars=[1], data=tsne_reduced, hue='label')

plt.show()