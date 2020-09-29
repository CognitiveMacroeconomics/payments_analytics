# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from dataiku import pandasutils as pdu
from target2_analytics.data.dataiku_parser import IkuParser
from target2_analytics.models.lstm import make_lstm
from tensorflow.keras.models import load_model
from tensorflow.keras import Model
from sklearn.manifold import TSNE
from sklearn.decomposition import PCA
from target2_analytics.data.windows import MemoryPreparer

#global params
WINDOW_SIZE = 10
HIDDEN_LAYER_SIZE = 16
EPOCHS = 100
BATCH_SIZE = 100


# Read recipe inputs
t2_parsed_train = dataiku.Dataset("t2_anon_sql_train_1y")
t2_parsed_train_df = t2_parsed_train.get_dataframe()
t2_parsed_validate = dataiku.Dataset("t2_anon_sql_validate_1y")
t2_parsed_validate_df = t2_parsed_validate.get_dataframe()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
mp = MemoryPreparer(window_size = WINDOW_SIZE)
t2_windowed_val_array = mp.prepare(t2_parsed_validate_df)
t2_windowed_train_array = mp.prepare(t2_parsed_train)
# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
#Make model
inp_shape = t2_parsed_train_df.iloc[0].shape
model = make_lstm(input_shape=inp_shape, hidden_layer_size=HIDDEN_LAYER_SIZE)
model.compile(optimizer="adam", loss="mse")
print(model.summary())

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
#Fit model
history = model.fit(t2_windowed_train_array, t2_windowed_train_array ,
             validation_data=(t2_windowed_train_array, t2_windowed_train_array),
             epochs=EPOCHS,
             batch_size=BATCH_SIZE)
# NB: DSS supports several kinds of APIs for reading and writin+g data. Please see doc.

hist_df = pd.DataFrame(history.history)

# Write recipe outputs
model_dir = dataiku.Folder("1y_lstm").get_info()["path"]
hist_df.to_csv("{}/{}".format(model_dir, "lstm_results.csv"))
model.save("{}/{}".format(model_dir, "lstm"))

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
fig = hist_df.plot().get_figure()
fig.savefig("{}/{}".format(model_dir, "lstm.jpg"))