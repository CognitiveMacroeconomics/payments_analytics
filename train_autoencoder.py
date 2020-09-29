# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from target2_analytics.models.autoencoder import make_autoencoder

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
#Make model
inp_shape = t2_parsed_train_df.iloc[0].shape
model = make_autoencoder(input_shape=inp_shape, hidden_layer_size=HIDDEN_LAYER_SIZE)
model.compile(optimizer="adam", loss="mse")
print(model.summary())

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
#Fit model
history = model.fit(t2_parsed_train_df, t2_parsed_train_df,
             validation_data=(t2_parsed_validate_df, t2_parsed_validate_df),
             epochs=EPOCHS,
             batch_size=BATCH_SIZE)
# NB: DSS supports several kinds of APIs for reading and writin+g data. Please see doc.

hist_df = pd.DataFrame(history.history)

# Write recipe outputs
model_dir = dataiku.Folder("1y_autoencoder").get_info()["path"]
hist_df.to_csv("{}/{}".format(model_dir, "autoencoder_results.csv"))
model.save("{}/{}".format(model_dir, "autoencoder"))

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
fig = hist_df.plot().get_figure()
fig.savefig("{}/{}".format(model_dir, "auto_encoder.jpg"))