import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from target2_analytics.models.lstm import make_lstm
from target2_analytics.data.windows import MemoryPreparer
from tensorflow.keras.losses import MSE

# Global parameters (adapt this to your needs)
WINDOW_SIZE = 50
HIDDEN_LAYER_SIZE = 16
EPOCHS = 100
BATCH_SIZE = 2128

# Read recipe inputs: adapt to own input from matrix parser, shoudl be pandas dataframe
t2_parsed_train = dataiku.Dataset("t2_parsed_train")
t2_parsed_train_df = t2_parsed_train.get_dataframe()
t2_parsed_validate = dataiku.Dataset("t2_parsed_validate")
t2_parsed_validate_df = t2_parsed_validate.get_dataframe()

# Prepare windowed array
mp_train = MemoryPreparer(window = True, window_size = WINDOW_SIZE, batch_size=BATCH_SIZE)
t2_windowed_train_gen = mp_train.prepare(t2_parsed_train_df)
mp_val = MemoryPreparer(window = True, window_size = WINDOW_SIZE, batch_size=BATCH_SIZE)
t2_windowed_validate_gen = mp_val.prepare(t2_parsed_validate_df)

#Make model 
inp_shape = (1, t2_windowed_train_array.shape[1])
model = make_lstm(input_shape=inp_shape, hidden_layer_size=HIDDEN_LAYER_SIZE)
model.compile(optimizer="adam", loss="mse")

#Fit model
history = model.fit(t2_windowed_train_gen, epochs=EPOCHS)
## To canada: somehow we lost the validate set here. I am not sure why. We should ask Timothy.

hist_df = pd.DataFrame(history.history)

# Write recipe outputs: adapt to own system
model_dir = dataiku.Folder("lstm_folder").get_info()["path"]
hist_df.to_csv("{}/{}".format(model_dir, "lstm_results.csv"), mode="a", header=False)
model.save("{}/{}".format(model_dir, "lstm"))
