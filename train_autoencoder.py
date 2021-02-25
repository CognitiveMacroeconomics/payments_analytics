import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from target2_analytics.data.windows import MemoryPreparer
from target2_analytics.models.autoencoder import make_autoencoder

#global params
WINDOW_SIZE = 10
HIDDEN_LAYER_SIZE = 16

# Read recipe inputs CANADA should adapt this to own system, just load the train and validate
# as a pandas dataframe, which you prepared before.
t2_parsed_train = dataiku.Dataset("t2_parsed_train")
t2_parsed_train_df = t2_parsed_train.get_dataframe()
t2_parsed_validate = dataiku.Dataset("t2_parsed_validate")
t2_parsed_validate_df = t2_parsed_validate.get_dataframe()

#Prepare matrix
mp = MemoryPreparer(window = False)
t2_val_array = mp.prepare(t2_parsed_validate_df)
t2_train_array = mp.prepare(t2_parsed_train_df)

#Make model 
inp_shape = (1, t2_train_array.shape[1])
model = make_autoencoder(input_shape=inp_shape, hidden_layer_size=HIDDEN_LAYER_SIZE)
model.compile(optimizer="adam", loss="mse")

#Fit model
history = model.fit(t2_train_array, t2_train_array,
             validation_data=(t2_val_array, t2_val_array),
             epochs=2,
             batch_size=100)

hist_df = pd.DataFrame(history.history)

# Write recipe outputs. Canada should adapt this 
model_dir = dataiku.Folder("model_folder").get_info()["path"]
hist_df.to_csv("{}/{}".format(model_dir, "autoencoder_results.csv"), mode="a", header=False)
model.save("{}/{}".format(model_dir, "autoencoder"))
