# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu



# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Read recipe inputs
lstm = dataiku.Folder("1y_lstm")
lstm_info = lstm.get_info()
model_path = lstm_info["path"]
t2_anon_sql_test_1y = dataiku.Dataset("t2_anon_sql_test_1y")
t2_anon_sql_test_1y_df = t2_anon_sql_test_1y.get_dataframe()
t2_anon_sql_validate_1y = dataiku.Dataset("t2_anon_sql_validate_1y")
t2_anon_sql_validate_1y_df = t2_anon_sql_test_1y.get_dataframe()
len(t2_anon_sql_validate_1y_df)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
model = load_model("{}/{}".format(model_path,"lstm"))
print("="*20, "Original Model", "="*20)
print(model.summary())
encoding_model = Model(inputs=[model.get_layer("input_1").input], outputs=[model.get_layer("lstm").output])
print("="*20, "Encoder", "="*20)
print(encoding_model.summary())

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
#Insert scenarios here and parse to format!
parser = IkuParser( bank_list = [760,21,729,897,652,431])
# Prepare windowed array
mp = MemoryPreparer(window_size = WINDOW_SIZE)
t2_windowed_val_array = mp.prepare(t2_parsed_validate_df)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
first = t2_windowed_val_array[0]
days = [np.mean(x[:,2]) for x in t2_windowed_val_array[:]]
days

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
encoded_val_records = encoding_model.predict(t2_windowed_val_array)
print("val shape: {}".format(encoded_val_records.shape))
dim_reduced_val_records = TSNE().fit_transform(encoded_val_records)
print("TSNE SHAPE: {}".format(dim_reduced_val_records.shape))

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
dim_reduced_val_df = pd.DataFrame(dim_reduced_val_records)
dim_reduced_val_df["label"] = days
dim_reduced_val_df

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
%matplotlib notebook
fig = sns.pairplot(x_vars=[0], y_vars=[1], data=dim_reduced_val_df, hue="label")
fig._legend.remove()
fig.fig.suptitle('TSNE reduced latent space')
fig.fig.legend(ncol=2, title="day")

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
pca = PCA(n_components=2)
pca_reduced = pd.DataFrame(pca.fit_transform(t2_anon_sql_validate_1y_df))
pca_reduced['label'] =  t2_anon_sql_validate_1y_df["DAY"]
pca_fig = sns.pairplot(x_vars=[0], y_vars=[1], data=pca_reduced, hue='label')

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
tsne = TSNE()
tsne_reduced = pd.DataFrame(tsne.fit_transform(t2_anon_sql_validate_1y_df))
tsne_reduced['label'] =  t2_anon_sql_validate_1y_df["DAY"]
tsne_fig = sns.pairplot(x_vars=[0], y_vars=[1], data=tsne_reduced, hue='label')

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Write recipe outputs
model_dir = dataiku.Folder("60VxYO1g").get_info()["path"]
#hist_df.to_csv("{}/{}".format(model_dir, "lstm_results.csv"))
model.save("{}/{}".format(model_dir, "lstm"))