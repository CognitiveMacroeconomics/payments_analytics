# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from dataiku import pandasutils as pdu
from target2_analytics.data.dataiku_parser import IkuParser
from tensorflow.keras.models import load_model
from tensorflow.keras import Model
from sklearn.manifold import TSNE
from sklearn.decomposition import PCA

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Read recipe inputs
autoencoder = dataiku.Folder("wdBkEnEr")
autoencoder_info = autoencoder.get_info()
model_path = autoencoder_info["path"]
t2_anon_sql_test_1y = dataiku.Dataset("t2_anon_sql_test_1y")
t2_anon_sql_test_1y_df = t2_anon_sql_test_1y.get_dataframe()
t2_anon_sql_validate_1y = dataiku.Dataset("t2_anon_sql_validatet_1y")
t2_anon_sql_validate_1y_df = t2_anon_sql_test_1y.get_dataframe()
len(t2_anon_sql_validate_1y_df)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
model = load_model("{}/{}".format(model_path,"autoencoder"))
print("="*20, "Encoder", "="*20)
print(model.summary())
encoding_model = Model(inputs=[model.get_layer("input_1").input], outputs=[model.get_layer("dense").output])
print("="*20, "Encoder", "="*20)
print(encoding_model.summary())

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
#Insert scenarios here and parse to format!
parser = IkuParser( bank_list = [760,21,729,897,652,431])

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
encoded_val_records = encoding_model.predict(t2_anon_sql_validate_1y_df)
dim_reduced_val_records = TSNE().fit_transform(encoded_val_records)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
dim_reduced_val_df = pd.DataFrame(dim_reduced_val_records)
dim_reduced_val_df["label"] = t2_anon_sql_validate_1y_df["DAY"]
dim_reduced_val_df

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
%matplotlib notebook
fig = sns.pairplot(x_vars=[0], y_vars=[1], data=dim_reduced_val_df, hue="label")
fig._legend.remove()
fig.fig.suptitle('TSNE reduced latent space')
fig.fig.legend(ncol=2, title="Hour")

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
autoencoder_results = dataiku.Folder("1y_autoencoder_results")
autoencoder_results_info = autoencoder_results.get_info()
fig.savefig("{}/{}".format(autoencoder_results_info["path"],"encoder_val_scatter_plot.png"))