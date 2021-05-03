import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

losses_data = pd.read_csv("model_results\\lstm_autoencoder_results_exp11.csv",
                    header=None,index_col=0, names=["loss","val_loss"] )


print(losses_data)

sns.set()
losses_data.plot()
plt.show()

