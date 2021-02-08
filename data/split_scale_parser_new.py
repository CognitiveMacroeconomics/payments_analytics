import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

class SplitScaleParser:
    
    def __init__(self, val_test_size=0.3, val_size=0.5):
        self.val_test_size = val_test_size
        self.val_size = val_size
        
    def split_train_val_test_index(self, df):
        train_set_ids, val_test_set_ids = train_test_split(list(df.index), 
                                                           test_size=self.val_test_size, 
                                                           shuffle=False)
        test_set_ids, val_set_ids = train_test_split(val_test_set_ids, 
                                                     test_size=self.val_size, 
                                                     shuffle=False)
        return (train_set_ids, val_set_ids, test_set_ids)

    def make_amount_scaler(self, df, train_set_ids):
         
          return (StandardScaler(copy=False).
                  fit(df.iloc[train_set_ids][["payment_amt"]])
                 )

# Maybe usable if we don't do chunks
#    def make_count_scaler(self, df, train_set_ids):
#         return (StandardScaler(copy=False).
#                 fit(df.iloc[train_set_ids][["NUMBER_OF_TRANSACTIONS"]])
#               )

    def scale_all(self, df, amount_scaler):
        try:
            df["AMOUNT_OF_TRANSACTION"] = amount_scaler.transform(df[["AMOUNT_OF_TRANSACTION"]])
            #  df["NUMBER_OF_TRANSACTIONS"] = count_scaler.transform(df[["NUMBER_OF_TRANSACTIONS"]])

            df["HOURS"] = df["HOURS"].astype(int)/24
            df["MINUTES"] = df["MINUTES"].astype(int)/60
            df["SECONDS"] = df["SECONDS"].astype(int)/60
            df["YEAR"] = (df["YEAR"].astype(int) - 2010)/20
            df["MONTH"] = df["MONTH"].astype(int)/12
            df["WEEKNUMBER"] = df["WEEKNUMBER"].astype(int)/53
            df["DAY"] = df["DAY"].astype(int)/31
            df["NUMBER_OF_TRANSACTIONS"] = df["NUMBER_OF_TRANSACTIONS"].astype(int)/50 #ugly, but hard to scale without aggregation first

        except Exception as e:
            print(df["AMOUNT_OF_TRANSACTION"])
            print(e)

        return df


