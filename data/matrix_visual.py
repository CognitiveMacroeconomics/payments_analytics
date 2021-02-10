import pandas as pd
import numpy as np
from pandas.io import gbq
import google.auth
import os

class BigQueryHandler:
    def __init__(self, query, prj_id):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'credentials.json'
        self.data_df = gbq.read_gbq(query, project_id = "acs-research-prj")

    def get_dataframe(self):
        return self.data_df





if __name__ == "__main__":

    query = "Select MATRIX_INDEX_AMOUNT, MATRIX_INDEX_COUNT from \
        acs-research-prj.deeplearning.payment_transaction_train_y\
        group by  MATRIX_INDEX_AMOUNT, MATRIX_INDEX_COUNT"

    prj_id = "acs-research-prj"
    bqh = BigQueryHandler(query, prj_id)
    df = bqh.get_dataframe()

    print(df)


    vector= np.zeros((1156))

    count = 0

    for index, row in df.iterrows():
        
        count += 1
        row_idx = int(row['MATRIX_INDEX_AMOUNT'])
        column_idx = int(row['MATRIX_INDEX_COUNT'])
        print("{} {} {} ".format(count, row_idx, column_idx))
        vector[row_idx] = 1
        vector[column_idx] = 1

    

    f = open("matrix.txt",'w')

    
    for i in range(1156):
        f.write("{} ".format(vector[i]))
        

    f.close()


