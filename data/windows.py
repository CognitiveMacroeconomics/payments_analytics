import pandas as pd
import numpy as np
import math
# Updated this import
from tensorflow.python.keras.utils import data_utils

class MemoryPreparer(data_utils.Sequence):

    def __init__(self, window=False, batch_size=128, window_size = 2,\
                n_time_col=7,\
                columns_to_drop = ['SENDER_BANK','RECEIVER_BANK'], tick=False):

        self.window = window
        self.window_size = window_size
        self.columns_to_drop = columns_to_drop
        self.n_time_col = n_time_col
        self.tick = tick
        self.batch_size = batch_size
        self.x = []
        self.feature_nr = 0 
        
    def drop_specific_columns(self, df):
        """Drops columns specified in the column_list from the dataframe df.
        This function was designed because at this point the aggregate still
        keeps the BIC and payment type columns for consistency. 
        Parameters:
        df (pd.DataFrame): dataframe
        columns_to_drop (list): list op column names which have to be dropped
        from df
        """
        for column in self.columns_to_drop:
            if column in df.columns:
                df = df.drop(columns=[column])
        return df
    
    def sort_by_time(self, df):
        return df.sort_values(by=['YEAR', 'MONTH', 'DAY', 'HOURS', 'MINUTES',\
                            'SECONDS'])
    
    #What 
    def make_matrix(self, df):
        """Expands the matrix using the matrix index columns. Rows with the same 
        timestamp are aggregated
        Parameters:
        df (pd.DataFrame) : Dataframe with the following columns in order:
                            YEAR, MONTH, WEEKNUMBER, DAY, HOURS, MINUTES,
                            SECONDS,MATRIX_INDEX_AMOUNT, MATRIX_INDEX_COUNT, 
                            AMOUNT_OF_TRANSACTION, NUMBER_OF_TRANSACTIONS.
        n_time_col (int) : Number of time stamp columns at the start of the
                            matrix.    
        """
        max_index = int(df['MATRIX_INDEX_COUNT'].max())
        empty_array = np.zeros(max_index + self.n_time_col + 1)
        # to make sure there is not accidental first match
        empty_array[0:self.n_time_col] = 100 

        big_matrix = empty_array.copy()
        matrix_row = empty_array.copy()

        for df_row in np.array(df):
            if (matrix_row[0:self.n_time_col] != df_row[0:self.n_time_col])\
                .any():

                big_matrix = np.vstack((big_matrix,matrix_row))
                matrix_row = empty_array.copy()
                matrix_row[0:self.n_time_col] = df_row[0:self.n_time_col]

            matrix_row[int(df_row[self.n_time_col + 0]) + self.n_time_col]\
                =df_row[self.n_time_col + 2]

            matrix_row[int(df_row[self.n_time_col + 1]) + self.n_time_col]\
                = df_row[self.n_time_col + 3]

        big_matrix = np.vstack((big_matrix,matrix_row))[2:,]

        return big_matrix    

    def make_windowed_array(self, matrix_2d):
        """Transforms a pandas dataframe of shape (n_samples, n_features) to an
        np.array of shape (n_samples, window_size, n_features). This is the
        output necesarry for the LSTM and GRU models.
        Parameters:
        df (pd.DataFrame): dataframe
        window_size (int): dimension of the window 
        """
        if not self.window:
            return matrix_2d
        
        if self.window_size > matrix_2d.shape[0]:
            raise ValueError("window_size cannot be larger than the number of\
                                samples")

        old_array = matrix_2d
        n_features = old_array.shape[1]
        n_samples = old_array.shape[0] - self.window_size + 1
        new_array = np.zeros([n_samples, self.window_size, n_features])

        for i in range(n_samples):
            new_array[i] = old_array[i : self.window_size + i, :]\
                                    .reshape(1, self.window_size, n_features)

        return new_array
    
    def __getitem__(self, idx):

        start = idx * self.batch_size 
        stop = (idx +1)* self.batch_size if ((idx +1)* self.batch_size)\
                <= len(self.x) else len(self.x)

        _batch = self.make_windowed_array(self.x[start:stop])

        return _batch, _batch
    
    def __len__(self):

        return math.ceil(len(self.x) / self.batch_size)

    def prepare(self,df):
        """Prepares dataset by dropping unnecessary columns and sorting by time
        """
        df_clean = self.drop_specific_columns(df)
        df_clean = self.sort_by_time(df_clean)
        #self.tick = True
        if not (self.tick):
            df_clean = self.make_matrix(df_clean)
        else: 
            df_clean = df_clean.values
        self.x = df_clean
        self.feature_nr = df_clean[0].shape[0]
        
        #Is this right??? I am return _batch
        _batch = self.make_windowed_array(self.x)
        
        print("Printing batch")
        print(_batch)
        print("shape of batch is:{}".format(_batch.shape))

        #print("feature nr is:{}".format(self.feature_nr))
        return _batch
        
        
        
        