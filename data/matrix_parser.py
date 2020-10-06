import numpy as np
import pandas as pd
import datetime
import time

class MatrixParser:
    
    def __init__(self, payment_mapping={"f":0,"g":1}, bank_list=[21, 906, 760]):
        self.payment_mapping = payment_mapping
        self.bank_mapping = self.__get_bank_mapping_from_list(bank_list)

    def __sanitize_records(self, records):
        """Filters for correct payment type"""
        #At this point we work with pre filtered records, therefore this is not used
        return [record for record in self.records if record["OPERATION_TYPE_CODE"] in self.payment_mapping] 
            
    def __get_bank_mapping_from_list(self, bank_list):
        """ Makes a dictionary from banklist, where the value is the later
        used index for this specific bank. A key 'other' is added for banks
        not in the bank list.
        
        Parameters:
        bank_list (list) = list of banks
        
        Example:
        [21, 906, 760] -> {21: 0, 906: 1, 760: 2, 'other': 3}
        """
        bank_mapping = {bank_list[i] : i for i in range(len(bank_list))}
        bank_mapping['other'] = len(bank_list)
        return bank_mapping            

    def __encode_banks(self, bank):
        """Return the correct index for a bank in bank_mapping 
        and maps all other banks to the 'other' index.  
        """
        if bank not in self.bank_mapping.keys():
            return self.bank_mapping['other']
        else:
            return self.bank_mapping[bank]

    def __get_matrix_index(self, sender, receiver, payment_type):
        """Returns the position of the sender, receiver combination
        in the flattened matrix. This matrix is then extended to include
        all payment types and also number of payments.
        
        Example:
        In the easy case with one payment type a payment from BANK 2 to 
        BANK 3 can be represented with the matrix:
        (0 0 0) 
        (0 0 1)
        (0 0 0)
        Here the rows are the sender, columns the receiver.
        The corresponding amount index is 5. Note that it is not 6, 
        since the first position is 0. The corresponding count index
        is 9 + 5 = 14.
        """
        matrix_size_1d = len(self.bank_mapping)
        matrix_size_2d = len(self.bank_mapping) ** 2
        number_of_payment_types = len(self.payment_mapping)
        
        sender_ix = self.__encode_banks(sender)
        receiver_ix = self.__encode_banks(receiver)
        payment_ix = self.payment_mapping[payment_type]
        
        flat_position_bank = sender_ix * matrix_size_1d + receiver_ix
        flat_position_amount = payment_ix * matrix_size_2d + flat_position_bank
        flat_position_count = number_of_payment_types * matrix_size_2d + flat_position_amount      
        
        return np.array([flat_position_amount, flat_position_count])
    
    def __time_conversion(self, date, time):
        """
        Takes the date and time variable and returns the year, month, weeknumber, day, hour, 
        minutes and seconds as an np.array
        Parameters:
        date (str): date formatted as %Y-%m-%d
        time (str): timestamp formatted as %H:%M:%S
        """
        dt_string = "{} {}".format(date, time)
        dt = datetime.datetime.strptime(dt_string, "%Y-%m-%d %H:%M:%S")
        return np.array([dt.year, dt.month, dt.isocalendar()[1], dt.day, dt.hour, dt.minute, dt.second]).ravel()
        
    def __to_vector(self, record):
        """
        Return a numpy array of one record, including the indexes for the one hot encoded bank to bank matrix.
        Both the index for the amount as the number of payments are included.
        """
        return np.concatenate((self.__time_conversion(record["SETTLEMENT_DATE"], record["TS_SETTL"]), 
                               [record["SENDER_BIC_8"], record["RECEIVER_BIC_8"]],
                               [self.payment_mapping[record["OPERATION_TYPE_CODE"]]],
                               self.__get_matrix_index(record["SENDER_BIC_8"], record["RECEIVER_BIC_8"], 
                                                       record["OPERATION_TYPE_CODE"]),                             
                               [record["AMOUNT_OF_TRANSACTION"]],
                               [1] #number of transactions
                              )).flatten()
    
    def __aggregate_time(self, data, aggregation_time=1):
        """
        Aggregate all records per aggregation_time, indicated by the first timestamp of  the period,
        and index/bank pair. The amount of transaction column is the total value of all transactions 
        in that time period and the number of transactions equally corresponds to to the number of 
        transaction in the same period.
        
        Parameters:
        data (np.array) = np.array of records
        aggregation_time (int) = aggregation time in seconds
        """
        df = pd.DataFrame(data)
        df.columns = self.get_column_names()
        
        return np.array(df
            .assign(time_bucket = lambda x: x.apply(lambda y: time.gmtime(datetime.timedelta(hours=y['HOURS'],
                                                                                             minutes=y['MINUTES'],
                                                                                             seconds=y['SECONDS']).seconds // 
                                                                          aggregation_time * aggregation_time), 
                                                    axis=1))
            .assign(HOURS = lambda x: x.apply( lambda y: y['time_bucket'].tm_hour, axis=1), 
                    MINUTES = lambda x: x.apply( lambda y: y['time_bucket'].tm_min, axis=1),
                    SECONDS = lambda x: x.apply( lambda y: y['time_bucket'].tm_sec, axis=1))
            .drop(['time_bucket'], axis=1)
            .groupby(['YEAR', 'MONTH', 'WEEKNUMBER', 'DAY', 'HOURS', 'MINUTES', 'SECONDS', 
                      'SENDER_BIC_8', 'RECEIVER_BIC_8', "OPERATION_TYPE_CODE_MAP",
                      'MATRIX_INDEX_AMOUNT', 'MATRIX_INDEX_COUNT'])
            .sum()
            .reset_index()
        )
    
    def parse(self, records, aggregation=False, aggregation_time=1):
        """
        Parses all records as an np.array, adding a matrix index for the later one_hot_encoding
        of the bank pairs. It is possible to get a time aggregate, with aggregation = True and
        supplying an aggregation time.
        
        Parameters:
        aggregation (bool) = records are aggregated over time period, default=False
        aggregation_time (int) = aggregation time in seconds
        """
        if not isinstance(records, list):
            raise TypeError
        all_records = np.array([self.__to_vector(record) for record in records])
        if aggregation:
            all_records = self.__aggregate_time(data=all_records, aggregation_time=aggregation_time)
        return all_records
    
    def get_column_names(self):
        """
        Return a list of column names of the vector returned by the to_vector function.
        """
        return (['YEAR', 'MONTH', 'WEEKNUMBER', 'DAY', 'HOURS', 'MINUTES', 'SECONDS'] +
                ['SENDER_BIC_8', 'RECEIVER_BIC_8'] + 
                ["OPERATION_TYPE_CODE_MAP"] + 
                ['MATRIX_INDEX_AMOUNT', 'MATRIX_INDEX_COUNT'] + 
                ['AMOUNT_OF_TRANSACTION','NUMBER_OF_TRANSACTIONS']
               )
    
