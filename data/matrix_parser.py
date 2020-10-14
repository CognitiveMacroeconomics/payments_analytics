import urllib
import time
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

class MatrixParser:

    def __init__(self, payment_mapping={"MT205":0,"MT103":1},\
                    bank_list=['ATBRCA','BLCMCA','BNDCCA']):

        self.payment_mapping = payment_mapping
        self.bank_mapping = self.__get_bank_mapping_from_list(bank_list)
        print(self.payment_mapping)
        print(self.bank_mapping)

    def __get_bank_mapping_from_list(self, bank_list):

        bank_mapping = {bank_list[i]: i for i in range(len(bank_list))}
        bank_mapping['other'] = len(bank_list)
        return bank_mapping

    def __time_conversion(self, date_time):
        
        return np.array([date_time.year, date_time.month,\
                        date_time.isocalendar()[1], date_time.day,\
                        date_time.hour, date_time.minute, date_time.second])\
                        .ravel()

    def __encode_banks(self, bank):

        if bank not in self.bank_mapping.keys():
            return self.bank_mapping['other']
        else:
            return self.bank_mapping[bank]

    def __get_matrix_index(self, sender, receiver, payment_type):

        matrix_size_1d = len(self.bank_mapping)
        matrix_size_2d = len(self.bank_mapping) ** 2
        number_of_payment_types = len(self.payment_mapping)

        sender_ix = self.__encode_banks(sender)
        receiver_ix = self.__encode_banks(receiver)
        payment_ix = self.payment_mapping[payment_type]
        
        flat_position_bank = sender_ix * matrix_size_1d + receiver_ix
        flat_position_amount = payment_ix * matrix_size_2d + flat_position_bank
        flat_position_count = number_of_payment_types * matrix_size_2d\
                                + flat_position_amount
    
        return np.array([flat_position_amount,flat_position_count])

    def __to_vector(self, record):
        
        return np.concatenate((self.__time_conversion(record['acp_time']),\
                                [record["sender_bank"],\
                                record["receiver_bank"]],\
                                [self.payment_mapping[record["payment_type"]]],\
                                self.__get_matrix_index(record["sender_bank"],\
                                record["receiver_bank"],\
                                record["payment_type"]),\
                                [float(record["payment_amt"])],[1])).flatten()


    def get_column_names(self):

        return(['YEAR', 'MONTH', 'WEEKNUMBER', 'DAY', 'HOURS', 'MINUTES',\
                'SECONDS']+['SENDER_BANK', 'RECEIVER_BANK']+['PAYMENT_TYPE']+\
                ['MATRIX_INDEX_AMOUNT', 'MATRIX_INDEX_COUNT']+\
                ['AMOUNT_OF_TRANSACTION','NUMBER_OF_TRANSACTIONS'])

    def __aggregate_time(self, data, aggregation_time=1):
        
        df = pd.DataFrame(data)
        df.columns = self.get_column_names()

        return np.array(df.assign(time_bucket = lambda x: x.apply(lambda y:\
                        time.gmtime(timedelta(hours=int(y['HOURS']),\
                        minutes=int(y['MINUTES']),seconds=int(y['SECONDS']))\
                        .seconds // aggregation_time * aggregation_time),\
                        axis=1)).assign(HOURS = lambda x:x.apply(lambda y:\
                        y['time_bucket'].tm_hour, axis=1), MINUTES=lambda x: x\
                        .apply(lambda y:y['time_bucket'].tm_min, axis=1),\
                        SECONDS=lambda x: x.apply(lambda y:y['time_bucket']\
                        .tm_sec, axis = 1)).drop(['time_bucket'], axis = 1)\
                        .groupby(['YEAR','MONTH','WEEKNUMBER','DAY','HOURS',\
                        'MINUTES','SECONDS','SENDER_BANK','RECEIVER_BANK',\
                        'PAYMENT_TYPE', 'MATRIX_INDEX_AMOUNT',\
                        'MATRIX_INDEX_COUNT']).sum().reset_index())
        

    def parse(self, records, aggregation = False, aggregation_time = 1):
        
        if not isinstance(records, list):
            raise TypeError
        
        all_records = np.array([self.__to_vector(record) for record in records])

        if aggregation:
            all_records = self.__aggregate_time(data=all_records,\
                                            aggregation_time=aggregation_time)
        return all_records
