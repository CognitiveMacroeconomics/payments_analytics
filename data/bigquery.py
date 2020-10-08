from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Date, DateTime, Numeric
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import numpy as np
import pandas as pd
import urllib
from datetime import datetime


Base = declarative_base()

class PaymentTransaction(Base):
    """
    """
    __tablename__ = 'sample_payments'
    msg_ref_id = Column(String, primary_key = True)
    cycle_date = Column(Date)
    payment_tranche = Column(String)
    trans_time = Column(DateTime)
    acp_time = Column(DateTime)
    payment_amt = Column(Numeric)
    part_id_from = Column(String)
    part_id_to = Column(String)
    swift_msg_id = Column(String)

class MatrixParser:

    def __init__(self, payment_mapping={"MT205":0,"MT103":1}, bank_list=[21, 906, 760]):
        self.payment_mapping = payment_mapping
        self.bank_mapping = self.__get_bank_mapping_from_list(bank_list)
        print(self.payment_mapping)
        print(self.bank_mapping)

    def __get_bank_mapping_from_list(self, bank_list):

        bank_mapping = {bank_list[i]: i for i in range(len(bank_list))}
        bank_mapping['other'] = len(bank_list)
        return bank_mapping

    def __time_conversion(self, date_time):
        
        return np.array([date_time.year, date_time.month, date_time.isocalendar()[1], date_time.day, date_time.hour, date_time.minute, date_time.second]).ravel()

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
        
        #print(sender_ix)
        #print(receiver_ix)
        #print(payment_ix)

        flat_position_bank = sender_ix * matrix_size_1d + receiver_ix
        flat_position_amount = payment_ix * matrix_size_2d + flat_position_bank
        flat_position_count = number_of_payment_types * matrix_size_2d + flat_position_amount

        #print(flat_position_amount)
        #print(flat_position_count)
    
        return np.array([flat_position_amount,flat_position_count])

    def __to_vector(self, record):
        #print(record['sender_bank'])
        a = np.concatenate((self.__time_conversion(record['acp_time']),[record["sender_bank"],record["receiver_bank"]],\
            [self.payment_mapping[record["payment_type"]]],\
            self.__get_matrix_index(record["sender_bank"],record["receiver_bank"],record["payment_type"]),\
                [float(record["payment_amt"])],[1])).flatten()

        print(a)
        

    def parse(self, records, aggregation = False, aggregation_time = 1):
        
        if not isinstance(records, list):
            raise TypeError
        
        all_records = np.array([self.__to_vector(record) for record in records])

        if aggregation:
            all_records = self.__aggregate_time(data=all_records, aggregation_time=aggregation_time)
        return all_records
    

class BigQueryHandler:
    """
    """
    def __init__(self,cred_path):
        bigquery_uri = f'bigquery://deeplearning-291017/deeplearnig'
        self.engine = create_engine(bigquery_uri, credentials_path=cred_path)
        self.Session = sessionmaker()
        self.Session.configure(bind=self.engine)
        self.session = self.Session()
        self.result = self.session.query(PaymentTransaction).order_by("acp_time").all()
        


if __name__ == "__main__":

    test = BigQueryHandler('credentials.json')

    #for row in test.result:
            #print("{}\t{}\t{}\t{}".format(row.msg_ref_id,row.cycle_date,row.part_id_from,row.part_id_to))

    

    #bank_list = ['ATBRCA','BCANCA','BLCMCA','BNDCCA','BNPACA','BOFACA','BOFMCA','CCDQCA','CIBCCA','CUCXCA','HKBCCA','ICICCA','MCBTCA','NOSCCA','ROYCCA','SBOSCA','TDOMCA']
    bank_list = ['ATBRCA','BLCMCA','BNDCCA','BOFACA','BOFMCA','CIBCCA','CUCXCA']

    df = pd.DataFrame([dict(acp_time = r.acp_time, payment_amt = r.payment_amt, sender_bank = r.part_id_from,\
        receiver_bank = r.part_id_to, payment_type = r.swift_msg_id)\
        for r in test.result])
    #print(df.head())    


    parser = MatrixParser(bank_list=bank_list)

    output_array = parser.parse(df.to_dict("records"), aggregation = False, aggregation_time = 300)
