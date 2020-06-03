from urllib import parse
from sqlalchemy import Column, Integer, String, DateTime, Float, Boolean, create_engine, between, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import numpy as np
import pandas as pd
import urllib
from datetime import datetime

Base = declarative_base()

class  TargetPayment(Base):
    """
    Class to handle dealings with the target table in the database record wise. 
    It specifies the right output type for each column and can return a numpy 
    array of a given row.
    
    Extension of the declarative_base class.
    """
    __tablename__ = 'target'
    id = Column(DateTime, primary_key = True)
    date = Column(DateTime)
    time = Column(Integer)
    sender = Column(Integer)
    receiver = Column(Integer) 
    value = Column(Float)
    payment_type = Column(String)
    country_sender = Column(Integer)
    country_receiver = Column(Integer)
    year = Column(Integer) 
    month = Column(Integer)
    cross_border =  Column(Boolean)
    
    def __repr__(self):
        repr_str = "<Payment(data={}, time={}, value={}>".format(self.date, self.time, self.value)
        return repr_str

    def encode_payment(self, cat):
        """
        A one hot encoder that maps the cat variable to a np array of dimension (1,2).
        This this case the following mapping is used: [ 'f' , 'g' ]
        """
        self.payment_mapping = {"f":0,"g":1}
        a = np.zeros(len(self.payment_mapping))
        a[self.payment_mapping[cat]] = 1
        return a 

    def encode_banks(self, bank):
        self.bank_mapping = {21: 0, 906: 1, 760: 2, 'other' : -1}       
        bank_encoder = np.zeros(len(self.bank_mapping))
        if bank not in self.bank_mapping.keys():
            bank_encoder[self.bank_mapping['other']] = 1
        else:
            bank_encoder[self.bank_mapping[bank]] = 1
        return bank_encoder

    def encode_sender_receiver(self, sender, receiver):
        return (self.encode_banks(sender).reshape(len(self.bank_mapping),1) * self.encode_banks(receiver)).flatten()

    def to_vector(self):
        """
        Return a numpy array of the query result, where all categorical variables are 
        one hot encoded.
        """
        return np.concatenate(([self.date.timestamp(), self.time, self.sender, self.receiver, self.value], 
                                self.encode_payment(self.payment_type), 
                                self.encode_sender_receiver(self.sender, self.receiver) * self.value)).flatten()

    def get_column_names(self):
        """
        Return a list of column names of vector returned for to_vector
        """
        return (['date', 'time', 'sender', 'receiver', 'value'] + 
                [key for key in self.payment_mapping.keys()] + 
                [str(sender) + '-' + str(receiver) for sender in self.bank_mapping.keys() for receiver in self.bank_mapping.keys()])

class TargetHandler:
    """
    Class to handle all connections and calls to the database.
    This might prove to be overkill, in which case it can be disregarded.
    """
    def __init__(self, server_location, database_name, user, password):
        conn_string = r';Server=tcp:{}, 1433;Database={};Uid={};Pwd={};Encrypt=no;TrustServerCertificate=no;Connection Timeout=30;'
        conn_string = conn_string.format(server_location, database_name, user, password)
        conn_string = 'Driver={ODBC Driver 17 for SQL Server}' + conn_string
        params = parse.quote_plus(conn_string) 
        db_uri = "mssql+pyodbc:///?odbc_connect=%s" % params
        self.engine = create_engine(db_uri)
        self.Session = sessionmaker()
        self.Session.configure(bind=self.engine)
        self.session = self.Session()

    def get_all(self, query_params = ('f','g'), date_range = False, duration = 1):
        """
        Return a numpy array of all target payments record where the payment type 
        matches the query_params.
        
        Parameters:
        query_params (str, str): string to indicate payments types

        By default only the following payment types are considered:
            f: interbank
            g: client
        """

        all_data = self.session \
            .query(TargetPayment) \
            .filter(TargetPayment.payment_type.in_(query_params)) \
            .all()

        all_data_df = pd.DataFrame(np.array([x.to_vector() for x in all_data]))
        all_data_df.columns = all_data[0].get_column_names()
        return all_data_df

    def get_date_range(self, query_params = ('f','g'), date_range = False):
        """
        Return a numpy array of all target payments record where the payment type 
        matches the query_params.
        
        Parameters:
        query_params (str, str): string to indicate payments types
        date_range (datetime, datetime): False or tuple with start and end date

        By default only the following payment types are considered:
            f: interbank
            g: client
        """
        if not date_range:
            return self.get_all(query_params=query_params)

        else:
            a = self.session \
                .query(TargetPayment) \
                .filter(TargetPayment.payment_type.in_(query_params)) \
                .filter(TargetPayment.date.between(date_range[0], date_range[1])) \
                .all()
            return np.array([x.to_vector() for x in a])
    
    def aggregate_time(self, df, duration=1, payment_type = ['f', 'g']):
        """
        Aggregate payments per duration

        Parameters:
        duration (int) = duration in seconds
        """

        return (df
            .assign(time_bucket=lambda x: np.floor(x['time'] / duration))
            .drop(['sender', 'receiver', 'value', 'time'], axis=1)
            .groupby(['date', 'time_bucket'] + payment_type)
            .sum()
            .reset_index()
        )

    
    def __repr__(self):
        return "<Database handler>"



if __name__ == "__main__":
    test = TargetHandler("localhost","tempdb","sa","123456QWERD!")
    a_test = test.get_all(duration = 600)
    agg_test = test.aggregate_time(a_test, duration = 600)
    #b_test = test.get_date_range(date_range=('2010-04-21', '2010-05-03'))
    print(a_test.shape)
    print(a_test.head())
    print(agg_test.head())
