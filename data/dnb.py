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

        Parameters:
        cat (str): 'f' or 'g'
        """
        self.payment_mapping = {"f":0,"g":1}
        a = np.zeros(len(self.payment_mapping))
        a[self.payment_mapping[cat]] = 1
        return a 

    def encode_banks(self, bank):
        """
        A one hot encoder that maps the bank variable to a np array of dimension (1, #number of banks + 1).
        The number of relevent banks can be chosen in the mapping, others are mapped to the other category,
        which is represented by the final index of the np array.

        Parameters:
        bank (int): code for the bank as recognized by bank_mapping.
        """
        self.bank_mapping = {21: 0, 906: 1, 760: 2, 'other' : -1}    
        # To do: mapping to seperate file?   
        bank_encoder = np.zeros(len(self.bank_mapping))
        if bank not in self.bank_mapping.keys():
            bank_encoder[self.bank_mapping['other']] = 1
        else:
            bank_encoder[self.bank_mapping[bank]] = 1
        return bank_encoder

    def encode_sender_receiver(self, sender, receiver):
        """
        Maps the one hot encoded sender and receiver to a sending/receiving matrix, which is then flattened to a one 
        dimentional array.

        Parameters:
        sender (np.array): one dimensinonal array where all elements are zero and the sender index defined by bank_mapping is 1.
        receiver (np.array): one dimensional array where all elements are zero and the receiver index defined by bank_mapping is 1.
        """
        return (self.encode_banks(sender).reshape(len(self.bank_mapping),1) * self.encode_banks(receiver)).flatten()

    def to_vector(self):
        """
        Return a numpy array of the query result, where all categorical variables are 
        one hot encoded. The sender and receiver bank are both returned as actual variables and as a 
        one hot encoded combination.
        """
        return np.concatenate(([self.date.timestamp(), self.time, self.sender, self.receiver, self.value], 
                                self.encode_payment(self.payment_type), 
                                self.encode_sender_receiver(self.sender, self.receiver) * self.value)).flatten()

    def get_column_names(self):
        """
        Return a list of column names of the vector returned by the to_vector function.
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

    def get_all(self, query_params = ('f','g')):
        """
        Return a pandas dataframe of all target payments records where the payment type 
        matches the query_params. The output contains both a sender and receiver column as
        one hot encoded combination of the two. 
        
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
        Return a pandas dataframe of all target payments records where the payment type 
        matches the query_params. The output contains both a sender and receiver column as
        one hot encoded combination of the two. 
        
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
            range_data = self.session \
                .query(TargetPayment) \
                .filter(TargetPayment.payment_type.in_(query_params)) \
                .filter(TargetPayment.date.between(date_range[0], date_range[1])) \
                .all()

            range_data_df = pd.DataFrame(np.array([x.to_vector() for x in range_data]))
            range_data_df.columns = range_data[0].get_column_names()
            return range_data_df
    
    def aggregate_time(self, df, duration=1, payment_type = ['f', 'g']):
        """
        Aggregate tick query output of get_all or get_data_range per duration.
        Output is a pandas dataframe with all categorial variables one hot encoded.

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
    a_test = test.get_date_range()
    b_test = test.get_date_range(date_range=('2010-04-21', '2010-05-03'))
    agg_test = test.aggregate_time(b_test, duration = 600)
    print(a_test.shape)
    print(a_test.head())
    print(agg_test.head())
