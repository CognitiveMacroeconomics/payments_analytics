import sqlalchemy as sa

import pyodbc
from urllib import parse
from sqlalchemy import Column, Integer, String, DateTime, Float, Boolean, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

import urllib

Base = declarative_base()

class  TargetPayment(Base):
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
        self.engine = sa.create_engine(db_uri)
        self.Session = sessionmaker()
        self.Session.configure(bind=self.engine)
        self.session = Session()

    def test_function(self):
        a = self.session \
            .query(TargetPayment) \
            .filter_by(id=1) \
            .first() 
        return a 

    def count(self):
        return 100000
    
    def __repr__(self):
        return "<Database handler>"



if __name__ == "__main__":
    test = TargetHandler("localhost","tempdb","sa","123456QWERD!")
    a_test = test.test_function()
    print(a_test)