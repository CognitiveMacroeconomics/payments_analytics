import sqlalchemy as sa

import pyodbc
from urllib import parse
from sqlalchemy import Column, Integer, String, DateTime, Float, Boolean, create_engine

from sqlalchemy.ext.declarative import declarative_base

import urllib

Base = declarative_base()

class  TargetPayment(Base):
    __tablename__ = 'Target'
#    id = Column(DateTime, primary_key = True)
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
        return f"Payment(data = {self.date}, time = {self.time}, value = {self.value}"


class TargetHandler:
    """
    Class to handle all connections and calls to the database.
    This might prove to be overkill, in which case it can be disregarded.
    """
    def __init__(self, db_uri, user, password, table_name, db_type="mssql"):
        params = parse.quote_plus((r'Driver={ODBC Driver 17 for SQL Server};Server=tcp:localhost, 1433;Database=tempdb;Uid=sa;Pwd=123456QWERD!;Encrypt=no;TrustServerCertificate=no;Connection Timeout=30;')) 
        db_uri = "mssql+pyodbc:///?odbc_connect=%s" % params
        self.engine = sa.create_engine(db_uri)

    def test_function(self):
        a = TargetPayment.query.filter_by(time = 53_100).first()
        return a 

    def count(self):
        return 100_000
    
    def __repr__(self):
        return "<Database handler>"



if __name__ == "__main__":
    test = TargetHandler("bs","bs","bs","bs","bs" )
    a_test = test.test_function()
    print(a_test)
    print("bs")