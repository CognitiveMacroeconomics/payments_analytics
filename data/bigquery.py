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
    __tablename__ = 'payment_transaction_2019'
    msg_ref_id = Column(String, primary_key = True)
    cycle_date = Column(Date)
    cycle_seq_num = Column(Integer)
    payment_tranche = Column(String)
    trans_time = Column(DateTime)
    acp_time = Column(DateTime)
    payment_amt = Column(Numeric)
    part_id_from = Column(String)
    part_brch_cd_from = Column(String)
    part_loc_cd_from = Column(String)
    part_id_to = Column(String)
    part_brch_cd_to = Column(String)
    part_loc_cd_to = Column(String)
    trans_class = Column(String)
    rjt_rsn_cd = Column(String)
    mir_date = Column(Date)
    mir_sess_nb = Column(Integer)
    mir_seq_nb = Column(Integer)
    status = Column(String)
    swift_msg_id = Column(String)

class BigQueryHandler:
    """
    """
    def __init__(self,cred_path):
        bigquery_uri = f'bigquery://deeplearning-291017/deeplearnig'
        self.engine = create_engine(bigquery_uri, credentials_path=cred_path)
        self.Session = sessionmaker()
        self.Session.configure(bind=self.engine)
        self.session = self.Session()