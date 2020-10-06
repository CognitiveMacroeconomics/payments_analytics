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
    #cycle_seq_num = Column(Integer)
    payment_tranche = Column(String)
    trans_time = Column(DateTime)
    acp_time = Column(DateTime)
    payment_amt = Column(Numeric)
    part_id_from = Column(String)
    #part_brch_cd_from = Column(String)
    #part_loc_cd_from = Column(String)
    part_id_to = Column(String)
    #part_brch_cd_to = Column(String)
    #part_loc_cd_to = Column(String)
    #trans_class = Column(String)
    #rjt_rsn_cd = Column(String)
    #mir_date = Column(Date)
    #mir_sess_nb = Column(Integer)
    #mir_seq_nb = Column(Integer)
    #status = Column(String)
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
        self.result = self.session.query(PaymentTransaction).all()
        for row in self.result:
            print("{}\t{}\t{}\t{}".format(row.msg_ref_id,row.cycle_date,row.part_id_from,row.part_id_to))


if __name__ == "__main__":
    test = BigQueryHandler('credentials.json')

    # ['ATBRCA','BCANCA','BLCMCA','BNDCCA','BNPACA','BOFACA','BOFMCA','CCDQCA','CIBCCA','CUCXCA','HKBCCA','ICICCA','MCBTCA','NOSCCA','ROYCCA','SBOSCA','TDOMCA']
    #bank_list = ['ATBRCA','BLCMCA','BNDCCA','BOFACA','BOFMCA','CIBCCA','CUCXCA'] 

    bank_list = []
    with open('bank_list_test.txt','r') as file:
      lines = file.readlines()

    for line in lines:
        bank_list.append(line.strip('\n'))

    print(bank_list)

