from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String,\
                        Date, DateTime, Numeric
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
import matrix_parser

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

    

class BigQueryHandler:
    """
    """
    def __init__(self,cred_path):
        bigquery_uri = f'bigquery://deeplearning-291017/deeplearnig'
        self.engine = create_engine(bigquery_uri, credentials_path=cred_path)
        self.Session = sessionmaker()
        self.Session.configure(bind=self.engine)
        self.session = self.Session()
        self.result = self.session.query(PaymentTransaction)\
                        .order_by("acp_time").all()
        


if __name__ == "__main__":

    test = BigQueryHandler('credentials.json') 

    #bank_list = ['ATBRCA','BCANCA','BLCMCA','BNDCCA','BNPACA','BOFACA',\
    #             'BOFMCA','CCDQCA','CIBCCA','CUCXCA','HKBCCA','ICICCA',\
    #             'MCBTCA','NOSCCA','ROYCCA','SBOSCA','TDOMCA']

    bank_list = ['ATBRCA','BLCMCA','BNDCCA','BOFACA','BOFMCA','CIBCCA','CUCXCA']

    df = pd.DataFrame([dict(acp_time=r.acp_time, payment_amt=r.payment_amt,\
                        sender_bank=r.part_id_from, receiver_bank=r.part_id_to,\
                        payment_type=r.swift_msg_id) for r in test.result])
    #print(df.head())    
    parser = MatrixParser(bank_list=bank_list)

    output_array = parser.parse(df.to_dict("records"), aggregation=True,\
                                aggregation_time=300)
