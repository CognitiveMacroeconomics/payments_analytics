# from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String,\
#                         Date, DateTime, Numeric
# from sqlalchemy.orm import sessionmaker
# from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
from pandas.io import gbq
import google.auth
import os
import matrix_parser
import split_scale_parser

# Base = declarative_base()

# class PaymentTransaction(Base):
#     """
#     """
#     __tablename__ = 'sample_payments'
#     msg_ref_id = Column(String, primary_key = True)
#     cycle_date = Column(Date)
#     payment_tranche = Column(String)
#     trans_time = Column(DateTime)
#     acp_time = Column(DateTime)
#     payment_amt = Column(Numeric)
#     part_id_from = Column(String)
#     part_id_to = Column(String)
#     swift_msg_id = Column(String)

    

# class BigQueryHandler:
#     """
#     """
#     def __init__(self,cred_path):
#         bigquery_uri = f'bigquery://deeplearning-291017/deeplearnig'
#         self.engine = create_engine(bigquery_uri, credentials_path=cred_path)
#         self.Session = sessionmaker()
#         self.Session.configure(bind=self.engine)
#         self.session = self.Session()
#         self.result = self.session.query(PaymentTransaction)\
#                         .order_by("acp_time").all()

class BigQueryHandler:

    def __init__(self,query, prj_id):

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'credentials.json'
        self.data_df = gbq.read_gbq(query, project_id="deeplearning-291017")

    def get_dataframe(self):
        return self.data_df


def parser_write_subset(df, data_name, set_name, scalar, parser_ss, parser_agg,\
                        nr_chunks=1):
    return None


if __name__ == "__main__":

    #test = BigQueryHandler('credentials.json')

    query = "SELECT * FROM deeplearning-291017.deeplearnig.sample_payments"
    prj_id = "deeplearning-291017"
    bqh = BigQueryHandler(query, prj_id)
    df = bqh.get_dataframe()

    print(df.head())

    #bank_list = ['ATBRCA','BCANCA','BLCMCA','BNDCCA','BNPACA','BOFACA',\
    #             'BOFMCA','CCDQCA','CIBCCA','CUCXCA','HKBCCA','ICICCA',\
    #             'MCBTCA','NOSCCA','ROYCCA','SBOSCA','TDOMCA']
   
    bank_list = ['ATBRCA','BLCMCA','BNDCCA','BOFACA','BOFMCA','CIBCCA','CUCXCA']

    #df = pd.DataFrame([dict(acp_time=r.acp_time, payment_amt=r.payment_amt,\
    #                    sender_bank=r.part_id_from, receiver_bank=r.part_id_to,\
    #                    payment_type=r.swift_msg_id) for r in test.result])
    #print(df.head())

    ############################################################################
    # parser = matrix_parser.MatrixParser(bank_list=bank_list)
    

    # output_array = parser.parse(df.to_dict("records"), aggregation=True,\
    #                              aggregation_time=300)

    # print(len(output_array))

    # test_data = pd.DataFrame(output_array, columns=parser.get_column_names())

    ############################################################################

    ############################################################################
    # parser_agg = matrix_parser.MatrixParser(bank_list=bank_list)
    # parser_ss = split_scale_parser.SplitScaleParser()

    # train_ids, val_ids, test_ids = parser_ss.split_train_val_test_index(df)
    # scaler_am = parser_ss.make_amount_scaler(df, train_ids)
    
    # output_array = parser_agg.parse(df.to_dict("records"), aggregation=True,\
    #                              aggregation_time=300)
                                 
    # data_df = pd.DataFrame(output_array, columns=parser_agg.get_column_names())
    # print(data_df.head())
    
    # scaled_data = parser_ss.scale_all(data_df, scaler_am)
    # print(scaled_data.head())
    ############################################################################

    ############################################################################
    
    # VAL_TEST_SIZE = 0.3
    # VAL_SIZE = 0.5
    # NR_CHUNKS = 1
    
    # parser_agg = matrix_parser.MatrixParser(bank_list=bank_list)
    # parser_ss =split_scale_parser. SplitScaleParser(\
    #                                 val_test_size=VAL_TEST_SIZE,\
    #                                 val_size=VAL_SIZE)

    # train_ids, val_ids, test_ids = parser_ss.split_train_val_test_index(df)
    # scaler = parser_ss.make_amount_scaler(df, train_ids)

    # def parser_write_subset(df.iloc[train_ids], "_1y", "train", scaler,\
    #                         parser_ss, parser_agg, nr_chunks = NR_CHUNKS)
    
    # def parser_write_subset(df.iloc[train_ids], "_1y", "validate", scaler,\
    #                         parser_ss, parser_agg, nr_chunks = NR_CHUNKS)

    # def parser_write_subset(df.iloc[train_ids], "_1y", "test", scaler,\
    #                         parser_ss, parser_agg, nr_chunks = NR_CHUNKS)


    
    ############################################################################
    

