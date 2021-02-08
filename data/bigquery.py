# from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String,\
#                         Date, DateTime, Numeric
# from sqlalchemy.orm import sessionmaker
# from sqlalchemy.ext.declarative import declarative_base
import pandas as pd
from pandas.io import gbq
import google.auth
import os
import matrix_parser_new
import split_scale_parser_new
import numpy as np


class BigQueryHandler:

    def __init__(self,query, prj_id):

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'credentials.json'
        self.data_df = gbq.read_gbq(query, project_id="acs-research-prj")

    def get_dataframe(self):
        return self.data_df

def parse_chunk(chunk, parser_agg):

    return pd.DataFrame(parser_agg.parse(chunk.to_dict("records"),\
                        aggregation=True, aggregation_time=3600),\
                            columns=parser_agg.get_column_names())
    


def parser_write_subset(df, data_name, set_name, scalar, parser_ss, parser_agg,\
                        nr_chunks=1):

    print("Starting with processing: {}".format(set_name))
    
    if data_name == "":
        table_name = "deeplearning.payment_transaction_{}".format(set_name)
    else:
        table_name = "deeplearning.payment_transaction_{}_{}".format(set_name, data_name)
    
    print(table_name)
    counter = 0
    
    #print("length of the dataframe is:{}".format(len(df)))
    #print("Number of chunks are:{}".format(nr_chunks))
    #print("Step size is:{}".format(len(df)/nr_chunks))
    
    step_size = int(np.ceil(len(df)/nr_chunks))
     
    #print("Step size is:{}".format(step_size))
    
    for step in range(0,len(df), step_size):
        
        #print("Step is:{}".format(step))
        
        if step+step_size<=len(df):
            chunk = df.iloc[list(range(step,step+step_size))]
            #print("Length of the chunk size is:{}".format(len(chunk)))
        else:
            chunk = df.iloc[step:len(df)]
            #print("Here")
            #print("Length of the chunk size is:{}".format(len(chunk)))

        print("Scaling iterations:{}\n".format(counter))
        

        if set_name != "test":
            #print("Chunk is:\n{}".format(chunk))
            chunk = parse_chunk(chunk, parser_agg)
            parser_ss.scale_all(chunk, scaler)
        
        chunk.to_gbq(destination_table=table_name,\
                project_id="acs-research-prj",\
                if_exists='append')
            
        print("/r{:0.4f}".format((counter*100/nr_chunks)), end="")
    
    print("\rFinished parsing and writing: {}".format(set_name), end="")
            


if __name__ == "__main__":

    #test = BigQueryHandler('credentials.json')

    query = "SELECT trans_time, payment_amt, part_id_from, part_id_to,\
            swift_msg_id FROM\
            acs-research-prj.deeplearning.payment_transaction order by trans_time"

    
    prj_id = "acs-research-prj"
    bqh = BigQueryHandler(query, prj_id)
    df = bqh.get_dataframe()



    df.rename(columns={'trans_time': 'time','payment_amt': 'payment_amt',\
                        'part_id_from': 'sender_bank',\
                        'part_id_to': 'receiver_bank',\
                        'swift_msg_id': 'payment_type'}, inplace=True)


    bank_list = ['ATBRCA','BLCMCA','BNDCCA','BNPACA','BOFACA',\
                 'BOFMCA','CCDQCA','CIBCCA','CUCXCA','HKBCCA','ICICCA',\
                 'MCBTCA','NOSCCA','ROYCCA','SBOSCA','TDOMCA']

    

    ############################################################################
    #parser = matrix_parser_new.MatrixParser(bank_list=bank_list)
    

    #output_array = parser.parse(df.to_dict("records"), aggregation=True,\
    #                              aggregation_time=40)

    #output_array = parser.parse(df.to_dict("records"), aggregation=False,\
    #                              aggregation_time=5)     
    

    #test_data = pd.DataFrame(output_array, columns=parser.get_column_names())

    #print(test_data)

    ############################################################################

    ############################################################################
    #parser_agg = matrix_parser_new.MatrixParser(bank_list=bank_list)
    #parser_ss = split_scale_parser_new.SplitScaleParser()

    #train_ids, val_ids, test_ids = parser_ss.split_train_val_test_index(df)
    #print(train_ids)
    #print(test_ids)
    #print(val_ids)
    
    #scaler_am = parser_ss.make_amount_scaler(df, train_ids)

    #scaler_am = parser_ss.make_count_scaler(df, train_ids)
    
    #output_array = parser_agg.parse(df.to_dict("records"), aggregation=True,\
    #                              aggregation_time=30)
                                 
    #data_df = pd.DataFrame(output_array, columns=parser_agg.get_column_names())
    #print(data_df)
    
    #scaled_data = parser_ss.scale_all(data_df, scaler_am)
    #print(scaled_data)
    ############################################################################

    ############################################################################
    
    VAL_TEST_SIZE = 0.3
    VAL_SIZE = 0.5
    NR_CHUNKS = 50
    
    parser_agg = matrix_parser_new.MatrixParser(bank_list=bank_list)
    parser_ss =split_scale_parser_new. SplitScaleParser(\
                                    val_test_size=VAL_TEST_SIZE,\
                                   val_size=VAL_SIZE)

    train_ids, val_ids, test_ids = parser_ss.split_train_val_test_index(df)
    scaler = parser_ss.make_amount_scaler(df, train_ids)

    #print(df.iloc[train_ids])

    parser_write_subset(df.iloc[train_ids],"y","train", scaler, parser_ss,\
                        parser_agg, nr_chunks = NR_CHUNKS)
    
    parser_write_subset(df.iloc[val_ids], "y", "validate", scaler,\
                           parser_ss, parser_agg, nr_chunks = NR_CHUNKS)

    parser_write_subset(df.iloc[test_ids], "y", "test", scaler,\
                           parser_ss, parser_agg, nr_chunks = NR_CHUNKS)


    ############################################################################
    

