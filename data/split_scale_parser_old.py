import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

class SplitScaleParser:
    
    def __init__(self, val_test_size=0.3, val_size=0.5):
        self.val_test_size = val_test_size
        self.val_size = val_size
        
    def split_train_val_test_index(self, df):
        train_set_ids, val_test_set_ids = train_test_split(list(df.index), 
                                                           test_size=self.val_test_size, 
                                                           shuffle=False)
        test_set_ids, val_set_ids = train_test_split(val_test_set_ids, 
                                                     test_size=self.val_size, 
                                                     shuffle=False)
        return (train_set_ids, val_set_ids, test_set_ids)

    def make_amount_scaler(self, df, train_set_ids):
         return (StandardScaler(copy=False).
                 fit(df.iloc[train_set_ids][["AMOUNT_OF_TRANSACTION"]])
                )

# Maybe usable if we don't do chuncks
#    def make_count_scaler(self, df, train_set_ids):
#         return (StandardScaler(copy=False).
#                 fit(df.iloc[train_set_ids][["NUMBER_OF_TRANSACTIONS"]])
#               )

    def scale_all(self, df, amount_scaler):
        df["AMOUNT_OF_TRANSACTION"] = amount_scaler.transform(df[["AMOUNT_OF_TRANSACTION"]])
      #  df["NUMBER_OF_TRANSACTIONS"] = count_scaler.transform(df[["NUMBER_OF_TRANSACTIONS"]])

        df["HOURS"] = df["HOURS"]/24
        df["MINUTES"] = df["MINUTES"]/60
        df["SECONDS"] = df["SECONDS"]/60
        df["YEAR"] = (int(df["YEAR"]) - 2010)/20
        df["MONTH"] = df["MONTH"]/12
        df["WEEKNUMBER"] = df["WEEKNUMBER"]/53
        df["DAY"] = df["DAY"]/31
        df["NUMBER_OF_TRANSACTIONS"] = df["NUMBER_OF_TRANSACTIONS"]/50 #ugly, but hard to scale without aggregation first

        return df


### DNB EXAMPLE ###
# First split than scale and pares. We do scaling on non-aggregated data (aggregation happens in parse).
# If you can load everything in memory I would recommend reversing the order.

# Split and Scale
#parser_agg = MatrixParser(bank_list = bank_list)
#parser_ss = SplitScaleParser(val_test_size=VAL_TEST_SIZE, val_size=VAL_SIZE)

#train_ids, val_ids, test_ids = parser_ss.split_train_val_test_index(t2_anonimized_sql_df)
#scaler = parser_ss.make_amount_scaler(t2_anonimized_sql_df, train_ids)

# Parse
#parse_write_subset(t2_anonimized_sql_df.iloc[train_ids], "_1y", "train", scaler, parser_ss, parser_agg, nr_chunks=NR_CHUNKS)
#parse_write_subset(t2_anonimized_sql_df.iloc[val_ids], "_1y", "validate", scaler, parser_ss, parser_agg, nr_chunks=NR_CHUNKS)
#parse_write_subset(t2_anonimized_sql_df.iloc[test_ids], "_1y", "test", scaler, parser_ss, parser_agg, nr_chunks=NR_CHUNKS)


#using the functions:
#def parse_chunk(chunk, parser_agg):
#   return pd.DataFrame(parser_agg.parse(chunk.to_dict("records"),
#                                         aggregation=True, aggregation_time=300),
#                        columns=parser_agg.get_column_names())   

    

#def parse_write_subset(df, data_name, set_name, scaler, parser_ss, parser_agg, nr_chunks=1):
#    print("Starting with processing: {}".format(set_name))
#    if data_name == '':
#        ikuset = dataiku.Dataset("t2_parsed_{}".format(set_name, data_name))
#    else:
#        ikuset = dataiku.Dataset("t2_anon_sql_{}{}".format(set_name, data_name))
#    print(ikuset.full_name)
#    counter=0
#    step_size = int(np.ceil(len(df)/nr_chunks))
#    for step in range(0,nr_chunks, step_size):
#        if step+step_size<=len(df):
#            chunk = df.iloc[list(range(step,step+step_size))]
#       else:
#            chunk = df.iloc[step:-1]
#        print("Scaling iteration:{}\n".format(counter))
#       if set_name != "test":
#            chunk = parse_chunk(chunk, parser_agg)
#            parser_ss.scale_all(chunk, scaler)
#        if step<1:
#            ikuset.write_with_schema(chunk, dropAndCreate=True)
#        else:
#            with ikuset.get_writer() as writer:
#                writer.write_dataframe(chunk)
#        print("/r{:0.4f}".format((counter*100/nr_chunks)) , end="")
#    print("\rFinished parsing and writing: {}".format(set_name), end="")    
