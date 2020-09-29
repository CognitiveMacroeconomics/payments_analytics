# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from target2_analytics.data.dataiku_parser import IkuParser
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.compose import ColumnTransformer

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
NUMBER_OF_BANKS = 6

def get_bank_list(filename, aantal=False, bank_nr = True):
    """Get list of banks from file.
    Paramters:
    filename (string): name of the datafile with banks
    bank_nr (bool): True returns anonymized numbers, False returns BIC6 strings
    """
    banks_df = dataiku.Dataset(filename).get_dataframe()
    if aantal != False:
        banks_df = banks_df[:aantal]
    if bank_nr:
        return banks_df['participants_nr'].tolist()
    else:
        return banks_df['participants'].tolist()

bank_list = get_bank_list("selected_banks_ordered_prepared", aantal=NUMBER_OF_BANKS)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Read recipe inputs
t2_anonimized_sql = dataiku.Dataset("t2_anonimized_sql")
t2_anonimized_sql_df = t2_anonimized_sql.get_dataframe()

if len(t2_anonimized_sql_df) <1:
    raise ValueError("No data present")

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
print(t2_anonimized_sql_df.info())
print(t2_anonimized_sql_df.memory_usage())

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
def parse_write_subset(df, name, scaler, nr_chunks=100):
    print("Starting with processing: {}".format(name))

    ikuset = dataiku.Dataset("t2_parsed_{}".format(name))
    print(ikuset.full_name)
    counter=0
    step_size = int(np.ceil(len(df)/nr_chunks))
    for step in range(0,nr_chunks, step_size):
        if step+step_size<=len(df):
            chunk = df.iloc[list(range(step,step+step_size))]
        else:
            chunk = df.iloc[step:-1]
        counter +=1
        print("Scaling iteration:{}\n".format(counter))
        if name != "test":
            chunk["AMOUNT_OF_TRANSACTION"] = scaler.transform(np.array(chunk["AMOUNT_OF_TRANSACTION"]).reshape(-1,1))
            chunk = parse_chunk(chunk)
            chunk["HOURS"] = chunk["HOURS"]/24
            chunk["MINUTES"] = chunk["MINUTES"]/60
            chunk["SECONDS"] = chunk["SECONDS"]/60
            chunk["YEAR"] = (chunk["YEAR"] - 2010)/10
            chunk["MONTH"] = chunk["MONTH"]/12
            chunk["WEEKNUMBER"] = chunk["WEEKNUMBER"]/53
            chunk["DAY"] = chunk["DAY"]/31
        if counter<2:
            ikuset.write_with_schema(chunk, dropAndCreate=True)
        else:
            with ikuset.get_writer() as writer:
                writer.write_dataframe(chunk)
        print("/r{:0.4f}".format((counter*100/nr_chunks)) , end="")
    print("\rFinished parsing and writing: {}".format(name), end="")


def parse_chunk(chunk):
    return pd.DataFrame(parser.parse(chunk.to_dict("records"), aggregation=True, aggregation_time=300),
                                  columns=parser.get_column_names())

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Split and Parse
#subset = [760,21,729,897,652,431] EW: now done in get bank list
parser = IkuParser(bank_list = bank_list)

train_set_ids, val_test_set_ids = train_test_split(list(t2_anonimized_sql_df.index), test_size=0.3, shuffle=False)
scaler = StandardScaler(copy=False).fit(np.array(t2_anonimized_sql_df.iloc[train_set_ids]["AMOUNT_OF_TRANSACTION"]).reshape(-1,1))

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
parse_write_subset(t2_anonimized_sql_df.iloc[train_set_ids], "train", scaler, nr_chunks=10000000)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
test_set_ids, val_set_ids = train_test_split(val_test_set_ids, test_size=0.5, shuffle=False)

parse_write_subset(t2_anonimized_sql_df.iloc[val_set_ids], "validate", scaler)
parse_write_subset(t2_anonimized_sql_df.iloc[test_set_ids],"test", scaler)