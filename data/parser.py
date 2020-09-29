import numpy as np
import pandas as pd
import datetime
import time

class IkuParser:
    
    def __init__(self, payment_mapping={"f":0,"g":1}, bank_list=[21, 906, 760]):
        self.payment_mapping = payment_mapping
        self.bank_mapping = self.__get_bank_mapping_from_list(bank_list)

    def __sanitize_records(self, records):
        return [record for record in self.records if record["OPERATION_TYPE_CODE"] in self.payment_mapping] #EW: mag dit weg als we met prepared/gefiltert werken?
         
    def __encode_payment(self, cat):
        """
        A one hot encoder that maps the cat variable to a np array of dimension (1,2).
        This this case the following mapping is used: [ 'f' , 'g' ]
        Parameters:
        cat (str): 'f' or 'g'
        """
        if (cat  in self.payment_mapping.keys()):
            a = np.zeros(len(self.payment_mapping))
            a[self.payment_mapping[cat]] = 1
            return a 
        else:
            raise TypeError
            
    def __get_bank_mapping_from_list(self, bank_list):
        bank_mapping = {bank_list[i] : i for i in range(len(bank_list))}
        bank_mapping['other'] = -1
        return bank_mapping            

    def __encode_banks(self, bank):
        """
        A one hot encoder that maps the bank variable to a np array of dimension (1, #number of banks + 1).
        The number of relevent banks can be chosen in the mapping, others are mapped to the other category,
        which is represented by the final index of the np array.
        Parameters:
        bank (int): code for the bank as recognized by bank_mapping.
        """
        # To do: mapping to seperate file?   
        bank_encoder = np.zeros(len(self.bank_mapping))
        if bank not in self.bank_mapping.keys():
            bank_encoder[self.bank_mapping['other']] = 1
        else:
            bank_encoder[self.bank_mapping[bank]] = 1
        return bank_encoder

    def __encode_sender_receiver(self, sender, receiver):
        """
        Maps the one hot encoded sender and receiver to a sending/receiving matrix, which is then flattened to a one 
        dimentional array.
        Parameters:
        sender (np.array): one dimensinonal array where all elements are zero and the sender index defined by bank_mapping is 1.
        receiver (np.array): one dimensional array where all elements are zero and the receiver index defined by bank_mapping is 1.
        """
        return (self.__encode_banks(sender).reshape(len(self.bank_mapping) ,1) * self.__encode_banks(receiver)).flatten()
    
    def __encode_banks_per_payment_type(self, sender, receiver, payment_type, amount):
        banks_array = 'bla'
        for payment_type in self.payment_mapping.keys():
            self.__encode_sender_receiver(sender, receiver)
            self.__encode_sender_receiver(sender, receiver) * self.__encode_payment(payment_type)[0] * amount
    
    def __time_conversion(self, date, time):
        """
        Takes the date and time variable and returns the year, month, weeknumber, day, hour, 
        minutes and seconds as an np.array
        Parameters:
        date (str): date formatted as %Y-%m-%d
        time (str): timestamp formatted as %H:%M:%S
        """
        dt_string = "{} {}".format(date, time)
        dt = datetime.datetime.strptime(dt_string, "%Y-%m-%d %H:%M:%S")
        return np.array([dt.year, dt.month, dt.isocalendar()[1], dt.day, dt.hour, dt.minute, dt.second]).ravel()
        
    def __to_vector(self, record):
        """
        Return a numpy array of one record, where all categorical variables are 
        one hot encoded. The sender and receiver bank are both returned as actual variables and as a 
        one hot encoded combination.
        """
        return np.concatenate((self.__time_conversion(record["SETTLEMENT_DATE"], record["TS_SETTL"]), 
                                [record["SENDER_BIC_8"], record["RECEIVER_BIC_8"]], 
                                self.__encode_payment(record["OPERATION_TYPE_CODE"]), 
                                [record["AMOUNT_OF_TRANSACTION"]],
                                self.__encode_sender_receiver(record["SENDER_BIC_8"], record["RECEIVER_BIC_8"]) * self.__encode_payment(record["OPERATION_TYPE_CODE"])[0] * record["AMOUNT_OF_TRANSACTION"], 
                                self.__encode_sender_receiver(record["SENDER_BIC_8"], record["RECEIVER_BIC_8"]) * self.__encode_payment(record["OPERATION_TYPE_CODE"])[0], #)).flatten() #,
                                self.__encode_sender_receiver(record["SENDER_BIC_8"], record["RECEIVER_BIC_8"]) * self.__encode_payment(record["OPERATION_TYPE_CODE"])[1] * record["AMOUNT_OF_TRANSACTION"], 
                                self.__encode_sender_receiver(record["SENDER_BIC_8"], record["RECEIVER_BIC_8"]) * self.__encode_payment(record["OPERATION_TYPE_CODE"])[1]
                              )).flatten()
    
    def __aggregate_time(self, data, aggregation_time=1):
        """
        Aggregate all records per aggregation_time, indicated by the first timestamp of  the period.
        Output is a np.array with all categorial variables one hot encoded.
        Tot match the output of the non-aggregated lines, the BIC columns are kept and filled with 0.
        The amount of transaction column is the total value of all transactions in that time period.
        Parameters:
        data (np.array) = np.array of records
        aggregation_time (int) = aggregation time in seconds
        """
        payment_list = [payment_type for payment_type in self.payment_mapping.keys()]
        df = pd.DataFrame(data)
        df.columns = self.get_column_names()
        
        return np.array(df
            .assign(time_bucket = lambda x: x.apply(lambda y: time.gmtime(datetime.timedelta(hours=y['HOURS'],minutes=y['MINUTES'],seconds=y['SECONDS']).seconds // 
                                                                 aggregation_time * aggregation_time), axis=1))
            .assign(HOURS = lambda x: x.apply( lambda y: y['time_bucket'].tm_hour, axis=1), 
                    MINUTES = lambda x: x.apply( lambda y: y['time_bucket'].tm_min, axis=1),
                    SECONDS = lambda x: x.apply( lambda y: y['time_bucket'].tm_sec, axis=1),
                    SENDER_BIC_8 = 0,
                    RECEIVER_BIC_8 = 0,
                    f = 0,
                    g = 0)
            .drop(['time_bucket'], axis=1)
            .groupby(['YEAR', 'MONTH', 'WEEKNUMBER', 'DAY', 'HOURS', 'MINUTES', 'SECONDS', 'SENDER_BIC_8', 'RECEIVER_BIC_8'] + payment_list)
            .sum()
            .reset_index()
        )
    
    def parse(self, records, aggregation=False, aggregation_time=1):
        """
        Parses all records as an np.array, possible to get a time aggregate
        Parameters:
        aggregation (bool) = records are aggregated over time period, default=False
        aggregation_time (int) = aggregation time in seconds
        """
        if not isinstance(records, list):
            raise TypeError
        all_records = np.array([self.__to_vector(record) for record in records])
        if aggregation:
            all_records = self.__aggregate_time(data=all_records, aggregation_time=aggregation_time)
        return all_records
    
    def get_column_names(self):
        """
        Return a list of column names of the vector returned by the to_vector function.
        """
        return (['YEAR', 'MONTH', 'WEEKNUMBER', 'DAY', 'HOURS', 'MINUTES', 'SECONDS', 'SENDER_BIC_8', 'RECEIVER_BIC_8'] + 
                [key for key in self.payment_mapping.keys()] + 
                ['AMOUNT_OF_TRANSACTION'] +
                [str(sender) + '-' + str(receiver) + ' AMOUNT_' + str(list(self.payment_mapping)[0]) for sender in self.bank_mapping.keys() for receiver in self.bank_mapping.keys()] +
                [str(sender) + '-' + str(receiver) + ' COUNT_' + str(list(self.payment_mapping)[0]) for sender in self.bank_mapping.keys() for receiver in self.bank_mapping.keys()] + #)
                [str(sender) + '-' + str(receiver) + ' AMOUNT_' + str(list(self.payment_mapping)[1]) for sender in self.bank_mapping.keys() for receiver in self.bank_mapping.keys()] +
                [str(sender) + '-' + str(receiver) + ' COUNT_' + str(list(self.payment_mapping)[1]) for sender in self.bank_mapping.keys() for receiver in self.bank_mapping.keys()])