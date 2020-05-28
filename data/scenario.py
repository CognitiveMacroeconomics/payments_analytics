class ScenarioGenerator:
    def __repr__(self):
        return "<Scenario handler>"




'''
####################################################################################
#
####################################################################################
import pandas as pd
import numpy as np

class Scenarios:
    def __init__(self, LVPS_df, bank_in_trouble, pay_type_choice):
        self.LVPS_df = LVPS_df
        self.bank_in_trouble = bank_in_trouble # problem bank
        self.pay_type_choice = pay_type_choice 

    # this method calculates per payment type (103, 202 and combined) the average daily
    # avarege outgoing turnover per problem bank in a period defined by the user.
    def daily_ave_11_12(self, date_begin, date_end):
        #####################################################################
        # string to take data from SQL
        # data needed: date, value, senders in trouble, pay_type_code.
        # use date_begin and date_end
        #####################################################################
        
        # below can also be done on the database directly.
        # per payment type
        if (self.pay_type_choice == 103):
            temp_df = self.LVPS_df[self.LVPS_df.pay_type.eq('g')]
            temp_df = temp_df[temp_df.sender.eq(self.bank_in_trouble)]
        elif (self.pay_type_choice == 202):
            temp_df = self.LVPS_df[self.LVPS_df.pay_type.eq('f')]
            temp_df = temp_df[temp_df.sender.eq(self.bank_in_trouble)]
        else:
            temp_df = self.LVPS_df
            temp_df = temp_df[temp_df.sender.eq(self.bank_in_trouble)]

        # take the dataframe with only the selected transactions (1.1, 1.2 or both)
        # calcute the average daily sum of these transactions for a given bank.
        # groupby date: we want daily sums
        # take sum of values of each day
        # then take the mean of all averages. 
        value_mean_selected_period = temp_df.groupby('date')['value'].sum().mean()
        
        return value_mean_selected_period

    # this method calculates for each time (now in seconds) the extra outflow factor
    def extra_outflow_factor(self, begin_time, end_time, days_to_extra_outflow, 
                             extra_ouflow, outflow_df):
        # an exponential outflow will be calculated such that:
        # 1) at t=0 the outflow is as it is (no extra outflow)
        # 2) as of the first second the outflow increases exponentially
        # 3) at t=1 is the time passed defined by the user: now 5 days
        # 4) all intermediate time steps have to be between 0 and 1
        # 5) only seconds passed during opening hours will be taken into account
        # which means the opening time of the first day of the scneario = 0
        # the closing time is the end of day
        # the opening time of the next day is "1 second" after the closing of the previous day
        # in other words, days are back to back without gaps (and no weekends, public holidays)
 
        # the equation is:
        # factor_increase = a*exp(alpha * beta * t)

        # ALPHA
        # 1) set alpha, which  is the factor in the exponent (e) to steer the increase speed, 
        # such that at t=1 (i.e. number of days defined by user) the extra has been realized
        # if alpha is 0.693 e^(alpha*0) = 1 and e^(alpha*1) = 2 
        alpha = 0.693 # ln(2)
        # datetime library timedeltafunction to_seconds (potential other coding option)
        # define the number of seconds during opening hours only in the predefined outflow period
        # e.g. 5 days
        duration_before_extra_outflow =  days_to_extra_outflow*(end_time - begin_time) * 3600
        # make a copy (no aliasing) of transaction data frame
        temp_mutate_df = self.LVPS_df.copy()

        #################################################################################################################
        # All payments after 1800 hours get time stamp 1800 hours (will only be a few and a few second or minutes)
        # ADD THIS
        ################################################################################################################

        # add time running nummer that has an increasing value over the different business days.
        # the opening will be set to 0 and the time passed in seconds.
        # the next business day will start number_of_business_hours*3600 seconds later than the previous business day.
        # weekends and holidays are ignored in the time passed.
        temp_mutate_df['running_time_nr'] = (temp_mutate_df.time - begin_time*3600 +
                                             temp_mutate_df.running_date_nr*((end_time -
                                                                              begin_time)*3600))

        # BETA
        # beta is number of seconds passed relative to the defined duration of extra outflows
        # e.g. 5 days. Days are continous (i.e. including time during opening hours. )
        temp_mutate_df['beta'] = (temp_mutate_df.running_time_nr/duration_before_extra_outflow)


        # take the value from the data frame that is the number for the daily average outflow of
        # the trouble bank for a payment type. 
        # Also take the outflow of 103202 as we need to know the total outflow of that bank.
        # in case both pay types are chosen: outflow_bank_pay_type_now= outflow_bank_pay_type_all
        outflow_bank_pay_type_now = outflow_df[(outflow_df['trouble_bank'] == self.bank_in_trouble)
                            & (outflow_df['payment_type'] == self.pay_type_choice)].iat[0,2]

# obsolete ???
#        outflow_bank_pay_type_all = outflow_df[(outflow_df['trouble_bank'] == self.bank_in_trouble)
#                            & (outflow_df['payment_type'] == 103202)].iat[0,2]

        # A: factor_exp 
        a = extra_ouflow/outflow_bank_pay_type_now
        print("a = ", a)
        
        p_map = {103:"f", 202:"g"}
       
        # fill in alpha, beta, a and b into the equation.
        # add the extra outflow of that bank to that(those) payment type(s).
        # do not forget the minus 1. 
        if (self.pay_type_choice not in p_map.keys()):
            temp_mutate_df['value'] = np.where((temp_mutate_df.sender == self.bank_in_trouble),
                    temp_mutate_df['value'] * (a*(np.exp(alpha*temp_mutate_df['beta']) ) ),
                    temp_mutate_df['value'])
            # temp_mutate_df.new_value.plot()            
        else:
            #mask = (temp_mutate_df['pay_type']==p_map[self.pay_type_choice]) & (temp_mutate_df['sender']==self.bank_in_trouble)
            temp_mutate_df['value'] = np.where(((temp_mutate_df.pay_type == p_map[self.pay_type_choice]) &
                                                (temp_mutate_df.sender == self.bank_in_trouble)),
                    temp_mutate_df['value'] * (a*(np.exp(alpha*temp_mutate_df['beta']) ) ),
                    temp_mutate_df['value'])
            
            
        return temp_mutate_df        


##############################################################################
# read in data as data frame
dummy_data_var = pd.read_csv("dummy_data_set_var_names.txt")

# select only relevant variable
dummy_data_var = dummy_data_var[['date',
                                   'time',
                                   'sender',
                                   'receiver',
                                   'value',
                                   'pay_type']]

# add a running number for the dates starting at 0.
# Dates have gaps for weekends and public holidays

# day integer function
uniqueDates = pd.DataFrame(dummy_data_var['date'].unique(), columns = ["date"])
uniqueDates['running_date_nr'] = np.arange(len(uniqueDates))

# combine with original data set.
dummy_data_var = pd.merge(dummy_data_var,
                 uniqueDates,
                 on='date')


# select three banks that face outflows
# in code terms the banks are the following
# A(BC) = 21
# B(AR) = 897
# C(MC) = 984
# problem_banks = {'a': 21, 'b': 897, 'c': 984}
problem_banks = {'a': (21, 15), 'b': (897, 150), 'c': (984, 1500)}
# define the extra ouflow of the banks
#outflow_amount = {'a':100, 'b':400, 'c':1000}

# type of flows that need to be adjuste
# 1 = client only (g)
# 2 = interbank only (f)
# 3 = both (g and f)
type_of_flows = [103, 202, 103202]
# list with possible outflows limited to three banks or to all banks
extra_outflow_to_whom = ["three", "all"]
# time: opening of the system
start_of_day_time = 7
# time: closing of the system
end_of_day_time = 18
# define number of days the extra outflow will be reached at daily basis
# this parameter will be used by the exponential increase function.
number_of_days_extra_outflow = 5
# beginning and end data of the data selection to calculate average daily ouflows per bank per paytype
begin_date = "2014-01-01"
end_date = "2014-03-31"
# create an empty list that will be filled with daily average outflows
# per bank per payment type
# later on this will be converted to a dataframe

list_daily_outflow_bank_pay_type = []


#for bank in problem_banks.values():
for bank in problem_banks:
    for paytype in type_of_flows:
        # create an instance
        a = Scenarios(dummy_data_var, 
                      problem_banks[bank][0], 
                      paytype)
        # calculate the daily average outflow of a certain bank, payment type
        # and between these two dates
        tmp_1 = a.daily_ave_11_12(begin_date, end_date)
        list_daily_outflow_bank_pay_type.append([problem_banks[bank][0], 
                                                 paytype, 
                                                 tmp_1])

# convert the list to a dataframe for later use
average_outflow_df = pd.DataFrame(list_daily_outflow_bank_pay_type,
                                                columns = ["trouble_bank",
                                                           "payment_type",
                                                           "mean_daily_outflow"])
list_extra_outflow_dfs = []
for bank in problem_banks:    
    for paytype in type_of_flows:
        a = Scenarios(dummy_data_var, 
                      problem_banks[bank][0], 
                      paytype)

        tmp_2 = a.extra_outflow_factor(start_of_day_time, 
                                     end_of_day_time, 
                                     number_of_days_extra_outflow, 
                                     problem_banks[bank][1], 
                                     average_outflow_df)

        list_extra_outflow_dfs.append([problem_banks[bank][0], 
                                       paytype, 
                                       tmp_2])


# convert the list to a dataframe for later use
scenario_outflow_dfs = pd.DataFrame(list_extra_outflow_dfs,
                                columns = ["trouble_bank",
                                           "payment_type",
                                           "scenario_df"])


# to do:
# 1) connect to database
# 2) calculate average daily outflow per bank using the beginning and end data on the database
# 3) Read in values to be used in functions from file 
# 4) based on period selected (e.g. 3 monts), calculate extra outflows for 20 days (4 weeks) starting 
# the extra outflows at day 1, then day 2 , then day 3 etc to have roughly 40 scenarios 
# (starting day the scenario shifts over roughly 2 months)
# 4) check and store/use output
# 5) convert time of payments after 1800 hours. Even though system closes at 1800, there can 
# be a few seconds/minutes delay in closing resulting in payments with settlement time after 1800 hours


'''
