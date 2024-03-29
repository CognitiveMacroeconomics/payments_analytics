import pandas as pd
import numpy as np


class ScenarioGenerator:
    '''
        LVPS_df is the TARGET2 or LVTS transaction data as input.         
        bank_in_trouble is the bank (number) that faces the liquidity problem
        pay_type_choice is the payment type that will "face" the extra outflow (103, 202/205 or both)
    '''
    def __init__(self, LVPS_df, bank_in_trouble, pay_type_choice, CA_or_NL):
        self.LVPS_df = LVPS_df
        self.xc bank_in_trouble = bank_in_trouble # number problem bank (just one) plus their extra ouflow
        self.pay_type_choice = pay_type_choice # one of the three possible pay_types: either 103 OR 202/205, or 103 AND 202/205
        self.CA_or_NL = CA_or_NL # can either be "CA" or "NL":upper or lower case is irrelevant. 

#    def __repr__(self):
#        return "<Scenario handler>"

    '''        
        this method calculates per payment type (103, 202 and combined) the average daily
        avarege outgoing turnover per problem bank in a period defined by the user.
    '''
    def daily_ave_client_interbank(self, date_begin, date_end):
        # filter LVPS_df such that only  the relevant payment type choice (pay_type_choice)
        #  or combination of it is kept in the dataframe
        # for each problem bank 
        # this differs between NL and CA
        # ... NL
        if self.CA_or_NL.upper() == "NL":
            if (pay_type_choice == 103):
                temp_df = self.LVPS_df[self.LVPS_df.pay_type.eq('g')]
                temp_df = temp_df[temp_df.sender.eq(bank_in_trouble)]
            elif (pay_type_choice == 202):
                temp_df = self.LVPS_df[self.LVPS_df.pay_type.eq('f')]
                temp_df = temp_df[temp_df.sender.eq(bank_in_trouble)]
            else:
                temp_df = self.LVPS_df
                temp_df = temp_df[temp_df.sender.eq(bank_in_trouble)]

        # ... CA
        elif self.CA_or_NL.upper() == "CA":
            if (pay_type_choice == 103):
                temp_df = self.LVPS_df[self.LVPS_df.pay_type.eq('103')]
                temp_df = temp_df[temp_df.sender.eq(bank_in_trouble)]
            elif (pay_type_choice == 202):
                temp_df = self.LVPS_df[self.LVPS_df.pay_type.eq('205')]
                temp_df = temp_df[temp_df.sender.eq(bank_in_trouble)]
            else:
                temp_df = self.LVPS_df
                temp_df = temp_df[temp_df.sender.eq(bank_in_trouble)]
        else:
            print("Hey buddy, It is either Canada or Netherlands!")

        '''
            take temp_df 
            calcute the average daily sum of these transactions for a given bank.
            groupby date: we want daily sums
            take sum of values of each day
            then take the mean of all averages. 
        '''
        value_mean_selected_period = temp_df.groupby('date')['value'].sum().mean()
        # add new value to the list, which started empty                
        list_daily_outflow_bank_pay_type.append([bank_in_trouble, pay_type_choice, value_mean_selected_period])

        return average_outflow_df


    # this method calculates for each timepoint (now in seconds) the extra outflow factor
    def extra_outflow_factor(self, begin_time, end_time, duration_of_scenario_in_days, extra_ouflow_value, mean_outflow_df):
        '''
            begin_time: opening of the system
            end_time: closing of the system
            days_to_extra_outflow: number of days the extra outflow given by the user will be realized (e.g. 5 days)
            extra_outflow_value: extra outflow reached by the end of the scenario period
            mean_outflow_df: dataframe wich contains for this bank the mean daily outflow per payment type

            an exponential outflow will be calculated such that:
            1) at t=0 the outflow is as it is (no extra outflow)
            2) as of the first second the outflow increases exponentially
            3) at t=1 is the time passed defined by the user: now 5 days
            4) all intermediate time steps have to be between 0 and 1
            5) only seconds passed during opening hours will be taken into account
            which means the opening time of the first day of the scneario = 0
            the closing time is the end of day
            the opening time of the next day is "1 second" after the closing of the previous day
            in other words, days are back to back without gaps (and no weekends, public holidays)
   
            the equation is:
            factor_increase = a*exp(alpha * beta * t)
        '''
        # ALPHA
        # 1) set alpha, which  is the factor in the exponent (exp) to steer the increase speed, 
        # such that at t=1 (i.e. number of days defined by user) the extra outflow has been realized
        # if alpha is 0.693 e^(alpha*0) = 1 and e^(alpha*1) = 2 
        alpha = 0.693 # ln(2)

        # define the number of seconds during opening hours only in the predefined outflow period
        # e.g. 5 days
        duration_of_scenario_seconds =  duration_of_scenario_in_days*(end_time - begin_time) * 3600
        
        # make a copy (no aliasing) of transaction data frame
        temp_mutate_df = self.LVPS_df.copy()

        # All payments after end_time (end of day selection, in TARGET2 this is 18.00 hours) get time stamp end_time. 
        # The closing of the system in T2 is not always exactly 1800 hour, but can close seconds to 
        # minutes later. To "combine" the business days (e.g. 5 days) to one long day it is necessary to cut off
        # payments (just) after user defined closing time.  
        temp_mutate_df.time[temp_mutate_df.time > end_time*3600] = end_time*3600

        # add time running nummer that has an increasing value over the different business days.
        # the opening will be set to 0 and the time passed in seconds.
        # the next business day will start number_of_business_hours*3600 seconds later than the previous business day.
        # weekends and holidays are ignored in the time passed.
        temp_mutate_df['running_time_nr'] = (temp_mutate_df.time - begin_time*3600 +
                                             temp_mutate_df.running_date_nr*((end_time -
                                                                              begin_time)*3600))

        # BETA
        # beta is number of seconds passed relative to the defined duration of extra outflows
        # e.g. 5 days. Days are continous (i.e. only including time during opening hours. )
        temp_mutate_df['beta'] = (temp_mutate_df.running_time_nr/duration_of_scenario_seconds)

        # take the value from the data frame that is the number for the daily average outflow of
        # the trouble bank for a payment type. 
        # Also take the outflow of 103202 as we need to know the total outflow of that bank.
        # in case both pay types are chosen: outflow_bank_pay_type_now= outflow_bank_pay_type_all
        outflow_bank_pay_type_now = outflow_df[(outflow_df['trouble_bank'] == self.bank_in_trouble)
                            & (outflow_df['payment_type'] == self.pay_type_choice)].iat[0,2]
        
        # a: factor_exp 
        a = extra_ouflow_value/outflow_bank_pay_type_now
        print("a = ", a)
        
        if self.CA_or_NL.upper() == "NL":
            p_map = {103:"f", 202:"g"}
        elif self.CA_or_NL.upper() == "NL":
            p_map = {103:"103", 202:"205"}
        else:
            print{"Hey guy, please select NL or CA!"}
            
        # fill in alpha (a), beta (b) into the equation.
        # add the extra outflow of that bank to that(those) payment type(s).
        # if all payment types will be modified
        if (self.pay_type_choice not in p_map.keys()):
            temp_mutate_df['value'] = np.where((temp_mutate_df.sender == self.bank_in_trouble),
                    temp_mutate_df['value'] * (a*(np.exp(alpha*temp_mutate_df['beta']) ) ),
                    temp_mutate_df['value'])
        # else one of the payment types will be modified.            
        else:
            temp_mutate_df['value'] = np.where(((temp_mutate_df.pay_type == p_map[self.pay_type_choice]) &
                                                (temp_mutate_df.sender == self.bank_in_trouble)),
                    temp_mutate_df['value'] * (a*(np.exp(alpha*temp_mutate_df['beta']) ) ),
                    temp_mutate_df['value'])
            
            
        return temp_mutate_df        


'''
#############################################################################################
# function makes connection to the data base and makes selection of scenario data
# based on the input parameters 
# Furthermore, 
def get_scenario_data(ymd_date_begin, ymd_date_end, int_begin_time, int_end_time):
    # make connection to data base for data selection: select_all
    ELLEN???
    # for now we could test it on the dummy data set. 
    # LVPS_data_df = pd.read_csv("dummy_data_set_var_names.txt")

    # depening on country of analysis 
    if CA_or_NL.upper() == "NL":
        LVPS_data_df = XYZ
        # keep only participants that belong to the N (nog 49) selected ones 
    elif CA_or_NL.upper() == "CA":
        LVPS_data_df = "canadian selection" 
        # Is there a selection needed for Canada?
    else:
        print("Hey friend: this is not the idea! It is either CA or NL!")
       
    # Add an integer for "continuous days" to remove weekends and public holidays
    # first find unique dates in the selected data 
    uniqueDates = pd.DataFrame(LVPS_data_df['date'].unique(), columns = ["date"])
    # sort the list ascending in time and give them a running number starting at 1
    # to the length of the number of days in your data sample
    uniqueDates['running_date_nr'] = np.arange(len(uniqueDates))

    # combine with original data set.
    LVPS_data_df = pd.merge(LVPS_data_df,
                    uniqueDates,
                    on='date')

    return LVPS_data_df

#############################################################################################
'''
'''
#############################################################################################
# 
def run_all_scenarios(ymd_date_begin, ymd_date_end, int_time_begin_day, int_time_end_day, 
                        dict_of_three_trouble_banks_and_outflows, string_CA_or_NL):
    # beginning and end data of the data selection to calculate average daily ouflows per bank per paytype
    begin_date = ymd_date_begin
    end_date = ymd_date_end
    # opening and closing time of the system
    start_of_day_time = int_time_begin_day # 7
    end_of_day_time = int_time_end_day # 18
    problem_banks_outflows = dict_of_three_trouble_banks_and_outflows
    CA_or_NL = string_CA_or_NL

    # get the numpy dataframe from the function get_scenario_data
    LVPS_df = get_scenario_data(begin_date, end_date, start_of_day_time, end_of_day_time, CA_or_NL)
            
    # type of flows that need to be adjustes when data is there.
    # 1 = client only:103 (g)
    # 2 = interbank only:202/205 (f)
    # 3 = both:103 and 202/205 (g and f)
    type_of_flows = [103, 202, 103202]

    # define number of days the extra outflow will be reached at daily basis
    # this parameter will be used by the exponential increase function.
    duration_of_scenario_days = 5

    # create an instance of class Scenario
    a = Scenarios(LVPS_data_df, problem_banks_outflows, type_of_flows, CA_or_NL)

    # call method: daily_ave_client_interbank to calculate the average daily payment flows 
    # between begin_date and end_date per problem bank
    # the pandas data frame will used to calculate additional outflows to existing outflows. 
    average_outflow_df = a.daily_ave_client_interbank(begin_date, end_date)
'''


########################################################################################################################
# potential scenarios
# 103_no_gaps_all
# 202_no_gaps_all
# 103_202_no_gaps_all
# 103_gaps_all
# 202_gaps_all
# 103_202_gaps_all
# 103_no_gaps_2b
# 202_no_gaps_2b
# 103_202_no_gaps_2b
# 103_gaps_2b
# 202_gaps_2b
# 103_202_gaps_2b

# mainfile part: which has to rewritten to connect to the database!!.
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
problem_banks = {'a': (21, 15), 'b': (897, 150), 'c': (984, 1500)}
# define the extra ouflow of the banks
#outflow_amount = {'a':100, 'b':400, 'c':1000}

# type of flows that need to be adjusted to the labels in the NL and CA database
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
duration_of_scenario_days = 5

# beginning and end data of the data selection to calculate average daily ouflows per bank per paytype
begin_date = "2014-01-01"
end_date = "2014-03-31"

# country code CA or NL
CA_NL_choice = "NL"

# create an empty list that will be filled with daily average outflows
# per bank per payment type
# later on this will be converted to a dataframe


###############################################################################################
list_daily_outflow_bank_pay_type = []

for bank in problem_banks:
    for paytype in type_of_flows:
        # create an instance
        a = Scenarios(dummy_data_var, 
                      problem_banks[bank][0], 
                      paytype,
                      CA_NL_choice)
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
###############################################################################################

# set which of the three banks you want to run your scenario
bank_in_trouble_selected = problem_banks[0][0]
# dito for pay type
pay_type_selected = type_of_flows[0]

list_extra_outflow_dfs = []

for bank in problem_banks:    
    if bank = bank_in_trouble_selected:
        for paytype in type_of_flows:
            if paytype = pay_type_selected:
                a = Scenarios(dummy_data_var, 
                            problem_banks[bank][0], 
                            paytype,
                            CA_NL_choice)

                tmp_2 = a.extra_outflow_factor(start_of_day_time, 
                                            end_of_day_time, 
                                            duration_of_scenario_days, 
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
