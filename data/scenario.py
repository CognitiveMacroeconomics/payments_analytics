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
        self.bank_in_trouble = bank_in_trouble # number problem bank (just one) plus their extra ouflow
        self.pay_type_choice = pay_type_choice # one of the three possible pay_types: either 103 OR 202/205, or 103 AND 202/205
        self.CA_or_NL = CA_or_NL # can either be "CA" or "NL":upper or lower case is irrelevant. 

    def __repr__(self):
        return "<Scenario handler>"

    '''        
        this method calculates per payment type (103, 202 and combined) the average daily
        avarege outgoing turnover per problem bank in a period defined by the user.
    '''
    # CHECK and remove: date_begin and date_end seems irrelevant at this stage... 
#    def daily_ave_client_interbank(self, date_begin, date_end):
    def daily_ave_client_interbank(self):
        # filter LVPS_df such that only  the relevant payment type choice (pay_type_choice)
        # or combination of it is kept in the dataframe
        # for each problem bank 
        # this differs between NL and CA
        # ... NL
        if self.CA_or_NL.upper() == "NL":
            if (pay_type_choice == "client"):
                temp_df = self.LVPS_df[self.LVPS_df.pay_type.eq('g')]
                temp_df = temp_df[temp_df.sender.eq(bank_in_trouble)]
            elif (pay_type_choice == "interbank"):
                temp_df = self.LVPS_df[self.LVPS_df.pay_type.eq('f')]
                temp_df = temp_df[temp_df.sender.eq(bank_in_trouble)]
            else:
                temp_df = self.LVPS_df
                temp_df = temp_df[temp_df.sender.eq(bank_in_trouble)]

        # ... CA
        elif self.CA_or_NL.upper() == "CA":
            if (pay_type_choice == "client"):
                temp_df = self.LVPS_df[self.LVPS_df.pay_type.eq('103')]
                temp_df = temp_df[temp_df.sender.eq(bank_in_trouble)]
            elif (pay_type_choice == "interbank"):
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
            groupby date (as we want daily sums)
            take sum of values of each day
            then take the mean of all averages. 
        '''
        value_mean_selected_period = temp_df.groupby('date')['value'].sum().mean()
        # add new value to the list, which started empty                
        list_daily_outflow_bank_pay_type.append([bank_in_trouble, pay_type_choice, value_mean_selected_period])

        return average_outflow_df


    # this method calculates for each timepoint (now in seconds) the extra outflow factor
    def extra_outflow_factor(self, begin_time, end_time, begin_date_scen, duration_of_scenario_in_days, extra_ouflow_value, dict_problem_banks, mean_outflow_df, outflow_to_all_or_a_few, cont_y_n):
        '''
            begin_time: opening of the system
            end_time: closing of the system
            begin_date_scen: date the n-day (5) scenario starts (date loops over all possible dates in the test range (begin_date and end_date)
            duration_of_scenario_in_days: number of days the extra outflow given by the user will be realized (e.g. 5 days)
            extra_outflow_value: extra outflow reached by the end of the scenario period defined by user
            dict_problem_banks: the dictionary of all three problem banks
            mean_outflow_df: dataframe wich contains for this bank the mean daily outflow per payment type
            outflow_to_all_or_a_few: will outflow be to all participants or just to the problem banks in the dict
            cont_y_n: continuous (no gaps) or not (gaps in outflow)

            an exponential outflow will be calculated such that:
            1) at t=0 the outflow is as it is (no extra outflow)
            2) as of the first second the outflow increases exponentially
            3) at t=1 is the end of the scenario: now 5 days
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
        # Also take the outflow of both (interbank and client) as we need to know the total outflow of that bank.
        # in case both pay types are chosen: outflow_bank_pay_type_now= outflow_bank_pay_type_all
        outflow_bank_pay_type_now = outflow_df[(outflow_df['trouble_bank'] == self.bank_in_trouble)
                            & (outflow_df['payment_type'] == self.pay_type_choice)].iat[0,2]
        
        # a: factor_exp 
        a = extra_ouflow_value/outflow_bank_pay_type_now
        print("a = ", a)
ASK TIMOTHY HERE        
        # depending on country the numbering for the payment types is different
        if self.CA_or_NL.upper() == "NL":
            # p_map = {103:"f", 202:"g"}
            p_map = {"client":"f", "interbank":"g"}
        elif self.CA_or_NL.upper() == "CA":
            # p_map = {103:"103", 202:"205"}
            p_map = {"client":"103", "interbank":"205"}
        else:
            print{"Hey guy, please select NL or CA!"}
            
        # fill in alpha (a), beta (b) into the equation.
        # add the extra outflow of that bank to that(those) payment type(s).
        # if all payment types will be modified
        if cont_y_n.upper() == "YES" or cont_y_n.upper() == "Y": 
            # depending on type of scenario increase values to all other banks ....
            if outflow_to_all_or_a_few.upper() == "ALL":
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
            # ... or just to the other two banks in the dictionary with banks in trouble.
            # We use the comparison with each of the three banks in the dict (from and to the same bank will not occur
            # after filtering the data set.) 
            else:
                if (self.pay_type_choice not in p_map.keys()):
                    # only multiply if sender is selected bank in trouble AND
                    # receiver is one of the three defined banks that can be in trouble (including selected bank). 
                    temp_mutate_df['value'] = np.where(((temp_mutate_df.sender == self.bank_in_trouble) & 
                                                    ((temp_mutate_df.receiver == dict_problem_banks[a][0]) |
                                                        temp_mutate_df.receiver == dict_problem_banks[b][0]) |
                                                        temp_mutate_df.receiver == dict_problem_banks[c][0]) 
                                                        ),
                            temp_mutate_df['value'] * (a*(np.exp(alpha*temp_mutate_df['beta']) ) ),
                            temp_mutate_df['value'])
                # else one of the payment types will be modified.            
                else:
                    # ... AND additionally
                    # payment type choice has to be the one selected by user.
                    temp_mutate_df['value'] = np.where(((temp_mutate_df.pay_type == p_map[self.pay_type_choice]) &
                                                        (temp_mutate_df.sender == self.bank_in_trouble) & 
                                                    ((temp_mutate_df.receiver == dict_problem_banks[a][0]) |
                                                        temp_mutate_df.receiver == dict_problem_banks[b][0]) |
                                                        temp_mutate_df.receiver == dict_problem_banks[c][0]) 
                                                        ),
                            temp_mutate_df['value'] * (a*(np.exp(alpha*temp_mutate_df['beta']) ) ),
                            temp_mutate_df['value'])
            # ele continuous outflow is "no" (meaning gaps in the extra outflows)
            else:
                # depending on type of scenario increase values to all other banks ....
                if outflow_to_all_or_a_few.upper() == "ALL":
                    if (self.pay_type_choice not in p_map.keys()):
                        temp_mutate_df['value'] = np.where(((temp_mutate_df.sender == self.bank_in_trouble) &
                                                             (temp_mutate_df.time_seconds < 32400 |
                                                              (temp_mutate_df.time_seconds >=36000 & temp_mutate_df.time_seconds < 43200) |
                                                              (temp_mutate_df.time_seconds >=46800 & temp_mutate_df.time_seconds < 54000) |
                                                              (temp_mutate_df.time_seconds >= 57600)
                                                             )                                                                
                                                            ),
                                temp_mutate_df['value'] * (a*(np.exp(alpha*temp_mutate_df['beta']) ) ),
                                temp_mutate_df['value'])
                    # else one of the payment types will be modified.            
                    else:
                        temp_mutate_df['value'] = np.where(((temp_mutate_df.pay_type == p_map[self.pay_type_choice]) &
                                                            (temp_mutate_df.sender == self.bank_in_trouble)&
                                                             (temp_mutate_df.time_seconds < 32400 |
                                                              (temp_mutate_df.time_seconds >=36000 & temp_mutate_df.time_seconds < 43200) |
                                                              (temp_mutate_df.time_seconds >=46800 & temp_mutate_df.time_seconds < 54000) |
                                                              (temp_mutate_df.time_seconds >= 57600)
                                                             )                                              
                                                            ),
                                temp_mutate_df['value'] * (a*(np.exp(alpha*temp_mutate_df['beta']) ) ),
                                temp_mutate_df['value'])   
                # ... or just to the other two banks in the dictionary with banks in trouble.
                # We use the comparison with each of the three banks in the dict (from and to the same bank will not occur
                # after filtering the data set.) 
                else:
                    if (self.pay_type_choice not in p_map.keys()):
                        # only multiply if sender is selected bank in trouble AND
                        # receiver is one of the three defined banks that can be in trouble (including selected bank). 
                        temp_mutate_df['value'] = np.where(((temp_mutate_df.sender == self.bank_in_trouble) & 
                                                        ((temp_mutate_df.receiver == dict_problem_banks[a][0]) |
                                                            temp_mutate_df.receiver == dict_problem_banks[b][0]) |
                                                            temp_mutate_df.receiver == dict_problem_banks[c][0]) &
                                                             (temp_mutate_df.time_seconds < 32400 |
                                                              (temp_mutate_df.time_seconds >=36000 & temp_mutate_df.time_seconds < 43200) |
                                                              (temp_mutate_df.time_seconds >=46800 & temp_mutate_df.time_seconds < 54000) |
                                                              (temp_mutate_df.time_seconds >= 57600)
                                                             )                                               
                                                            ),
                                temp_mutate_df['value'] * (a*(np.exp(alpha*temp_mutate_df['beta']) ) ),
                                temp_mutate_df['value'])
                    # else one of the payment types will be modified.            
                    else:
                        # ... AND additionally
                        # payment type choice has to be the one selected by user.
                        temp_mutate_df['value'] = np.where(((temp_mutate_df.pay_type == p_map[self.pay_type_choice]) &
                                                            (temp_mutate_df.sender == self.bank_in_trouble) & 
                                                        ((temp_mutate_df.receiver == dict_problem_banks[a][0]) |
                                                            temp_mutate_df.receiver == dict_problem_banks[b][0]) |
                                                            temp_mutate_df.receiver == dict_problem_banks[c][0])  &
                                                             (temp_mutate_df.time_seconds < 32400 |
                                                              (temp_mutate_df.time_seconds >=36000 & temp_mutate_df.time_seconds < 43200) |
                                                              (temp_mutate_df.time_seconds >=46800 & temp_mutate_df.time_seconds < 54000) |
                                                              (temp_mutate_df.time_seconds >= 57600)
                                                             )                                              
                                                            ),
                                temp_mutate_df['value'] * (a*(np.exp(alpha*temp_mutate_df['beta']) ) ),
                                temp_mutate_df['value'])

        return temp_mutate_df        


#############################################################################################
# function makes connection to the data base and makes selection of scenario data
# based on the input parameters 
# Takes all transactions for the whole scenario range (3 to 6 months) 
def get_scenario_data(ymd_date_begin, ymd_date_end, int_begin_time, int_end_time, CA_or_NL):
    # make connection to data base for data selection: select_all
    # ELLEN/TIM???
    # TAKE data only between the two dates and ...
    # ... only between begin and end time.

    # depening on country of analysis 
    if CA_or_NL.upper() == "NL":
        LVPS_data_df = XYZ
        # keep only participants that belong to the N (nog 49) selected ones 
    elif CA_or_NL.upper() == "CA":
        LVPS_data_df = "canadian selection" 
        # Is there a selection needed for Canada?
    else:
        print("Hey friend: this is not the idea! It is either CA or NL!")

    # for now we could test it on the dummy data set. 
    LVPS_data_df = pd.read_csv("D:/01.dummy_data/dummy_data_set_var_names.txt")

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
    # end of "def get_scenario_data"
############################################################################################# 

def calc_mean_flows_per_bank_per_pay_type(LVPS_input_df, dict_problem_banks, list_type_of_pay_flows, country_CA_or_NL):
    list_daily_outflow_bank_pay_type = []

    for bank in dict_problem_banks:
        for paytype in list_type_of_pay_flows:
            # create an instance
            a = Scenarios(LVPS_data_df, 
                          problem_banks[bank][0],
                          paytype,
                          country_CA_or_NL)
            # calculate the daily average outflow of a certain bank, payment type
            # and between these two dates
            tmp_1 = a.daily_ave_client_interbank()
            list_daily_outflow_bank_pay_type.append([problem_banks[bank][0], 
                                                     paytype, 
                                                     tmp_1])

    # convert the list to a dataframe for later use
    average_outflow_df = pd.DataFrame(list_daily_outflow_bank_pay_type,
                                                    columns = ["trouble_bank",
                                                               "payment_type",
                                                               "mean_daily_outflow"])

    return average_outflow_df
    # end "def calc_mean_flows_per_bank_per_pay_type"

def calc_extra_ouflow_problem_bank(LVPS_input_df, average_outflow_df, dict_problem_bank, list_type_of_pay_flows, bank_in_trouble_selected, 
        pay_type_selected, start_of_day_time, end_of_day_time, begin_date_scenario, duration_of_scenario_days, extra_outflow_to_whom, continuous_yes_no)
    # set which of the three banks you want to run your scenario
    # bank_in_trouble_selected = problem_banks[0][0]
    # dito for pay type
    # pay_type_selected = type_of_flows[0]

    list_extra_outflow_dfs = []

    for bank in dict_problem_banks:    
        if bank = bank_in_trouble_selected:
            for paytype in list_type_of_pay_flows:
                if paytype = pay_type_selected:
                    a = Scenarios(LVPS_input_df, 
                                  problem_banks[bank][0], 
                                  paytype,
                                  CA_NL_choice)

                    tmp_2 = a.extra_outflow_factor(start_of_day_time, 
                                                   end_of_day_time, 
                                                   begin_date_scenario,
                                                   duration_of_scenario_days, 
                                                   problem_banks[bank][1], 
                                                   problem_banks,
                                                   extra_outflow_to_whom,
                                                   average_outflow_df,
                                                   continuous_yes_no[0]
                                                  )

                    list_extra_outflow_dfs.append([problem_banks[bank][0], 
                                                paytype, 
                                                tmp_2])

    # convert the list to a dataframe for later use
    scenario_outflow_dfs = pd.DataFrame(list_extra_outflow_dfs,
                                    columns = ["trouble_bank",
                                            "payment_type",
                                            "scenario_df"])


def run_scenario(country_name, bank_in_trouble_selected, pay_type_selected, extra_outflow_to_whom, continuous_yes_no):
    '''
    input for this function
    - country (CA or NL)
    - give name (number) of bank ik trouble,
    - which pay type if affected 
    - to whom goes extra outflow (all banks or only list of three problem banks)
    - continuous otflow or with gaps
    '''
    # set several input values for Cananda and the Netherlands
    # 
    # independent of country: 
    # 
    # 1) set payment type(s)
    # 1 = client only (g), 2 = interbank only (f), 3 = both (g and f)
    type_of_flows = ["client", "interbank", "both"]

    # 2) define number of days (length) of scenario
    duration_of_scenario_days = 5

    # nr 3 and 4 obsolete???
    # 3) list with possible outflows limited to three banks or to all banks
    # extra_outflow_to_all_or_a_few = ["three", "all"]
    
    # 4) continous outflow or not (gaps)
    # continuous_yes_no = ["yes", "no"]

    if country.upper() == "CA":
        # define 3 problem banks (names, numbers) and their outflows
        # for NL we selected
        # A(RBC) = 21,  B(BOM) = 897, C(CIBC) = 984
        problem_banks = {'a': ("RBC", 15), 'b': ("BOM", 150), 'c': ("CIBC", 1500)}
    
        # time: opening of the system
        start_of_day_time = 8
        # time: closing of the system
        end_of_day_time = 18
    
        # beginning and end data of the data selection to calculate average daily ouflows per bank per paytype
        begin_date = "2014-01-01"
        end_date = "2014-03-31"

    elif country.upper() == "NL":
        # define 3 problem banks (names, numbers) and their outflows
        # for NL we selected
        # A(BC) = 21,  B(AR) = 897, C(MC) = 984
        problem_banks = {'a': (21, 15), 'b': (897, 150), 'c': (984, 1500)}
    
        # time: opening of the system
        start_of_day_time = 7
        # time: closing of the system
        # in T2 the end of day time is 1800 but there can be transactions up until 18. 45 in some circucumstance.
        # Payments after 1800 hours will be set to 1800 hours later on. 
        end_of_day_time = 19
    
        # beginning and end data of the data selection to calculate average daily ouflows per bank per paytype
        begin_date = "2014-01-01"
        end_date = "2014-03-31"
    
    else:
        print("Hey my friend, It is either Canada or Netherlands!")

    bank_counter_verifier = 0
    pay_type_counter_verifier = 0
    # check whether the "bank_in_trouble_selected" is one of the banks in the problem_bank dictionary
    for bank in problem_banks:
        if bank == problem_banks[bank][0]:
            bank_counter_verifier = bank_counter_verifier + 1
    
    if bank_counter_verifier == 0:
        print("This is not a correct bank name or number, please check the argument given to the function or the 'problem_bank' dictionary in the code")
    
    # the same for the pay_type_selected in the "type_of_flows" 
    for pay in pay_types:
        if pay == pay_types[pay]:
            pay_tyoe_counter_verifier = pay_type_counter_verifier + 1
            break

    if bank_counter_verifier == 0:
        print("This is not a correct payment type, please check the argument given to the function or the 'pay_type' dictionary in the code")
        break:
             
    # call function: get_scenario_data
    LVPS_data_range_df = get_scenario_data(begin_date, end_date, start_of_day_time, end_of_day_time, country_name)
    
    # call function: calc_mean_flows_per_bank_per_pay_type, use dataframe from previous function call as input for this one
    average_outflow_df = calc_mean_flows_per_bank_per_pay_type(LVPS_data_range_df, dict_problem_banks, list_type_of_flows, country_name)

!!!!!!!!!!!!!!!!!!! CEHCK for loop range !!!!!!!!!!!
discuss with Timoth and Ellen
    for begin_date_scenario in range (begin_date:end_date-duration_of_scenario_days):
    # call function: calc_extra_ouflow_problem_bank  
    scenario_data = calc_extra_ouflow_problem_bank(LVPS_data_range_df, 
                                                   average_outflow_df,
                                                   problem_banks,  
                                                   type_of_pay_flows, 
                                                   bank_in_trouble_selected, 
                                                   pay_type_selected,
                                                   start_of_day_time, 
                                                   end_of_day_time,  
                                                   begin_date_scenario, 
                                                   duration_of_scenario_days,
                                                   extra_outflow_to_whom, 
                                                   continuous_yes_no    




# call run scenario
# first argument:
# - CA or NL (case insensitive)
# - bank name or number: look at possible banks in dictionary for your country. An error message will be given if bank does not exist in the hard coded dictionary
# - payment type is client, interbank or both in function call. Programme assume both if it is not client or interbank
# - to all or a few": it is either "ALL" (case insensitive) of something else
# - continous_yes_no: it is either "YES" or "Y" (case insensitive) for yes or something else
run_scenario(country_name, bank_in_trouble_selected, pay_type_selected, extra_outflow_to_whom, continuous_yes_no)
run_scenario("CA", 15, "client", "ALL", "YES")
run_scenario("Nl", 15, "interbank", "jkdkfjkak", "ddfasf")

Questions:
line 140 and 452