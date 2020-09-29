import pandas as pd
import numpy as np
import sys 
import dataiku 



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
    def daily_ave_client_interbank(self):
        '''
            filter LVPS_df such that only the relevant payment type choice (pay_type_choice)
            or combination of it is kept in the dataframe
            for each problem bank 
            this differs between NL and CA
        '''        
        # NL
        if self.CA_or_NL.upper() == "NL":
            if (self.pay_type_choice.upper() == "CLIENT"):
                temp_df = self.LVPS_df[self.LVPS_df.OPERATION_TYPE_CODE.eq('g')]
                temp_df = temp_df[temp_df.SENDER_BIC_8.eq(self.bank_in_trouble)]
            elif (self.pay_type_choice.upper() == "INTERBANK"):
                temp_df = self.LVPS_df[self.LVPS_df.OPERATION_TYPE_CODE.eq('f')]
                temp_df = temp_df[temp_df.SENDER_BIC_8.eq(self.bank_in_trouble)]
            elif (self.pay_type_choice.upper() == "BOTH"):
                temp_df = self.LVPS_df
                temp_df = temp_df[temp_df.SENDER_BIC_8.eq(self.bank_in_trouble)]
            else:
                print("This payment type is not allowed. Please, choose client, interbank or both")
                sys.exit()             
        # CA
        elif self.CA_or_NL.upper() == "CA":
            if (self.pay_type_choice.upper() == "CLIENT"):
                temp_df = self.LVPS_df[self.LVPS_df.pay_type.eq('MT103')]
                temp_df = temp_df[temp_df.sender.eq(self.bank_in_trouble)]
            elif (self.pay_type_choice.upper() == "INTERBANK"):
                temp_df = self.LVPS_df[self.LVPS_df.pay_type.eq('MT205')]
                temp_df = temp_df[temp_df.sender.eq(self.bank_in_trouble)]
            elif (self.pay_type_choice.upper() == "BOTH"):
                temp_df = self.LVPS_df
                temp_df = temp_df[temp_df.sender.eq(self.bank_in_trouble)]
            else:
                print("This payment type is not allowed. Please, choose client, interbank or both")
                sys.exit()
        # if mistake in country name
        else:
            print("Hey buddy, It is either Canada or Netherlands!")
            sys.exit()

        
        '''
            take temp_df 
            calcute the average daily sum of these transactions for a given bank.
            groupby date (as we want daily sums)
            take sum of values of each day
            then take the mean of all averages. 
        '''
        
        average_outflow_df = temp_df.groupby('SETTLEMENT_DATE')['AMOUNT_OF_TRANSACTION'].sum().mean()
        return average_outflow_df


    # this method calculates for each timepoint (now in seconds) the extra outflow factor
    def extra_outflow_factor(self, 
                             begin_time, 
                             end_time, 
                             begin_date_scen, 
                             duration_of_scenario_in_days, 
                             extra_ouflow_value, 
                             dict_problem_banks, 
                             mean_outflow_df, 
                             outflow_to_all_or_a_few, 
                             cont_y_n,
                             increase_fact):
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
            factor_increase = a*delta*exp(alpha * t_beta)
        '''
                
        '''
            ALPHA
            1) set alpha, which  is the factor in the exponent (exp) to steer the increase speed, 
            such that at t=1 (i.e. number of days defined by user) the extra outflow has been realized
            if alpha is 0.693 e^(alpha*0) = 1 and e^(alpha*1) = 2 
        '''
        alpha = 0.693 # ln(2)
        '''
            define the number of seconds during opening hours only in the predefined outflow period
            e.g. 5 days
        '''
        duration_of_scenario_seconds =  duration_of_scenario_in_days*(end_time - begin_time) * 3600
        print("duration scenario",duration_of_scenario_seconds)
        # make a copy (no aliasing) of transaction data frame
        temp_mutate_df = self.LVPS_df.copy()

        '''
            All payments after end_time (end of day selection, in TARGET2 this is 18.00 hours) get time stamp end_time. 
            The closing of the system in T2 is not always exactly 1800 hour, but can close seconds to 
            minutes later. To "combine" the business days (e.g. 5 days) to one long day it is necessary to cut off
            payments (just) after user defined closing time.  
        '''

        temp_mutate_df.TS_SETTL_SECONDS[temp_mutate_df.TS_SETTL_SECONDS > end_time*3600] = end_time*3600

        '''
            add time running nummer that has an increasing value over the different business days.
            the opening will be set to 0 and the time passed in seconds.
            the next business day will start number_of_business_hours*3600 seconds later than the previous business day.
            weekends and holidays are ignored in the time passed.
        '''
        temp_mutate_df['running_time_nr'] = (temp_mutate_df.TS_SETTL_SECONDS - begin_time*3600 +
                                             temp_mutate_df.running_date_nr*((end_time -
                                                                              begin_time)*3600))

        '''
            BETA
            beta is number of seconds passed relative to the defined duration of extra outflows
            e.g. 5 days. Days are continous (i.e. mean_outflow_dfonly including time during opening hours. )
        '''
        temp_mutate_df['beta'] = (temp_mutate_df.running_time_nr/duration_of_scenario_seconds)

        '''
            take the value from the data frame that is the number for the daily average outflow of
            the trouble bank for a payment type. 
            Also take the outflow of both (interbank and client) as we need to know the total outflow of that bank.
            in case both pay types are chosen: outflow_bank_pay_type_now= outflow_bank_pay_type_all
        '''
        outflow_bank_pay_type_now = mean_outflow_df[(mean_outflow_df['trouble_bank'] == self.bank_in_trouble)
                            & (mean_outflow_df['payment_type'] == self.pay_type_choice)].iat[0,2]
        
 
        # a: factor_exp 
        # it is possible to scale the extra outflow of a certain payment type to the total outflow.
        #a = extra_ouflow_value[1]/outflow_bank_pay_type_now
        # if a = 1 there is no scaling factor. 
        a = 1
        
        # delta is the user defined increase factor divided by 2. 
        # This delta is necessary as the exponenent in the equation without this factor 
        # would always double the outflow by the end of the scenario (end of fifth business day.)
        # exp(alpha * 0) = 1 and exp(alpha * 1) = 2
        delta = increase_fact/2
        # depending on country the payment types are named different differently
        if self.CA_or_NL.upper() == "NL":
            # p_map = {103:"f", 202:"g"}
            p_map = {"client":"f", "interbank":"g"}
        elif self.CA_or_NL.upper() == "CA":
            # p_map = {103:"103", 202:"205"}
            p_map = {"client":"MT103", "interbank":"MT205"}
        else:
            print("Hey guy, please select NL or CA!")
            sys.exit()

        '''
            fill in alpha (a), beta (b) into the equation.
            add the extra outflow of that bank to that(those) payment type(s).
            if all payment types will be modified
        '''
        if cont_y_n.upper() == "YES" or cont_y_n.upper() == "Y": 
            # depending on type of scenario increase values to all other banks ....
            if outflow_to_all_or_a_few.upper() == "ALL":
                # if payment type is not only client or interbank but both) ...
                # ... and all values in the given dataframe will be modified by the multiplication factor
                if (self.pay_type_choice not in p_map.keys()):
                    temp_mutate_df['AMOUNT_OF_TRANSACTION'] = np.where((temp_mutate_df.SENDER_BIC_8 == self.bank_in_trouble),
                                                                        temp_mutate_df['AMOUNT_OF_TRANSACTION'] * (a*delta*(np.exp(alpha*temp_mutate_df['beta']))),
                                                                        temp_mutate_df['AMOUNT_OF_TRANSACTION'])
                # else only the values of one of the payment types need to be modified.            
                else:
                    temp_mutate_df['AMOUNT_OF_TRANSACTION'] = np.where(((temp_mutate_df.OPERATION_TYPE_CODE == p_map[self.pay_type_choice]) &
                                                                        (temp_mutate_df.SENDER_BIC_8 == self.bank_in_trouble)),
                                                                         temp_mutate_df['AMOUNT_OF_TRANSACTION'] * (a*delta*(np.exp(alpha*temp_mutate_df['beta']))),
                                                                         temp_mutate_df['AMOUNT_OF_TRANSACTION'])   
            # ... or just to the other two banks in the dictionary with banks in trouble.
            # We use the comparison with each of the three banks in the dict (from and to the same bank will not occur
            # after filtering the data set.) 
            else:
                # Again: if payment type is not only client or interbank but both) ...
                # ... and all values in the given dataframe will be modified by the multiplication factor
                if (self.pay_type_choice not in p_map.keys()):
                    # only multiply if sender is selected bank in trouble AND
                    # receiver is one of the three defined banks that can be in trouble (including selected bank). 
                    temp_mutate_df['AMOUNT_OF_TRANSACTION'] = np.where(((temp_mutate_df.SENDER_BIC_8 == self.bank_in_trouble) & 
                                                                        ((temp_mutate_df.RECEIVER_BIC_8 == dict_problem_banks['a'][0]) |
                                                                          (temp_mutate_df.RECEIVER_BIC_8 == dict_problem_banks['b'][0]) |
                                                                          (temp_mutate_df.RECEIVER_BIC_8 == dict_problem_banks['c'][0])) 
                                                                        ),
                                                                          temp_mutate_df['AMOUNT_OF_TRANSACTION'] * (a*delta*(np.exp(alpha*temp_mutate_df['beta']))), 
                                                                          temp_mutate_df['AMOUNT_OF_TRANSACTION'])
                # again: else only the values of one of the payment types need to be modified.            
                else:
                    # ... AND additionally
                    # payment type choice has to be the one selected by user.
                    temp_mutate_df['AMOUNT_OF_TRANSACTION'] = np.where(((temp_mutate_df.OPERATION_TYPE_CODE == p_map[self.pay_type_choice]) &
                                                                         (temp_mutate_df.SENDER_BIC_8 == self.bank_in_trouble) & 
                                                                         ((temp_mutate_df.RECEIVER_BIC_8 == dict_problem_banks['a'][0]) |
                                                                          (temp_mutate_df.RECEIVER_BIC_8 == dict_problem_banks['b'][0]) |
                                                                          (temp_mutate_df.RECEIVER_BIC_8 == dict_problem_banks['c'][0])) 
                                                                        ),
                                                                          temp_mutate_df['AMOUNT_OF_TRANSACTION'] * (a*delta*(np.exp(alpha*temp_mutate_df['beta']))),
                                                                          temp_mutate_df['AMOUNT_OF_TRANSACTION'])

        # else continuous outflow is "no" (meaning gaps in the extra outflows)
        elif cont_y_n.upper() == "NO" or cont_y_n.upper() == "N":
            print("continuous is no")
            # depending on type of scenario increase values to all other banks ....
            if outflow_to_all_or_a_few.upper() == "ALL":
                print("outflow to = ALL")
                # if payment type is not only client or interbank but both) ...
                # ... and all values in the given dataframe will be modified by the multiplication factor
                if (self.pay_type_choice not in p_map.keys()):
                    print("pay type choice = both")

                    temp_mutate_df['AMOUNT_OF_TRANSACTION'] = np.where(((temp_mutate_df.SENDER_BIC_8 == self.bank_in_trouble) &
                                                                       (((temp_mutate_df.TS_SETTL_SECONDS < 32400) |
                                                                        ((temp_mutate_df.TS_SETTL_SECONDS >= 36000) & (temp_mutate_df.TS_SETTL_SECONDS < 43200)) |
                                                                        ((temp_mutate_df.TS_SETTL_SECONDS >= 46800) & (temp_mutate_df.TS_SETTL_SECONDS < 54000)) |
                                                                        (temp_mutate_df.TS_SETTL_SECONDS >= 57600)))                                                                
                                                                       ),
                                                                        temp_mutate_df['AMOUNT_OF_TRANSACTION'] * (a*delta*(np.exp(alpha*temp_mutate_df['beta']) ) ),
                                                                        temp_mutate_df['AMOUNT_OF_TRANSACTION'])
                # else only the values of one of the payment types need to be modified.            
                else:
                    print("pay type choice = client OR interbank")

                    temp_mutate_df['AMOUNT_OF_TRANSACTION'] = np.where(((temp_mutate_df.OPERATION_TYPE_CODE == p_map[self.pay_type_choice]) &
                                                                        (temp_mutate_df.SENDER_BIC_8 == self.bank_in_trouble) &
                                                                        (((temp_mutate_df.TS_SETTL_SECONDS < 32400) |
                                                                         ((temp_mutate_df.TS_SETTL_SECONDS >= 36000) & (temp_mutate_df.TS_SETTL_SECONDS < 43200)) |
                                                                         ((temp_mutate_df.TS_SETTL_SECONDS >= 46800) & (temp_mutate_df.TS_SETTL_SECONDS < 54000)) |
                                                                         (temp_mutate_df.TS_SETTL_SECONDS >= 57600)))                                              
                                                                        ),
                                                                          temp_mutate_df['AMOUNT_OF_TRANSACTION'] * (a*delta*(np.exp(alpha*temp_mutate_df['beta']) ) ),
                                                                          temp_mutate_df['AMOUNT_OF_TRANSACTION'])   
            # ... or just to the other two banks in the dictionary with banks in trouble.
            # We use the comparison with each of the three banks in the dict (from and to the same bank will not occur
            # after filtering the data set.) 
            else:
                # Again: if payment type is not only client or interbank but both) ...
                # ... and all values in the given dataframe will be modified by the multiplication factor
                if (self.pay_type_choice not in p_map.keys()):
                    # only multiply if sender is selected bank in trouble AND
                    # receiver is one of the three defined banks that can be in trouble (including selected bank). 
                    temp_mutate_df['AMOUNT_OF_TRANSACTION'] = np.where(((temp_mutate_df.SENDER_BIC_8 == self.bank_in_trouble) & 
                                                        ((temp_mutate_df.RECEIVER_BIC_8 == dict_problem_banks['a'][0]) |
                                                         (temp_mutate_df.RECEIVER_BIC_8 == dict_problem_banks['b'][0]) |
                                                         (temp_mutate_df.RECEIVER_BIC_8 == dict_problem_banks['c'][0])) &
                                                        (((temp_mutate_df.TS_SETTL_SECONDS < 32400) |
                                                         ((temp_mutate_df.TS_SETTL_SECONDS >= 36000) & (temp_mutate_df.TS_SETTL_SECONDS < 43200)) |
                                                         ((temp_mutate_df.TS_SETTL_SECONDS >= 46800) & (temp_mutate_df.TS_SETTL_SECONDS < 54000)) |
                                                         (temp_mutate_df.TS_SETTL_SECONDS >= 57600)))                                               
                                                        ),
                                                         temp_mutate_df['AMOUNT_OF_TRANSACTION'] * (a*delta*(np.exp(alpha*temp_mutate_df['beta']))),
                                                         temp_mutate_df['AMOUNT_OF_TRANSACTION'])
                # again: else only the values of one of the payment types need to be modified.            
                else:
                    # ... AND additionally
                    # payment type choice has to be the one selected by user.
                    temp_mutate_df['AMOUNT_OF_TRANSACTION'] = np.where(((temp_mutate_df.OPERATION_TYPE_CODE == p_map[self.pay_type_choice]) &
                                                        (temp_mutate_df.SENDER_BIC_8 == self.bank_in_trouble) & 
                                                        ((temp_mutate_df.RECEIVER_BIC_8 == dict_problem_banks['a'][0]) |
                                                         (temp_mutate_df.RECEIVER_BIC_8 == dict_problem_banks['b'][0]) |
                                                         (temp_mutate_df.RECEIVER_BIC_8 == dict_problem_banks['c'][0]))  &
                                                         (((temp_mutate_df.TS_SETTL_SECONDS < 32400) |
                                                          ((temp_mutate_df.TS_SETTL_SECONDS >= 36000) & (temp_mutate_df.TS_SETTL_SECONDS < 43200)) |
                                                          ((temp_mutate_df.TS_SETTL_SECONDS >= 46800) & (temp_mutate_df.TS_SETTL_SECONDS < 54000)) |
                                                          (temp_mutate_df.TS_SETTL_SECONDS >= 57600)))                                              
                                                        ),
                                                         temp_mutate_df['AMOUNT_OF_TRANSACTION'] * (a*delta*(np.exp(alpha*temp_mutate_df['beta']))),
                                                         temp_mutate_df['AMOUNT_OF_TRANSACTION'])

                # else continuous outflow is "no" (meaning gaps in the extra outflows)
        else:
            print("This option for continous yes or no is not available. Choose Yes or Y for continuous or NO or N for non continuous. The options are case incensitive.")
            sys.exit()

        return temp_mutate_df        



#############################################################################################



def calc_extra_ouflow_problem_bank(LVPS_input_df, average_outflow_df, dict_problem_bank, 
                                   list_type_of_pay_flows, bank_in_trouble_selected, pay_type_selected, 
                                   start_of_day_time, end_of_day_time, begin_date_scenario, 
                                   duration_of_scenario_days, extra_outflow_to_whom, continuous_yes_no):
    # set which of the three banks you want to run your scenario
    # bank_in_trouble_selected = problem_banks[0][0]
    # dito for pay type
    # pay_type_selected = type_of_flows[0]

    list_extra_outflow_dfs = []

    for bank in dict_problem_banks:    
        if bank == bank_in_trouble_selected:
            for paytype in list_type_of_pay_flows:
                if paytype == pay_type_selected:
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
    
    return scenario_outflow_dfs



##################################################################################################3
##################################################################################################3
##################################################################################################3
# NEW CODE START (REWRITE): August 20, 2020


# set the names of the three problem banks per country    
def define_dict_problem_banks(country_name):
    if country_name.upper() == "CA":
        # define 3 problem banks (names, numbers) and their outflows
        # for NL we selected
        # A(RBC) = 21,  B(BOM) = 897, C(CIBC) = 984
        problem_banks = {'a': ("ROYCCA", 1), 'b': ("BNDCCA", 1), 'c': ("CIBCCA", 1)}
    elif country_name.upper() == "NL":
        # define 3 problem banks (names, numbers) and their outflows
        # for NL we selected
        # A(BC) = 21,  B(AR) = 897, C(MC) = 984
        problem_banks = {'a': [21, 1], 'b': [897, 1], 'c': [984, 1]}
    else:
        print("Hey my friend, It is either Canada or Netherlands! Check the country code (CA or NL) argument in the function call.")
        sys.exit()
    return problem_banks
    

# set the list of payment types
def define_list_pay_types():
        list_pay_types = ["client", "interbank", "both"]
        return list_pay_types 

                 
'''
 function makes connection to the data base and makes selection of scenario data
 based on the input parameters 
 Takes all transactions for the whole scenario range (3 to 6 months) 
'''
def get_scenario_data(ymd_date_begin, ymd_date_end, int_begin_time, int_end_time, CA_or_NL):

    # depening on country of analysis 
    # NL
    if CA_or_NL.upper() == "NL":

        # variable names in data base
        # - SETTLEMENT_DATE,
        # - TS_SETTL
        # - TS_SETTL_SECONDS
        # - SENDER_BIC8
        # - RECEIVER_BIC8
        # - AMOUNT_OF_TRANSACTION
        # - OPERATION_TYPE_CODE
        # - SENDER_CB_COUNTRY
        # - RECEIVER_CB_COUNTRY
        # - DOM_cb

        # Read recipe inputs: 
        # This has to become link to data base with selection based on begin_date and end_date
        t2_val = dataiku.Dataset("t2_anonimized_sql")
        #generator = RTGSSequence(100,1,ikudata=t2_anonymized_prepared, record_count=10000000)
        a = t2_val.iter_dataframes(chunksize=2347698)
        # test chuck
        # a = t2_val.iter_dataframes(chunksize=60000)
        for subset in a:
            break

        # dataframe: pandas
        # keep only participants that belong to the N (nog 49) selected ones 
        LVPS_data_df = subset
        
        ### THIS IS JUST TO TEST THE CODE        
        ### REMOVE AFTER TESTING
        # the number of the transaction amount is set to one to test whether the multiplication works as 
        # desired. 
        # LVPS_data_df.AMOUNT_OF_TRANSACTION = 1
    # CA
    elif CA_or_NL.upper() == "CA":
        LVPS_data_df = "canadian selection" 
        # Is there a selection needed for Canada?
    else:
        print("Hey friend: this is not the idea! It is either CA or NL!")
        sys.exit()
          
    # Add an integer for "continuous days" to remove weekends and public holidays
    # first find unique dates in the selected data 
    LVPS_data_df['SETTLEMENT_DATE'] = pd.to_datetime(LVPS_data_df.SETTLEMENT_DATE)    

    uniqueDates = pd.DataFrame(LVPS_data_df['SETTLEMENT_DATE'].unique(), columns = ["SETTLEMENT_DATE"])
    uniqueDates = uniqueDates.sort_values(by = 'SETTLEMENT_DATE')
    # sort the list ascending in time and give them a running number starting at 1
    # to the length of the number of days in your data sample
    uniqueDates['running_date_nr'] = np.arange(len(uniqueDates))

    # combine with original transaction data set.
    LVPS_data_df = pd.merge(LVPS_data_df,
                            uniqueDates,
                            on='SETTLEMENT_DATE')   

    return LVPS_data_df
    # end of "def get_scenario_data"
    
    
# calculates the average daily outflow:
# per bank 
# per payment type 
# per day over a long (predefined range)   
def calc_ave_day_outflows(country_name):       
    # define dicionary with the three problem banks including the outflow factor. 
    dict_problem_banks = define_dict_problem_banks(country_name)
    
    # define the list of payment types 
    list_type_of_pay_flows = define_list_pay_types()
    
    # set the begin/end of day time and begin and end date of the period to
    # be used for calculating the average daily flows per bank per payment type
    if country_name.upper() == "CA":    
        # time: opening of the system
        start_of_day_time = 8
        # time: closing of the system
        end_of_day_time = 18
    
        # beginning and end data of the data selection to calculate average daily ouflows per bank per paytype
        begin_date = "2014-01-01"
        end_date = "2014-03-31"

    elif country_name.upper() == "NL":
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
        sys.exit()
    
    # call the data base for reading the data to be used to calculate mean outlfows
    # per bank per payment type per day. 
    LVPS_data_range_df = get_scenario_data(begin_date, 
                                           end_date, 
                                           start_of_day_time, 
                                           end_of_day_time, 
                                           country_name)    

    a5 = len(LVPS_data_range_df.SETTLEMENT_DATE)
    print("length dataframe for testing purpose = ",a5)
    
    list_daily_outflow_bank_pay_type = []
    
    for bank in dict_problem_banks:   
        for paytype in range(len(list_type_of_pay_flows)):
            # create an instance
            a = ScenarioGenerator(LVPS_data_range_df, 
                                  dict_problem_banks[bank][0],
                                  list_type_of_pay_flows[paytype],
                                  country_name)
            # calculate the daily average outflow of a certain bank, payment type
            # and between these two dates
            tmp_1 = a.daily_ave_client_interbank()
            # print("print tmp_1 value for testing purpose = ",tmp_1)
            
            list_daily_outflow_bank_pay_type.append([dict_problem_banks[bank][0], 
                                                     list_type_of_pay_flows[paytype], 
                                                     tmp_1])

    # convert the list to a dataframe for later use
    average_outflow_df = pd.DataFrame(list_daily_outflow_bank_pay_type,
                                                    columns = ["trouble_bank",
                                                               "payment_type",
                                                               "mean_daily_outflow"])
    
    return average_outflow_df
    # end function calc_ave_day_outflows


    
def calc_extra_outflows(average_outflow_df, 
                       bank_in_trouble_selected, 
                       pay_type_selected, 
                       begin_date_scenario, 
                       extra_outflow_to_whom, 
                       continuous_yes_no,
                       country_name,
                       increase_factor):
    # set which of the three banks you want to run your scenario
    # bank_in_trouble_selected = problem_banks[0][0]
    # dito for pay type
    # pay_type_selected = type_of_flows[0]
    
    # define dicionary with the three problem banks including the outflow factor. 
    dict_problem_banks = define_dict_problem_banks(country_name)
    
    # define the list of payment types 
    list_type_of_pay_flows = define_list_pay_types()
    
    duration_of_scenario_days = 5
    
    # set the begin/end of day time and begin and end date of the period to
    # be used for calculating the average daily flows per bank per payment type
    if country_name.upper() == "CA":    
        # time: opening of the system
        start_of_day_time = 8
        # time: closing of the system
        end_of_day_time_data = 19
        end_of_day_time_scenario = 18
    
        # beginning and end data of the data selection to calculate average daily ouflows per bank per paytype
        begin_date = "2014-01-01"
        end_date = "2014-03-31"

    elif country_name.upper() == "NL":
        # time: opening of the system
        start_of_day_time = 7
        # time: closing of the system
        # in T2 the end of day time is 1800 but there can be transactions up until 18. 45 in some circucumstance.
        # Payments after 1800 hours will be set to 1800 hours later on. 
        end_of_day_time_data = 19
        end_of_day_time_scenario = 18
        # beginning and end data of the data selection to calculate average daily ouflows per bank per paytype
        begin_date = "2014-01-01"
        end_date = "2014-03-31"
    else:
        print("Hey my friend, It is either Canada or Netherlands!")
        sys.exit()
    
    bank_counter_verifier = 0
    pay_type_counter_verifier = 0

    # check whether the "bank_in_trouble_selected" is one of the banks in the problem_bank dictionary
    for bank in dict_problem_banks:
        if bank_in_trouble_selected == dict_problem_banks[bank][0]:
            bank_counter_verifier = bank_counter_verifier + 1
    
    # if bank_counter_verifier is 0 the bank in trouble does not exist in dictionary
    # stop the code. 
    if bank_counter_verifier == 0:
        print("This is not a correct bank name or number, please check the argument given to the function or the 'problem_bank' dictionary in the code")
        sys.exit()
    
    # the same for the pay_type_selected in the "type_of_flows" 
    for pay in range(len(list_type_of_pay_flows)):
        if pay_type_selected == list_type_of_pay_flows[pay]:
            pay_type_counter_verifier = pay_type_counter_verifier + 1

    # if bank_counter_verifier is 0 the bank in trouble does not exist in dictionary
    # stop the code. 
    if pay_type_counter_verifier == 0:
        print("This is not a correct payment type, please check the argument given to the function or the 'pay_type' dictionary in the code")
        sys.exit()        
        
    # modfify the problem bank dict values of the outflows with the increase_factor and the mean values above.
    for bank in dict_problem_banks:
        if country_name.upper() == "CA":
            tmp1 = average_outflow_df.loc[(average_outflow_df['trouble_bank'] == dict_problem_banks[bank][0]) & (average_outflow_df['payment_type'] == pay_type_selected)]
            mean_value_problem_bank = tmp1.iloc[:,2].tolist() # tolist to get rid off index
            dict_problem_banks[bank][1] = mean_value_problem_bank*increase_factor
        elif country_name.upper() == "NL":
            tmp1 = average_outflow_df.loc[(average_outflow_df['trouble_bank'] == dict_problem_banks[bank][0]) & (average_outflow_df['payment_type'] == pay_type_selected)]
            mean_value_problem_bank = tmp1.iloc[:,2].tolist()
            dict_problem_banks[bank][1] = mean_value_problem_bank*increase_factor
        else:
            print("Hey buddy: That is not CA or NL eh!")    
            sys.exit()

        
    # get data
    LVPS_data_range_df = get_scenario_data(begin_date, 
                                           end_date, 
                                           start_of_day_time, 
                                           end_of_day_time_data, 
                                           country_name)
    
    for bank in dict_problem_banks:    
        if dict_problem_banks[bank][0] == bank_in_trouble_selected:
            for paytype in range(len(list_type_of_pay_flows)):
                if list_type_of_pay_flows[paytype] == pay_type_selected:
                    # create instance of class ScenarioGenerator
                    new_scenario = ScenarioGenerator(LVPS_data_range_df, 
                                                     dict_problem_banks[bank][0], 
                                                     list_type_of_pay_flows[paytype],
                                                     country_name)

                    # get dataframe with modified transaction amount based on scenario parameters.
                    scenario_outflow_dfs = new_scenario.extra_outflow_factor(start_of_day_time, 
                                                                             end_of_day_time_scenario, 
                                                                             begin_date_scenario,
                                                                             duration_of_scenario_days, 
                                                                             dict_problem_banks[bank][1], 
                                                                             dict_problem_banks,
                                                                             average_outflow_df,
                                                                             extra_outflow_to_whom,
                                                                             continuous_yes_no,
                                                                             increase_factor
                                                                             )

    return scenario_outflow_dfs
    
