import pandas as pd
import uuid

import datetime
import calendar
#################################################
def table_check_for_duplicates(table_to_check):
    has_duplicates = table_to_check.duplicated().any()

    if has_duplicates:
        print("Yes, there are duplicates in the DataFrame.")
    else:
        print("No, there are no duplicates in the DataFrame.")
##################################################
def customer_name_extractor(old_df,new_table):
    new_customer = {
          'customer_id': '',
          'customer_name': '',
          'customer_phone_number': ''
      } 		
    for index, row in old_df.iterrows():
        if row['CustomerName'] not in new_table['customer_name'].values:
        # Add new customer to the table
            new_customer['customer_id'] = str(uuid.uuid4())
            new_customer['customer_name'] = row['CustomerName']
            new_customer ['customer_phone_number'] = row['CustomerContact']
            df_dictionary = pd.DataFrame([new_customer])
            new_table = pd.concat([new_table, df_dictionary], ignore_index=True)
        else:
            pass
    
    return new_table
################################################
def provider_name_extractor(old_df,new_table):
    new_provider = {
          'providerid': '',
          'providername': ''
      }
    for index, row in old_df.iterrows():
        if row['ProviderName'] not in new_table['providername'].values:
        # Add new provider to the table
            new_provider['providerid'] = str(uuid.uuid4())
            new_provider['providername'] = row['ProviderName']
            # Turn Dictionary into a Data Frame
            df_dictionary = pd.DataFrame([new_provider])
            new_table = pd.concat([new_table, df_dictionary], ignore_index=True)
        else:
            pass
    
    return new_table
###################################################
def date_extractor(old_df,new_table):
    new_date = {
          'dateid': '',
          'dateday': '',
          'dateweekday':'',
          'datemonth':'',
          'dateyear': '',
          'date': ''
          
      }				
    for index, row in old_df.iterrows():
        if row['date'] not in new_table['date'].values:
            # essential to take the date onluy as a string 
            date_string = row['date']
            # Here I adpating the date string so that I can extract meaningfull information
            date_object = datetime.datetime.strptime(date_string, '%Y-%m-%d') 

            date_string_list = date_string.split('-')
            month_number = date_object.month
            month_name = calendar.month_name[month_number]

            #new date table 
            new_date['dateid'] = str(uuid.uuid4())
            new_date['dateday'] = date_string_list[2]
            new_date['dateweekday'] = date_object.strftime('%A')
            new_date['datemonth'] = month_name
            new_date['dateyear'] = date_string_list[0]
            new_date['date'] = date_string


            # Turn Dictionary into a Data Frame
            df_dictionary = pd.DataFrame([new_date])
            new_table = pd.concat([new_table, df_dictionary], ignore_index=True)
        else:
            pass
    
    return new_table
########################################################
def billing_extractor(old_df,new_table,table_of_id):
 
    new_list = []
    
    for index, row in old_df.iterrows():
        #Add new provider to the table
         new_bill = {  # This ensures a new dictionary is created each time
        'billingid': str(uuid.uuid4()),
        'pricingplan': row['PricingPlan'],
        'tariffchanges': row['TariffChanges'],
        'billingdisputes': row['BillingDisputes'],
        'providerid': table_of_id[table_of_id['providername'] == row['ProviderName']]['providerid'].values[0]}
        	
            
            # Turn Dictionary into a Data Frame
         new_list.append(new_bill)
       
    new_table = pd.DataFrame(new_list)

    
    return new_table
####################################################
def regulatory_compliance_extractor(old_df,new_table,provider_id_table,date_id_table):
 
    new_list = []
    
    for index, row in old_df.iterrows():
        #Add new provider to the table
         new_comp = {  # This ensures a new dictionary is created each time
        'complianceid': str(uuid.uuid4()),
        'regulatoryrequirements': row['RegulatoryRequirements'],
        'compliancestatuss': row['ComplianceStatus'],
        'dateid': date_id_table[date_id_table['date'] == row['date']]['dateid'].values[0],
        'providerid':provider_id_table[provider_id_table['providername'] == row['ProviderName']]['providerid'].values[0]}
        
 	
		


            # Turn Dictionary into a Data Frame
         new_list.append(new_comp)
       
    new_table = pd.DataFrame(new_list)

    
    return new_table
##############################################
def service_outage_extractor(old_df,new_table,date_id_table):
 
    new_list = []
    
    for index, row in old_df.iterrows():
        #Add new provider to the table
         new_comp = {  # This ensures a new dictionary is created each time
        'outageid': str(uuid.uuid4()),
        'outagetype': row['OutageType'],
        'duration': row['Duration'],
        'affectedAreas': row['AffectedAreas'],
        'description':row['Description'],
        'dateid': date_id_table[date_id_table['date'] == row['date']]['dateid'].values[0]
        }
        
            
            # Turn Dictionary into a Data Frame
         new_list.append(new_comp)
       
    new_table = pd.DataFrame(new_list)

    
    return new_table