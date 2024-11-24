import pandas as pd
import uuid
import datetime
import calendar
from etl_packages import *

##########Creating Datframes from each CSV############
billing_df = pd.read_csv('billing_pricing_dataset.csv')
complaints_df = pd.read_csv('complaints_dataset.csv')
customer_feedback_df = pd.read_csv('customer_feedback_dataset.csv')
provider_performance_df = pd.read_csv('provider_performance_dataset.csv')
reg_compliance_df = pd.read_csv('regulatory_compliance_dataset.csv')
service_outages_df = pd.read_csv('service_outages_dataset.csv')

########DATA CLEANING#####
billing_df = billing_df.dropna()
complaints_df = complaints_df.dropna()
customer_feedback_df = customer_feedback_df.dropna()
provider_performance_df = provider_performance_df.dropna()
service_outages_df = service_outages_df.dropna()
reg_compliance_df = reg_compliance_df.dropna()
reg_compliance_df = reg_compliance_df.rename(columns={'AuditDate':'Date'})

#########CREATING DATAFRAMES#####
