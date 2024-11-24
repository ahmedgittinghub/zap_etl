import psycopg2 
import os
from dotenv import load_dotenv

#### CONNECTION TO DB #####
DATABASE= os.environ.get("postgres_db")
HOST=os.environ.get("postgres_host")
USER=os.environ.get("postgres_user")
PASSWORD=os.environ.get("postgres_pass")

load_dotenv()

def setup_db_connection(database=DATABASE,host= HOST,user= USER,password= PASSWORD):
    connection = psycopg2.connect(database=database,host=host,user=user,password=password)
    cursor = connection.cursor()
    print("We connected!")
    return connection, cursor


connection, cursor = setup_db_connection()

##### Duplicate CHECK ######
def duplicate_drop(new_table, database_table, field):
    """
    Removes duplicates from `new_table` based on `field` matching rows in `database_table`.

    Parameters:
        new_table (pd.DataFrame): The DataFrame to clean.
        database_table (pd.DataFrame): The DataFrame to check for duplicates.
        field (str): The column name to compare.

    Returns:
        pd.DataFrame: The cleaned DataFrame with duplicates removed.
    """
    # Find duplicates using a vectorized operation
    duplicates = new_table[field].isin(database_table[field])
    # duplicates essentially creates a yes or check list for entry, where if there is duplicate 

    # Count duplicates
    counter = duplicates.sum()
    # Drop duplicates
    new_table = new_table[~duplicates]
    # ~ this here  flips the true into false and false into true. 

    print(f'The number of duplicate rows found and dropped is {counter}')
    return new_table

##### LOAD DATE ######

def load_date_into_table():
    sql = """
        INSERT INTO date_table (dateid, dateday,dateweekday,datemonth,dateyear,date) 
        VALUES (%s, %s, %s, %s, %s, %s)
        """
    for index, row in date_table.iterrows():
        data_values = (row['dateid'],row['dateday'],row['dateweekday'],
                   row['datemonth'],row['dateyear'],row['date'])
        cursor.execute(sql, data_values)
        connection.commit()

##### LOAD CUSTOMER DATA ######

def load_customer_data_into_table():
    sql = """
        INSERT INTO customer_table (customer_id, customer_name, customer_phone_number) 
        VALUES (%s, %s, %s)
        """
    for index, row in customer_table.iterrows():
        customer_values = (row['customer_id'],row['customer_name'],row['customer_phone_number'])
        cursor.execute(sql, customer_values)
        connection.commit()

##### LOAD PROVIDER DATA ######

def load_provider_data_into_table():
    sql = """
        INSERT INTO provider_table (providerid, providername) 
        VALUES (%s, %s)
        """
    for index, row in provider_table.iterrows():
        provider_values = (row['providerid'],row['providername'])
        cursor.execute(sql, provider_values)
        connection.commit()

#### LOAD BILLING DATA ####

def load_billing_data_into_table():
    sql = """
        INSERT INTO billing_table (billingid, pricingplan, tariffchanges, billingdisputes, providerid) 
        VALUES (%s, %s, %s, %s, %s)
        """
    for index, row in billing_table.iterrows():
        billing_values = (row['billingid'],row['pricingplan'],row['tariffchanges'],row['billingdisputes'],row['providerid'])
        cursor.execute(sql, billing_values)
        connection.commit()

#### LOAD COMPLIANCE DATA ####

def load_compliance_data_into_table():
    sql = """
        INSERT INTO regulatory_compliance_table (complianceid, regulatoryrequirements, compliancestatuss, dateid, providerid) 
        VALUES (%s, %s, %s, %s, %s)
        """
    for index, row in regulatory_compliance_table.iterrows():
        comp_values = (row['complianceid'],row['regulatoryrequirements'],row['compliancestatuss'],row['dateid'],row['providerid'])
        cursor.execute(sql, comp_values)
        connection.commit()	

#### LOAD SERVIS OUTAGES ####

def load_service_outages_data_into_table():
    sql = """
        INSERT INTO service_outages_table (outageid, outagetype, duration, affectedAreas, description, dateid) 
        VALUES (%s, %s, %s, %s, %s, %s)
        """
    for index, row in service_outages_table.iterrows():
        outage_values = (row['outageid'],row['outagetype'],row['duration'],row['affectedAreas'],row['description'],row['dateid'])
        cursor.execute(sql, outage_values)
        connection.commit()	









