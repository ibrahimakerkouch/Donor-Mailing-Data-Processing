# Interacting with the operating system, such as file paths and directories.
import os
# Data manipulation and transformation
import pandas as pd
import numpy as np
# Retrieving file pathnames matching a specified pattern
import glob 
# Regular expressions
import re
# String operations and constants
import string
# Progress bar for tracking progress
from progress.bar import Bar
# Database connections and executing SQL queries
from sqlalchemy import create_engine
import urllib

def extract_data(customer_folder, Job_ID, Customer_ID, list_fields_file):
    """Extract data from a CSV, Excel and Json files."""
    ### Importing all fields with standardized formats
    list_fields = pd.read_excel(list_fields_file)

    ### Converting to key : value format
    list_fields = list_fields.set_index('Fields')['Standardized'].to_dict()

    ### Defining an empty dataframe
    df_global = pd.DataFrame()
    
    ### Defining the aggregation dataframe
    df_aggr = pd.DataFrame()
    
    ### Importing the raw data
    raw_data = glob.glob(customer_folder + "\\*")
    
    ### Ensuring the repository has files.
    if len(raw_data) > 0:

        ### Define the total number of steps
        total_steps = len(raw_data)
    
        ### Create a progress bar with a custom prefix and suffix
        bar = Bar('Processing', max=total_steps, suffix='%(percent)d%%')
    
        for i in range(total_steps):
            item = raw_data[i]
            if item.endswith("xlsx"):
                df_stagia = pd.read_excel(item, dtype=str)
                df_stagia = df_stagia.replace('nan', np.nan)
            if item.endswith("csv"):
                df_stagia = pd.read_csv(item, dtype=str)
                df_stagia = df_stagia.replace('nan', np.nan)
            if item.endswith("json"):
                df_stagia = pd.read_json(item, dtype=False)
                df_stagia = df_stagia.replace('nan', np.nan)
            
            ## standardizing the names 
            df_stagia = df_stagia.rename(columns = list_fields)
            df_stagia["FileName"] = os.path.basename(item)
            df_stagia["Panel"] = df_stagia["Panel"].astype(int)
                      
            # Filter out empty DataFrames before concat
            dfs_to_concat = [df for df in [df_global, df_stagia] if not df.empty]
    
            ## Combining the data
            df_global = pd.concat(dfs_to_concat, axis=0, ignore_index=True)
            
            df_aggr = pd.concat([df_aggr, pd.DataFrame({
                'CustomerID': Customer_ID,
                'JobID': Job_ID,
                'PanelID': [df_stagia.loc[0, "Panel"]],
                'FileName': os.path.basename(item),
                'Stage': ['Extract'],
                'ActionTaken': ["Collected"],
                'Reason': [np.nan],
                'FieldCategory' : [np.nan],
                'FieldsAffected': [np.nan],
                'RecordCount': [len(df_stagia)]
            })], ignore_index=True)
            
            ## Move the progress bar forward
            bar.next()
    
        ## Finish the progress bar
        bar.finish()

    return(df_global, df_aggr)

def transform_data(df_global, list_states_file, list_IMB_components_file):
    ### Define the total number of steps
    total_steps = 16

    ### Create a progress bar with a custom prefix and suffix
    bar = Bar('Processing', max=total_steps, suffix='%(percent)d%%')
    
    ### Importing all abbreviations with standardized formats
    #list_abbrevs = pd.read_excel(list_abbrevs_file)

    ### Converting to key : value format
    #list_abbrevs = list_abbrevs.set_index('Word')['Abbrev'].to_dict()

    ### Importing all states
    list_states = pd.read_excel(list_states_file)
    
    ### Importing all IMB components with max length
    list_IMB_components = pd.read_excel(list_IMB_components_file)
    
    ## Move the progress bar forward
    bar.next()
    
    ## Defining the dataframes for log and invalid records
    df_log = pd.DataFrame()
    
    ## Convert to the right data types
    df_global["DonationAmount"] = df_global["DonationAmount"].astype(float)
    df_global["DonorID"] = df_global["DonorID"].astype(int)
    
    ## Move the progress bar forward
    bar.next()
    
    ## Get rid of records didn’t meet the requirements for the mail processing:
    # Contact Info Fields
    contact_fields = ['AddressLine1', 'City', 'State', 'ZIP5', 'DonationAmount']
    for field in contact_fields:
        # Log dropped rows
        df_stagia = df_global[df_global[field].isna()].copy()
        df_stagia['Stage'] = 'Transform'
        df_stagia['ActionTaken'] = 'Dropped'
        df_stagia['Reason'] = 'Missing'
        df_stagia['FieldCategory'] = "Contact Info"
        df_stagia['FieldsAffected'] = field

        # Gather invalid rows for logs
        df_log = pd.concat([df_log, df_stagia], axis=0, ignore_index=True)
    
    ## Move the progress bar forward
    bar.next()
    
    # Postal Automation & USPS Compliance
    usps_fields = ['ZIP4', 'MailCode', 'DeliveryPoint', 'BarcodeIdentifier', 'ServiceTypeID', 'MailerID', 'SerialNumber']
    for field in usps_fields:
        # Log dropped rows
        df_stagia = df_global[df_global[field].isna()].copy()
        df_stagia['Stage'] = 'Transform'
        df_stagia['ActionTaken'] = 'Dropped'
        df_stagia['Reason'] = 'Missing'
        df_stagia['FieldCategory'] = "Postal"
        df_stagia['FieldsAffected'] = field

        # Gather invalid rows for logs
        df_log = pd.concat([df_log, df_stagia], axis=0, ignore_index=True)
    
    ## Move the progress bar forward
    bar.next()
    
    # Demographics & Geography
    demographics_fields = ["Representative", "Senator", "CongressionalDistrict", "CongressionalName"]
    for field in demographics_fields:
        # Log dropped rows
        df_stagia = df_global[df_global[field].isna()].copy()
        df_stagia['Stage'] = 'Transform'
        df_stagia['ActionTaken'] = 'Dropped'
        df_stagia['Reason'] = 'Missing'
        df_stagia['FieldCategory'] = "Demographics"
        df_stagia['FieldsAffected'] = field

        # Gather invalid rows for logs
        df_log = pd.concat([df_log, df_stagia], axis=0, ignore_index=True)
    
    ## Move the progress bar forward
    bar.next()

    ## Standardize name casing and trimming the columns Title, FirstName, LastName and Suffix
    trimming_fields = ["Title", "FirstName", "LastName", "Suffix"]
    for field in trimming_fields:
        df_global[field] = df_global[field].apply(lambda x: "" if pd.isna(x) else x)
        df_global[field] = df_global[field].apply(lambda x: str(x).strip())
    
    ## Move the progress bar forward
    bar.next()
    
    ## Construct GreetingLine when missing (e.g., "Dear FirstName LastName"). 
    df_global["GreetingLine"] = "Dear " + df_global["Title"] + " " + df_global["FirstName"] + " " + df_global["LastName"] + " " + df_global["Suffix"]
    df_global["GreetingLine"] = df_global["GreetingLine"].apply(lambda x: re.sub(r"\s+", " ", str(x).strip()))
    df_global["GreetingLine"] = df_global["GreetingLine"].apply(lambda x: "Dear friend" if x == "Dear" else  x)
    
    df_stagia = df_global[df_global["GreetingLine"] == "Dear friend"]
    df_stagia['Stage'] = 'Transform'
    df_stagia['ActionTaken'] = 'Processed'
    df_stagia['Reason'] = 'Unavailable'
    df_stagia['FieldCategory'] = "Contact Info"
    df_stagia['FieldsAffected'] = "GreetingLine"
    
    # Gather invalid rows for logs
    df_log = pd.concat([df_log, df_stagia], axis=0, ignore_index=True)
    
    ## Move the progress bar forward
    bar.next()
    
    ## Standardize address formats and abbreviations
    #df_global["AddressLine1"] = df_global["AddressLine1"].astype(str).str.upper().str.strip().replace(list_abbrevs, regex=True)
    #df_global["AddressLine1"] = df_global["AddressLine1"].str.title()
    
    ## Move the progress bar forward
    #bar.next()
    
    ## Validate state codes
    df_stagia = df_global[~df_global["State"].isin(list_states["Abbrev"])].copy()
    df_stagia['Stage'] = 'Transform'
    df_stagia['ActionTaken'] = 'Dropped'
    df_stagia['Reason'] = 'Incorrect'
    df_stagia['FieldCategory'] = "Contact Info"
    df_stagia['FieldsAffected'] = "State"
    
    # Gather the invalid rows for logs
    df_log = pd.concat([df_log, df_stagia], axis=0, ignore_index=True)
    
    ## Move the progress bar forward
    bar.next()
    
    ## Clarify the length of IMB components:
    for i in range(0, len(list_IMB_components)):
        df_stagia = df_global[df_global[list_IMB_components.loc[i,"Field"]].str.len() != list_IMB_components.loc[i,"Max Length"]].copy()
        df_stagia = df_stagia[~pd.isna(df_stagia[list_IMB_components.loc[i,"Field"]])].copy()
        df_stagia['Stage'] = 'Transform'
        df_stagia['ActionTaken'] = 'Dropped'
        df_stagia['Reason'] = 'Invalid length'
        df_stagia['FieldCategory'] = "Postal"
        df_stagia['FieldsAffected'] = list_IMB_components.loc[i,"Field"]
        
        # Gather the invalid rows for logs
        df_log = pd.concat([df_log, df_stagia], axis=0, ignore_index=True)
    
    ## Move the progress bar forward
    bar.next()
    
    ## Standardizing Phone and Fax Format
    phone_fax = ["Phone", "Fax"]
    for field in phone_fax:
        df_global[field] = df_global[field].apply(lambda x: x if pd.isna(x) else re.sub("^(\\+?|[0]{2}?)1-", "", x))
        list_punct = list(string.punctuation)
        list_punct = re.escape("".join(list_punct))
        df_global[field] = df_global[field].apply(lambda x: x if pd.isna(x) else re.sub(f"[{list_punct}]", "", x))
        df_global[field] = df_global[field].apply(lambda x: x if pd.isna(x) else re.sub("([0-9]{3})([0-9]{3})([0-9]{4})(x[0-9])?", r"(\1)\2-\3\4", x))
        
        ## Check if Phone or Fax is missing
        df_stagia = df_global[df_global[field].isna()].copy()
        df_stagia['Stage'] = 'Transform'
        df_stagia['ActionTaken'] = 'Dropped'
        df_stagia['Reason'] = 'Missing'
        df_stagia['FieldCategory'] = "Contact Info"
        df_stagia['FieldsAffected'] = field
        
        # Gather the invalid rows for logs
        df_log = pd.concat([df_log, df_stagia], axis=0, ignore_index=True)
        
        ## Validate email formats using regex
        phone_fax_regex = r"\([0-9]{3}\)[0-9]{3}-[0-9]{4}(x[0-9]+)?"
        df_stagia = df_global[~df_global[field].isna()].copy()
        df_stagia = df_stagia[~df_stagia[field].str.match(phone_fax_regex)].copy()
        df_stagia['Stage'] = 'Transform'
        df_stagia['ActionTaken'] = 'Dropped'
        df_stagia['Reason'] = 'Invalid format'
        df_stagia['FieldCategory'] = "Contact Info"
        df_stagia['FieldsAffected'] = field
        
        # Gather the invalid rows for logs
        df_log = pd.concat([df_log, df_stagia], axis=0, ignore_index=True)
        
    ## Move the progress bar forward
    bar.next()
    
    ## Check if Email is missing
    #email_series = df_global["Email"].astype(str)  # Series
    df_stagia = df_global[df_global["Email"].isna()].copy()
    df_stagia['Stage'] = 'Transform'
    df_stagia['ActionTaken'] = 'Dropped'
    df_stagia['Reason'] = 'Missing'
    df_stagia['FieldCategory'] = "Contact Info"
    df_stagia['FieldsAffected'] = "Email"
    
    # Gather the invalid rows for logs
    df_log = pd.concat([df_log, df_stagia], axis=0, ignore_index=True)
    
    ## Move the progress bar forward
    bar.next()
    
    ## Validate email formats using regex
    email_regex = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    df_stagia = df_global[~df_global["Email"].isna()].copy()
    df_stagia = df_stagia[~df_stagia["Email"].str.match(email_regex)].copy()
    df_stagia['Stage'] = 'Transform'
    df_stagia['ActionTaken'] = 'Dropped'
    df_stagia['Reason'] = 'Invalid format'
    df_stagia['FieldCategory'] = "Contact Info"
    df_stagia['FieldsAffected'] = "Email"
    
    # Gather the invalid rows for logs
    df_log = pd.concat([df_log, df_stagia], axis=0, ignore_index=True)
    
    ## Move the progress bar forward
    bar.next()
    
    ## Deduplicate records using DonorID
    mandatory_fields = ['DonorID', 'Email', 'Phone']
    for field in mandatory_fields:
        duplicate_mask = df_global.duplicated(subset=[field], keep="last")
        df_stagia = df_global[duplicate_mask].copy()
        df_stagia['Stage'] = 'Transform'
        df_stagia['ActionTaken'] = 'Dropped'
        df_stagia['Reason'] = 'Duplicate'
        df_stagia['FieldCategory'] = "Contact Info"
        df_stagia['FieldsAffected'] = field
        
        # Gather the invalid rows for logs
        df_log = pd.concat([df_log, df_stagia], axis=0, ignore_index=True)
    
    # Move the progress bar forward
    bar.next()
    
    ## Keep only valid rows
    df_global = df_global[~df_global["DonorID"].isin(df_log["DonorID"])]
    
    ## Move the progress bar forward
    bar.next()
    
    ## Renaming columns
    df_global = df_global.rename(columns = {"Panel":"PanelID"})
    df_log = df_log.rename(columns = {"Panel":"PanelID"})
    
    ## Move the progress bar forward
    bar.next()
    
    ## Keeping the main columns for the log table
    df_log = df_log[['PanelID', 'DonorID', 'FileName', 'Stage','ActionTaken', 'Reason', 'FieldCategory', 'FieldsAffected']]
    
    ## Move the progress bar forward
    bar.next()
    
    ## Finish the progress bar
    bar.finish()
        
    return (df_global, df_log)

def load_data(Customer_ID, Job_ID, Job_Name, MailDate, df_global, df_log, df_aggr):
    ## Define the total number of steps
    total_steps = 7

    ### Create a progress bar with a custom prefix and suffix
    bar = Bar('Processing', max=total_steps, suffix='%(percent)d%%')
      
    # Defining the connection string
    ## database_type = 'mssql'
    driver = 'ODBC Driver 17 for SQL Server'  # ODBC driver name
    host = 'localhost,1435'
    database = 'DonorMailingDataProcessing'
    username = "sa"
    password = "Str0ng!Pass123"
    
    params = urllib.parse.quote_plus(
        f"DRIVER={driver};SERVER={host};DATABASE={database};UID={username};PWD={password}"
    )
    
    # Create the engine
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    conn = engine.raw_connection()
    cursor = conn.cursor()
    cursor.fast_executemany = True
    
    ## Move the progress bar forward
    bar.next()
    
    # Counting the total by the main columns 
    df_aggr_stagia = (
        df_log.groupby(
            ["PanelID", "FileName", "Stage", "ActionTaken", "Reason", "FieldCategory", "FieldsAffected"]
        )
        .size()
        .reset_index(name="RecordCount")
    )
    
    # Add Job_ID and Customer_ID columns
    df_aggr_stagia["JobID"] = Job_ID
    df_aggr_stagia["CustomerID"] = Customer_ID
    df_global["JobID"] = Job_ID
    
    ## Move the progress bar forward
    bar.next()
    
    # Concatenate the log dataframes
    df_aggr = pd.concat([df_aggr, df_aggr_stagia], ignore_index=True)
    
    ## Move the progress bar forward
    bar.next()
    
    # Insert the job
    df_jobs = pd.DataFrame({'JobID' : [Job_ID], "JobName" : [Job_Name], 'MailDate' : [MailDate], 'CustomerID' : [Customer_ID], })
    
    query_jobs = """
    insert into Jobs(JobID, JobName, MailDate, CustomerID) values(?, ?, ?, ?)
    """
    cursor.executemany(query_jobs, list(df_jobs.itertuples(index=False, name=None)))
        
    ## Move the progress bar forward
    bar.next()
    
    # Drop irrelevant columns
    df_global.drop(columns = ["FileName"], inplace=True)
    
    # Concatenate the columns' names for the query
    columns_names = list(df_global.columns.values)
    columns_names = ', '.join(columns_names)
    
    # Fix data types
    df_global = df_global.fillna('').astype(str)
    df_global["JobID"] = df_global["JobID"].astype(int)
    df_global["PanelID"] = df_global["PanelID"].astype(int)
    df_global['DonationAmount'] = pd.to_numeric(df_global['DonationAmount'])
    
    # Inserting Donors records
    query_donors = f"""
    INSERT INTO Donors({columns_names}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?)
    """
    cursor.executemany(query_donors, list(df_global.itertuples(index=False, name=None)))
    
    ## Move the progress bar forward
    bar.next()
    
    # Add a Job ID column
    df_log["JobID"] = Job_ID
    
    # Concatenate the columns' names for the query
    columns_names = list(df_log.columns.values)
    columns_names = ', '.join(columns_names)
    
    query_JobsPanels = f"""
    INSERT INTO JobsPanels({columns_names}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cursor.executemany(query_JobsPanels, list(df_log.itertuples(index=False, name=None)))
    
    ## Move the progress bar forward
    bar.next()
    
    # Concatenate the columns' names for the query
    columns_names = list(df_aggr.columns.values)
    columns_names = ', '.join(columns_names)
    
    # Fix NAN values
    df_aggr = df_aggr.fillna('').astype(str)
    
    # Inserting Aggregation for log records
    query_AggregationLog = f"""
    INSERT INTO AggregationLog({columns_names}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cursor.executemany(query_AggregationLog, list(df_aggr.itertuples(index=False, name=None)))
    
    # Close all connections in the engine pool
    conn.commit()
    conn.close()
    engine.dispose()

    ## Move the progress bar forward
    bar.next()
    
    ## Finish the progress bar
    bar.finish()
    
    return 0