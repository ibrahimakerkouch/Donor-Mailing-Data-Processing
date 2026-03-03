# Interacting with the Python runtime environment.
import sys
# Interacting with the operating system, such as file paths and directories.
import os

### Specify the path where function files are stored
wd = "D:/Projects/Python/Donor-Mailing-Data-Processing"
# wd = os.getcwd()
srcpath = os.path.join(wd,"scripts")

### Add the path to the system path
sys.path.append(srcpath)

### Loading the custom libraries
from functions_etl_pipeline import extract_data, transform_data, load_data

### Specify the identifier of the processed customer
Customer_ID = 4
Customer_name = "Delta Enterprises"

### Specify the identifier of the processed job
Job_ID = 4
Job_Name = "00004 - Wildlife Conservation Survey"
MailDate = '2026-02-20'

### Specify the path where data files are stored 
customer_folder = os.path.join(wd, "data", Customer_name)

### Specify the path where input files are stored 
list_fields_file = os.path.join(wd, "inputs", "list_fields.xlsx")
list_states_file = os.path.join(wd, "inputs", "list_states.xlsx")
list_IMB_components_file = os.path.join(wd, "inputs", "list_IMB_components.xlsx")

### Running the ETL pipeline.
print("⏳ Executing the ETL pipeline...")

print("⏳ Extract...")
df_global, df_aggr = extract_data(customer_folder, Job_ID, Customer_ID, list_fields_file)
print("✅ Extract complete!")

## Verify if the dataframe holds any records.
if df_global.shape[0] > 0 :    
    print("⏳ Transform...")
    df_global, df_log = transform_data(df_global, list_states_file, list_IMB_components_file)
    print("✅ Transform complete!")
    
    print("⏳ Load...")
    load_data(Customer_ID, Job_ID, Job_Name, MailDate, df_global, df_log, df_aggr)
    print("✅ Load complete!")
    
else:
    print("🚫 No data found.")

print("✅ Completed the ETL pipeline...")
