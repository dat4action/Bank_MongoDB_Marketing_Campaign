# Import the packages
import os
import warnings

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

from database import connect_to_mongodb
from database import import_csv_to_collection
from utilities import df_to_csv

warnings.filterwarnings("ignore")


# Get the absolute path of the directory containing the script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the file path to bank.csv relative to the script's location
data_file = os.path.join(script_dir, '..', 'data/raw_data/' ,'bank_marketing.csv')

# Read the dataset from the CSV file
df = pd.read_csv(data_file)



# Splitting the data into three datasets
df_client=df[['client_id','age','job','marital','education','credit_default','housing','loan']]
df_campaign=df[['client_id','contact','month','day','duration','campaign','pdays','previous','poutcome','y']]
df_economics=df[['emp_var_rate', 'cons_price_idx','cons_conf_idx', 'euribor3m', 'nr_employed']]

# *****************************#
# Renaming columns as Required #
# *****************************#

# Dicts for renaming columns
client_renaming={'client_id':'id'}
campaign_renaming={'duration':'contact_duration','previous':'previous_campaing_contacts','y':'campaign_outcome','poutcome':'previous_outcome','campaign':'number_contacts'}
economic_renaming={'euribor3m':'euribor_three_months','nr_employed':'number_employed'}

# Renaming columns
df_client.rename(columns=client_renaming,inplace=True)
df_campaign.rename(columns=campaign_renaming,inplace=True)
df_economics.rename(columns=economic_renaming,inplace=True)

# *******************************************#
# Clean ant transform dataset as Required    #
# *******************************************#

# Copy of the datasets
client_df=df_client.copy()
campaign_df=df_campaign.copy()
economics_df=df_economics.copy()

# Changing "." to "_" , and unkown to np.nan in education column
client_df['education']=client_df['education'].str.replace('.','_',regex=False)
client_df['education']=client_df['education'].replace('unknown',np.nan)

# Removing periods from the "job" column
client_df['job']=client_df['job'].str.strip('.')

# Converting "success" and "failure" in the "previous_outcome" and "campaign_outcome" columns to binary (1 or 0), 
# along with the changing "nonexistent" to NumPy's null values in "previous_outcome"
rep_dict2={"success":1,"failure":0,"no":0,"yes":1,"nonexistent":np.nan}

campaign_df[['previous_outcome','campaign_outcome']]=campaign_df[['previous_outcome','campaign_outcome']].replace(rep_dict2)

# Adding a column called campaign_id in campaign, where all rows have a value of 1.
campaign_df=campaign_df.assign(campaign_id=1)

# Creation of a datetime column called last_contact_date, in the format of "year-month-day", where the year is 2022, and the month and day values 
# are taken from the "month" and "day" columns.
year='2022'
campaign_df['last_contact_date'] = pd.to_datetime(campaign_df['month'].astype(str) + '-' + campaign_df['day'].astype(str) + '-' +str(year), format='%b-%d-%Y').dt.strftime('%Y-%m-%d')
campaign_df.head()

# Droping month and day columns in campaign_df
campaign_df.drop(['month','day'],axis=1,inplace=True)
campaign_df.head()

# *******************************************#
# Saving the Three data Frames as csv files  #
# *******************************************#

df_to_csv(client_df,"client")
df_to_csv(campaign_df,"campaign")
df_to_csv(economics_df,"economics")

# **************************************************#
# Connection to Mongo and creation of collections   #
# **************************************************#

db=connect_to_mongodb("Bank_Marketing")

import_csv_to_collection('data/clean_data/client.csv', "client_collection",db)
import_csv_to_collection('data/clean_data/campaign.csv', "campaign_collection",db)
import_csv_to_collection('data/clean_data/economics.csv', "economics_collection",db)