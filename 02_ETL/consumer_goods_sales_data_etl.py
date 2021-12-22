# -- Importing Libraries -- #

print('\n')
print('Importing libraries to perform ETL...')

import pandas as pd
import sqlite3
import pyfiglet

from datetime import date, timedelta
from fx_optimal_date_table import generate_period_table

import warnings
warnings.filterwarnings('ignore')

print('Initiating ETL Process...')
print('\n')

# -- Starting ETL Process --#

etl_title = "CONSUMER GOODS SALES DATA ETL"
ascii_art_title = pyfiglet.figlet_format(etl_title, font='small')
print(ascii_art_title)
print('\n')

# -- Connecting to Dataset -- #

print('Connecting to source dataset')

dim_locations = pd.read_csv("../01_SOURCE/Dim_Locations.csv", index_col=None)
fact_orders = pd.read_csv("../01_SOURCE/Fact_Orders.csv", index_col=None)

print('\n')

# -- Transforming "dim_locations" data --#

print('Transforming "dim_locations" data')
print(f'Shape of "dim_locations" dataset: {dim_locations.shape}')
print(f'Columns in "dim_locations" dataset: {list(dim_locations.columns)}')
print('\n')

# -- Removing Unnecessary Columns --#

print('Removing unnecessary columns')

dim_locations = dim_locations.iloc[:,:-1]

print(f'Shape of dataframe after removal of unnecessary columns: {dim_locations.shape}')
print(f'Columns in "dim_locations" dataframe after removal of unnecessary columns: {list(dim_locations.columns)}')
print('\n')

# -- Creating "dim_period" table -- #

print('Construction "dim_period" table')

fact_orders['Order Date'] = pd.to_datetime(fact_orders['Order Date'])

start_date = fact_orders['Order Date'].min()
end_date = fact_orders['Order Date'].max()

dim_period = generate_period_table(start_date, end_date)

dim_period.to_csv('../03_DATA/Dim_Period.csv', encoding='utf-8', index=False)

print(f'Shape of "dim_period" dataset: {dim_period.shape}')
print(f'Columns in "dim_period" dataset: {list(dim_period.columns)}')
print('\n')

# -- Exporting Data to CSV File -- #

print('Exporting the dataframes to CSV files...')

dim_locations.to_csv('../03_DATA/FLATFILES/Dim_Locations.csv', encoding='utf-8', index=False)
dim_period.to_csv('../03_DATA/FLATFILES/Dim_Period.csv', encoding='utf-8', index=False)
fact_orders.to_csv('../03_DATA/FLATFILES/Fact_Orders.csv', encoding='utf-8', index=False)

print('Data exported to CSV...')
print('\n')


# -- SQLite Database -- #
# ===================== #

# -- Connecting to Database --#

print('Connecting to Database')

connection = sqlite3.connect('../03_DATA/DATABASE/US_Consumer_Goods_Sales.db')
cursor = connection.cursor()

print('\n')

# -- Inserting Data into Database Tables --#

print('Inserting data into database tables')

dim_locations.to_sql('dim_locations', connection, if_exists='replace', index=False)
dim_period.to_sql('dim_period', connection, if_exists='replace', index=False)
fact_orders.to_sql('fact_orders', connection, if_exists='replace', index=False)

connection.commit()

print('Data added to the database successfully')
print('\n')

# -- Closing the database --#

connection.commit()

print('Database connected closed')
print('\n')
print('ETL Process completed !!!')






