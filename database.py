"""
Setting up the database

Assumptions:
1) carbon intensity of wind energy is the mean of offshore and onshore wind prodcution
2) The mean of carbon intensity of biomass includes the carbon intensity of waste energy
"""

#imports
import requests
import json
import sqlite3
import pandas as pd
import numpy as np

################################################
#Carbon intensities - from JSON  file on github
################################################

# store data
data_github = requests.get(
    'https://raw.githubusercontent.com/electricitymap/electricitymap-contrib/master/config/co2eq_parameters.json'
)
data_github = data_github.json()

#Select Default values of emissionFactors
data = data_github['emissionFactors']['defaults']

#extract names and corresponding mean carbon emission values
energy_type = [x for x in data.keys()]
value = [v['value'] for v in data.values()]

#convert to pandas dataframe
list_of_tuples = list(zip(energy_type, value))
data = pd.DataFrame(list_of_tuples, columns=['energy_type', 'value'])


################################################
#Electricty generation by production type - csv
################################################

dk2 = pd.read_csv('data/Gen_Type_DK2.csv', sep=";")

#drop columns with only missing values
dk2.replace('n/e', np.NaN, inplace=True)
dk2.dropna(axis=1, how='all', inplace=True)


#Change column names (so that they correspond to column names in
# carbon intensity file)
dk2.columns = [
    'Area', 'MTU', 'Biomass', 'gas', 'coal', 'oil', 'solar', 'waste',
    'wind_off', 'wind_on'
]

#combine waste and biomass into one category: biomass
dk2['biomass'] = dk2['Biomass'] + dk2['waste']

#combine offshore and onshore wind into one category: wind
dk2['wind'] = dk2['wind_off'] + dk2['wind_on']

dk2 = dk2.drop(columns=['Biomass', 'waste', 'wind_on', 'wind_off'])

#seperate hours from days

#DATE
dk2['date'] = dk2['MTU'].str[:10]
dk2['date'] = pd.to_datetime(dk2['date'], format='%d.%m.%Y')

#TIME
dk2['time'] = dk2['MTU'].str[11:16]

#compress dataframe to only necessary columns
dk2 = dk2.loc[:, 'gas':'time']

################################################
###########Setting up SQL Database##############
################################################

# Creating database and connecting
conn = sqlite3.connect('energy.db')
cursor = conn.cursor()

#making table for carbon intensity values
cursor.executescript("""
    DROP TABLE IF EXISTS carbon;

    CREATE TABLE energy (
        energy_type TEXT PRIMARY KEY,
        value INTEGER
        )
    """)

#Adding data to table
data.to_sql('carbon', conn, if_exists='append', index=False)


#making table for energy generation

cursor.executescript("""
    DROP TABLE IF EXISTS production;

    CREATE TABLE production (
        gas INTEGER,
        coal INTEGER,
        oil INTEGER,
        solar INTEGER,
        biomass INTEGER,
        wind INTEGER,
        date DATETIME,
        time DATETIME
        )
    """)

#Adding data to table
dk2.to_sql('production', conn, if_exists='append', index=False)


# Close connection
cursor.close()
conn.close()
