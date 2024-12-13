# Reformat the CPS and SPS linear extent field survey data
# Gray McKenna 
# 2024-08-15

import pandas as pd

# load data --> copied into this folder from K:, see notes for full file path

cps = pd.read_csv(r"kelp_data_sources\bull_kelp_2019_cps.csv")
sps = pd.read_csv(r"kelp_data_sources/dnr2017_ss.csv")

# preview tables

print("CPS and SPS original table format: ")
print(cps.head())
print(sps.head())

### SPS ###
# these have been split so many site codes are duplicated
# need to return a kelp presence of 1 if kelp was present in any portion of site

sps = sps.groupby('SITE_CODE', as_index=False).agg(
    {'kelp':'max'}
).rename(columns={'kelp': 'presence'})
sps['year'] = '2017'
sps['source'] = 'sps_boat_survey'

# check that field is unique
def check_key(df, key_column):
    df_key = pd.Series(df[key_column])
    print("Key field is unique: ")
    print(df_key.is_unique)

check_key(sps, 'SITE_CODE')

### CPS ###
# concatenate cps SITE_NO and REGION cols into SITE_CODE
cps['SITE_NO_str'] = cps['SITE_NO'].astype(str)

def add_leading_zero(i): # add leading zeroes to sites where that got dropped 
    # bc SITE_NO is an int64 field
    if len(i) < 4:
        return '0' + i
    return i

cps['SITE_NO_str'] = cps['SITE_NO_str'].apply(add_leading_zero)

cps['SITE_CODE'] = cps['REGION'] + cps['SITE_NO_str']

# return 1 if any subset of site has presence
cps = cps.groupby('SITE_CODE', as_index=False).agg(
    {'kelp':'max'}
).rename(columns={'kelp': 'presence'})
cps['year'] = '2019'
cps['source'] = 'cps_boat_survey'

# check that site_code is unique 
check_key(cps, 'SITE_CODE')

print("New CPS and SPS format:")
print(sps.head())
print(cps.head())

# save to csv in results folder
sps.to_csv("kelp_data_synth_results\\sps_2017.csv")
cps.to_csv("kelp_data_synth_results\\cps_2019.csv")
