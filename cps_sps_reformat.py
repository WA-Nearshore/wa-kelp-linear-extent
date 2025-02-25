# Reformat the CPS & SPS linear extent field survey data

import arcpy
import pandas as pd
import os
from arcgis.features import GeoAccessor, GeoSeriesAccessor

arcpy.env.overwriteOutput = True

def reset_ws(): 
    arcpy.env.workspace = os.getcwd()

reset_ws()

# prep data ------------------------------------------------------------

# cps data from K:\Kelp\2019_cps_field\spatial_data\bull_kelp_cps_2019.gdb
cps_orig= "kelp_data_sources\\bull_kelp_cps_2019.gdb\\bull_kelp_2019"

# sps data from K:\kelp\projects\historical_comparison_sps_2018\spatial_data\SS_kelp.gdb
sps_orig = "kelp_data_sources\\SS_kelp.gdb\\dnr2017_ss"

# copy datasets to scratch
cps_fc = "scratch.gdb\\cps"
sps_fc = "scratch.gdb\\sps"

arcpy.management.CopyFeatures(cps_orig, cps_fc)
arcpy.management.CopyFeatures(sps_orig, sps_fc)

# calculate length field 
arcpy.management.CalculateField(cps_fc, "length", "!SHAPE_LENGTH!")
arcpy.management.CalculateField(sps_fc, "length", "!SHAPE_LENGTH!")

# convert to df 
cps_df = pd.DataFrame.spatial.from_featureclass(cps_fc)
sps_df = pd.DataFrame.spatial.from_featureclass(sps_fc)

# calculate sps resence and abundance -----------------------------------

# presence
sps_pres = sps_df.groupby('SITE_CODE', as_index=False).agg(
    {'kelp':'max'}
).rename(columns={'kelp': 'presence'})
sps_pres['year'] = '2017'
sps_pres['source'] = 'WADNR_sps_boat_survey'

# abundance
# calculate total_length for each SITE_CODE
sps_df['length'] = pd.to_numeric(sps_df['length'])
sps_df['total_length'] = sps_df.groupby('SITE_CODE')['length'].transform('sum')

# calculate weight of each section
sps_df['weight'] = sps_df['length'] / sps_df['total_length']

# get weighted presence for each section
sps_df['w_pres'] = sps_df['weight'] * sps_df['kelp']

# sum weighed presence by SITE_CODE
sps_ab = (sps_df.groupby('SITE_CODE')
                .agg(sum_w_pres=('w_pres', 'sum'))
                .reset_index())

# convert to abundance
sps_ab['abundance'] = pd.cut(sps_ab['sum_w_pres'],
                                    bins=[-float('inf'), 0, 0.25, 0.5, 0.75, float('inf')],
                                    labels=[0, 1, 2, 3, 4])

# drop extra col
sps_ab = sps_ab.drop(columns=['sum_w_pres'])

# combine 
sps_result = pd.merge(sps_pres, sps_ab, how="left", on=["SITE_CODE"])

# check that field is unique
def check_key(df, key_column):
    df_key = pd.Series(df[key_column])
    print("Key field is unique: ")
    print(df_key.is_unique)

check_key(sps_result, 'SITE_CODE')

# calculate cps presence and abundance -----------------------------------

# concatenate cps SITE_NO and REGION cols into SITE_CODE
cps_df['SITE_NO_str'] = cps_df['SITE_NO'].astype(str)

def add_leading_zero(i): # add leading zeroes to sites where that got dropped 
    # bc SITE_NO is an int64 field
    if len(i) < 4:
        return '0' + i
    return i

cps_df['SITE_NO_str'] = cps_df['SITE_NO_str'].apply(add_leading_zero)

cps_df['SITE_CODE'] = cps_df['REGION'] + cps_df['SITE_NO_str']

# return 1 if any subset of site has presence
cps_pres = cps_df.groupby('SITE_CODE', as_index=False).agg(
    {'kelp':'max'}
).rename(columns={'kelp': 'presence'})
cps_pres['year'] = '2019'
cps_pres['source'] = 'WADNR_cps_boat_survey'

# calculate total_length for each SITE_CODE
cps_df['total_length'] = cps_df.groupby('SITE_CODE')['LENGTH'].transform('sum')

# calculate weight of each section
cps_df['weight'] = cps_df['LENGTH'] / cps_df['total_length']

# use presence to convert forest presence to 1 and incidental presence to 0.5
# in other words, under weight incidental presence
cps_df['kelp_qual'] = cps_df['type'].map({
    'i': 0.5,
    'f': 1,
    'a': 0
})

# get weighted presence for each section
cps_df['w_pres'] = cps_df['weight'] * cps_df['kelp_qual']

# sum weighed presence by SITE_CODE
cps_ab = (cps_df.groupby('SITE_CODE')
                .agg(sum_w_pres=('w_pres', 'sum'))
                .reset_index())

# convert to abundance
cps_ab['abundance'] = pd.cut(cps_ab['sum_w_pres'],
                                    bins=[-float('inf'), 0, 0.25, 0.5, 0.75, float('inf')],
                                    labels=[0, 1, 2, 3, 4])

# drop extra col
cps_ab = cps_ab.drop(columns=['sum_w_pres'])

# combine 
cps_result = pd.merge(cps_pres, cps_ab, how="left", on=["SITE_CODE"])
# check that site_code is unique 
check_key(cps_result, 'SITE_CODE')

# export results ------------------------------------------------------
print("New CPS and SPS format:")
print(sps_result.head())
print(cps_result.head())

# save to csv in results folder
sps_result.to_csv("kelp_data_synth_results\\sps_2017.csv")
cps_result.to_csv("kelp_data_synth_results\\cps_2019.csv")
