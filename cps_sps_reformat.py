# Reformat the CPS & SPS linear extent field survey data

# set env -----------------------------------------------------------
import arcpy
import sys
import pandas as pd
import os
from arcgis.features import GeoAccessor, GeoSeriesAccessor
import fns
sys.path.append(os.getcwd())
arcpy.env.overwriteOutput = True

fns.reset_ws()

# prep data ------------------------------------------------------------

# cps data from K:\Kelp\2019_cps_field\spatial_data\bull_kelp_cps_2019.gdb
cps_orig= "kelp_data_sources\\bull_kelp_cps_2019.gdb\\bull_kelp_2019"
# consider switching this to fringe at some point 

# sps data from K:\kelp\projects\historical_comparison_sps_2018\spatial_data\SS_kelp.gdb
sps_orig = "kelp_data_sources\\SS_kelp.gdb\\dnr2017_ss"

print(f"Running analsysis on {cps_orig} and {sps_orig}...")
# abundance containers
abundance_containers = "LinearExtent.gdb\\abundance_containers"

print("Copying features to scratch.gdb...")
# copy datasets to scratch
cps_fc = "scratch.gdb\\cps"
sps_fc = "scratch.gdb\\sps"

arcpy.management.CopyFeatures(cps_orig, cps_fc)
arcpy.management.CopyFeatures(sps_orig, sps_fc)

# convert to df 
print("Converting to dataframes...")
cps_df = pd.DataFrame.spatial.from_featureclass(cps_fc)
sps_df = pd.DataFrame.spatial.from_featureclass(sps_fc)

# calculate sps presence and abundance -----------------------------------
print("Processing SPS data...")
print("Calculating presence...")
# presence
sps_pres = sps_df.groupby('SITE_CODE', as_index=False).agg(
    {'kelp':'max'}
).rename(columns={'kelp': 'presence'})
sps_pres['year'] = '2017'
sps_pres['source'] = 'WADNR_sps_boat_survey'
print("Presence results: ")
print(sps_pres.head())

# abundance
# create a filtered version of the dataset with only line segments w/ kelp present 
print("Filteritng dataset to presence features only...")
sps_df_filt = sps_df[sps_df['kelp']== 1]
sps_fc_filt = "scratch.gdb\\sps_kelp_only"
sps_df_filt.spatial.to_featureclass(location=sps_fc_filt, overwrite=True)

# run the function 
print("Calculating abundance...")
sps_ab = fns.calc_abundance_lines(abundance_containers, [sps_fc_filt])
sps_ab = sps_ab.drop('fc_name', axis=1)
print("Abundance esults: ")
print(sps_ab.head())

# combine 
print("Combining results...")
sps_result = pd.merge(sps_pres, sps_ab, how="left", on=["SITE_CODE"])
print(sps_result.head())

# check that field is unique
def check_key(df, key_column):
    df_key = pd.Series(df[key_column])
    print("Key field is unique: ")
    print(df_key.is_unique)

check_key(sps_result, 'SITE_CODE')

# calculate cps presence and abundance -----------------------------------
print("On to CPS now...")
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

# abundance
# create a filtered version of the dataset with only line segments w/ kelp present 
cps_df_filt = cps_df[cps_df['kelp']== 1]
cps_fc_filt = "scratch.gdb\\cps_kelp_only"
cps_df_filt.spatial.to_featureclass(location=cps_fc_filt, overwrite=True)

# run the function 
cps_ab = fns.calc_abundance_lines(abundance_containers, [cps_fc_filt])
cps_ab = cps_ab.drop('fc_name', axis=1)

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

fns.clear_scratch()