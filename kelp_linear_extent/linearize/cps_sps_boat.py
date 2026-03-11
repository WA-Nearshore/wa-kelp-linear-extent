# Reformat the CPS & SPS linear extent field survey data

# 2026 code improvements = complete 2026-03-11. no change to data or analysis.

# set environment -------------------------------------------------------

import sys
import os
import arcpy
import pandas as pd
from arcgis.features import GeoAccessor, GeoSeriesAccessor # these are used to create sedfs

# project root is the folder within which the entire kelp_linear_extent module is located (2 levels up from this file)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
print("Project working directory:")
print(PROJECT_ROOT)
sys.path.append(PROJECT_ROOT) # this lets the project function library be found as a module

import kelp_linear_extent.fns as fns # project function library

arcpy.env.overwriteOutput = True # overwrite outputs 

# set workspace to parent folder
fns.reset_ws()

# set up scratch workspace
SCRATCH_WS = fns.config_scratch()

# USER INPUTS ----------------------------------------------------------

dataset_name_sps = "WADNR_sps_boat_survey"
dataset_name_cps = "WADNR_cps_boat_survey"
abundance_containers = os.path.join(PROJECT_ROOT, "LinearExtent.gdb\\abundance_containers")

# cps data from K:\Kelp\2019_cps_field\spatial_data\bull_kelp_cps_2019.gdb
cps_orig= os.path.join(PROJECT_ROOT,"kelp_data_sources\\bull_kelp_cps_2019.gdb\\bull_kelp_2019")
# consider switching this to fringe at some point 

# sps data from K:\kelp\projects\historical_comparison_sps_2018\spatial_data\SS_kelp.gdb
sps_orig = os.path.join(PROJECT_ROOT, "kelp_data_sources\\SS_kelp.gdb\\dnr2017_ss")

# prep data ------------------------------------------------------------

print(f"Running analsysis on {cps_orig} and {sps_orig}...")


print(f"Copying features to {SCRATCH_WS}")
# copy datasets to scratch
cps_fc = os.path.join(SCRATCH_WS, "cps")
sps_fc = os.path.join(SCRATCH_WS,"sps")

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
sps_pres['source'] = dataset_name_sps
print("Presence results: ")
print(sps_pres.head())

# abundance
# create a filtered version of the dataset with only line segments w/ kelp present 
print("Filtering dataset to presence features only...")
sps_df_filt = sps_df[sps_df['kelp']== 1]
sps_fc_filt = os.path.join(SCRATCH_WS, "sps_kelp_only")
sps_df_filt.spatial.to_featureclass(location=sps_fc_filt, overwrite=True)

# run the function 
print("Calculating abundance...")
sps_ab = fns.calc_abundance_lines(abundance_containers, [sps_fc_filt], PROJECT_ROOT)
sps_ab = sps_ab.drop('fc_name', axis=1)
print("Abundance results: ")
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
cps_pres['source'] = dataset_name_cps

# abundance
# create a filtered version of the dataset with only line segments w/ kelp present 
cps_df_filt = cps_df[cps_df['kelp']== 1]
cps_fc_filt = os.path.join(SCRATCH_WS, "cps_kelp_only")
cps_df_filt.spatial.to_featureclass(location=cps_fc_filt, overwrite=True)

# run the function 
cps_ab = fns.calc_abundance_lines(abundance_containers, [cps_fc_filt], PROJECT_ROOT)
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
os.makedirs(f"{PROJECT_ROOT}\\kelp_data_linear_outputs", exist_ok=True)
sps_out_results = os.path.join(PROJECT_ROOT, f"kelp_data_linear_outputs\\{dataset_name_sps}_result.csv")
sps_result.to_csv(sps_out_results)

cps_out_results = os.path.join(PROJECT_ROOT, f"kelp_data_linear_outputs\\{dataset_name_cps}_result.csv")
cps_result.to_csv(cps_out_results)

print("Saved as csvs here:")
print(sps_out_results)
print(cps_out_results)
 
fns.clear_scratch()