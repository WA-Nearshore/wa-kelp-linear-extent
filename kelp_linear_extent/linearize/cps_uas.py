# Summarize WADNR/Suquamish CPS UAS baseline survey data for linear extent

# 2026 updates = complete 2026-03-13

# Kelp bed data all copied 2025-01-30
# K:\kelp\projects\2023_Suquamish_CPS_mapping\data\uas_data\derived_results\Suquamish_UAS_survey_bed_extents.gdb

# Note: Orthomosaic_Boundaries is a multipart fc with only 1 feature
# Manually exploded to single part, spatial joined beds (1:m) to get year, split by year, renamed to Ortho2023, Ortho2024 

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

# USER INPUT -----------------------------------------------------------

dataset_name = "WADNR_Suquamish_CPS_UAS_surveys" # this will be appended to data records 
containers = os.path.join(PROJECT_ROOT, "LinearExtent.gdb", "kelp_containers_v2")
cov_cat_containers = os.path.join(PROJECT_ROOT, "LinearExtent.gdb\\abundance_containers")
kelp_data_path = os.path.join(PROJECT_ROOT, "kelp_data_sources\\Suquamish_UAS_survey_bed_extents.gdb")

# prep data ---------------------------------------------------

print(f"Using {containers} as container features")

# set path to kelp data
kelp_bed = f"{kelp_data_path}\\Suquamish_UAS_all_bed_extents"

# split feature class into one fc per year 
arcpy.analysis.SplitByAttributes(kelp_bed, SCRATCH_WS, ['Year'])
arcpy.env.workspace = SCRATCH_WS
split_fcs = arcpy.ListFeatureClasses("T*")
print("UAS data split into one feature class per year:")
for f in split_fcs: 
    print(f)
fns.reset_ws()

# append file path
split_fcs = [f"{SCRATCH_WS}\\{fc}" for fc in split_fcs]

# get list of survey boundaries --> see notes above for pre-processing
arcpy.env.workspace = kelp_data_path
ortho_fcs = arcpy.ListFeatureClasses("Ortho2*")
print("Survey boundaries:")
for f in ortho_fcs: 
    print(f)
fns.reset_ws()

# append file path
ortho_fcs = [f"{kelp_data_path}\\{fc}" for fc in ortho_fcs]

# create paired list of kelp beds, survey areas
fc_list = [(split_fcs[0], ortho_fcs[0]), (split_fcs[1], ortho_fcs[1])]
print("Data to be analyzed: ")
for kelp, svy in fc_list: 
    print(f"Kelp data: {kelp}")
    print(f"Survey boundary: {svy}")

# calculate presence --------------------------------------------------

print("Calculating presence...")
sumwithin_fcs = fns.sum_kelp_within(fc_list, containers, variable_survey_area=True)

# convert to sdfs
sdf_list = fns.df_from_fc(sumwithin_fcs, dataset_name)

print("This is the structure of the sdfs:")
print(sdf_list[1].head())

# merge results to one df 
presence = pd.concat(sdf_list)
print("Presence data merged to one df")
print(presence.head())

# calculate coverage category -------------------------------------------------

print("Calculating coverage category...")
cov_cat = fns.calc_cov_cat(cov_cat_containers, split_fcs)

cov_cat['year'] = cov_cat['fc_name'].str[-4:]
cov_cat = cov_cat.drop(columns=['fc_name'])
print("Compiled coverage category results:")
print(cov_cat.head())

# combine results ----------------------------------------------

print("Combining results to one dataframe")
results = pd.merge(presence, cov_cat, how='left', on=['SITE_CODE','year'])
print(results.head())

# Write to csv
os.makedirs(f"{PROJECT_ROOT}\\kelp_data_linear_outputs", exist_ok=True)
out_results = os.path.join(PROJECT_ROOT, f"kelp_data_linear_outputs\\{dataset_name}_result.csv")
results.to_csv(out_results)
print(f"Saved as csv here: {out_results}")

# clear workspace
fns.clear_scratch()