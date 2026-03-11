# Summarize WADNR/Suquamish CPS UAS baseline survey data for linear extent

# Kelp bed data all copied 2025-01-30
# K:\kelp\projects\2023_Suquamish_CPS_mapping\data\uas_data\derived_results\Suquamish_UAS_survey_bed_extents.gdb

# Note: Orthomosaic_Boundaries is a multipart fc with only 1 feature
# Manually exploded to single part, spatial joined beds (1:m) to get year, split by year, renamed to Ortho2023, Ortho2024 

# set environment ----------------------------------------------

import os
import arcpy
import arcpy.management
import arcpy.analysis
import arcpy.conversion
import pandas as pd
import numpy as np
import sys
from arcgis.features import GeoAccessor, GeoSeriesAccessor

sys.path.append(os.getcwd())

# import the project function library 
import fns

arcpy.env.overwriteOutput = True

# set workspace to parent folder
fns.reset_ws()

# prep data ---------------------------------------------------

# containers
containers = "LinearExtent.gdb\\kelp_containers_v2"

print(f"Using {containers} as container features")

# set path to kelp data
kelp_data_path = "kelp_data_sources\\Suquamish_UAS_survey_bed_extents.gdb"
kelp_bed = f"{kelp_data_path}\\Suquamish_UAS_all_bed_extents"

# split feature class into one fc per year 
arcpy.analysis.SplitByAttributes(kelp_bed, "scratch.gdb", ['Year'])
arcpy.env.workspace = os.path.join(os.getcwd(), "scratch.gdb")
split_fcs = arcpy.ListFeatureClasses("T*")
print("UAS data split into one feature class per year:")
for f in split_fcs: print(f)
fns.reset_ws()

# append file path
split_fcs = [f"scratch.gdb\\{fc}" for fc in split_fcs]

# get list of survey boundaries --> see notes above for pre-processing
arcpy.env.workspace = os.path.join(os.getcwd(), kelp_data_path)
ortho_fcs = arcpy.ListFeatureClasses("Ortho2*")
print("Survey boundaries:")
for f in ortho_fcs: print(f)
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
fns.sum_kelp_within(fc_list, containers, variable_survey_area=True)

# get list of summarize within output fcs
arcpy.env.workspace = os.path.join(os.getcwd(), "scratch.gdb")
sumwithin_fcs = arcpy.ListFeatureClasses('sum*')
sumwithin_fcs = [f"scratch.gdb\\{fc}" for fc in sumwithin_fcs]
fns.reset_ws()

# convert to sdfs
sdf_list = fns.df_from_fc(sumwithin_fcs, "WADNR_Suquamish_CPS_UAS_surveys")

print("This is the structure of the sdfs:")
print(sdf_list[1].head())

# merge results to one df 
presence = pd.concat(sdf_list)
print("Presence data merged to one df")
print(presence.head())

# calculate abundance -------------------------------------------------

print("Calculating abundance...")
abundance_containers = "LinearExtent.gdb\\abundance_containers"
abundance = fns.calc_abundance(abundance_containers, split_fcs)

abundance['year'] = abundance['fc_name'].str[-4:]
abundance = abundance.drop(columns=['fc_name'])
print("Compiled abundance results:")
print(abundance.head())

# combine results ----------------------------------------------

print("Combining results to one dataframe")
results = pd.merge(presence, abundance, how='left', on=['SITE_CODE','year'])
print(results.head())

# Write to csv
out_result = "cps_uas_synth.csv"
results.to_csv(f"kelp_data_synth_results\\{out_result}")
print(f"Saved as csv here: kelp_data_synth_results\\{out_result}")

# clear workspace
fns.clear_scratch()