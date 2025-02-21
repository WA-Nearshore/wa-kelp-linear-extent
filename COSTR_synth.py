# Summarize COSTR kelp data to linear extent

# COSTR data 
# download date: 2024-06-17
# link: https://fortress.wa.gov/dnr/adminsa/gisdata/datadownload/kelp_canopy_strait_coast.zip

import arcpy
import os
import sys
import arcpy.conversion
import pandas as pd
import numpy as np
from arcgis.features import GeoAccessor, GeoSeriesAccessor

sys.path.append(os.getcwd())

# import the project function library 
import fns

arcpy.env.overwriteOutput = True

fns.reset_ws()

#### Load Data ####

# Containers
containers = "LinearExtent.gdb\\kelp_containers_v2"

print("Using " + containers + " as container features")

# Set workspace to gdb with kelp data sources
kelp_data_path = "kelp_data_sources\\kelp_canopy_strait_coast\\kelp_canopy_coast_strait.gdb"
arcpy.env.workspace = kelp_data_path

# COSTR kelp data
costr_fcs = arcpy.ListFeatureClasses('kelp1*')  # add all fcs from 1900s
costr_fcs.extend(arcpy.ListFeatureClasses('kelp2*')) # add all fcs from 2000s

print("Datasets to be summarized: ")
print(costr_fcs)

# append path to fcs in list 
costr_fcs = [kelp_data_path + "\\" + fc for fc in costr_fcs]

fns.reset_ws()

#### Clip Containers to COSTR survey area ####

costr_bnd = kelp_data_path + "\\map_index"
arcpy.analysis.Clip(containers, costr_bnd, "scratch.gdb\\containers_COSTR")

containers = "scratch.gdb\\containers_COSTR"

print("Container fc clipped to " + costr_bnd.rsplit('\\', 1)[-1])

#### Summarize Within ####

# Run summarize within each year of data with containers
fns.sum_kelp_within(costr_fcs)

#### Save results to tables ####

# get list of summarize within output fcs
arcpy.env.workspace = os.path.join(os.getcwd(), "scratch.gdb")
sumwithin_fcs = arcpy.ListFeatureClasses('sum*')

sdf_list = fns.df_from_fc(sumwithin_fcs)

# apply function to list of summarize within outputs
sdf_list = df_from_fc(sumwithin_fcs)

print("This is the structure of the sdfs:")
print(sdf_list[1].head())

# Merge to one df
all_data = pd.concat(sdf_list)
all_data

print("All years of data have been merged to one df")
print(" ")
print(all_data.head())

# Write to csv
all_data.to_csv("kelp_data_synth_results\\COSTR_synth_test.csv")
print("Saved as csv here: kelp_data_synth_results\\COSTR_synth_test.csv")

# Clear scratch gdb to keep project size down
fns.clear_scratch()
