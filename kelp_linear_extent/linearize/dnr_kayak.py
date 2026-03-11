# Linearize DNR kelp kayak survey data

# 2026 update = complete, 2026-03-10

# Data copied from K:\kelp\bull_kelp_kayak\2024\data_processing\gdb\DNR_bull_kelp_kayak_2025.gdb on 2026-01-02

# This dataset is a little funky in that there are small 'absence' polygons at sites where there was an annual survey to confirm there was no kelp 
# Different sites surveyed each year --> if there is no absence polygon, it wasn't surveyed
# Note 1 --> check to make sure that the annual survey boundary features T2013-T2024 were successfully deleted from the kayak .gdb before running 
# Note 2 --> split feature classes does not respect overwriteOutput=True. if the script breaks in the middle, you must manually delete intermediate data.
# Yes this makes it very annoying to debug 

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

dataset_name = "WADNR_Kayak" # this will be appended to data records 
containers = os.path.join(PROJECT_ROOT, "LinearExtent.gdb", "kelp_containers_v2")
abundance_containers = os.path.join(PROJECT_ROOT, "LinearExtent.gdb\\abundance_containers")
kelp_data_path = os.path.join(PROJECT_ROOT, "kelp_data_sources\\DNR_bull_kelp_kayak_2025.gdb") 

# prep data ------------------------------------------------------------
fc = os.path.join(kelp_data_path, "bed_perimeter_surveys_2013_2025_aggregates")

print(f"Using {containers} as container features")
print(f"Dataset to be linearized: {fc}")

# split kelp beds by year
arcpy.analysis.SplitByAttributes(fc, SCRATCH_WS, ['year_'])
arcpy.env.workspace = SCRATCH_WS
split_fcs = arcpy.ListFeatureClasses("T*")
print("Kayak data split into one feature class per year:")
for f in split_fcs: print(f)
fns.reset_ws()

# append file path
split_fcs = [os.path.join(SCRATCH_WS, f"{fc}") for fc in split_fcs]

# copy kayak site boundaries to scratch (all in one feature class, no year attribute)
site_bnd_orig = f"{kelp_data_path}\\site_boundaries_2025_SPS_all"
site_bnd = os.path.join(SCRATCH_WS, "site_bnd")
arcpy.management.CopyFeatures(site_bnd_orig, site_bnd)

# spatial join kelp beds (1:m) - small absence polygons denote absence surveys
print("Joining kelp beds to boundaries to get year...")
site_bnd_join = os.path.join(SCRATCH_WS, "site_bnd_join")
arcpy.analysis.SpatialJoin(site_bnd, fc, site_bnd_join, "JOIN_ONE_TO_MANY")
print(arcpy.GetMessages())

# split by year, writing into data source gdb to avoid naming confusions since name is T* for split ops
arcpy.analysis.SplitByAttributes(site_bnd_join, kelp_data_path, ['year_'])
arcpy.env.workspace = kelp_data_path
site_bnd_split = arcpy.ListFeatureClasses("T*")
site_bnd_split = [f"{kelp_data_path}\\{fc}" for fc in site_bnd_split]
fns.reset_ws()

# delete absence polygons (where Shape_Area < VALUE)
for fc in split_fcs: 
    with arcpy.da.UpdateCursor(fc, ["SHAPE@", "Shape_Area"]) as cursor:
        for row in cursor:
            # Check if Shape_Area is less than 3.6
            if row[1] < 3.6:
                # Delete the feature
                cursor.deleteRow()
                print(f"Deleted feature with area {row[1]} in {fc}")

# compile results to list
fc_list = list(zip(split_fcs, site_bnd_split))
print("Data to be analyzed: ")
for kelp, svy in fc_list:
    print(f"Kelp data: {kelp}")
    print(f"Survey boundary: {svy}")

# calculate presence ---------------------------------------
print("Calculating presence...")
sumwithin_fcs = fns.sum_kelp_within(fc_list, containers, variable_survey_area=True)
print(f"Out fcs: {sumwithin_fcs}")

# convert to dfs
print("Converting to sdfs...")
sdf_list = fns.df_from_fc(sumwithin_fcs, dataset_name)

print("This is the structure of the sdfs:")
print(sdf_list[1].head())

# merge to one df
print("Combining to one dataframe")
presence = pd.concat(sdf_list)

# calculate abundance --------------------------------------
print("Calculating abundance...")
abundance = fns.calc_abundance(abundance_containers, split_fcs, PROJECT_ROOT)

# add the year col
abundance['year'] = abundance['fc_name'].str[-4:]
abundance = abundance.drop(columns=['fc_name'])
print("Abundance data: ")
print(abundance.head())

# combine and export -----------------------------------------
results = pd.merge(presence, abundance, how="left", on=["SITE_CODE", "year"])

# Write to csv
os.makedirs(f"{PROJECT_ROOT}\\kelp_data_linear_outputs", exist_ok=True)
out_results = os.path.join(PROJECT_ROOT, "kelp_data_linear_outputs\\dnr_kayak_synth.csv")
results.to_csv(out_results)
print(f"Saved as csv here: {out_results}")
 