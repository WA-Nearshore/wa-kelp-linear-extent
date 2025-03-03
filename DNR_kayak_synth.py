# Summarize kayak data to kelp linear extent

# Data copied from K:\kelp\bull_kelp_kayak\2024\data_processing\gdb\DNR_bull_kelp_kayak_2024.gdb on 2025-01-14...

# This dataset is a little funky in that there are small 'absence' polygons at sites where there was an annual survey to confirm there was no kelp 
# Different sites surveyed each year --> if there is no absence polygon, it wasn't surveyed

# set environment -------------------------------------------------------
import arcpy
import arcpy.analysis
import arcpy.conversion
import pandas as pd
import numpy as np
from arcgis.features import GeoAccessor, GeoSeriesAccessor
import os
import sys

sys.path.append(os.getcwd())

# import the project function library 
import fns

arcpy.env.overwriteOutput = True

fns.reset_ws()

# prep data ------------------------------------------------------------

# containers
containers = "LinearExtent.gdb\\kelp_containers_v2"
print(f"Using {containers} as container features")

# kayak aggregate annual polygons (all in one feature class)
kelp_data_path = "kelp_data_sources\\DNR_bull_kelp_kayak_2024.gdb" 
fc = f"{kelp_data_path}\\bed_perimeter_surveys_2013_2024_aggregates"

print(f"Dataset to be summarized: {fc}")

# split kelp beds by year
arcpy.analysis.SplitByAttributes(fc, "scratch.gdb", ['year_'])
arcpy.env.workspace = os.path.join(os.getcwd(), "scratch.gdb")
split_fcs = arcpy.ListFeatureClasses("T*")
print("Kayak data split into one feature class per year:")
for f in split_fcs: print(f)
fns.reset_ws()

# append file path
split_fcs = [f"scratch.gdb\\{fc}" for fc in split_fcs]

# copy kayak site boundaries to scratch (all in one feature class, no year attribute)
site_bnd_orig = f"{kelp_data_path}\\site_boundaries_2024_SPS_all"
site_bnd = "scratch.gdb\\site_bnd"
arcpy.management.CopyFeatures(site_bnd_orig, site_bnd)

# spatial join kelp beds (1:m) - small absence polygons denote absence surveys
print("Joining kelp beds to boundaries to get year...")
site_bnd_join = "scratch.gdb\\site_bnd_join"
arcpy.analysis.SpatialJoin(site_bnd, fc, site_bnd_join, "JOIN_ONE_TO_MANY")
print(arcpy.GetMessages())

# split by year, writing into data source gdb to avoid naming confusions since default name is T* for split ops
arcpy.analysis.SplitByAttributes(site_bnd_join, kelp_data_path, ['year_'])
arcpy.env.workspace = os.path.join(os.getcwd(), kelp_data_path)
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
fns.sum_kelp_within(fc_list, containers, variable_survey_area=True)

arcpy.env.workspace = os.path.join(os.getcwd(), "scratch.gdb")
sumwithin_fcs = arcpy.ListFeatureClasses("sum*")
sumwithin_fcs = [f"scratch.gdb\\{fc}" for fc in sumwithin_fcs]
print("Fcs to convert to tables:")
print(sumwithin_fcs)
fns.reset_ws()

sdf_list = fns.df_from_fc(sumwithin_fcs, "WADNR_Kayak")

print("This is the structure of the sdfs:")
print(sdf_list[1].head())

# merge to one df
print("Combining to one dataframe")
presence = pd.concat(sdf_list)

# calculate abundance --------------------------------------
print("Calculating abundance...")
abundance_containers = "LinearExtent.gdb\\abundance_containers"
abundance = fns.calc_abundance(abundance_containers, split_fcs)

# add the year col
abundance['year'] = abundance['fc_name'].str[-4:]
abundance = abundance.drop(columns=['fc_name'])
print("Abundance data: ")
print(abundance.head())

# combine and export -----------------------------------------
results = pd.merge(presence, abundance, how="left", on=["SITE_CODE", "year"])

# Write to csv
out_results = "kelp_data_synth_results\\dnr_kayak_synth.csv"
results.to_csv(out_results)
print(f"Saved as csv here: {out_results}")

# clear scratch
fns.clear_scratch()

# clear the split survey boundaries --> this isn't working for some reason
for fc in site_bnd_split:
    print(f"Deleting {fc}...")
    arcpy.management.DeleteFeatures(fc)