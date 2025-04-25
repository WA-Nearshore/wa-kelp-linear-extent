# Summarize Samish Indian Nation's kelp polygons from assorted years of aerial surveys to linear extent

# files from K:\kelp\VScanopy\data\SJI\Samish_spatial_data_2021_delivery
############# 2004/2006 logic isn't working. need to pre-process 
# There are some other issues with this Dataset in my QAQC notes that need to be addressed with Danielle and/or someone from Samish

# set environment ---------------------------------------------------------
import arcpy
import arcpy.analysis
import arcpy.conversion
import pandas as pd
import numpy as np
import os
import sys
from arcgis.features import GeoAccessor, GeoSeriesAccessor

sys.path.append(os.getcwd())

# import the project function library 
import fns

arcpy.env.overwriteOutput = True

fns.reset_ws()

# prep data ------------------------------------------------------------

# containers
containers = "LinearExtent.gdb\\kelp_containers_v2"
print(f"Using {containers} as container features")

# San Juans polygons
kelp_data_path = "kelp_data_sources\\Samish_spatial_data_2021_delivery"
kelp_shps = []
for file in os.listdir(kelp_data_path):
    if file.endswith(".shp"):
        kelp_shps.append(file)
print(kelp_shps)

# For now, drop the skagit data since I don't have the footprint --> could handle as presence only
kelp_shps.remove('SkagitCO_2019_Kelp.shp')

# append parent file path
kelp_shps = [f"{kelp_data_path}\\{shp}" for shp in kelp_shps]
print("Datasets to be summarized:")
for shp in kelp_shps:
    print(shp)

# convert shapefiles to feature classes in scratch.gdb
print("Converting to feature classes...")
kelp_fcs = []
for shp in kelp_shps:
    out_fc = f"kelp_{shp[-13:-9]}"
    arcpy.conversion.FeatureClassToFeatureClass(shp, "scratch.gdb", out_fc)
    kelp_fcs.append(f"scratch.gdb\\{out_fc}")
    print(f"{shp} converted to fc:{out_fc}")

# make a copy of kelp_2006 and call it 2004
print("Creating the 2004 fc...")
arcpy.conversion.FeatureClassToFeatureClass("scratch.gdb\\kelp_2006", "scratch.gdb", "kelp_2004")
# add to list
kelp_fcs.append("scratch.gdb\\kelp_2004")
# will be handled separately in the presence function

# ensure that list is earliest year first
kelp_fcs.sort(key=lambda x: int(x.split('_')[1]))
print("Sorted list:")
print(kelp_fcs)

# each year has its own boundary
aoi2004 = f"{kelp_data_path}\\SanJuan_Footprint_2004\\2004_2006_kelp_image_index.shp"
aoi2006 = f"{kelp_data_path}\\SanJuan_Footprint_2006\\2004_2006_kelp_image_index.shp"
aoi2016 = f"{kelp_data_path}\\Boundary_2016\Boundary_2016.shp"
aoi2019 = f"{kelp_data_path}\\Boundary_2019\Boundary_2019.shp"

# create paired list of survey boundaries and kelp data
# the 2004 and 2006 results are merged into 1 kelp fc 
fc_list = [(kelp_fcs[0], aoi2004), (kelp_fcs[1], aoi2006), (kelp_fcs[2], aoi2016), (kelp_fcs[3], aoi2019)]

# calculate presence -------------------------------------------------------
print("Calculating presence...")
fns.sum_kelp_within(fc_list, containers, variable_survey_area=True)

# get list of summarize within output fcs
arcpy.env.workspace = os.path.join(os.getcwd(), "scratch.gdb")
sumwithin_fcs = arcpy.ListFeatureClasses('sum*')

sdf_list = fns.df_from_fc(sumwithin_fcs, "Samish_AerialSurveys")

fns.reset_ws()

print("This is the structure of the presence sdfs:")
print(sdf_list[1].head())

# merge to one df
presence = pd.concat(sdf_list)

print("All years of data have been merged to one df")
print(" ")
print(presence.head())

# calculate abundance ------------------------------------------------------
print("Calculating abundance...")
abundance_containers = "LinearExtent.gdb\\abundance_containers"
abundance = fns.calc_abundance(abundance_containers, kelp_fcs)

# add the year col
abundance['year'] = abundance['fc_name'].str[-4:]
abundance = abundance.drop(columns=['fc_name'])
print("Reformatted abundance table:")
print(abundance.head())

# process skagitco 2019 data ------------------------------------------------
print("Processing Skagit 2019 data as presence only...")
# no survey footprint exists so we treat as presence only
skagit2019shp = f"{kelp_data_path}\\SkagitCO_2019_Kelp.shp"

# convert to fc and add to a 1-element list
arcpy.conversion.FeatureClassToFeatureClass(skagit2019shp, "scratch.gdb", "ska2019")
skagit2019fc = ["scratch.gdb\\ska2019"]

# get presence
fns.sum_kelp_within(skagit2019fc, containers)
sumska2019 = ["scratch.gdb\\sumwithinska2019"]
ska_presence_list = fns.df_from_fc(sumska2019, "Samish_AerialSurveys")

# extract the dataframe from the list format
ska_presence = ska_presence_list[0]
print("Skagit2019 presence data:")
print(ska_presence.head())

# drop rows where presence = 0 
ska_presence = ska_presence[ska_presence['presence'] != 0]

# get abundance
ska_abundance = fns.calc_abundance(abundance_containers, skagit2019fc)

# add the year col
ska_abundance['year'] = ska_abundance['fc_name'].str[-4:]
ska_abundance = ska_abundance.drop(columns=['fc_name'])
print("Reformatted skagit abundance table:")
print(ska_abundance.head())

ska_results = pd.merge(ska_presence, ska_abundance, how="left", on=["SITE_CODE", "year"])

# compile and export --------------------------------------------------
results = pd.merge(presence, abundance, how="left", on=["SITE_CODE", "year"])

# add skagit results
results = pd.concat([results, ska_results])

print("Results table:")
print(results.head())

# if there are sites with 2004 and 2006, we need to drop the 2004 results. 
# The 2004 & 2006 footprints overlap, but the kelp fc is a composite of both of these. Assume the more recent year was used but who knows, actually.
# This doesn't work
site_codes_with_2006 = results[results['year'] == 2006]['SITE_CODE'].unique()
results = results[~((results['year'] == 2004) & (results['SITE_CODE'].isin(site_codes_with_2006)))]

results.to_csv("kelp_data_synth_results\\sji_synth.csv")
print("Saved as csv here: kelp_data_synth_results\\sji_synth.csv")

# Clear scratch gdb to keep project size down
fns.clear_scratch()

