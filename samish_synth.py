# Summarize Samish Indian Nation's kelp polygons from assorted years of aerial surveys to linear extent

# files from K:\kelp\VScanopy\data\SJI\Samish_spatial_data_2021_delivery
# an updated 2022 dataset from K:\kelp\VScanopy\data\SJI\sji_2022_mapping_project_materials
# Created a modified survey boundary for 2016 onward based on conversations with Sophia and Todd
# 2004 and 2006 overlap, using modified ortho tiles dissolved along the centerline of overlap as that survey area
# Sophia sent 2023 data via email on 2025-05-30
# 2022 data was exported to a shapefile from sji_2022_mapping_project_materials\bed_delineation\2023_11_21_delivery_from_sophia\Data for Helen\Data for Helen\Kelp_Digitization_2006_to_2022.gdb\Samish_Digitized_Kelp_2022"
# Into the 2021 deliverable folder, renamed to match the other .shps


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

# Remove skagit for now, possibly handle as presence only if mysteries are solved
kelp_shps.remove('SkagitCO_2019_Kelp.shp')
kelp_shps.remove('Samish_Digitized_Kelp_Skagit_CO_SepOct2017.shp')

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

# 2004, 2006, then 2016 onward have different boundaries
aoi2004 = f"{kelp_data_path}\\SamishBoundariesGEM.gdb\\image_index_2004_NoOverlaps"
aoi2006 = f"{kelp_data_path}\\SamishBoundariesGEM.gdb\\image_index_2006_NoOverlaps"
aoi2016 = f"{kelp_data_path}\\SamishBoundariesGEM.gdb\\boundary_2016onward"

# create paired list of survey boundaries and kelp data
# the 2004 and 2006 results are merged into 1 kelp fc 
fc_list = [
    (kelp_fcs[0], aoi2004),
    (kelp_fcs[1], aoi2006)
] + [
    (kelp_fc, aoi2016) for kelp_fc in kelp_fcs[2:]
]

print("Paired list:")
print(fc_list)

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

# process skagitco data ------------------------------------------------
# this is currently being excluded because year and survey boundary are actually unknown
# print("Processing Skagit data as presence only...")
# no survey footprint exists so we treat as presence only

# def presence_only_calc(input_shapefile, output_name, kelp_data_path, containers, abundance_containers, scratch_gdb="scratch.gdb")

 #   # convert shapefile to feature class
 #   fc_path = f"{scratch_gdb}\\{output_name}"
 #   arcpy.conversion.FeatureClassToFeatureClass(input_shapefile, scratch_gdb, output_name)
 #   kelp_fc = [fc_path]

    # get presence
 #   fns.sum_kelp_within(kelp_fc, containers)
 #   sum_fc = f"{scratch_gdb}\\sumwithin{output_name}"
 #   presence_list = fns.df_from_fc([sum_fc], "Samish_AerialSurveys")
 #   presence_df = presence_list[0]
 #   print(f"{output_name} presence data:")
 #   print(presence_df.head())

    # Filter out zero-presence rows
 #   presence_df = presence_df[presence_df['presence'] != 0]

    # Get abundance
 #   abundance_df = fns.calc_abundance(abundance_containers, kelp_fc)
 #   abundance_df['year'] = abundance_df['fc_name'].str[-4:]
 #   abundance_df = abundance_df.drop(columns=['fc_name'])
 #   print(f"Reformatted {output_name} abundance table:")
 #   print(abundance_df.head())

    # Merge and return
 #  merged_df = pd.merge(presence_df, abundance_df, how="left", on=["SITE_CODE", "year"])
 #   return merged_df

#skagit2019shp = f"{kelp_data_path}\\SkagitCO_2019_Kelp.shp"
#ska_results_2019 = presence_only_calc(skagit2019shp, "ska2019", kelp_data_path, containers, abundance_containers)

#skagit2017shp = f"{kelp_data_path}\\Samish_Digitized_Kelp_Skagit_CO_SepOct2017.shp"
#ska_results_2017 = presence_only_calc(skagit2017shp, "ska2019", kelp_data_path, containers, abundance_containers)

# compile and export --------------------------------------------------
results = pd.merge(presence, abundance, how="left", on=["SITE_CODE", "year"])

# add skagit results
#results = pd.concat([results, ska_results_2017, ska_results_2019])

print("Results table:")
print(results.head())


results.to_csv("kelp_data_synth_results\\sji_synth.csv")
print("Saved as csv here: kelp_data_synth_results\\sji_synth.csv")

# Clear scratch gdb to keep project size down
fns.clear_scratch()

