# Summarize Samish Indian Nation's kelp polygons from assorted years of aerial surveys to linear extent

# 2026 update = currently failing 100014 at sum within kelp_2022. No Idea what gives. FIX SPATIAL REFERNECES 

# files from ...kelp\VScanopy\data\SJI\Samish_spatial_data_2021_delivery
# an updated 2022 dataset from ...kelp\VScanopy\data\SJI\sji_2022_mapping_project_materials
# Created a modified survey boundary for 2016 onward based on conversations with Sophia and Todd
# 2004 and 2006 overlap, using modified ortho tiles dissolved along the centerline of overlap as that survey area
# Sophia sent 2023 data via email on 2025-05-30
# 2022 data was exported to a shapefile from sji_2022_mapping_project_materials\bed_delineation\2023_11_21_delivery_from_sophia\Data for Helen\Data for Helen\Kelp_Digitization_2006_to_2022.gdb\Samish_Digitized_Kelp_2022"
# Into the 2021 deliverable folder, renamed to match the other .shps
# SanJuanCO_2019 had a bad field; manually removed 
# 2024 data copied down from network and renamed to match other .shps Apr 2026

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

import kelp_linear_extent_code.fns as fns # project function library

arcpy.env.overwriteOutput = True # overwrite outputs 

# set workspace to parent folder
fns.reset_ws()

# set up scratch workspace
SCRATCH_WS = fns.config_scratch()

# USER INPUT ----------------------------------------------------------

dataset_name = "Samish_AerialSurveys"
containers = os.path.join(PROJECT_ROOT, "LinearExtent.gdb\\kelp_containers_v2")
cov_cat_containers = os.path.join(PROJECT_ROOT, "LinearExtent.gdb\\abundance_containers")
kelp_data_path = os.path.join(PROJECT_ROOT,"kelp_data_sources\\Samish_spatial_data_2021_delivery")

# prep data ------------------------------------------------------------

# containers
print(f"Using {containers} as container features")

# San Juans polygons
kelp_shps = []
for file in os.listdir(kelp_data_path):
    if file.endswith(".shp"):
        kelp_shps.append(file)
print(kelp_shps)

# append parent file path
kelp_shps = [f"{kelp_data_path}\\{shp}" for shp in kelp_shps]
print("Datasets to be summarized:")
for shp in kelp_shps:
    print(shp)

# convert shapefiles to feature classes in scratch.gdb
print("Converting to feature classes...")
kelp_fcs = []

# get correct spatial reference object
sr = arcpy.Describe(containers).spatialReference

for shp in kelp_shps:
    out_fc = f"kelp_{shp[-13:-9]}"
    arcpy.management.Project(shp, f"in_memory/{out_fc}",sr) # project to match containers
    arcpy.conversion.FeatureClassToFeatureClass(f"in_memory/{out_fc}", SCRATCH_WS, out_fc)
    arcpy.management.Delete(f"in_memory/{out_fc}")
    kelp_fcs.append(f"{SCRATCH_WS}\\{out_fc}")
    print(f"{shp} converted to fc:{out_fc}")

# make a copy of kelp_2006 and call it 2004
print("Creating the 2004 fc...")
arcpy.conversion.FeatureClassToFeatureClass(
    kelp_fcs[0], SCRATCH_WS, "kelp_2004"
)
# add to list
kelp_fcs.append(os.path.join(SCRATCH_WS,"kelp_2004"))
# will be handled separately in the presence function
print("Added to list.")

# ensure that list is earliest year first
kelp_fcs.sort(key=lambda x: int(x.split("_")[1]))
print("Sorted list:")
print(kelp_fcs)

# 2004, 2006, then 2016 onward have different boundaries - copy over to scratch gdb
print("Prepping survey boundary layers...")
aoi2004_fc = f"{kelp_data_path}\\SamishBoundariesGEM.gdb\\image_index_2004_NoOverlaps_sp"
arcpy.conversion.FeatureClassToFeatureClass(aoi2004_fc, SCRATCH_WS, "aoi2004")
aoi2004 = os.path.join(SCRATCH_WS, "aoi2004")

aoi2006_fc = f"{kelp_data_path}\\SamishBoundariesGEM.gdb\\image_index_2006_NoOverlaps_sp"
arcpy.conversion.FeatureClassToFeatureClass(aoi2006_fc, SCRATCH_WS, "aoi2006")
aoi2006 = os.path.join(SCRATCH_WS, "aoi2006")

aoi2016_fc = f"{kelp_data_path}\\SamishBoundariesGEM.gdb\\boundary_2016onward_sp"
arcpy.conversion.FeatureClassToFeatureClass(aoi2016_fc, SCRATCH_WS, "aoi2016")
aoi2016 = os.path.join(SCRATCH_WS, "aoi2016")

# create paired list of survey boundaries and kelp data
# the 2004 and 2006 results are merged into 1 kelp fc
fc_list = [(kelp_fcs[0], aoi2004), (kelp_fcs[1], aoi2006)] + [
    (kelp_fc, aoi2016) for kelp_fc in kelp_fcs[2:]
]

print("Paired list:")
print(fc_list)

# calculate presence -------------------------------------------------------
print("Calculating presence...")
fns.sum_kelp_within(fc_list, containers, variable_survey_area=True)

# get list of summarize within output fcs
arcpy.env.workspace = os.path.join(SCRATCH_WS)
sumwithin_fcs = arcpy.ListFeatureClasses("sum*")

sdf_list = fns.df_from_fc(sumwithin_fcs, dataset_name)

fns.reset_ws()

print("This is the structure of the presence sdfs:")
print(sdf_list[1].head())

# merge to one df
presence = pd.concat(sdf_list)

print("All years of data have been merged to one df")
print(" ")
print(presence.head())

# calculate coverage category ------------------------------------------------------
print("Calculating coverage category...")
cov_cat = fns.calc_cov_cat(cov_cat_containers, kelp_fcs)

# add the year col
cov_cat["year"] = cov_cat["fc_name"].str[-4:]
cov_cat = cov_cat.drop(columns=["fc_name"])
print("Reformatted cov cat table:")
print(cov_cat.head())

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

# skagit2019shp = f"{kelp_data_path}\\SkagitCO_2019_Kelp.shp"
# ska_results_2019 = presence_only_calc(skagit2019shp, "ska2019", kelp_data_path, containers, abundance_containers)

# skagit2017shp = f"{kelp_data_path}\\Samish_Digitized_Kelp_Skagit_CO_SepOct2017.shp"
# ska_results_2017 = presence_only_calc(skagit2017shp, "ska2019", kelp_data_path, containers, abundance_containers)

# compile and export --------------------------------------------------
results = pd.merge(presence, cov_cat, how="left", on=["SITE_CODE", "year"])

# add skagit results
# results = pd.concat([results, ska_results_2017, ska_results_2019])

print("Results table:")
print(results.head())

# Write to csv
os.makedirs(f"{PROJECT_ROOT}\\kelp_data_linear_outputs", exist_ok=True)
out_results = os.path.join(PROJECT_ROOT, f"kelp_data_linear_outputs\\{dataset_name}_result.csv")
results.to_csv(out_results)
print(f"Saved as csv here: {out_results}")

# Clear scratch gdb to keep project size down
fns.clear_scratch()
