# MRC Kayak data synthesis for linear extent

# 2026 update = complete, 2026-03-10

# Copy of MRC kayak data was shared with DNR for Indicator updates - "AllYearsAllSurveys_DNRMaster_2025"
# Treating data as presence only, because survey areas shift between years and we do not have each year's site boundaries
# and frequently bed extents go beyond the site boundaries anyways

# Just noting that there are multiple beds per year in this dataset which is not currently handled with this logic
# the years are summed together in sumwithin --> if ANY bed from a year is in a container, it is 'present'
# Note they changed the name of the year column in 2025 from 'Year' to 'Survey_Year'

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

# USER INPUT ----------------------------------------------------
dataset_name = "MRC_Kayak"
containers = os.path.join(PROJECT_ROOT, "LinearExtent.gdb\\kelp_containers_v2")
abundance_containers = os.path.join(PROJECT_ROOT, "LinearExtent.gdb\\abundance_containers")
kelp_data_path = os.path.join(
    PROJECT_ROOT,
    "kelp_data_sources\\mrc_kayak_data\\AllYearsAllSurveys_DNRMaster_2025.gdb\AllYearsAllSurveys_Master",
)

# prep data -----------------------------------------------------

print(f"Using {containers} as container features")
print(f"Dataset to be summarized: {kelp_data_path}")

# not clipping containers, will only include results where presence = 1

# project to state plane south NAD83
kelp_bed_fc = os.path.join(SCRATCH_WS, "AllYearsAllSurveys")

print("Projecting dataset to WA State Plane South...")
arcpy.management.Project(
    in_dataset=kelp_data_path,
    out_dataset=kelp_bed_fc,
    out_coor_system='PROJCS["NAD_1983_HARN_StatePlane_Washington_South_FIPS_4602_Feet",GEOGCS["GCS_North_American_1983_HARN",DATUM["D_North_American_1983_HARN",SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic"],PARAMETER["False_Easting",1640416.666666667],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",-120.5],PARAMETER["Standard_Parallel_1",45.83333333333334],PARAMETER["Standard_Parallel_2",47.33333333333334],PARAMETER["Latitude_Of_Origin",45.33333333333334],UNIT["Foot_US",0.3048006096012192]]',
    transform_method=None,
    in_coor_system='PROJCS["NAD_1983_HARN_StatePlane_Washington_North_FIPS_4601_Feet",GEOGCS["GCS_North_American_1983_HARN",DATUM["D_North_American_1983_HARN",SPHEROID["GRS_1980",6378137.0,298.257222101]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Lambert_Conformal_Conic"],PARAMETER["False_Easting",1640416.666666667],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",-120.8333333333333],PARAMETER["Standard_Parallel_1",47.5],PARAMETER["Standard_Parallel_2",48.73333333333333],PARAMETER["Latitude_Of_Origin",47.0],UNIT["Foot_US",0.3048006096012192]]',
    preserve_shape="NO_PRESERVE_SHAPE",
    max_deviation=None,
    vertical="NO_VERTICAL",
)
print(arcpy.GetMessages())

# split into one fc per year
arcpy.analysis.SplitByAttributes(kelp_bed_fc, SCRATCH_WS, ["Survey_Year"])
arcpy.env.workspace = SCRATCH_WS
split_fcs = arcpy.ListFeatureClasses("T*")
print("MRC Kayak data split into one feature class per year:")
for fc in split_fcs:
    print(fc)

# append parent filepath
split_fcs = [f"{SCRATCH_WS}\\{fc}" for fc in split_fcs]
fns.reset_ws()

# calculate presence ---------------------------------------------------
print("Calculating presence....")
sumwithin_fcs = fns.sum_kelp_within(split_fcs, containers)
print(f"Out fcs: {sumwithin_fcs}")

fns.reset_ws()

# convert to table
print("Converting results to dataframes...")
sdf_list = fns.df_from_fc(sumwithin_fcs, dataset_name)

# compile to one df
presence = pd.concat(sdf_list)
print(f"Number of rows: {len(presence)}")
# drop any rows where presence = 0 because we are treating this as presence-only
print("Dropping rows where presence = 0... treating data as presence only")
presence = presence[presence["presence"] != 0]
print(f"Number of rows: {len(presence)}")

# calculate abundance --------------------------------------------------
print("Calculating abundance...")
abundance = fns.calc_abundance(abundance_containers, split_fcs, PROJECT_ROOT)

# add year col
abundance["year"] = abundance["fc_name"].str[-4:]
abundance = abundance.drop(columns=["fc_name"])
print("Abundance data: ")
print(abundance.head())

# compile and export --------------------------------------------------
results = pd.merge(presence, abundance, how="left", on=["SITE_CODE", "year"])
print("Compiled results:")
print(results.head())

# write to csv
os.makedirs(f"{PROJECT_ROOT}\\kelp_data_linear_outputs", exist_ok=True)
out_results = os.path.join(PROJECT_ROOT, f"kelp_data_linear_outputs\\{dataset_name}_result.csv")
results.to_csv(out_results)
print(f"Saved as csv here: {out_results}")

# clear workspace
fns.clear_scratch()
