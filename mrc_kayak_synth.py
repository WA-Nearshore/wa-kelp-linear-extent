# MRC Kayak data synthesis for linear extent

# Data was sent to me via fwd email from Jeff W on 2024-11-22 and includes a .gdb with data through 2023 "AllYearsAllSurveys_DNRMaster"
# Treating data as presence only, because survey areas shift between years and we do not have each year's site boundaries
# and frequently bed extents go beyond the site boundaries anyways

# Just noting that there are multiple beds per year in this dataset which is not currently handled with this logic
# the years are summed together in sumwithin --> if ANY bed from a year is in a container, it is present

# set environment -------------------------------------------------
import arcpy
import arcpy.analysis
import arcpy.conversion
import arcpy.management
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

# prep data -----------------------------------------------------

# containers
containers = "LinearExtent.gdb\\kelp_containers_v2"
print(f"Using {containers} as container features")
# not clipping containers, will only include results where presence = 1

# kayak annual polygons (all in one feature class)

# project to state plane south NAD83
orig_fc = os.path.join(
    os.getcwd(),
    "kelp_data_sources\\mrc_kayak_data\\AllYearsAllSurveys_DNRMaster.gdb\AllYearsAllSurveys_Master",
)

print(f"Dataset to be summarized: {orig_fc}")

kelp_bed_fc = os.path.join(os.getcwd(), "scratch.gdb\\AllYearsAllSurveys")

print("Projecting dataset to WA State Plane South...")
arcpy.management.Project(
    in_dataset=orig_fc,
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
arcpy.analysis.SplitByAttributes(kelp_bed_fc, "scratch.gdb", ["Year"])
arcpy.env.workspace = os.path.join(os.getcwd(), "scratch.gdb")
split_fcs = arcpy.ListFeatureClasses("T*")
print("MRC Kayak data split into one feature class per year:")
for fc in split_fcs:
    print(fc)

# append parent filepath
split_fcs = [f"scratch.gdb\\{fc}" for fc in split_fcs]
fns.reset_ws()

# calculate presence ---------------------------------------------------
print("Calculating presence....")
fns.sum_kelp_within(split_fcs, containers)

# get list of summarize within output fcs
arcpy.env.workspace = os.path.join(os.getcwd(), "scratch.gdb")
sumwithin_fcs = arcpy.ListFeatureClasses("sum*")
print("Sum within results: ")
print(sumwithin_fcs)

# rename fcs to have year at end
for fc in sumwithin_fcs:
    fc_desc = arcpy.Describe(fc)
    new_name = f"sum{str(fc_desc.name[-6:-2])}"
    arcpy.management.Rename(fc, new_name)
    print(f"{fc_desc.name} renamed to {new_name}")

# reset list
sumwithin_fcs_renamed = arcpy.ListFeatureClasses("sum*")
sumwithin_fcs_renamed = [f"scratch.gdb\\{fc}" for fc in sumwithin_fcs_renamed]
fns.reset_ws()

# convert to table
print("Converting results to dataframes...")
sdf_list = fns.df_from_fc(sumwithin_fcs_renamed, "MRC_Kayak")

# compile to one df
presence = pd.concat(sdf_list)
print(f"Number of rows: {len(presence)}")
# drop any rows where presence = 0 because we are treating this as presence-only
print("Dropping rows where presence = 0...")
presence = presence[presence["presence"] != 0]
print(f"Number of rows: {len(presence)}")

# calculate abundance --------------------------------------------------
print("Calculating abundance...")
abundance_containers = "LinearExtent.gdb\\abundance_containers"
abundance = fns.calc_abundance(abundance_containers, split_fcs)

# add year col
abundance["year"] = abundance["fc_name"].str[1:5]
abundance = abundance.drop(columns=["fc_name"])
print("Abundance data: ")
print(abundance.head())

# compile and export --------------------------------------------------
result = pd.merge(presence, abundance, how="left", on=["SITE_CODE", "year"])
print("Compiled results:")
print(result.head())

out_result = "kelp_data_synth_results\\MRC_kayak_synth.csv"
result.to_csv(out_result)
print(f"Results saved here: {out_result}")

# clear workspace
fns.clear_scratch()
