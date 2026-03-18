# PSRF / Port of Seattle Elliott Bay Linear Extent Surveys
# Last Update: 2026-03-16

# Manual pre-processing:
# -- Several years projected from WGS84 to NAD83 Harn WA St Plane S (US Ft)
# -- Survey boundary polygons manually drawn based on maps in 2024 PSRF Port of Seattle report 
# -- All of Elliott Bay (Magnolia to Jack Block Park) surveyed 2022 onwards. In 2021, area from mouth of Duwamish to Pier 70 was not surveyed


# set environment -------------------------------------------------------

import sys
import os
import arcpy
import pandas as pd
from arcgis.features import GeoAccessor, GeoSeriesAccessor # noqa: F401 # these are used to create sedfs

# project root is the folder within which the entire kelp_linear_extent module is located (2 levels up from this file)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
print("Project working directory:")
print(PROJECT_ROOT)
sys.path.append(PROJECT_ROOT) # this lets the project function library be found as a module

import kelp_linear_extent.fns as fns # noqa: E402 # project function library

arcpy.env.overwriteOutput = True # overwrite outputs 

# set workspace to parent folder
fns.reset_ws()

# set up scratch workspace
SCRATCH_WS = fns.config_scratch()

# USER INPUT -----------------------------------------------------------

dataset_name = "PSRF_Elliott_Bay_Linear_Surveys"
containers = os.path.join(PROJECT_ROOT, "LinearExtent.gdb", "kelp_containers_v2")
cov_cat_containers = os.path.join(PROJECT_ROOT, "LinearExtent.gdb\\abundance_containers")
kelp_data_path = os.path.join(PROJECT_ROOT, "kelp_data_sources\\PSRF_BulbCount_datashare.gdb") 

# prep data -------------------------------------------------------------
print(f"Using {containers} as container features")


# get list of kelp fcs
arcpy.env.workspace = kelp_data_path
kelp_fcs = arcpy.ListFeatureClasses("POSKelp*")
for fc in kelp_fcs: 
    print(fc)
kelp_fcs = [os.path.join(kelp_data_path, fc) for fc in kelp_fcs]

# get list of survey boundaries
svy_bnd_fcs = arcpy.ListFeatureClasses("survey*")
for fc in svy_bnd_fcs:
    print(fc)
svy_bnd_fcs = [os.path.join(kelp_data_path, fc) for fc in svy_bnd_fcs]

# create paired list of kelp fcs and svy bnds
fc_list = [(kelp_fcs[0], svy_bnd_fcs[0]),
           (kelp_fcs[1], svy_bnd_fcs[1]),
           (kelp_fcs[2], svy_bnd_fcs[1]),
           (kelp_fcs[3], svy_bnd_fcs[1]),
           (kelp_fcs[4], svy_bnd_fcs[1])] 

print("Input datasets to be linearized:")
for kelp, svy in fc_list:
    print(f"Kelp data: {kelp}")   
    print(f"Survey boundary: {svy}")     

# calculate presence ---------------------------------------------------

print("Calculating presence...")
sumwithin_fcs = fns.sum_kelp_within(fc_list, containers, variable_survey_area=True, kelp_geometry_type="line")

# convert to sdfs
print("Converting results to data frames...")
sdf_list = fns.df_from_fc(sumwithin_fcs, dataset_name, kelp_geometry_type="line")

print("This is the structure of the sdfs:")
print(sdf_list[1].head())

# merge results to one df 
presence = pd.concat(sdf_list)
print("Presence data merged to one df")
print(presence.head())

# calculate coverage category ------------------------------------------

print("Calculating coverage category...")
cov_cat = fns.calc_cov_cat(cov_cat_containers, kelp_fcs, kelp_geometry_type="line")

cov_cat['year'] = cov_cat['fc_name'].str[-4:]
cov_cat = cov_cat.drop(columns=['fc_name'])
print("Compiled coverage category results:")
print(cov_cat.head())


# combine and export --------------------------------------------------
results = pd.merge(presence, cov_cat, how="left", on=["SITE_CODE", "year"])

# Write to csv
os.makedirs(f"{PROJECT_ROOT}\\kelp_data_linear_outputs", exist_ok=True)
out_results = os.path.join(PROJECT_ROOT, f"kelp_data_linear_outputs\\{dataset_name}_result.csv")
results.to_csv(out_results)
print(f"Saved as csv here: {out_results}")

fns.clear_scratch()