# Vashon Nature Center kelp kayak surveys

# Sent via email on 2026-06-20 because the link was not working in the Shoreline Monitoring Toolbox
# Normally data should be available from https://www.shoremonitoring.org/bull-kelp/database/

# Data is shared as a lpkx --> 1st step is to import feature classes from lpkx to VNC geodatabase in kelp_data_sources//VNC

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

import kelp_linear_extent_code.fns as fns # noqa: E402 # project function library

arcpy.env.overwriteOutput = True # overwrite outputs 

# set workspace to parent folder
fns.reset_ws()

# set up scratch workspace
SCRATCH_WS = fns.config_scratch()

# USER INPUT -----------------------------------------------------------

dataset_name = "VashonNatureCenter_Kayak"
years = ["2023", "2024", "2025"] # list years for which data is currently available
kelp_data_path =  os.path.join(PROJECT_ROOT, "kelp_data_sources\\VNC\\VNC.gdb")
containers = os.path.join(PROJECT_ROOT, "LinearExtent.gdb\\lines_and_containers\\kelp_containers_v3")
cov_cat_containers = os.path.join(PROJECT_ROOT, "LinearExtent.gdb\\lines_and_containers\\cov_cat_containers")

# prep data ------------------------------------------------------------

# merge kelp beds by year
arcpy.env.workspace = kelp_data_path
kelp_fc_names = arcpy.ListFeatureClasses()
print("The following feature classes are available:")
for fc in kelp_fc_names:
    print(fc)

kelp_fc_list = [f"{kelp_data_path}//{fc_name}" for fc_name in kelp_fc_names] # append parent path

fns.reset_ws()

merged_fc_list = []

for year in years:

    # merge
    year_fcs = [fc for fc in kelp_fc_list if year in fc]
    merged_fc = f"{SCRATCH_WS}//kelp_{year}_WGS84"
    print(f"Merging {year_fcs} into {merged_fc}...")
    arcpy.management.Merge(year_fcs, merged_fc)

    # project to state plane s
    desc = arcpy.Describe(containers)
    spatialref = desc.spatialReference # grab sr from containers
    print(f"Projecting {merged_fc} to {spatialref.name}")
    merged_fc_sp = f"{SCRATCH_WS}//kelp_{year}"
    arcpy.management.Project(merged_fc, merged_fc_sp, spatialref)

    merged_fc_list.append(merged_fc_sp)

    
print("Merged feature classes:")
for fc in merged_fc_list:
    print(fc)

# project 


# calculate presence ---------------------------------------------------
print("Calculating presence....")
pres_fcs = fns.calc_presence(merged_fc_list, containers)
print(f"Out fcs: {pres_fcs}")

fns.reset_ws()

# convert to table
print("Converting results to dataframes...")
sdf_list = fns.df_from_fc(pres_fcs, dataset_name)

# compile to one df
presence = pd.concat(sdf_list)
print(f"Number of rows: {len(presence)}")
# drop any rows where presence = 0 because we are treating this as presence-only
print("Dropping rows where presence = 0... treating data as presence only")
presence = presence[presence["presence"] != 0]
print(f"Number of rows: {len(presence)}")

# calculate coverage category --------------------------------------------------
print("Calculating coverage category...")
cov_cat = fns.calc_cov_cat(cov_cat_containers, merged_fc_list)

# add year col
cov_cat["year"] = cov_cat["fc_name"].str[-4:]
cov_cat = cov_cat.drop(columns=["fc_name"])
print("Coverage category data: ")
print(cov_cat.head())

# compile and export --------------------------------------------------
results = pd.merge(presence, cov_cat, how="left", on=["SITE_CODE", "year"])
print("Compiled results:")
print(results.head())

# write to csv
os.makedirs(f"{PROJECT_ROOT}\\kelp_data_linear_outputs", exist_ok=True)
out_results = os.path.join(PROJECT_ROOT, f"kelp_data_linear_outputs\\{dataset_name}_result.csv")
results.to_csv(out_results)
print(f"Saved as csv here: {out_results}")

# clear workspace
fns.clear_scratch()