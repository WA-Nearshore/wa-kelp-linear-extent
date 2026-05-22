# Summarize COSTR & AQRES data to kelp linear extent

# Need to update for 2026 now that these datasets are combined in a single gdb
# current version from "..\kelp\projects\2025_COSTR_AQRES_1989_2024\data_download\WA_floating_kelp_coast_strait_reserves.gdb"
# accessed 2026-02-19
# Manually create two survey boundary fcs - only COSTR Map Indexes til 2011, then COSTR+AQRES Map Indexes 2011 onward

# set up environment --------------------------------------

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

import kelp_linear_extent_code.fns as fns # noqa: E402 project function library

arcpy.env.overwriteOutput = True # overwrite outputs 

# set workspace to parent folder
fns.reset_ws()

# set up scratch workspace
SCRATCH_WS = fns.config_scratch()

# USER INPUT ------------------------------------------------

dataset_name = "WADNR_COSTR_AQRES"
kelp_data_path = os.path.join(PROJECT_ROOT, "kelp_data_sources\\WA_floating_kelp_coast_strait_reserves.gdb")
containers = os.path.join(PROJECT_ROOT, "LinearExtent.gdb\\lines_and_containers\\kelp_containers_v3")
cov_cat_containers = os.path.join(PROJECT_ROOT, "LinearExtent.gdb\\lines_and_containers\\cov_cat_containers")

# prep data ------------------------------------------------

print(f"Using {containers} as container features")
arcpy.env.workspace = f"{kelp_data_path}\\annual_data"

# list feature classes
kelp_fc_names = arcpy.ListFeatureClasses()  

print("Datasets to be linearized:")
for fc in kelp_fc_names:
    print(f"{fc}")

# append path to fcs in list
kelp_fcs = [f"{kelp_data_path}\\annual_data\\{fc}" for fc in kelp_fc_names]

# ensure list is sorted first year - last year
kelp_fcs = sorted(kelp_fcs, key=lambda x: int(x[-4:]))
print(kelp_fcs)

# reset workspace to parent folder
fns.reset_ws(PROJECT_ROOT)

# prepare survey boundaries --> split up map_index_polygons
# need: 1989 only for 1989-2009, 1989 + 2010 for 2010, all for 2011+

print("Preparing survey boundaries...")
# set up list
svy_bnd = [f"{kelp_data_path}\\map_index_89_09", 
           f"{kelp_data_path}\\map_index_10", 
           f"{kelp_data_path}\\map_index_polygons"]

if not arcpy.Exists(svy_bnd[2]):
    raise Exception(f"WARNING: {svy_bnd} DOES NOT EXIST")

if not arcpy.Exists(svy_bnd[0]):
    print("Creating 89-09 survey boundary")
    arcpy.conversion.ExportFeatures(svy_bnd[2],
                                    svy_bnd[0],
                                    '"first_yr" < 2010')

if not arcpy.Exists(svy_bnd[1]):
    print("Creating 2010 survey boundary")
    arcpy.conversion.ExportFeatures(svy_bnd[2],
                                    svy_bnd[1],
                                    '"first_yr" < 2011')

print("Survey boundaries: ")
for fc in svy_bnd: 
    print(fc)

# combine lists 
paired_fc_list = [
    (a, svy_bnd[0]) if i < 20
    else (a, svy_bnd[1]) if i == 20
    else (a, svy_bnd[2])
    for i, a in enumerate(kelp_fcs)
]

for pair in paired_fc_list: 
    print(pair)

# calculate presence  -----------------------------------------
pres_fcs = fns.calc_presence(paired_fc_list, containers, variable_survey_area=True) # this function also creates scratch.gdb if it does not exist
print(pres_fcs)

# convert fcs to dfs
sdf_list = fns.df_from_fc(in_features=pres_fcs, source_name=dataset_name)

print("This is the structure of the sdfs:")
print(sdf_list[1].head())

# merge results to one df
presence = pd.concat(sdf_list)
print("Presence data merged to one df")
print(presence.head())

# calculate abundance ---------------------------------------
print("Calculating coverage category....")

cov_cat = fns.calc_cov_cat(cov_cat_containers, kelp_fcs)

cov_cat["year"] = cov_cat["fc_name"].str[-4:]
cov_cat = cov_cat.drop(columns=["fc_name"])
print("Compiled cov cat results:")
print(cov_cat.head())

# combine results -------------------------------------------

print("Combining results to one dataframe")
results = pd.merge(presence, cov_cat, how="left", on=["SITE_CODE", "year"])
print(results.head())

# Write to csv
os.makedirs(f"{PROJECT_ROOT}\\kelp_data_linear_outputs", exist_ok=True)
out_results = os.path.join(PROJECT_ROOT, f"kelp_data_linear_outputs\\{dataset_name}_result.csv")
results.to_csv(out_results)
print(f"Saved as csv here: {out_results}")

# Clear scratch gdb to keep project size down
fns.clear_scratch()