# Summarize COSTR & AQRES data to kelp linear extent

# Need to update for 2026 now that these datasets are combined in a single gdb
# current version from "..\kelp\projects\2025_COSTR_AQRES_1989_2024\data_download\WA_floating_kelp_coast_strait_reserves.gdb"
# accessed 2026-02-19

# set up environment --------------------------------------

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

# configure scratch
fns.config_scratch()

# USER INPUT ------------------------------------------------

dataset_name = "WADNR_COSTR_AQRES"
containers = os.path.join(PROJECT_ROOT, "LinearExtent.gdb", "kelp_containers_v2")
kelp_data_path = os.path.join(PROJECT_ROOT, "kelp_data_sources\\kelp_canopy_aquatic_reserves_adjusted.gdb")
abundance_containers = os.path.join(PROJECT_ROOT, "LinearExtent.gdb\\abundance_containers")

# prep data ------------------------------------------------

print(f"Using {containers} as container features")
arcpy.env.workspace = kelp_data_path

# list AQRES feature classes
aqres_fcs = arcpy.ListFeatureClasses("kelp1*")  # add all fcs from 2010s
aqres_fcs.extend(arcpy.ListFeatureClasses("kelp2*"))  # add all fcs from 2020s

print("Datasets to be linearized:")
for fc in aqres_fcs:
    print(f"{fc}")

# append path to fcs in list
aqres_fcs = [f"{kelp_data_path}\\{fc}" for fc in aqres_fcs]

# reset workspace to parent folder
fns.reset_ws(PROJECT_ROOT)

# clip containers to survey area
aqres_bnd = os.path.join(PROJECT_ROOT, f"{kelp_data_path}\\map_index_ar")
containers_clip = os.path.join(PROJECT_ROOT, "scratch.gdb\\containers_AQRES")
arcpy.analysis.Clip(containers, aqres_bnd, containers_clip)

print("Container fc clipped to " + aqres_bnd.rsplit("\\", 1)[-1])

# calculate presence  -----------------------------------------

sumwithin_fcs = fns.sum_kelp_within(aqres_fcs, containers_clip, PROJECT_ROOT) # this function also creates scratch.gdb if it does not exist
print(sumwithin_fcs)

# rename fcs to have year at end (AQRES-specific step)
try:
    for fc in sumwithin_fcs:
        fc_desc = arcpy.Describe(fc)
        new_name = f"sum20{str(fc_desc.name[-4:-2])}"
        arcpy.management.Rename(fc, new_name)
        print(f"{fc_desc.name} renamed to {new_name}")
except Exception as e: 
    print(print(f"Unable to rename feature classes: {e}"))

# reset list
arcpy.env.workspace = os.path.join(PROJECT_ROOT, "scratch.gdb")
sumwithin_fcs = arcpy.ListFeatureClasses("sum*")
sumwithin_fcs = [os.path.join(PROJECT_ROOT, f"scratch.gdb\\{fc}") for fc in sumwithin_fcs]
print(f"Reset feature class list: {sumwithin_fcs}")
fns.reset_ws(PROJECT_ROOT)

# convert fcs to dfs
sdf_list = fns.df_from_fc(in_features=sumwithin_fcs, source_name="WADNR_AQRES")

print("This is the structure of the sdfs:")
print(sdf_list[1].head())

# merge results to one df
presence = pd.concat(sdf_list)
print("Presence data merged to one df")
print(presence.head())

# calculate abundance ---------------------------------------


abundance = fns.calc_abundance(abundance_containers, aqres_fcs)

abundance["year"] = "20" + abundance["fc_name"].str[4:6]
abundance = abundance.drop(columns=["fc_name"])
print("Compiled abundance results:")
print(abundance.head())

# combine results -------------------------------------------

print("Combining results to one dataframe")
results = pd.merge(presence, abundance, how="left", on=["SITE_CODE", "year"])
print(results.head())

# Write to csv
out_result = "AQRES_synth.csv"
results.to_csv(os.path.join(PROJECT_ROOT, f"kelp_data_linear_outputs\\{out_result}"))
print(f"Saved as csv here: kelp_linear_outputs\\{out_result}")

# Clear scratch gdb to keep project size down
fns.clear_scratch()