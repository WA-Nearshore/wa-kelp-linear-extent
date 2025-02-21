# Summarize AQRES data to kelp linear extent

# AQRES Data
# Latest download date 2025-02-20
# Link: https://fortress.wa.gov/dnr/adminsa/gisdata/datadownload/kelp_canopy_aquatic_reserves.zip

##### setup environment ####

import sys
import os
import arcpy
import arcpy.management
import arcpy.analysis
import arcpy.conversion
import pandas as pd
from arcgis.features import GeoAccessor, GeoSeriesAccessor

sys.path.append(os.getcwd())

# import the project function library 
import fns

arcpy.env.overwriteOutput = True

fns.reset_ws()

#### load data ####

# containers
containers = "LinearExtent.gdb\\kelp_containers_v2"
print(f"Using {containers} as container features")

# set workspace to gdb with kelp data sources
kelp_data_path = r"kelp_data_sources\kelp_canopy_aquatic_reserves\kelp_canopy_aquatic_reserves.gdb"
arcpy.env.workspace = kelp_data_path

# list AQRES feature classes
aqres_fcs = arcpy.ListFeatureClasses('kelp1*')  # add all fcs from 2010s
aqres_fcs.extend(arcpy.ListFeatureClasses('kelp2*')) # add all fcs from 2020s

for fc in aqres_fcs: print(f"Datasets to be summarized: {fc}")

# append path to fcs in list 
aqres_fcs = [f"{kelp_data_path}\\{fc}" for fc in aqres_fcs]

# reset workspace to parent folder
fns.reset_ws()

#### clip containers to survey area ####
aqres_bnd = f"{kelp_data_path}\\map_index_ar"
arcpy.analysis.Clip(containers, aqres_bnd, "scratch.gdb\\containers_AQRES")

# reset container variable to the clipped version
containers = "scratch.gdb\\containers_AQRES"

print("Container fc clipped to " + aqres_bnd.rsplit('\\', 1)[-1])

#### run summarize within ####
fns.sum_kelp_within(aqres_fcs, containers)

#### Save results to tables ####

# get list of summarize within output fcs
arcpy.env.workspace = os.path.join(os.getcwd(),"scratch.gdb")
sumwithin_fcs = arcpy.ListFeatureClasses("sum*")
print("Resulting fcs: ")
for f in sumwithin_fcs: print(f"{f}")

# rename fcs to have year at end 
for fc in sumwithin_fcs:
    fc_desc = arcpy.Describe(fc)
    new_name = f"sum20{str(fc_desc.name[-4:-2])}"
    print(f"original fc name: f{fc_desc.name}")

    arcpy.management.Rename(fc, new_name)
    print(f"{fc_desc.name} renamed to {new_name}")

# reset list 
sumwithin_fcs = arcpy.ListFeatureClasses("sum*")
sumwithin_fcs = [f"scratch.gdb\\{fc}" for fc in sumwithin_fcs]
fns.reset_ws()

# convert fcs to dfs
sdf_list = fns.df_from_fc(sumwithin_fcs, "WADNR_AQRES")

print("This is the structure of the sdfs:")
print(sdf_list[1].head())

# Merge to one df
all_data = pd.concat(sdf_list)
all_data

print("All years of data have been merged to one df")
print(" ")
print(" ")
print(all_data.head())

# Write to csv
all_data.to_csv("kelp_data_synth_results\\AQRES_synth_test.csv")
print("Saved as csv here: kelp_data_synth_results\\AQRES_synth_test.csv")

# Clear scratch gdb to keep project size down
#fns.clear_scratch()