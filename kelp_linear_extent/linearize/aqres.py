# Summarize AQRES data to kelp linear extent

# AQRES Data
# Latest download date 2025-06-11
# Link: https://fortress.wa.gov/dnr/adminsa/gisdata/datadownload/kelp_canopy_aquatic_reserves.zip

# Currently working off the version from K:\kelp\kelp_aquatic_reserves\deliverables\dnr_kelp_processing_2022_2024\kelp_canopy_aquatic_reserves_adjusted.
# Manually renamed the two fcs kelp10_23ar... so those summary fcs dont get included in this script

# Gray note to self - AQRES 2010 only surveyed a very small area. 
# From Lisa: The three map indexes around Smith Island were surveyed completely in 2010 (1.1, 1.2, & 1.3). Mapping for all other areas started in 2011. 

# set up environment --------------------------------------

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

# set workspace to parent folder
fns.reset_ws()

# prep data ------------------------------------------------

# containers
containers = "LinearExtent.gdb\\kelp_containers_v2"
print(f"Using {containers} as container features")

# set workspace to gdb with kelp data sources
kelp_data_path = r"kelp_data_sources\kelp_canopy_aquatic_reserves_adjusted.gdb"
arcpy.env.workspace = kelp_data_path

# list AQRES feature classes
aqres_fcs = arcpy.ListFeatureClasses('kelp1*')  # add all fcs from 2010s
aqres_fcs.extend(arcpy.ListFeatureClasses('kelp2*')) # add all fcs from 2020s

for fc in aqres_fcs: print(f"Datasets to be summarized: {fc}")

# append path to fcs in list 
aqres_fcs = [f"{kelp_data_path}\\{fc}" for fc in aqres_fcs]

# reset workspace to parent folder
fns.reset_ws()

# clip containers to survey area 
aqres_bnd = f"{kelp_data_path}\\map_index_ar"
arcpy.analysis.Clip(containers, aqres_bnd, "scratch.gdb\\containers_AQRES")

# reset container variable to the clipped version
containers = "scratch.gdb\\containers_AQRES"

print("Container fc clipped to " + aqres_bnd.rsplit('\\', 1)[-1])

# calculate presence  -----------------------------------------

fns.sum_kelp_within(aqres_fcs, containers)

# get list of summarize within output fcs
arcpy.env.workspace = os.path.join(os.getcwd(),"scratch.gdb")
sumwithin_fcs = arcpy.ListFeatureClasses("sum*")
print("Resulting fcs: ")
for f in sumwithin_fcs: print(f"{f}")

# rename fcs to have year at end 
for fc in sumwithin_fcs:
    fc_desc = arcpy.Describe(fc)
    new_name = f"sum20{str(fc_desc.name[-4:-2])}"
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

# merge results to one df 
presence = pd.concat(sdf_list)
print("Presence data merged to one df")
print(presence.head())

# calculate abundance ---------------------------------------

print("Clipping abundance containers to survey boundary...")
abundance_containers = "LinearExtent.gdb\\abundance_containers"
arcpy.analysis.Clip(abundance_containers, aqres_bnd, "scratch.gdb\\ab_containers_AQRES")
abundance_containers = "scratch.gdb\\ab_containers_AQRES"

abundance = fns.calc_abundance(abundance_containers, aqres_fcs)

abundance['year'] = "20" + abundance['fc_name'].str[4:6]
abundance = abundance.drop(columns=['fc_name'])
print("Compiled abundance results:")
print(abundance.head())

# combine results -------------------------------------------

print("Combining results to one dataframe")
results = pd.merge(presence, abundance, how='left', on=['SITE_CODE','year'])
print(results.head())

# Write to csv
out_result = "AQRES_synth.csv"
results.to_csv(f"kelp_data_synth_results\\{out_result}")
print(f"Saved as csv here: kelp_data_synth_results\\{out_result}")

# Clear scratch gdb to keep project size down
fns.clear_scratch()