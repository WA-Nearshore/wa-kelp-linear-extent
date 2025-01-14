# Summarize COSTR kelp data to linear extent
# Gray McKenna
# 2024-08-15

# COSTR data 
# download date: 2024-06-17
# link: https://fortress.wa.gov/dnr/adminsa/gisdata/datadownload/kelp_canopy_strait_coast.zip

import arcpy
import os
import arcpy.conversion
import pandas as pd
import numpy as np
from arcgis.features import GeoAccessor, GeoSeriesAccessor

# Set environment
arcpy.env.overwriteOutput = True

# store parent folder workspace in function 
def reset_ws(): 
    arcpy.env.workspace = os.getcwd()

reset_ws()

#### Load Data ####

# Containers
containers = "LinearExtent.gdb\\kelp_containers_v2"

print("Using " + containers + " as container features")

# Set workspace to gdb with kelp data sources
kelp_data_path = "kelp_data_sources\\kelp_canopy_strait_coast\\kelp_canopy_coast_strait.gdb"
arcpy.env.workspace = kelp_data_path

# COSTR kelp data
costr_fcs = arcpy.ListFeatureClasses('kelp1*')  # add all fcs from 1900s
costr_fcs.extend(arcpy.ListFeatureClasses('kelp2*')) # add all fcs from 2000s

print("Datasets to be summarized: ")
print(costr_fcs)

# append path to fcs in list 
costr_fcs = [kelp_data_path + "\\" + fc for fc in costr_fcs]

reset_ws()

#### Clip Containers to COSTR survey area ####

costr_bnd = kelp_data_path + "\\map_index"
arcpy.analysis.Clip(containers, costr_bnd, "scratch.gdb\\containers_COSTR")

containers = "scratch.gdb\\containers_COSTR"

print("Container fc clipped to " + costr_bnd.rsplit('\\', 1)[-1])

#### Summarize Within ####

# Run summarize within each year of data with containers

def sum_kelp_within(fc_list):

    for fc in fc_list:

        fc_desc = arcpy.Describe(fc)
        
        out_fc_path = ("scratch.gdb/sumwithin" + fc_desc.name).replace(" ", "")
        
        arcpy.analysis.SummarizeWithin(
            in_polygons = containers,
            in_sum_features = fc,
            out_feature_class = out_fc_path
        )

        print("Summarize Within complete for " + fc_desc.name)

sum_kelp_within(costr_fcs)

#### Save results to tables ####

# get list of summarize within output fcs
arcpy.env.workspace = "scratch.gdb"
sumwithin_fcs = arcpy.ListFeatureClasses('sum*')
sumwithin_fcs = ["scratch.gdb\\" + fc for fc in sumwithin_fcs]
reset_ws()

# create function to convert fcs to dfs and store in list
def df_from_fc(in_features):
    sdf_list = []
    for feature in in_features:
        
        fc_desc = arcpy.Describe(feature)
        fc_year = fc_desc.name[-4:] # extract year

        sdf = pd.DataFrame.spatial.from_featureclass(feature) #use geoaccessor to convert fc to df
        sdf = sdf.filter(['SITE_CODE', 'sum_Area_SQUAREKILOMETERS'], axis = 1) #drop unneeded SHAPE cols
        sdf['year'] = fc_year
        sdf['source'] = 'COSTR'
        sdf['presence'] = np.where(sdf['sum_Area_SQUAREKILOMETERS'] > 0, 1, 0)
        sdf['sum_area_ha'] = sdf['sum_Area_SQUAREKILOMETERS'] * 100

        sdf_list.append(sdf)

        print("Converted " + fc_desc.name + " to sdf and added to list")

    return sdf_list    

# apply function to list of summarize within outputs
sdf_list = df_from_fc(sumwithin_fcs)

print("This is the structure of the sdfs:")
print(sdf_list[1].head())

# Merge to one df
all_data = pd.concat(sdf_list)
all_data

print("All years of data have been merged to one df")
print(" ")
print(all_data.head())

# Write to csv
all_data.to_csv("kelp_data_synth_results\\COSTR_synth.csv")
print("Saved as csv here: kelp_data_synth_results\\COSTR_synth.csv")

# Clear scratch gdb to keep project size down
arcpy.env.workspace = "scratch.gdb"
scratch_fcs = arcpy.ListFeatureClasses()
for fc in scratch_fcs:
    arcpy.Delete_management(fc)
    print(f"Deleted feature class: {fc}")
