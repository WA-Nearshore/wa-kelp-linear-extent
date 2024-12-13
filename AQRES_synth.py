# Summarize AQRES data to kelp linear extent
# Gray McKenna
# 2024-08-15

# AQRES Data
# Download data 2024-06-17
# Link: https://fortress.wa.gov/dnr/adminsa/gisdata/datadownload/kelp_canopy_aquatic_reserves.zip

import arcpy
import arcpy.conversion
import pandas as pd
import numpy as np
from arcgis.features import GeoAccessor, GeoSeriesAccessor

# Set environment
arcpy.env.overwriteOutput = True

# store parent folder workspace in function 
project_folder = r"D:\DNR_files\Documents\ArcGIS\Projects\LinearExtent"

def reset_ws(): 
    arcpy.env.workspace = project_folder

reset_ws()

#### Load Data ####

# Containers
containers = "LinearExtent.gdb\\kelp_containers_v1_fixsitecode"
print("Using " + containers + " as container features")

# Set workspace to gdb with kelp data sources
kelp_data_path = r"kelp_data_sources\kelp_canopy_aquatic_reserves\kelp_canopy_aquatic_reserves.gdb"
arcpy.env.workspace = kelp_data_path

# List AQRES feature classes
aqres_fcs = arcpy.ListFeatureClasses('kelp1*')  # add all fcs from 1900s
aqres_fcs.extend(arcpy.ListFeatureClasses('kelp2*')) # add all fcs from 2000s

for fc in aqres_fcs: print("Datasets to be summarized: " + fc)

# append path to fcs in list 
aqres_fcs = [kelp_data_path + "\\" + fc for fc in aqres_fcs]

# reset workspace to parent folder
reset_ws()

#### Clip Containers to Survey Area ####
aqres_bnd = kelp_data_path + "\\map_index_ar"
arcpy.analysis.Clip(containers, aqres_bnd, "scratch.gdb\\containers_AQRES")

containers = "scratch.gdb\\containers_AQRES"

print("Container fc clipped to " + aqres_bnd.rsplit('\\', 1)[-1])

#### Summarize Within ####

# Run summarize within each year of data with containers
for fc in aqres_fcs:

    fc_desc = arcpy.Describe(fc)

    arcpy.analysis.SummarizeWithin(
        in_polygons = containers,
        in_sum_features = fc,
        out_feature_class = ("scratch.gdb" + "\\sumwithin" + fc_desc.name)
    ) # save results in scratch gdb 

    print("Summarize Within complete for " + fc_desc.name)

#### Save results to tables ####

# get list of summarize within output fcs
arcpy.env.workspace = "scratch.gdb"
sumwithin_fcs = arcpy.ListFeatureClasses("sum*")
sumwithin_fcs = ["scratch.gdb\\" + fc for fc in sumwithin_fcs]
reset_ws()

# create function to convert fcs to dfs and store in list
def df_from_fc(in_features):
    sdf_list = []
    for feature in in_features:
        
        fc_desc = arcpy.Describe(feature)
        fc_year = ("20" + str(fc_desc.name[-4:-2])) # extract year

        sdf = pd.DataFrame.spatial.from_featureclass(feature) #use geoaccessor to convert fc to df
        sdf = sdf.filter(['SITE_CODE', 'sum_Area_SQUAREKILOMETERS'], axis = 1) #drop unneeded SHAPE cols
        sdf['year'] = fc_year
        sdf['source'] = 'AQRES'
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
print(" ")
print(all_data.head())

# Write to csv
all_data.to_csv("kelp_data_synth_results\\AQRES_synth.csv")
print("Saved as csv here: kelp_data_synth_results\\AQRES_synth.csv")

# Clear scratch gdb to keep project size down
arcpy.env.workspace = "scratch.gdb"
scratch_fcs = arcpy.ListFeatureClasses()
for fc in scratch_fcs:
    arcpy.Delete_management(fc)
    print(f"Deleted feature class: {fc}")