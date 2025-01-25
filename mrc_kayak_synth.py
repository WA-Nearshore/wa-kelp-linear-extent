#MRC Kayak data synth

# Data was sent to me via fwd email from Jeff W on 2024-11-22 and includes a .gdb with data through 2023
# Treating data as presence only, because there is not available annual survey extents and survey areas shift between years

import arcpy
import arcpy.analysis
import arcpy.conversion
import arcpy.management
import pandas as pd
import numpy as np
from arcgis.features import GeoAccessor, GeoSeriesAccessor
import os

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

# Kayak annual polygons (all in one feature class)
kelp_bed_fc = "\\kelp_data_sources\\AllYearsAllSurveys_DNRMaster.gdb\\AllYearsAllSurveys_DNRMaster.gdb\\AllYearsAllSurveys_Master" 
print("Dataset to be summarized: " + kelp_bed_fc)

### Define functions

# Reproject kayak beds to match containers
def reproject_kelp(containers, kelp_bed_fc):
    container_sr = arcpy.Describe(containers).SpatialReference
    arcpy.Project_management(kelp_bed_fc, "scratch.gdb\\kelp_project", container_sr)
    kelp_bed_fc = "scratch.gdb\\kelp_project"
# not clipping containers, will only include results where presence = 1 

#### Split feature class into one fc per year ####
def split_fc_by_year(kelp_bed_fc):
    arcpy.analysis.SplitByAttributes(kelp_bed_fc, "scratch.gdb", ['Year'])
    arcpy.env.workspace = "scratch.gdb"
    split_fcs = arcpy.ListFeatureClasses("T*")
    print("MRC Kayak data split into one feature class per year:")
    for fc in split_fcs: print(fc)

    reset_ws()
    return split_fcs

#### Summarize Within ####

def summarize_kelp_within(split_fcs, containers):

    containers_copy = "scratch.gdb\\containers"
    arcpy.management.CopyFeatures(containers, containers_copy)

    arcpy.env.workspace = "scratch.gdb"

    for fc in split_fcs:

        fc_desc = arcpy.Describe(fc)
        out_feature_class = (os.getcwd() + "scratch.gdb\\sum" + fc_desc.name)
        try:
            arcpy.analysis.SummarizeWithin(
                containers_copy,
                fc,
                out_feature_class,
                "ONLY_INTERSECTING"
            ) 

            print("Summarize Within complete for " + fc_desc.name)
        except arcpy.ExecuteError:
            print(arcpy.GetMessages())
        except Exception as e:
            print(f"Error: {str(e)}")

    # get list of summarize within output fcs
    sumwithin_fcs = arcpy.ListFeatureClasses('sum*')
    sumwithin_fcs = ["scratch.gdb\\" + fc for fc in sumwithin_fcs]
    print(sumwithin_fcs)
    reset_ws()
    return sumwithin_fcs

# create function to convert fcs to dfs and store in list
def df_from_fc(in_features):
    sdf_list = []
    for feature in in_features:
        
        fc_desc = arcpy.Describe(feature)
        fc_year = (str(fc_desc.name[4:8])) # extract year

        sdf = pd.DataFrame.spatial.from_featureclass(feature) #use geoaccessor to convert fc to sdf
        sdf = sdf.filter(['SITE_CODE', 'sum_Area_SQUAREKILOMETERS'], axis = 1) #drop unneeded cols
        sdf['year'] = fc_year
        sdf['source'] = 'MRC_kayak'
        sdf['presence'] = 1 # only returned containers with some presence above, so all values should be 1 
        sdf['sum_area_ha'] = sdf['sum_Area_SQUAREKILOMETERS'] * 100 # convert to hectares
        sdf_list.append(sdf)

        print("Converted " + fc_desc.name + " to sdf and added to list")

    return sdf_list    

# APPLY FUNCTIONS
#reproject_kelp(containers, kelp_bed_fc)
split_fcs = split_fc_by_year(kelp_bed_fc)
sumwithin_fcs = summarize_kelp_within(split_fcs, containers)
sdf_list = df_from_fc(sumwithin_fcs)

print("This is the structure of the sdfs:")
print(sdf_list[1].head())

# Merge to one df
all_data = pd.concat(sdf_list)
all_data

print("All years of data have been merged to one df")
print(" ")
all_data.head()

# Write to csv
all_data.to_csv("kelp_data_synth_results\\MRC_kayak_synth.csv")
print("Saved as csv here: kelp_data_synth_results\\MRC_kayak_synth.csv")

# Clear scratch gdb to keep project size down
arcpy.env.workspace = "scratch.gdb"
scratch_fcs = arcpy.ListFeatureClasses('T*')
for fc in scratch_fcs:
    arcpy.Delete_management(fc)
    print(f"Deleted feature class: {fc}")