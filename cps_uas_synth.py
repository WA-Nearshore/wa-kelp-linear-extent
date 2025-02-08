# Suquamish CPS UAS Synth

# Kelp bed data all copied from 
# K:\kelp\projects\2023_Suquamish_CPS_mapping\data\uas_data\derived_results\Suquamish_UAS_survey_bed_extents.gdb

#### SET ENVIRONMENT ####
import arcpy
import os
import pandas as pd
import numpy as np
from dbfread import DBF
from arcgis.features import GeoAccessor, GeoSeriesAccessor

arcpy.env.overwriteOutput = True

# store parent folder workspace in function 
def reset_ws(): 
    arcpy.env.workspace = os.getcwd()

reset_ws()

#### Load Data ####

# Containers
containers = "LinearExtent.gdb\\kelp_containers_v2"

print("Using " + containers + " as container features")

# Set path to kelp data
kelp_data_path = "kelp_data_sources\\Suquamish_UAS_survey_bed_extents.gdb"
kelp_bed = kelp_data_path + "\\Suquamish_UAS_all_bed_extents"

#### Clip Containers to orthomosaic boundaries aka survey areas ####
svy_bnd = kelp_data_path + "\\Orthomosaic_Boundaries"
arcpy.analysis.Clip(containers, svy_bnd, "scratch.gdb\\containers_cps_uas")

containers = "scratch.gdb\\containers_cps_uas"

print("Container fc clipped to " + svy_bnd.rsplit('\\', 1)[-1])


#### Split feature class into one fc per year ####
arcpy.analysis.SplitByAttributes(kelp_bed, "scratch.gdb", ['Year'])
arcpy.env.workspace = "scratch.gdb"
split_fcs = arcpy.ListFeatureClasses("T*")
print("UAS data split into one feature class per year:")
for f in split_fcs: print(f)
reset_ws()

# Append file path
split_fcs = ["scratch.gdb\\" + fc for fc in split_fcs]

#### Summarize Within ####

# split bed perimeters into feature classes per year (2023 and 2024)

# Run summarize within each year of data with containers

def sum_kelp_within(fc_list):

    for fc in fc_list:

        fc_desc = arcpy.Describe(fc)
        
        out_fc_path = ("scratch.gdb//sumwithin" + fc_desc.name).replace(" ", "")
        
        arcpy.analysis.SummarizeWithin(
            in_polygons = containers,
            in_sum_features = fc,
            out_feature_class = out_fc_path
        )

        print("Summarize Within complete for " + fc_desc.name)

sum_kelp_within(split_fcs)

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
        sdf['source'] = 'WADNR_CPS_UAS'
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
all_data.to_csv("kelp_data_synth_results\\cps_uas_synth.csv")
print("Saved as csv here: kelp_data_synth_results\\cps_uas_synth.csv")

# Clear scratch gdb to keep project size down
arcpy.env.workspace = "scratch.gdb"
scratch_fcs = arcpy.ListFeatureClasses()
for fc in scratch_fcs:
    arcpy.Delete_management(fc)
    print(f"Deleted feature class: {fc}")