# Summarize kayak data to kelp linear extent
# Gray McKenna
# 2024-08-15

# Data copied from K:\kelp\bull_kelp_kayak\2024\data_processing\gdb\DNR_bull_kelp_kayak_2024.gdb on 2025-01-14...

# This dataset is a little funky in that there are small 'absence' polygons at sites where there was an annual survey to confirm there was no kelp 
# Different sites surveyed each year --> if there is no absence polygon, it wasn't surveyed

import arcpy
import arcpy.analysis
import arcpy.conversion
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

# Kayak aggregate annual polygons (all in one feature class)
kelp_data_path = "kelp_data_sources\\DNR_bull_kelp_kayak_2024.gdb" 
fc = kelp_data_path + "\\bed_perimeter_surveys_2013_2024_aggregates"

print("Dataset to be summarized: " + fc)

#### Clip containers by survey boundaries ###

# Note: for this dataset, this is just to speed up processing
# Absence surveys = polygons with a 0 value for area_ha field 
kayak_bnd = kelp_data_path +  "\\site_boundaries_2024_SPS_all"
arcpy.analysis.Clip(containers, kayak_bnd, "scratch.gdb\\containers_kayak")
containers = "scratch.gdb\\containers_kayak"

#print("Container fc clipped to " + kayak_bnd.rsplit('\\', 1)[-1])

#### Split feature class into one fc per year ####
arcpy.analysis.SplitByAttributes(fc, "scratch.gdb", ['year_'])
arcpy.env.workspace = "scratch.gdb"
split_fcs = arcpy.ListFeatureClasses("T*")
print("Kayak data split into one feature class per year:")
for f in split_fcs: print(f)

# Append file path
split_fcs = ["scratch.gdb\\" + fc for fc in split_fcs]

reset_ws()

#### Summarize Within ####

for fc in split_fcs:

    fc_desc = arcpy.Describe(fc)

    arcpy.analysis.SummarizeWithin(
        in_polygons = containers,
        in_sum_features = fc,
         # save results in scratch gdb 
        out_feature_class = ("scratch.gdb" + "\\sumwithin" + fc_desc.name)
    ) 

    print("Summarize Within complete for " + fc_desc.name)

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
        fc_year = (str(fc_desc.name[-4:])) # extract year

        sdf = pd.DataFrame.spatial.from_featureclass(feature) #use geoaccessor to convert ftr to df
        sdf = sdf.filter(['SITE_CODE', 'sum_Area_SQUAREKILOMETERS'], axis = 1) #drop unneeded cols
        # remove cols where sum_Area_ is actually zero - that indicates that site was not surveyed that year 
        sdf = sdf[sdf.sum_Area_SQUAREKILOMETERS != 0]
        sdf['year'] = fc_year
        sdf['source'] = 'DNR_kayak'
        sdf['presence'] = np.where(sdf['sum_Area_SQUAREKILOMETERS'] > 3.3445e-7, 1, 0) # calculate presence, this tiny # = the 'absence' polygons
        sdf['sum_area_ha'] = sdf['sum_Area_SQUAREKILOMETERS'] * 100 # convert to hectares
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
all_data.head()

# Write to csv
all_data.to_csv("kelp_data_synth_results\\kayak_synth.csv")
print("Saved as csv here: kelp_data_synth_results\\kayak_synth.csv")

# Clear scratch gdb to keep project size down
arcpy.env.workspace = "scratch.gdb"
scratch_fcs = arcpy.ListFeatureClasses()
for fc in scratch_fcs:
    arcpy.Delete_management(fc)
    print(f"Deleted feature class: {fc}")