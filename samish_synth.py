# Summarize Samish Indian Nation's kelp polygons from drone surveys to linear extent

# files from K:\kelp\VScanopy\data\SJI\Samish_spatial_data_2021_delivery

# set environment ---------------------------------------------------------
import arcpy
import arcpy.analysis
import arcpy.conversion
import pandas as pd
import numpy as np
import os
import sys
from arcgis.features import GeoAccessor, GeoSeriesAccessor

sys.path.append(os.getcwd())

# import the project function library 
import fns

arcpy.env.overwriteOutput = True

fns.reset_ws()

# prep data ------------------------------------------------------------

# containers
containers = "LinearExtent.gdb\\kelp_containers_v2"
print(f"Using {containers} as container features")

# San Juans polygons
kelp_data_path = "kelp_data_sources\\Samish_spatial_data_2021_delivery"
kelp_shps = []
for file in os.listdir(kelp_data_path):
    if file.endswith(".shp"):
        kelp_shps.append(file)
print(kelp_shps)

# For now, drop the skagit data since I don't have the footprint --> could handle as presence only
kelp_shps.remove('SkagitCO_2019_Kelp.shp')

# append parent file path
kelp_shps = [f"{kelp_data_path}\\{shp}" for shp in kelp_shps]
print("Datasets to be summarized:")
for shp in kelp_shps:
    print(shp)

# convert shapefiles to feature classes in scratch.gdb
kelp_fcs = []

for shp in kelp_shps:
    out_fc = f"kelp_{shp[-13:-9]}"
    arcpy.conversion.FeatureClassToFeatureClass(shp, "scratch.gdb", out_fc)
    kelp_fcs.append(f"scratch.gdb\\{out_fc}")

print("Shapefiles converted to feature classes:")
print(kelp_fcs)

# each year has its own boundary
aoi2004 = f"{kelp_data_path}\\SanJuan_Footprint_2004\\2004_2006_kelp_image_index.shp"
aoi2006 = f"{kelp_data_path}\\SanJuan_Footprint_2006\\2004_2006_kelp_image_index.shp"
aoi2016 = f"{kelp_data_path}\\Boundary_2016\Boundary_2016.shp"
aoi2019 = f"{kelp_data_path}\\Boundary_2019\Boundary_2019.shp"

# create paired list of survey boundaries and kelp data
# the 2004 and 2006 results are merged 
fc_list = [(kelp_fcs[0], aoi2004), (kelp_fcs[0], aoi2006), (kelp_fcs[1], aoi2016, kelp_fcs[2], aoi2019)]

arcpy.analysis.Clip(containers, aoi2004, containers2004)
print("Created " + containers2004 + " using " + aoi2004 + " as survey boundary")
arcpy.analysis.Clip(containers, aoi2006, containers2006)
print("Created " + containers2006 + " using " + aoi2006 + " as survey boundary")
arcpy.analysis.Clip(containers, aoi2016, containers2016)
print("Created " + containers2016 + " using " + aoi2016 + " as survey boundary")
arcpy.analysis.Clip(containers, aoi2019, containers2019)
print("Created " + containers2019 + " using " + aoi2019 + " as survey boundary")


#### Summarize Within ####

def sum_kelp_within(fc, container):

    fc_desc = arcpy.Describe(fc)
    fc_year = container[-4:]
    out_fc_path = ("scratch.gdb/sumwithin" + fc_year)
    
    arcpy.analysis.SummarizeWithin(
        in_polygons = container,
        in_sum_features = fc,
        out_feature_class = out_fc_path
    )

    print("Summarize Within complete for " + fc_desc.name + " year = " + fc_year)

sum_kelp_within(kelp_fcs[0], containers2004)
sum_kelp_within(kelp_fcs[0], containers2006)
sum_kelp_within(kelp_fcs[1], containers2016)
sum_kelp_within(kelp_fcs[2], containers2019)


#### Convert to dfs ####
# get a list of sumwithin fcs
arcpy.env.workspace = "scratch.gdb"
sumwithin_fcs = arcpy.ListFeatureClasses('sum*')
sumwithin_fcs = ["scratch.gdb\\" + fc for fc in sumwithin_fcs]
reset_ws()

# create function to convert fcs to dfs and store in list
def df_from_fc(in_features, source):
    sdf_list = []
    for feature in in_features:
        
        fc_desc = arcpy.Describe(feature)
        fc_year = fc_desc.name[-4:] # extract year

        sdf = pd.DataFrame.spatial.from_featureclass(feature) #use geoaccessor to convert fc to df
        sdf = sdf.filter(['SITE_CODE', 'sum_Area_SQUAREKILOMETERS'], axis = 1) #drop unneeded SHAPE cols
        sdf['year'] = fc_year
        sdf['source'] = source
        sdf['presence'] = np.where(sdf['sum_Area_SQUAREKILOMETERS'] > 0, 1, 0)
        sdf['sum_area_ha'] = sdf['sum_Area_SQUAREKILOMETERS'] * 100

        sdf_list.append(sdf)

        print("Converted " + fc_desc.name + " to sdf and added to list")

    return sdf_list    

# apply function to list of summarize within outputs
sdf_list_1 = df_from_fc(sumwithin_fcs[:2], 'Friends of the San Juans')
sdf_list_2 = df_from_fc(sumwithin_fcs[-2:], 'San Juan County')
sdf_list = sdf_list_1 + sdf_list_2

print("Structure of dfs: ")
print(sdf_list[1].head())

# Merge to one df
all_data = pd.concat(sdf_list)

print("All years of data have been merged to one df")
print(" ")
print(all_data.head())

# Write to csv
all_data.to_csv("kelp_data_synth_results\\sji_synth.csv")
print("Saved as csv here: kelp_data_synth_results\\sji_synth.csv")

# Clear scratch gdb to keep project size down
arcpy.env.workspace = "scratch.gdb"
scratch_fcs = arcpy.ListFeatureClasses()
for fc in scratch_fcs:
    arcpy.Delete_management(fc)
    print(f"Deleted feature class: {fc}")
