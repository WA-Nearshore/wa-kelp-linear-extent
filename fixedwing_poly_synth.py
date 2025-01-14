## Summarize Classified Imagery POLYGONS from Fixed Wing Aerial Program
## For linear extent dataset 
# Gray McKenna
# 2024-12-10

# Classified Imagery from Fixed Wing Aerial Imagery Program 
# Copies of rasters brought down from K: on 2024-08-30, polygonized on 2024-11-26
# Classified rasters: K:\kelp\fixed_wing_aerial_imagery\imagery_products\2022\classified_rasters
# Classified polygons: K:\kelp\fixed_wing_aerial_imagery\imagery_products\2022\classified_polygons\fixed_wing_classified_polygons_2022.gdb
# Ortho tiles (footprints): K:\kelp\fixed_wing_aerial_imagery\imagery_products\2022\orthomosaics\ADM\GIS_data\Admiralty_Inlet_Flight_Index.gdb\Ortho_Tiles

#### SET ENVIRONMENT ####
import arcpy
import os
import pandas as pd
import numpy as np
from dbfread import DBF
from arcgis.features import GeoAccessor, GeoSeriesAccessor

# store parent folder workspace in function 
def reset_ws(): 
    arcpy.env.workspace = os.getcwd()

reset_ws()

# Set overwrite output to true
arcpy.env.overwriteOutput = True

#### DEFINE INPUTS ####

containers = "LinearExtent.gdb\\kelp_containers_v2"
kelp_data_path = "kelp_data_sources\\fixed_wing_aerial_imagery\\"


# MAKE A LIST OF TOUPLES
#for x,y  = [(fp, fc), (type, ype)]

# footprints: 
ADM_fp = kelp_data_path + "Admiralty_Inlet_Flight_Index.gdb\\Flight_Index\\Ortho_Tiles"
NCO_fp = kelp_data_path + "220158_Open_Coast_Flight_Index.gdb\\Flight_Index\\Ortho_Tile_Index"
NPS_fp = kelp_data_path + "220282_North_Puget_Sound_Flight_Index.gdb\\Flight_Index\\Ortho_Tile_Index"
SJF_fp = kelp_data_path + "220158_Strait_Juan_de_Fuca_Flight_Index.gdb\\Flight_Index\\Ortho_Tile_Index"
SJI_fp = kelp_data_path + "220282_San_Juan_Flight_Index.gdb\\Flight_Index\\Ortho_Tile_Index"
SQX_fp = kelp_data_path + "220158_Squaxin_Flight_Index.gdb\\Flight_Index\\Ortho_Tile_Index"
SWH_fp = kelp_data_path + "220158_Saratoga_Whidbey_Flight_Index.gdb\\Flight_Index\\Ortho_Tile_Index"
TAC_fp = kelp_data_path + "220158_Tacoma_Narrows_Flight_Index.gdb\\Flight_Index\\Ortho_Tile_Index"
AQR_fp = kelp_data_path + "220282_Aquatic_Reserves_Flight_Index.gdb\\Flight_Index\\Ortho_Tile_Index"

# polygons: 
ADM_fc = kelp_data_path + "fixed_wing_classified_polygons_2022.gdb\\ADM_2022"
NCO_fc = kelp_data_path + "fixed_wing_classified_polygons_2022.gdb\\NCO_2022"
NPS_fc = kelp_data_path + "fixed_wing_classified_polygons_2022.gdb\\NPS_2022"
SJF_fc = kelp_data_path + "fixed_wing_classified_polygons_2022.gdb\\SJF_2022"
SJI_fc = kelp_data_path + "fixed_wing_classified_polygons_2022.gdb\\SJI_2022"
SQX_fc = kelp_data_path + "fixed_wing_classified_polygons_2022.gdb\\SQX_2022"
SWH_fc = kelp_data_path + "fixed_wing_classified_polygons_2022.gdb\\SWH_2022"
TAC_fc = kelp_data_path + "fixed_wing_classified_polygons_2022.gdb\\TAC_2022"

# merge aquatic reserves fc since there's one flight index file for all of those
CHP_fc = kelp_data_path + "fixed_wing_classified_polygons_2022.gdb\\CHP_2022"
CYP_fc = kelp_data_path + "fixed_wing_classified_polygons_2022.gdb\\CYP_2022"
SMI_fc = kelp_data_path + "fixed_wing_classified_polygons_2022.gdb\\SMI_2022"

AQR_fc = "scratch.gdb\\AQR_2022"
arcpy.management.Merge(
    inputs = (CHP_fc, CYP_fc, SMI_fc), 
    output = AQR_fc
)

#### DEFINE FUNCTIONS ####

# function to get linear extent for each classified raster
# inputs:
## container_path = path to container feature class (fc in .gdb)
## fc_path = path to classified polygon (fc in .gdb)
## footprint_path = path to footprint of area surveyed (i.e. Ortho Tiles, fc in .gdb)
def get_linear_extent_from_classified_polygon(container_path, fc_path, footprint_path):

    # Check spatial reference of inputs
    container_sr = arcpy.Describe(container_path).spatialReference.name
    fc_sr = arcpy.Describe(fc_path).spatialReference.name
    footprint_sr = arcpy.Describe(footprint_path).spatialReference.name

    if container_sr == fc_sr == footprint_sr:
        print("")
    else:
        print("WARNING: Inputs have different spatial references")
        print("Container sr = " + container_sr)
        print("Classified imagery sr = "  + fc_sr)
        print("Footprint sr = " + footprint_sr)

    # Clip containers to survey area footprint
    print("Clipping containers to survey area...")
    arcpy.analysis.Clip(container_path, footprint_path, "scratch.gdb\\containers_clip")
    containers = "scratch.gdb\\containers_clip"

    # Run summarize within function

    fc_desc = arcpy.Describe(fc_path)
    out_fc = "scratch.gdb" + "\\sumwithin" + fc_desc.name

    arcpy.analysis.SummarizeWithin(
        in_polygons = containers,
        in_sum_features = fc_path,
        out_feature_class = out_fc
    ) # save results in scratch gdb 

    print("Summarize Within complete for " + fc_desc.name)
    print("Analysis Complete")


#### Apply Summarize Within ####

# apply to all AOIs
get_linear_extent_from_classified_polygon(containers, ADM_fc, ADM_fp)
get_linear_extent_from_classified_polygon(containers, NCO_fc, NCO_fp)
get_linear_extent_from_classified_polygon(containers, NPS_fc, NPS_fp)
get_linear_extent_from_classified_polygon(containers, SJF_fc, SJF_fp)
get_linear_extent_from_classified_polygon(containers, SJI_fc, SJI_fp)
get_linear_extent_from_classified_polygon(containers, SQX_fc, SQX_fp)
get_linear_extent_from_classified_polygon(containers, SWH_fc, SWH_fp)
get_linear_extent_from_classified_polygon(containers, TAC_fc, TAC_fp)
get_linear_extent_from_classified_polygon(containers, AQR_fc, AQR_fp)

#################
# Convert results to tables and combine
arcpy.env.workspace = "scratch.gdb"
sumwithin_fcs = arcpy.ListFeatureClasses("sum*")
sumwithin_fcs = ["scratch.gdb\\" + fc for fc in sumwithin_fcs]
print(sumwithin_fcs)
reset_ws()

# create function to convert fcs to dfs and store in list
def df_from_fc(in_features, year):
    sdf_list = []
    for feature in in_features:
        
        fc_desc = arcpy.Describe(feature)
        fc_year = year # extract year

        sdf = pd.DataFrame.spatial.from_featureclass(feature) #use geoaccessor to convert fc to df
        sdf = sdf.filter(['SITE_CODE', 'sum_Area_SQUAREKILOMETERS'], axis = 1) #drop unneeded SHAPE cols
        sdf['year'] = fc_year
        sdf['source'] = 'fixedwing'
        sdf['presence'] = np.where(sdf['sum_Area_SQUAREKILOMETERS'] > 0, 1, 0)
        sdf['sum_area_ha'] = sdf['sum_Area_SQUAREKILOMETERS'] * 100


        sdf_list.append(sdf)

        print("Converted " + fc_desc.name + " to sdf and added to list")

    return sdf_list   

# apply function to list of summarize within outputs
sdf_list = df_from_fc(sumwithin_fcs, 2022)

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
all_data.to_csv("kelp_data_synth_results\\fixedwing_poly_synth.csv")
print("Saved as csv here: kelp_data_synth_results\\fixedwing_poly_synth.csv")

# Clear scratch gdb to keep project size down
arcpy.env.workspace = "scratch.gdb"
scratch_fcs = arcpy.ListFeatureClasses()
for fc in scratch_fcs:
    arcpy.Delete_management(fc)
    print(f"Deleted feature class: {fc}")



