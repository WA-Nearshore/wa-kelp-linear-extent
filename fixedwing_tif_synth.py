## Summarize Classified Imagery from Fixed Wing Aerial Program
## For linear extent dataset 
# Gray McKenna
# 2024-08-30

# Classified Imagery from Fixed Wing Aerial Imagery Program
# Copied ADM 2024-8-30, SQX & SWH 2024-09-04
# Classified rasters: K:\kelp\fixed_wing_aerial_imagery\imagery_products\2022\classified_rasters
# Ortho tiles (footprints): K:\kelp\fixed_wing_aerial_imagery\imagery_products\2022\orthomosaics\ADM\GIS_data\Admiralty_Inlet_Flight_Index.gdb\Ortho_Tiles


### CURRENT ISSUE WITH THIS SCRIPT: NOT RETURNING 0s FOR ABSENCE, MOSTLY RETURNING NAS


#### SET ENVIRONMENT ####
import arcpy
import os
import pandas as pd
import numpy as np
from dbfread import DBF

project_folder = r"D:\DNR_files\Documents\ArcGIS\Projects\LinearExtent"
arcpy.env.workspace = project_folder

# Set overwrite output to true
arcpy.env.overwriteOutput = True

# Use 75% of cores on machine
arcpy.env.parallelProcessingFactor = "75%"

#### DEFINE FUNCTIONS ####

# function to get linear extent for each classified raster
# inputs:
## container_path = path to container feature class (fc in .gdb)
## tif_path = path to classified raster (tif)
## footprint_path = path to footprint of area surveyed (i.e. Ortho Tiles, fc in .gdb)
def get_linear_extent_from_classified_raster(container_path, tif_path, footprint_path):

    # Check spatial reference of inputs
    container_sr = arcpy.Describe(container_path).spatialReference.name
    tif_sr = arcpy.Describe(tif_path).spatialReference.name
    footprint_sr = arcpy.Describe(footprint_path).spatialReference.name

    if container_sr == tif_sr == footprint_sr:
        print("")
    else:
        print("WARNING: Inputs have different spatial references")
        print("Container sr = " + container_sr)
        print("Classified imagery sr = "  + tif_sr)
        print("Footprint sr = " + footprint_sr)

    # Clip containers to survey area footprint
    print("Clipping containers to survey area...")
    arcpy.analysis.Clip(container_path, footprint_path, "scratch.gdb\\containers_clip")
    containers = "scratch.gdb\\containers_clip"

    # Calculate zonal stats
    arcpy.CheckOutExtension("Spatial")

    # define output 
    out_table_name = "kelp_data_synth_results\\" + tif_path[-29:-21] + ".dbf"
    
    print("Running zonal stats for " + tif_path[-29:-21] + "...")
    arcpy.sa.ZonalStatisticsAsTable(
    in_zone_data = containers,
    zone_field = "SITE_CODE",
    in_value_raster = tif_path,
    out_table = out_table_name,
    statistics_type = "SUM")

    # convert dbf to dataframe - this will not return any values for containers outside of masked area
    print("Converting to zonal stats table to dataframe...")
    df = pd.DataFrame(list(DBF(out_table_name)))
  
    # delete dbf
    os.remove(out_table_name)

    print("Analysis Complete")
    return df

# function to reformat zonal stats result table to match master synthesis table schema
# inputs
## df = df resulting from get_linear_extent_from_classified_raster
## imagery_year = year of data collection 
def reformat_df(df, imagery_year):
    print("Reformatting dataframes to match synthesis schema...")
    df = df.filter(['SITE_CODE', 'SUM'], axis = 1) #drop unneeded cols
    df['sum_Area_SQUAREKILOMETERS'] = df['SUM'] * 9.2903e-8 # convert cells to km^2
    df['year'] = imagery_year
    df['source'] = 'fixed_winged_imagery'
    df['presence'] = np.where(df['SUM'] > 0, 1, 0)
    df['sum_area_ha'] = df['sum_Area_SQUAREKILOMETERS'] * 100
    df = df.drop(['SUM'], axis = 1)
    print("Dataframe new schema:")
    print(df.head())
    return df

# function to apply other two functions in a loop
# tif_list = list of classified rasters (tifs)
# footprint_list = list of survey area footprints (i.e. ortho tiles)
# order of AOIs must be the same in both lists 
def batch_calc_linear_extent(container_path, tif_list, footprint_list, imagery_year): 

    # initialize empty list
    df_list= []

    # loop through tifs corresponding footprints to calc linear extent
    for tif, footprint in zip(tif_list, footprint_list):
        df = get_linear_extent_from_classified_raster(container_path, tif, footprint)
        formatted_df = reformat_df(df, imagery_year)
        df_list.append(formatted_df)

    results = pd.concat(df_list)
    print("Results combined to one dataframe")
    return results

#### APPLY FUNCTIONS ####
# define vars
containers = "LinearExtent.gdb\\kelp_containers_v1_fixsitecode"

kelp_data_path = "kelp_data_sources\\fixed_wing_aerial_imagery\\"
# tifs: 
ADM = kelp_data_path + "ADM_2022_ClassifiedRaster.tif"
SQX = kelp_data_path + "SQX_2022_ClassifiedRaster.tif"
SWH = kelp_data_path + "SWH_2022_ClassifiedRaster.tif"
NPS = kelp_data_path + "NPS_2022_ClassifiedRaster.tif"
CYP = kelp_data_path + "CYP_2022_ClassifiedRaster.tif"
#tifs = [ADM, SQX, SWH]


# footprints: 
ADM_fp = kelp_data_path + "Admiralty_Inlet_Flight_Index.gdb\\Flight_Index\\Ortho_Tiles"
SQX_fp = kelp_data_path + "220158_Squaxin_Flight_Index.gdb\\Flight_Index\\Ortho_Tile_Index"
SWH_fp = kelp_data_path + "220158_Saratoga_Whidbey_Flight_Index.gdb\\Flight_Index\\Ortho_Tile_Index"
NPS_fp = kelp_data_path +  "220282_North_Puget_Sound_Flight_Index.gdb\\Flight_Index\\Ortho_Tile_Index"
CYP_fp = kelp_data_path + ""

#footprints = [ADM_fp, SQX_fp, SWH_fp]

#batch_calc_linear_extent(containers, tifs, footprints, 20  22)
# the batch function is currently throwing an error when it gets to SQX

# run funs individually for AOIs

ADM_df = get_linear_extent_from_classified_raster(containers, ADM, ADM_fp)
ADM_df_format = reformat_df(ADM_df, 2022)
ADM_df_format.to_csv("kelp_data_synth_results\\ADM_2022_synth.csv")

SWH_df = get_linear_extent_from_classified_raster(containers, SWH, SWH_fp)
SWH_df_format = reformat_df(SWH_df, 2022)
SWH_df_format.to_csv("kelp_data_synth_results\\SWH_2022_synth.csv")

NPS_df = get_linear_extent_from_classified_raster(containers, NPS, NPS_fp)
NPS_df_format = reformat_df(NPS_df, 2022)
NPS_df_format.to_csv("kelp_data_synth_results\\NPS_2022_synth.csv")


