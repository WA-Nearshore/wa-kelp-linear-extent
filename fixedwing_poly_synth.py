# Summarize Classified Imagery POLYGONS from Fixed Wing Aerial Program
# For linear extent dataset 

# Classified Imagery from Fixed Wing Aerial Imagery Program 
# Copies of rasters brought down from K: on 2024-08-30, polygonized on 2024-11-26
# Classified polygons: K:\kelp\fixed_wing_aerial_imagery\imagery_products\2022\classified_polygons\fixed_wing_classified_polygons_2022.gdb
# Ortho tiles (footprints): K:\kelp\fixed_wing_aerial_imagery\imagery_products\2022\orthomosaics\ADM\GIS_data\Admiralty_Inlet_Flight_Index.gdb\Ortho_Tiles (etc)

# Note: CHP, CYP, SMI were manually merged to one fc (AQR) because there is only 1 ortho tile index for all of them

# set environment ------------------------------------------------------
import arcpy
import os
import pandas as pd
import numpy as np
from dbfread import DBF
import sys
from arcgis.features import GeoAccessor, GeoSeriesAccessor

# import the project function library 
import fns

arcpy.env.overwriteOutput = True

# set workspace to parent folder
fns.reset_ws()

# prep data ----------------------------------------------------------

containers = "LinearExtent.gdb\\kelp_containers_v2"
kelp_data_path = "kelp_data_sources\\fixed_wing_aerial_imagery\\"

# set dictionary of AOI survey areas (flight_index\\ortho_tiles)
flight_indices = {
    "ADM": "Admiralty_Inlet_Flight_Index.gdb\\Flight_Index\\Ortho_Tiles",
    "NCO": "220158_Open_Coast_Flight_Index.gdb\\Flight_Index\\Ortho_Tile_Index",
    "NPS": "220282_North_Puget_Sound_Flight_Index.gdb\\Flight_Index\\Ortho_Tile_Index",
    "SJF": "220158_Strait_Juan_de_Fuca_Flight_Index.gdb\\Flight_Index\\Ortho_Tile_Index",
    "SJI": "220282_San_Juan_Flight_Index.gdb\\Flight_Index\\Ortho_Tile_Index",
    "SQX": "220158_Squaxin_Flight_Index.gdb\\Flight_Index\\Ortho_Tile_Index",
    "SWH": "220158_Saratoga_Whidbey_Flight_Index.gdb\\Flight_Index\\Ortho_Tile_Index",
    "TAC": "220158_Tacoma_Narrows_Flight_Index.gdb\\Flight_Index\\Ortho_Tile_Index",
    "AQR": "220282_Aquatic_Reserves_Flight_Index.gdb\\Flight_Index\\Ortho_Tile_Index"
}

# set dictionary for classified polygons
polygons = {key: f"fixed_wing_classified_polygons_2022.gdb\\{key}_2022" for key in flight_indices}

# compile to list of paired values, including Aquatic Reserves separately
fc_list = [(polygons[key], flight_indices[key]) for key in polygons]

# print data 
print("Data to be analyzed: ")
for kelp, svy in fc_list:
    print(f"Kelp data: {kelp}")
    print(f"Survey boundary: {svy}")

# append parent folder to all elements
fc_list = [[kelp_data_path + kelp, kelp_data_path + svy] for kelp, svy in fc_list]

# calculate presence ------------------------------------------------------
fns.sum_kelp_within(fc_list, containers, unique_survey=True)

# convert results to tables and combine
arcpy.env.workspace = os.path.join(os.getcwd(), "scratch.gdb")
sumwithin_fcs = arcpy.ListFeatureClasses("sum*")
sumwithin_fcs = [f"scratch.gdb\\{fc}" for fc in sumwithin_fcs]
print("Fcs to convert to tables:")
print(sumwithin_fcs)
fns.reset_ws()

sdf_list = fns.df_from_fc(sumwithin_fcs, "WADNR_FixedWingAerialImagery")

print("This is the structure of the sdfs:")
print(sdf_list[1].head())

# merge to one df
print("Combining to one dataframe")
presence = pd.concat(sdf_list)

# calculate abundance ------------------------------------------------------
abundance_containers = "LinearExtent.gdb\\abundance_containers"
abundance = fns.calc_abundance(abundance_containers, fc_list, unique_survey=True)

# add the year col
abundance['year'] = abundance['fc_name'].str[-4:]
abundance = abundance.drop(columns=['fc_name'])

# tidy and export ----------------------------------------------------------

# check if site codes are unique --> this logic needs to be updated when more years are added 
try: 
    if presence['SITE_CODE'].is_unique == False:
        print("The following sites have more than 1 record for the most recent year:")
        dupes = presence[presence.duplicated('SITE_CODE', keep=False) == True].sort_values('SITE_CODE')
        print(dupes)

        print("Selecting record with larger kelp area...")
        # If multiple records exist for a single site, select the one with max kelp area 
        max_presence_per_site = presence.groupby('SITE_CODE')['sum_Area_HECTARES'].transform('max')

        # Grab those rows
        all_data_max_pres = presence[presence['sum_area_ha'] == max_presence_per_site]

        # Drop the remaining duplicates (should just be where presence = 0 for both)
        presence = all_data_max_pres.drop_duplicates()

    else:
        print("All sites have unique records from fixed wing dataset")
except: 
    print("Unable to check for duplicative records")

print("All years of data have been merged to one df")

results = pd.merge(presence, abundance, how="left", on=["SITE_CODE", "year"])

# Write to csv
out_results = "kelp_data_synth_results\\fixedwing_poly_synth_TEST.csv"
results.to_csv(out_results)
print(f"Saved as csv here: {out_results}")





