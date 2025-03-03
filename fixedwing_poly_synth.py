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

sys.path.append(os.getcwd())

# import the project function library 
import fns

arcpy.env.overwriteOutput = True

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

# compile to list of paired values
fc_list = [(polygons[key], flight_indices[key]) for key in polygons]

# print data 
print("Data to be analyzed: ")
for kelp, svy in fc_list:
    print(f"Kelp data: {kelp}")
    print(f"Survey boundary: {svy}")

# append parent folder to all elements
fc_list = [[kelp_data_path + kelp, kelp_data_path + svy] for kelp, svy in fc_list]

# calculate presence ------------------------------------------------------
fns.sum_kelp_within(fc_list, containers, variable_survey_area=True)

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
print(f"Total records: {len(presence)}")

# calculate abundance ------------------------------------------------------
print("Calculating abundance....")
abundance_containers = "LinearExtent.gdb\\abundance_containers"
kelp_fcs = [pair[0] for pair in fc_list]
abundance = fns.calc_abundance(abundance_containers, kelp_fcs)

# add the year col
abundance['year'] = abundance['fc_name'].str[-4:]
abundance = abundance.drop(columns=['fc_name'])
print("Reformatted abundance table:")
print(abundance.head())

# tidy and export ----------------------------------------------------------

# check if site codes are unique --> this logic needs to be updated when more years are added 
def check_unique(df, field):
    if df['SITE_CODE'].is_unique == False:
        print("The following sites have more than 1 record for the most recent year:")
        dupes = df[df.duplicated('SITE_CODE', keep=False) == True].sort_values('SITE_CODE')
        print(dupes)

        print(f"Selecting record with largest value for {field}")
        # If multiple records exist for a single site, select the one with max kelp area 
        max_presence_per_site = df.groupby('SITE_CODE')[field].transform('max')

        # Grab those rows
        all_data_max_pres = df[df[field] == max_presence_per_site]

        # Drop the remaining duplicates (should just be where presence = 0 for both)
        df = all_data_max_pres.drop_duplicates()
        print('All sites now have a single record')

        return df

    else:
        print("All sites have unique records from fixed wing dataset")


print("Tidying presence data...")
presence = check_unique(presence, 'sum_Area_HECTARES')
print("Tidying abundance data...")
abundance = check_unique(abundance, 'abundance')

results = pd.merge(presence, abundance, how="left", on=["SITE_CODE", "year"])
print(f"Total records: {len(results)}")

# Write to csv
out_results = "kelp_data_synth_results\\fixedwing_poly_synth.csv"
results.to_csv(out_results)
print(f"Saved as csv here: {out_results}")

# clear scratch
fns.clear_scratch()



