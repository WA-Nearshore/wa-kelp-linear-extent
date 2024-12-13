#### SET ENVIRONMENT ####
import arcpy
import os
import pandas as pd
import numpy as np
from dbfread import DBF
from arcgis.features import GeoAccessor, GeoSeriesAccessor

# Set project folder and workspace
project_folder = r"E:\DNR_files\Documents\ArcGIS\Projects\LinearExtent"
arcpy.env.workspace = project_folder
arcpy.env.overwriteOutput = True

# Helper function to create file paths dynamically
def get_file_path(base_dir, subfolder, file_name, extension='.gdb'):
    return os.path.join(base_dir, subfolder, file_name + extension)

# Define data paths and feature classes
kelp_data_path = r"kelp_data_sources\fixed_wing_aerial_imagery"

flight_index_files = {
    'ADM': 'Admiralty_Inlet_Flight_Index',
    'NCO': '220158_Open_Coast_Flight_Index',
    'NPS': '220282_North_Puget_Sound_Flight_Index',
    'SJF': '220158_Strait_Juan_de_Fuca_Flight_Index',
    'SJI': '220282_San_Juan_Flight_Index',
    'SQX': '220158_Squaxin_Flight_Index',
    'SWH': '220158_Saratoga_Whidbey_Flight_Index',
    'TAC': '220158_Tacoma_Narrows_Flight_Index',
    'AQR': '220282_Aquatic_Reserves_Flight_Index'
}

# Footprint paths
footprint_paths = {key: get_file_path(kelp_data_path, f"{val}_Flight_Index.gdb\\Flight_Index", "Ortho_Tile_Index") 
                   for key, val in flight_index_files.items()}

# Polygons paths
polygon_paths = {key: get_file_path(kelp_data_path, "fixed_wing_classified_polygons_2022.gdb", f"{key}_2022") 
                 for key in flight_index_files.keys()}

# Merge aquatic reserves polygons
aquatic_reserves = ['CHP', 'CYP', 'SMI']
AQ_reserves_fc = "scratch.gdb\\AQR_2022"
arcpy.management.Merge(inputs=[polygon_paths[res] for res in aquatic_reserves], output=AQ_reserves_fc)

#### DEFINE FUNCTIONS ####

# Function to check and log spatial reference consistency
def check_spatial_reference(*inputs):
    srs = [arcpy.Describe(input).spatialReference.name for input in inputs]
    if len(set(srs)) != 1:
        print(f"WARNING: Inputs have different spatial references: {srs}")
    return srs[0]  # return the reference if valid

# Function to process linear extent from classified polygons
def get_linear_extent_from_classified_polygon(container_fc, polygon_fc, footprint_fc):
    print(f"Processing {polygon_fc}...")

    # Check spatial reference consistency
    check_spatial_reference(container_fc, polygon_fc, footprint_fc)

    # Clip containers to survey area
    containers_clip = "scratch.gdb\\containers_clip"
    arcpy.analysis.Clip(container_fc, footprint_fc, containers_clip)

    # Perform summarize within
    fc_desc = arcpy.Describe(polygon_fc)
    out_fc = f"scratch.gdb\\sumwithin_{fc_desc.name}"
    arcpy.analysis.SummarizeWithin(containers_clip, polygon_fc, out_fc)
    print(f"Summarize Within completed for {fc_desc.name}")

# Apply Summarize Within to all AOIs (Area of Interest)
for key in footprint_paths:
    get_linear_extent_from_classified_polygon(containers, polygon_paths[key], footprint_paths[key])

#### Convert results to DataFrames and combine ####
arcpy.env.workspace = "scratch.gdb"
sumwithin_fcs = arcpy.ListFeatureClasses("sum*")
sumwithin_fcs = [os.path.join("scratch.gdb", fc) for fc in sumwithin_fcs]
arcpy.env.workspace = project_folder

# Function to convert feature class to pandas DataFrame
def convert_fc_to_df(feature_class):
    print(f"Converting {feature_class} to DataFrame...")
    fc_desc = arcpy.Describe(feature_class)
    year = str(fc_desc.name[-4:])  # Extract year from feature class name

    sdf = pd.DataFrame.spatial.from_featureclass(feature_class)
    sdf = sdf[['SITE_CODE', 'sum_Area_SQUAREKILOMETERS']]
    sdf['year'] = year
    sdf['source'] = 'fixedwing'
    sdf['presence'] = (sdf['sum_Area_SQUAREKILOMETERS'] > 0).astype(int)
    sdf['sum_area_ha'] = sdf['sum_Area_SQUAREKILOMETERS'] * 100  # Convert to hectares
    return sdf

# Convert all summarize within results to DataFrames
dfs = [convert_fc_to_df(fc) for fc in sumwithin_fcs]

# Combine all DataFrames into one
all_data = pd.concat(dfs, ignore_index=True)
print(f"All data merged into a single DataFrame with {len(all_data)} rows.")

# Save to CSV
output_csv = r"kelp_data_synth_results\fixedwing_poly_synth.csv"
all_data.to_csv(output_csv, index=False)
print(f"Saved combined results to: {output_csv}")

# Clean up scratch gdb
arcpy.env.workspace = "scratch.gdb"
for fc in arcpy.ListFeatureClasses():
    arcpy.Delete_management(fc)
    print(f"Deleted feature class: {fc}")
