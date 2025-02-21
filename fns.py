
# function library
import os
import arcpy
import arcpy.management
import arcpy.analysis
import arcpy.conversion
import pandas as pd
import numpy as np
from arcgis.features import GeoAccessor, GeoSeriesAccessor

arcpy.env.overwriteOutput = True

# store parent folder workspace in function 
def reset_ws(): 
    arcpy.env.workspace = os.getcwd()

# summarize within 

def sum_kelp_within(fc_list, containers): 
    # fc_list = list of feature class of kelp beds
    # containers = containers for summarize within

    for fc in fc_list:

        # get the describe object for the feature class
        fc_desc = arcpy.Describe(fc)

        # set the out path to the include the feature class yer
        out_fc_path = (f"scratch.gdb/sumwithin{fc_desc.name}")

        # run summarize within
        arcpy.analysis.SummarizeWithin(
            in_polygons = containers,
            in_sum_features = fc,
            out_feature_class = out_fc_path,
            shape_unit = "HECTARES"
        )

        print(f"Summarize Within complete for {fc_desc.name}")

# feature class to dataframe

def df_from_fc(in_features, source_name):
    # in_features: list of feature classes to convert to dataframes 
    # source_name: string to be used as source name in table
    # note: input features MUST have year as last 4 characters of name for this to work 
    sdf_list = []
    for feature in in_features:
        
        fc_desc = arcpy.Describe(feature)

        sdf = pd.DataFrame.spatial.from_featureclass(feature) 
        sdf = sdf.filter(['SITE_CODE', 'sum_Area_HECTARES'], axis = 1) #drop unneeded SHAPE cols
        sdf['year'] = fc_desc.name[-4:]
        sdf['source'] = source_name
        sdf['presence'] = np.where(sdf['sum_Area_HECTARES'] > 0, 1, 0)

        sdf_list.append(sdf)

        print("Converted " + fc_desc.name + " to sdf and added to list")

    return sdf_list    

# clear scratch to keep project size down 
def clear_scratch():
    arcpy.env.workspace = "scratch.gdb"
    scratch_fcs = arcpy.ListFeatureClasses()
    for fc in scratch_fcs:
        arcpy.Delete_management(fc)
        print(f"Deleted feature class: {fc}")
