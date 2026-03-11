
# function library
import os
import arcpy
import arcpy.management
import arcpy.analysis
import arcpy.conversion
import pandas as pd
import numpy as np
import sys
from arcgis.features import GeoAccessor, GeoSeriesAccessor

arcpy.env.overwriteOutput = True

# store parent folder workspace in function 
def reset_ws(): 
    arcpy.env.workspace = os.getcwd()

# summarize within 

def sum_kelp_within(fc_list, containers, variable_survey_area=False): 
    # fc_list = list of feature class of kelp beds OR paired list of kelp feature classes, kelp survey area
    # containers = containers for summarize within ALREADY CLIPPED TO SURVEY EXTENT if variable_survey_area=False

    # if each year/survey needs its own survey area: 
    if variable_survey_area:
        for kelp_fc, svy_fc in fc_list: 
            print("Beginning sum within for: ")
            print(f"Kelp data: {kelp_fc}")
            print(f"Survey boundary: {svy_fc}")
           
            # clip containers to survey area footprint
            print(f"Clipping containers to {svy_fc}...")
            containers_clip = "scratch.gdb\\containers_clip"
            arcpy.analysis.Clip(containers, svy_fc, containers_clip)
            
            # run summarize within function
            fc_desc = arcpy.Describe(kelp_fc)
            out_fc = f"scratch.gdb\\sumwithin{fc_desc.name}"

            # set env scratch space to env to avoid error 100014
            arcpy.env.scratchWorkspace = arcpy.env.workspace

            print("Running sum within...")
            try:
                arcpy.analysis.SummarizeWithin(
                    in_polygons = containers_clip,
                    in_sum_features = kelp_fc,
                    out_feature_class = out_fc, 
                    shape_unit="HECTARES"
                ) # save results in scratch gdb 

                print("Summarize Within complete for " + fc_desc.name)
            except arcpy.ExecuteError:
                arcpy.AddError(arcpy.GetMessages(2))
            except:
                e=sys.exc_info()[1]
                print(e.args[0])
                

    # if survey area is constant across years, containers are clipped upstream:
    else:
        for fc in fc_list:

            # get the describe object for the feature class
            fc_desc = arcpy.Describe(fc)

            # set the out path to the include the feature class name
            out_fc_path = (f"scratch.gdb/sumwithin{fc_desc.name}".replace(" ",""))
            print(f"Running sum within for {fc_desc.name}...")

            # set env scratch space to env to avoid error 100014
            arcpy.env.scratchWorkspace = arcpy.env.workspace

            try:
                # run summarize within
                arcpy.analysis.SummarizeWithin(
                    in_polygons = containers,
                    in_sum_features = fc,
                    out_feature_class = out_fc_path,
                    shape_unit = "HECTARES"
                )

                print(f"Summarize Within complete for {fc_desc.name}")
            except arcpy.ExecuteError:
                arcpy.AddError(arcpy.GetMessages(2))
            except:
                e=sys.exc_info()[1]
                print(e.args[0])
                
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
    arcpy.env.workspace = os.path.join(os.getcwd(), "scratch.gdb")
    scratch_fcs = arcpy.ListFeatureClasses()
    for fc in scratch_fcs:
        arcpy.Delete_management(fc)
        print(f"Deleted feature class: {fc}")


# tool for calculating proportional presence (abundance) of polygon kelp beds along line segments
def calc_abundance(abundance_containers, kelp_fcs):
    
    #initial result sdf list 
    df_list = []

    # summarize within --> do NOT clip abundance containers to survey area, 
    for fc in kelp_fcs:

        # get the describe object for the feature class
        fc_desc = arcpy.Describe(fc)

        # set the out path to memory
        out_fc = (f"scratch.gdb//ab{fc_desc.name}")

            # try setting scratch env...
        arcpy.env.scratchWorkspace = arcpy.env.workspace 

        # run summarize within
        try: 
            print(f"Running Abundance SummarizeWithin for {fc_desc.name}...")
            arcpy.analysis.SummarizeWithin(
                in_polygons = abundance_containers,
                in_sum_features = fc,
                out_feature_class = out_fc,
                shape_unit = "HECTARES"
            )
            print(f"Result written to {out_fc}")
        except arcpy.ExecuteError:
            arcpy.AddError(arcpy.GetMessages(2))
        except:
            e=sys.exc_info()[1]
            print(e.args[0])

        print("Converting to df...")
        df = pd.DataFrame.spatial.from_featureclass(out_fc)

        # calculate total_length for each SITE_CODE
        df['total_length'] = df.groupby('SITE_CODE')['length_m'].transform('sum')

        # calculate weight of each subdivided section based on original feature length
        df['weight'] = df['length_m'] / df['total_length']

        # calculate presence
        df['presence'] = df['sum_Area_HECTARES'].apply(lambda x: 1 if x > 0 else 0)

        # get weighted presence for each section
        df['w_pres'] = df['weight'] * df['presence']

        # sum weighted presence across site codes
        result = (df.groupby('SITE_CODE')
                .agg(sum_w_pres=('w_pres', 'sum'))
                .reset_index())

        # categorize abundance based on weighted presence 
        print("Calculating abundance...")
        result['abundance'] = pd.cut(result['sum_w_pres'],
                                    bins=[-float('inf'), 0, 0.25, 0.5, 0.75, float('inf')],
                                    labels=[0, 1, 2, 3, 4])
        
        # keep only relevant columns
        result = result[['SITE_CODE', 'abundance']]
        
        # add a column with name of fc input. will need to be uniquely reformatted per data source to get year 
        result['fc_name'] = str(fc_desc.name)

        # view result
        print("Abundance result:")
        print(result.head())

        # delete the feature class from memory
        arcpy.management.Delete(out_fc)

        # append result to df list
        df_list.append(result)

    abundance_results = pd.concat(df_list)
    return (abundance_results)

# create a function to calculate abundance (aka proportional presence) for line-based datasets
# needs to run on a copy of line based datasets that have been filtered to ONLY kelp presence features
def calc_abundance_lines(abundance_containers, kelp_fcs):
    
    #initial result sdf list 
    df_list = []

    # summarize within --> do NOT clip abundance containers to survey area, 
    for fc in kelp_fcs:

        # get the describe object for the feature class
        fc_desc = arcpy.Describe(fc)

        # set the out path to memory
        out_fc = (f"scratch.gdb//ab_{fc_desc.name}")

            # try setting scratch env...
        arcpy.env.scratchWorkspace = arcpy.env.workspace 

        # run summarize within
        try: 
            print(f"Running Abundance SummarizeWithin for {fc_desc.name}...")
            arcpy.analysis.SummarizeWithin(
                in_polygons = abundance_containers,
                in_sum_features = fc,
                out_feature_class = out_fc,
                shape_unit = "METERS"
            )
            print(f"Result written to {out_fc}")
        except arcpy.ExecuteError:
            arcpy.AddError(arcpy.GetMessages(2))
        except:
            e=sys.exc_info()[1]
            print(e.args[0])

        print("Converting to df...")
        df = pd.DataFrame.spatial.from_featureclass(out_fc)

        # calculate total_length for each SITE_CODE
        df['total_length'] = df.groupby('SITE_CODE')['length_m'].transform('sum')

        # calculate weight of each subdivided section based on original feature length
        df['weight'] = df['length_m'] / df['total_length']

        # calculate presence by returning 1 if any kelp presence in that segment 
        df['presence'] = df['sum_Length_METERS'].apply(lambda x: 1 if x > 0 else 0)

        # get weighted presence for each section
        df['w_pres'] = df['weight'] * df['presence']

        # sum weighted presence across site codes
        result = (df.groupby('SITE_CODE')
                .agg(sum_w_pres=('w_pres', 'sum'))
                .reset_index())

        # categorize abundance based on weighted presence 
        print("Calculating abundance...")
        result['abundance'] = pd.cut(result['sum_w_pres'],
                                    bins=[-float('inf'), 0, 0.25, 0.5, 0.75, float('inf')],
                                    labels=[0, 1, 2, 3, 4])
        
        # keep only relevant columns
        result = result[['SITE_CODE', 'abundance']]
        
        # add a column with name of fc input. will need to be uniquely reformatted per data source to get year 
        result['fc_name'] = str(fc_desc.name)

        # view result
        print("Abundance result:")
        print(result.head())

        # delete the feature class from memory
        arcpy.management.Delete(out_fc)

        # append result to df list
        df_list.append(result)

    abundance_results = pd.concat(df_list)
    return (abundance_results)