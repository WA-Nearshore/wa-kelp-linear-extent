
# function library
import os
from pathlib import Path
import arcpy
import pandas as pd
import numpy as np
from arcgis.features import GeoAccessor, GeoSeriesAccessor 

arcpy.env.overwriteOutput = True

# utilities ---------------------------------------------------------------------------------------------
# store parent folder workspace in function 
def reset_ws(PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))): 
    """
    Resets the arcpy workspace to the project root folder
    By default, the root folder is the the folder that contains the kelp_linear_extent package (three folders up from current script)
    """
    arcpy.env.workspace = PROJECT_ROOT

# configure a scratch workspace
def config_scratch(PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))):
    """
    Creates a scratch.gdb or clears scratch.gdb if it already exists.
    Scratch ws will be in project root by default. is the folder containing the kelp_linear_extent package.
    Optionally specify a different parent folder for scratch.gdb using PROJECT_ROOT = "".
    Returns the scratch workspace as a file path, to be used as a variable elsewhere in the script. 
    """
    SCRATCH_WS = os.path.join(PROJECT_ROOT, "scratch.gdb")
    print("Configuring scratch workspace...")
    if not arcpy.Exists(SCRATCH_WS):
        arcpy.management.CreateFileGDB(PROJECT_ROOT, "scratch.gdb")
        print(f"Created new gdb at {SCRATCH_WS}")
    else:
        print(f"Scratch workspace already exists at {SCRATCH_WS}. Clearing files... ")
        clear_scratch(SCRATCH_WS)

    return SCRATCH_WS

# clear scratch workspace
def clear_scratch(SCRATCH_WS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scratch.gdb")):   
    """
    Clears the default scratch workspace. Useful at the end of analysis. Optionally, set to a different gdb to delete all feature classes. 
    """
    arcpy.env.workspace = SCRATCH_WS
    scratch_fcs = arcpy.ListFeatureClasses()
    for fc in scratch_fcs:
        arcpy.Delete_management(fc)
        print(f"Deleted feature class: {fc}")


# main tools ------------------------------------------------------------------------------------
# function to calculate presence
def sum_kelp_within(fc_list, containers, SCRATCH_WS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scratch.gdb"), 
                    variable_survey_area=False, kelp_geometry_type="polygon"): 
    """
    * **fc_list**: list of feature class of kelp beds OR paired list of kelp feature classes, kelp survey area if variable_survey_area=True
    * **containers**: for summarize within ALREADY CLIPPED TO SURVEY EXTENT if variable_survey_area=False
    * **SCRATCH_WS**: workspace for outputting sumwithin results; defaults to path from config_scratch
    * **variable_survey_area**: defaults to FALSE if the same area was surveyed every year. Change to TRUE if any years had different survey area
    * **kelp_geometry_type**: "polygon" [default] when kelp presence is polygons or "line" if kelp presence is lines
    """
    if kelp_geometry_type == "polygon":
        unit = "HECTARES"
    elif kelp_geometry_type == "line":
        unit = "METERS"
    else:
        raise ValueError("kelp_geometry_type must be line or polygon")

    # intialize list of output fcs
    sumwithin_fcs = []

    # if each year/survey needs its own survey area: 
    if variable_survey_area:
        for kelp_fc, svy_fc in fc_list: 
            print("Beginning sum within for: ")
            print(f"Kelp data: {kelp_fc}")
            print(f"Survey boundary: {svy_fc}")

            # clip containers to survey area footprint
            print("Clipping containers to survey boundary...")
            containers_clip = os.path.join(SCRATCH_WS, "containers_clip")
            arcpy.analysis.Clip(containers, svy_fc, containers_clip)
            
            # get the describe object for the kelp feature class
            fc_desc = arcpy.Describe(kelp_fc)

            # set the out path for each fc 
            out_fc = os.path.join(SCRATCH_WS, f"sum{fc_desc.name}".replace(" ",""))
            sumwithin_fcs.append(out_fc)

            print(f"Running sum within for {fc_desc.name}...")
            try:
                # run summarize within
                arcpy.analysis.SummarizeWithin(
                    in_polygons = containers_clip,
                    in_sum_features = kelp_fc,
                    out_feature_class = out_fc, 
                    shape_unit=unit
                ) # save results in scratch gdb 

                print("Summarize Within complete for " + fc_desc.name)
            except arcpy.ExecuteError: 
                print(f"Failed to generate {out_fc}")
                arcpy.AddError(arcpy.GetMessages())
                break
            except Exception as e:
                print(e.args[0])
                break

    # if survey area is constant across years, containers are clipped upstream in the linearizing script
    else:
        for fc in fc_list:

            # get the describe object for the feature class
            fc_desc = arcpy.Describe(fc)

            # Set the out path for each fc 
            out_fc = os.path.join(SCRATCH_WS, f"sum{fc_desc.name}".replace(" ",""))
            sumwithin_fcs.append(out_fc)

            print(f"Running sum within for {fc_desc.name}...")
            try:
                # run summarize within
                arcpy.analysis.SummarizeWithin(
                    in_polygons = containers,
                    in_sum_features = fc,
                    out_feature_class = out_fc,
                    shape_unit = unit
                )

                print(f"Summarize Within complete for {fc_desc.name}")
            except arcpy.ExecuteError:
                arcpy.AddError(arcpy.GetMessages())
                break
            except Exception as e:
                print(e.args[0])
                break

    # return list of resulting feature classes        
    return sumwithin_fcs
                    
# feature class to dataframe
def df_from_fc(in_features, source_name, kelp_geometry_type="polygon"):

    """
    converts list of features into a list of formatted dataframes
    * **in_features**: list of feature classes to convert to dataframes 
    * **source_name**: string to be used as source name in table
    * **kelp_geometry_type**: "polygon" [default] when kelp presence is polygons or "line" if kelp presence is lines
    * note: input features MUST have year as last 4 characters of name for this to work 
    """
    if kelp_geometry_type == "polygon":
        pres_col = "sum_Area_HECTARES"
    elif kelp_geometry_type == "line":
        pres_col = "sum_Length_METERS"
    else:
        raise ValueError("kelp_geometry_type must be line or polygon")

    # if each year/survey needs its own survey area: 
    sdf_list = []
    for feature in in_features:
        
        fc_desc = arcpy.Describe(feature)

        sdf = pd.DataFrame.spatial.from_featureclass(feature) 

        sdf = sdf.filter(['SITE_CODE', pres_col], axis = 1) #drop unneeded SHAPE cols
        sdf['year'] = fc_desc.name[-4:]
        sdf['source'] = source_name
        sdf['presence'] = np.where(sdf[pres_col] > 0, 1, 0)

        sdf_list.append(sdf)
        print("Converted " + fc_desc.name + " to sdf and added to list")

    return sdf_list    

# tool for calculating coverage category of polygon kelp beds along line segments
def calc_cov_cat(cov_cat_containers, kelp_fcs, SCRATCH_WS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scratch.gdb"), 
                                                                         kelp_geometry_type = "polygon"):
    """
    Calculates coverage category for polygon kelp presence features 
    * **cov_cat_containers**: feature class with the subdivided containers
    * **kelp_fcs**: list of feature classes with kelp presence polygons to be analyzed
    * **kelp_geometry_type**: "polygon" [default] when kelp presence is polygons or "line" if kelp presence is lines
    * note, lines must be ONLY presence lines (filter out absence lines upstream)
    * **PROJECT_ROOT**: path to the parent folder 
    """
    #initial result sdf list 
    df_list = []
    
    if kelp_geometry_type == "polygon":
        unit = "HECTARES"
        pres_col = "sum_Area_HECTARES"
    elif kelp_geometry_type == "line":
        unit = "METERS"
        pres_col = "sum_Length_METERS"


    # summarize within --> do NOT clip cov cat containers to survey area, 
    for fc in kelp_fcs:

        # get the describe object for the feature class
        fc_desc = arcpy.Describe(fc)

        # set the out path for the analyzed feature classes 
        out_fc = os.path.join(SCRATCH_WS, f"ab{fc_desc.name}")

        # run summarize within
        try: 
            print(f"Running Coverage Category SummarizeWithin for {fc_desc.name}...")
            print(f"Results will be written to {out_fc}")
            arcpy.analysis.SummarizeWithin(
                in_polygons = cov_cat_containers,
                in_sum_features = fc,
                out_feature_class = out_fc,
                shape_unit = unit
            )
            print(f"Result written to {out_fc}")
        except arcpy.ExecuteError:
            arcpy.AddError(arcpy.GetMessages(2))
            break
        except Exception as e:
            print(e.args[0])
            break

        print("Converting to df...")
        df = pd.DataFrame.spatial.from_featureclass(out_fc)

        # calculate total_length for each SITE_CODE
        df['total_length'] = df.groupby('SITE_CODE')['length_m'].transform('sum')

        # calculate weight of each subdivided section based on original feature length
        df['weight'] = df['length_m'] / df['total_length']

        # calculate presence
        df['presence'] = df[pres_col].apply(lambda x: 1 if x > 0 else 0)

        # get weighted presence for each section
        df['w_pres'] = df['weight'] * df['presence']

        # sum weighted presence across site codes
        result = (df.groupby('SITE_CODE')
                .agg(sum_w_pres=('w_pres', 'sum'))
                .reset_index())

        # categorize cov cat based on weighted presence 
        print("Calculating coverage category...")
        result['coverage_cat'] = pd.cut(result['sum_w_pres'],
                                    bins=[-float('inf'), 0, 0.25, 0.5, 0.75, float('inf')],
                                    labels=[0, 1, 2, 3, 4])
        
        # keep only relevant columns
        result = result[['SITE_CODE', 'coverage_cat']]
        
        # add a column with name of fc input. will need to be uniquely reformatted per data source to get year 
        result['fc_name'] = str(fc_desc.name)

        # view result
        print("Abundance result:")
        print(result.head())

        # delete the feature class from memory
        arcpy.management.Delete(out_fc)

        # append result to df list
        df_list.append(result)

    cov_cat_results = pd.concat(df_list)
    return cov_cat_results