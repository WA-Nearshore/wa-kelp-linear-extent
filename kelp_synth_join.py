# Combine linear extent data synthesis results and join to lines

import arcpy
import arcpy.management
import pandas as pd
import numpy as np
from pathlib import Path
import os
import datetime
from arcgis.features import GeoAccessor, GeoSeriesAccessor

# set environment -----------------------------------------------------------------------

arcpy.env.overwriteOutput = True

# store parent folder workspace in function 
def reset_ws(): 
    arcpy.env.workspace = os.getcwd()

reset_ws()

# load data -------------------------------------------------------------------------------

# Kelp data summarize within results tables
synth_folder = Path("kelp_data_synth_results")
tbls = list(synth_folder.glob('*.csv'))
print("Synth results tables available:")
for t in tbls: print(t)

# linear extent fc 
lines = "LinearExtent.gdb//all_lines_clean_v2"
print(f"Using {lines} as line segment feature class")

# define functions -----------------------------------------------------------------------

def csv_to_pd(tbls):

    pd.set_option('display.max_columns', 7)
    
    dfs = []

    for t in tbls:
        df = pd.read_csv(t)
        print(f"{t} converted to dataframe:")
        print(df.head())
        print(df.dtypes)
        print(f"Total records: {len(df)}")
        dfs.append(df)

    return dfs

def combine_results(synth_dfs):
    pd.set_option('display.max_rows', 7)
    #### Bind rows ####
    all_synth = pd.concat(synth_dfs)
    print("Joined results df: ")
    print(all_synth.head(5))

    # handle instances where presence and abundance disagree
    # if presence == 0 and abundance > 0, make abundance 0
    # if presence == 1 and abundance == 0, make abundance 1
    all_synth['abundance'] = all_synth.apply(
    lambda row: 0 if row['presence'] == 0 and row['abundance'] > 0 else
                1 if row['presence'] == 1 and row['abundance'] == 0 else
                row['abundance'], axis=1)
    
    # save this as the 'all_records" table. 
    print("Compiled all results and written to csv: all_records.csv" )
    all_synth.to_csv("all_records.csv")
    print(f"Total records: {len(all_synth)}")

    #### Select most recent year for each site_code ####

    # Find most recent year for each SITE_CODE
    most_recent_year = all_synth.groupby('SITE_CODE')['year'].transform('max')

    # Grab those rows
    most_recent = all_synth[all_synth['year'] == most_recent_year]

    # Check if site_code is unique
    if most_recent['SITE_CODE'].is_unique == False:
        print("The following sites have more than 1 record for the most recent year:")
        dupes = most_recent[most_recent.duplicated('SITE_CODE', keep=False) == True].sort_values('SITE_CODE')
        print(dupes)
        print("Selecting source with maximum area")
    else:
        print("All sites have unique records for most recent year")
    
    # count number of records for most recent year
    most_recent.loc[:, 'n_records_most_rec'] = most_recent.groupby('SITE_CODE')['SITE_CODE'].transform('count')

    # for years with multiple records, select row with max kelp area 
    most_recent['sum_Area_HECTARES'].fillna(-9999, inplace=True) # replace NULL values with -9999 so the below code works
    most_rec_max = most_recent[most_recent['sum_Area_HECTARES'] == most_recent.groupby('SITE_CODE')['sum_Area_HECTARES'].transform('max')]

    # Set index to site_code 
    most_rec_max =  most_rec_max.set_index('SITE_CODE')

    # Drop this random garbage column
    most_rec_max =  most_rec_max.drop(['Unnamed: 0'], axis = 1)

    print("Preview of most recent year table:")
    print(most_rec_max.head(5))

    # write to a csv
    most_rec_max.to_csv('most_recent.csv')
    print("Written to csv: most_recent.csv")

def join_results_to_lines(tbl, lines, out_lines, all_records=False):
    #### Join to line segments fc ####
    # note to self: need to delete the extra fields length_m and SITE_CODE_1 from the outputs 
    # also: could use fieldmapping to set the field types more appropriately here 

    if all_records:
        print("Converting all_records and lines to pd dataframes...")
        # convert lines to a sdf 
        sdf = pd.DataFrame.spatial.from_featureclass(lines)

        # load tbl to dataframe
        tbl_df = pd.read_csv(tbl)
        print(f"{len(tbl_df)} records in table")

        # join one-to-many
        print("Merging...")
        joined = pd.merge(sdf, tbl_df, how="outer", on="SITE_CODE")
        print(f"Resulting table has {len(joined)} records")
        print(joined.head())

        # write to feature class
        print("Writing to feature class...")
        joined.spatial.to_featureclass(location=out_lines, overwrite=True)
        print(f"Feature class created: {out_lines}") 

    else:
        # Copy all lines feature
        arcpy.management.CopyFeatures(lines, out_lines)
        print("Copied " + lines + " for join")

        # Validate join
        arcpy.management.ValidateJoin(out_lines, 'SITE_CODE', tbl, 'SITE_CODE')
        print(arcpy.GetMessages())

        # Join
        arcpy.management.JoinField(
            in_data = out_lines,
            in_field = 'SITE_CODE',
            join_table = tbl,
            join_field = 'SITE_CODE'
        )

        print('Most recent kelp presence data has been joined to lines')
        print('Results available at ' + out_lines)

# create most recent  ------------------------------------------------------------------
synth_dfs = csv_to_pd(tbls)

combine_results(synth_dfs)

most_rec_fc = "LinearExtent.gdb//linear_extent_most_recent"
join_results_to_lines('most_recent.csv', lines, most_rec_fc)

# create the all records dataset ----------------------------------------------------

join_results_to_lines(tbl='all_records.csv', 
                    lines=lines, 
                     out_lines=os.path.join(os.getcwd(), "LinearExtent.gdb","linear_extent_all_records"), 
                      all_records=True)

# Append metadata ----------------------------------------------------------------------
def apply_metadata(feature_class, metadata_file_path):
    # Create a metadata object for the feature class
    metadata_object = arcpy.metadata.Metadata(feature_class)

    # Read the metadata from the XML file into the metadata object
    metadata_object.importMetadata(metadata_file_path)

    # Apply the loaded metadata to the feature class
    metadata_object.save()

    print(f"Metadata from file {metadata_file_path} has been successfully applied to {feature_class}.")

most_rec_meta = "linear_extent_most_recent_v2.xml"
all_records_meta = "linear_extent_all_records.xml"

apply_metadata(most_rec_fc, most_rec_meta)
apply_metadata("LinearExtent.gdb\\linear_extent_all_records", all_records_meta)

# that's it -------------------------------------------------------------------------------
print("Fin.")
