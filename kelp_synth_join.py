# Combine linear extent data synthesis results and join to lines

import arcpy
import arcpy.management
import pandas as pd
import numpy as np
from pathlib import Path
import os

#### Set Environment ####
arcpy.env.overwriteOutput = True

# store parent folder workspace in function 
def reset_ws(): 
    arcpy.env.workspace = os.getcwd()

reset_ws()

#### Load Data ####

# Kelp data summarize within results tables
synth_folder = Path("kelp_data_synth_results")
tbls = list(synth_folder.glob('*.csv'))
print("Synth results tables available:")
for t in tbls: print(t)

# linear extent fc 
lines = "LinearExtent.gdb//all_lines_clean_v2"
print("Using " + lines + " as line segment feature class")

#### Create function to read a list of csv tbls to pandas dataframes ####
def csv_to_pd(tbls):
    
    dfs = []

    for t in tbls:
        df = pd.read_csv(t)
        print(df.head())
        dfs.append(df)

    return dfs


#### Define main function ####
def combine_results(synth_dfs):

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
    all_synth.to_csv("all_records.csv")

    #### Select most recent year for each site_code ####

    # Find most recent year for each SITE_CODE
    most_recent_year = all_synth.groupby('SITE_CODE')['year'].transform('max')

    # Grab those rows
    most_recent = all_synth[all_synth['year'] == most_recent_year]

    # Check if site_code is unique
    pd.set_option('display.max_rows', None)
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

def join_results_to_lines(tbl, lines, out_lines):
    #### Join to line segments fc ####

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

#### Run ####
synth_dfs = csv_to_pd(tbls)

combine_results(synth_dfs)

join_results_to_lines('most_recent.csv', lines, "LinearExtent.gdb//linear_extent_most_recent")

# this doesn't work because the underlying function cant handle 1:M cardinality
#join_results_to_lines('all_records.csv', lines, "LinearExtent.gdb//linear_extent_all_records")