# Combine data synthesis results
# Gray McKenna
# 2024-08-16

import arcpy
import arcpy.management
import pandas as pd
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
def get_most_recent(synth_dfs):

    #### Bind rows ####
    all_synth = pd.concat(synth_dfs)
    print("Joined results df: ")
    print(all_synth.head())

    # save this 
    pd.DataFrame.to_csv(all_synth, "all_records.csv")

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
    else:
        print("All sites have unique records for most recent year")

    # Set index to site_code 
    most_recent = most_recent.set_index('SITE_CODE')

    # Drop this random garbage column
    most_recent = most_recent.drop(['Unnamed: 0'], axis = 1)

    print("Preview of most recent year table:")
    print(most_recent.head())

    # write to a csv
    pd.DataFrame.to_csv(most_recent, 'most_recent.csv')
    print("Written to csv: most_recent.csv")

def join_results_to_lines(tbl, lines):
    #### Join to line segments fc ####

    # Copy all lines feature
    out_lines = "LinearExtent.gdb//linear_extent_most_recent"
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

get_most_recent(synth_dfs)

join_results_to_lines('most_recent.csv', lines)