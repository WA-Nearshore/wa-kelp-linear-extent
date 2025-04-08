# ShoreZone data synth

# does not use project function library 

# Data downloaded 2024-07-30 from: 
# 20240730 https://fortress.wa.gov/dnr/adminsa/gisdata/datadownload/state_DNR_ShoreZone.zip
# minor manual preprocessing -> see note about years below

# set env -----------------------------------------------
import arcpy
import os
import pandas as pd
import numpy as np
from arcgis.features import GeoAccessor, GeoSeriesAccessor
import matplotlib.pyplot as plt
import sys
sys.path.append(os.getcwd())

# import the project function library 
import fns

arcpy.env.overwriteOutput = True

fns.reset_ws()

# prep data ---------------------------------------------

# Containers
containers = "LinearExtent.gdb\\kelp_containers_v2"

print("Using " + containers + " as container features")

# Set path to kelp data
kelp_lines = "kelp_data_sources\\state_DNR_ShoreZone\\shorezone_themes.gdb\\fkelplin"

# set path to svy lines 
svy_lines = "kelp_data_sources\\state_DNR_ShoreZone\\shorezone.gdb\\szline" 
# Just need VIDEO_DATE field from this to get the year 

# prepare data ------------------------------

# buffer fkelplin by ~100m
print("Buffering lines...")
buff_lines = "scratch.gdb\\fkelplin_buff10m"
arcpy.analysis.Buffer(kelp_lines, buff_lines, '10 METERS')

# Remove overlaps 
print("Removing overlaps...")
buff_lines_RO = "scratch.gdb\\fkelplin_buf10m_removeO"
arcpy.analysis.RemoveOverlapMultiple(
    in_features=buff_lines,
    out_feature_class=buff_lines_RO,
    method="CENTER_LINE",
    join_attributes="ALL"
)

# add field for presence (if FLOATKELP is not absent, its present)
print("Calculating presence..")
arcpy.management.CalculateField(
    in_table = buff_lines_RO, 
    field = 'presence',
    expression = 'get_presence(!FLOATKELP!)',
    expression_type="PYTHON3",
    code_block="""def get_presence(x):
    if x == 'ABSENT':
        return 0
    else:
        return 1""",
    field_type="SHORT"
)

# Join szline
print("Getting year attribute...")
buff_line_join = arcpy.management.AddJoin(buff_lines_RO, 'UNIT_ID', svy_lines, 'UNIT_ID')

# grab year from the date field 
# Note --> for a 42 of the szline features, video date is 0
# manually calculated those missing values from the BIO_MAP_DT field in ArcGIS Pro before running this
buff_line_year = arcpy.management.CalculateField(
    in_table = buff_line_join, 
    field = 'year',
    expression = '!szline.VIDEO_DATE!',
    expression_type="PYTHON3",
    code_block="""""",
    field_type="SHORT"
)

# Remove join
buff_line = arcpy.management.RemoveJoin(buff_line_join)

print("Creating a version of the data with kelp features only...")
# filter feature class to only kelp presence features
buff_kelp_only = "scratch.gdb\\buff_kelp_only"
arcpy.management.CopyFeatures(buff_line, buff_kelp_only)
with arcpy.da.UpdateCursor(buff_kelp_only, ["FLOATKELP","OID@"]) as cursor:
    for row in cursor:
        if row[0] == 'ABSENT':
            cursor.deleteRow()

# select appropriate year for each segment ----------------------

# Run an intersect
print("Intersecting shorezone data with containers...")
kelp_int = "scratch.gdb\kelplin_cont_int"
arcpy.analysis.PairwiseIntersect(
    in_features=[buff_line, containers],
    out_feature_class=kelp_int,
    join_attributes="ALL",
    cluster_tolerance=None, 
    output_type="INPUT"
)
# Add area field 
print("Calculating area...")
arcpy.management.CalculateField(
    in_table = kelp_int, 
    field = 'area',
    expression='!shape.area!'
)

# Export the intersect table to pd dataframe
df = pd.DataFrame.spatial.from_featureclass(kelp_int)
df['area'] = pd.to_numeric(df['area'])
df['area'] = df['area'].fillna(0)

print("Selecting year for each segment...")

# Select year for each site with the maximum area in container
site_year = (df.groupby(['SITE_CODE', 'year'])
             .agg(year_area=('area', 'sum')) 
             .reset_index())

site_year_max = site_year.loc[site_year.groupby('SITE_CODE')['year_area'].idxmax()]

# Check if values are unique
print("Year dataframe:")
print(site_year_max.head())
print(site_year_max.info())

# calculate presence --------------------------------------------

fns.sum_kelp_within([buff_kelp_only], containers)

presence = fns.df_from_fc(["scratch.gdb\\sumwithinbuff_kelp_only"], "WADNR_ShoreZone")
presence = pd.concat(presence)
print("Presence result:")
print(presence.head())
print(presence.info())

# calculate abundance -----------------------------------------

abundance_containers = "LinearExtent.gdb\\abundance_containers"
abundance = fns.calc_abundance(abundance_containers, [buff_kelp_only])
print("Abundance result:")
print(abundance.head())
print(abundance.info())
# compile and export ---------------------------------------

print("Compiling presence and abundance...")
result = pd.merge(presence, abundance, how='left', on = 'SITE_CODE')
print(result.head())
print(result.info())
print("Adding year column...")
result = pd.merge(result, site_year_max, how='left', on = 'SITE_CODE')
print(result.head())
print(result.info())

# reformat table 
result['year'] = result['year_y']
result = result[['SITE_CODE', 'abundance', 'presence', 'year', 'source']]
print("Final results table:")
print(result.info())
print(result.head())

# Write the result to CSV
result.to_csv(r"kelp_data_synth_results\shorezone_synth.csv")
print("Fin.")