# ShoreZone data synth

# 2026 script improvements = complete 2026-03-16, no data changes

# Data downloaded 2024-07-30 from: 
# 20240730 https://fortress.wa.gov/dnr/adminsa/gisdata/datadownload/state_DNR_ShoreZone.zip
# minor manual preprocessing -> see note about years below

# some years still returning as zero this is probably because of the diff methods between presence/cov cat

# set environment -------------------------------------------------------

import sys
import os
import arcpy
import pandas as pd
from arcgis.features import GeoAccessor, GeoSeriesAccessor # noqa: F401 # these are used to create sedfs

# project root is the folder within which the entire kelp_linear_extent module is located (2 levels up from this file)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
print("Project working directory:")
print(PROJECT_ROOT)
sys.path.append(PROJECT_ROOT) # this lets the project function library be found as a module

import kelp_linear_extent_code.fns as fns # noqa:E402  # project function library

arcpy.env.overwriteOutput = True # overwrite outputs 

# set workspace to parent folder
fns.reset_ws()

# set up scratch workspace
SCRATCH_WS = fns.config_scratch()

# USER INPUT -----------------------------------------------------------

dataset_name = "WADNR_ShoreZone"
containers = os.path.join(PROJECT_ROOT, "LinearExtent.gdb\\kelp_containers_v2")
cov_cat_containers = os.path.join(PROJECT_ROOT, "LinearExtent.gdb\\abundance_containers")

# Set path to kelp data
kelp_lines = os.path.join(PROJECT_ROOT, "kelp_data_sources\\state_DNR_ShoreZone\\shorezone_themes.gdb\\fkelplin")

# set path to svy lines 
svy_lines = os.path.join(PROJECT_ROOT, "kelp_data_sources\\state_DNR_ShoreZone\\shorezone.gdb\\szline")
# Just need VIDEO_DATE field from this fc to get the year 

# prepare data ------------------------------
print(f"Using {containers} as container features")

# buffer fkelplin by ~100m
print("Buffering lines...")
buff_lines = os.path.join(SCRATCH_WS, "fkelplin_buff10m")
arcpy.analysis.Buffer(kelp_lines, buff_lines, '10 METERS')

# Remove overlaps 
print("Removing overlaps...")
buff_lines_RO = os.path.join(SCRATCH_WS,"fkelplin_buf10m_removeO")
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
buff_kelp_only = os.path.join(SCRATCH_WS, "buff_kelp_only")
arcpy.management.CopyFeatures(buff_line, buff_kelp_only)
with arcpy.da.UpdateCursor(buff_kelp_only, ["FLOATKELP","OID@"]) as cursor:
    for row in cursor:
        if row[0] == 'ABSENT':
            cursor.deleteRow()

# select appropriate year for each segment ----------------------

# Run an intersect
print("Intersecting shorezone data with containers...")
kelp_int = os.path.join(SCRATCH_WS,"kelplin_cont_int")
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

sumwithin_kelp = fns.sum_kelp_within([buff_kelp_only], containers) # even though the base data is lines, the kelp data is now polygons (buffered)

presence = fns.df_from_fc(sumwithin_kelp, dataset_name)
presence = pd.concat(presence)
print("Presence result:")
print(presence.head())
print(presence.info())

# calculate coverage category -----------------------------------------

cov_cat = fns.calc_cov_cat(cov_cat_containers, [buff_kelp_only]) # even though the base data is lines, the kelp data is now polygons (buffered)
print("Coverage category result:")
print(cov_cat.head())
print(cov_cat.info())
# compile and export ---------------------------------------

print("Compiling presence and coverage category results...")
result = pd.merge(presence, cov_cat, how='left', on = 'SITE_CODE')
print(result.head())
print(result.info())
print("Adding year column...")
result = pd.merge(result, site_year_max, how='left', on = 'SITE_CODE')
print(result.head())
print(result.info())

# reformat table 
result['year'] = result['year_y']
result = result[['SITE_CODE', 'coverage_cat', 'presence', 'year', 'source']]
print("Final results table:")
print(result.info())
print(result.head())

# Remove any year == 0 (aka the shorezone shoreline, even buffered, is not reasonably within a container)
result = result.dropna(subset=["year"], axis=0)

# Write the result to CSV
# Write to csv
os.makedirs(f"{PROJECT_ROOT}\\kelp_data_linear_outputs", exist_ok=True)
out_results = os.path.join(PROJECT_ROOT, f"kelp_data_linear_outputs\\{dataset_name}_result.csv")
result.to_csv(out_results)
print(f"Saved as csv here: {out_results}")