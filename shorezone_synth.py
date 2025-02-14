# ShoreZone data synth

# Data downloaded 2024-07-30 from: 
# 20240730 https://fortress.wa.gov/dnr/adminsa/gisdata/datadownload/state_DNR_ShoreZone.zip

#### SET ENVIRONMENT ####
import arcpy
import os
import pandas as pd
import numpy as np
from arcgis.features import GeoAccessor, GeoSeriesAccessor
import matplotlib.pyplot as plt

arcpy.env.overwriteOutput = True

# store parent folder workspace in function 
def reset_ws(): 
    arcpy.env.workspace = os.getcwd()

reset_ws()

#### Load Data ####

# Containers
containers = "LinearExtent.gdb\\kelp_containers_v2"

print("Using " + containers + " as container features")

# Set path to kelp data
kelp_lines = "kelp_data_sources\\state_DNR_ShoreZone\\shorezone_themes.gdb\\fkelplin"

# set path to svy lines 
svy_lines = "kelp_data_sources\\state_DNR_ShoreZone\\shorezone.gdb\\szline" 
# Just need VIDEO_DATE field from this to get the year 

### Reformat ShoreZone Data ####

# buffer fkelplin by ~100m
buff_lines = "scratch.gdb\\fkelplin_buff100m"
arcpy.analysis.Buffer(kelp_lines, buff_lines, '100 METERS')

# Remove overlaps 
buff_lines_RO = "scratch.gdb\\fkelplin_buf100m_removeO"
arcpy.analysis.RemoveOverlapMultiple(
    in_features=buff_lines,
    out_feature_class=buff_lines_RO,
    method="CENTER_LINE",
    join_attributes="ALL"
)

# add field for presence (if FLOATKELP is not absent, its present)
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
buff_line_join = arcpy.management.AddJoin(buff_lines_RO, 'UNIT_ID', svy_lines, 'UNIT_ID')

# grab year from the date field 
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

# Run an intersect
kelp_int = "scratch.gdb\kelplin_cont_int"
arcpy.analysis.PairwiseIntersect(
    in_features=[buff_line, containers],
    out_feature_class=kelp_int,
    join_attributes="ALL",
    cluster_tolerance=None, 
    output_type="INPUT"
)
# Add area field 
arcpy.management.CalculateField(
    in_table = kelp_int, 
    field = 'area',
    expression='!shape.area!'
)

# Export the intersect table to pd dataframe
df = pd.DataFrame.spatial.from_featureclass(kelp_int)
df['area'] = pd.to_numeric(df['area'])
df['area'] = df['area'].fillna(0)

# Compute abundance category per site
sum_area = (df.groupby(['SITE_CODE', 'FLOATKELP'])
            .agg(total_area=('area', 'sum'))
            .reset_index()
            .pivot_table(index='SITE_CODE', columns='FLOATKELP', values='total_area', aggfunc='sum')
            .fillna(0))

# Calculate presence percentage and abundance category
sum_area['pres_pct'] = (sum_area['CONTINUOUS'] + sum_area['PATCHY'] * 0.5) / (sum_area['CONTINUOUS'] + sum_area['PATCHY'] + sum_area['ABSENT'])
sum_area['abundance_cat'] = pd.cut(sum_area['pres_pct'], 
                                   bins=[-np.inf, 0, 0.25, 0.5, 0.75, np.inf], 
                                   labels=[0, 1, 2, 3, 4])

# Presence category (binary: 1 if presence > 0, else 0)
sum_area['presence'] = np.where(sum_area['pres_pct'] > 0, 1, 0)

# Plot the histogram for abundance categories
sum_area['abundance_cat'].value_counts().sort_index().plot(kind='bar')
plt.xlabel('Abundance Category')
plt.ylabel('Frequency')
plt.title('Abundance Category Distribution')
plt.show()

# Select year for each site with the maximum area
site_year = (df.groupby(['SITE_CODE', 'year'])
             .agg(year_area=('area', 'sum'))
             .reset_index())

# Now, select the row with the maximum area per site
site_year_max = site_year.loc[site_year.groupby('SITE_CODE')['year_area'].idxmax()]

# Check if values are unique
print(f"Unique SITE_CODEs in site_year: {site_year['SITE_CODE'].nunique()}")
print(f"Unique SITE_CODEs in sum_area: {sum_area.index.nunique()}")

# Join the results on SITE_CODE
result = pd.merge(sum_area, site_year, on='SITE_CODE')

# Write the result to CSV
result.to_csv(r"kelp_data_synth_results\shorezone_results.csv")