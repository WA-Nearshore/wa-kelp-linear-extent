# 1984 data analysis
# data from K:\kelp\projects\2024_westseattle_magnolia_1984_imagery\WestSeattleMagnolia1984_final.gdb

# set env -----------------------------------------------------------
import arcpy
import sys
import pandas as pd
import os
from arcgis.features import GeoAccessor, GeoSeriesAccessor
import fns
sys.path.append(os.getcwd())
arcpy.env.overwriteOutput = True

fns.reset_ws()

# prep data ------------------------------------------------------------

fc = "kelp_data_sources\\WestSeattleMagnolia1984\\WestSeattleMagnolia1984_final.gdb\\bull_kelp_1984_edits_reviewed"

# abundance containers
abundance_containers = "LinearExtent.gdb\\abundance_containers"

# convert to df 
print("Converting to dataframe...")
df = pd.DataFrame.spatial.from_featureclass(fc)

# use SITE_NO field to derive appropriate SITE_CODE
df['SITE_CODE'] = 'cps' + df['SITE_NO'].astype(str).str.zfill(4)

# filter down to surveyed segments 
df = df[df['surveyed'] == 1]

# calculate presence --------------------------------------------

print("Calculating presence...")

pres = df.groupby('SITE_CODE', as_index=False).agg(
    {'kelp_presence':'max'}
).rename(columns={'kelp_presence':'presence'})
pres['year']='1984'
pres['source']='WADNR_1984_Seattle_Imagery'
print("Presence results:")
print(pres.head())

# calculate abundance -----------------------------------------------

# create a filtered version of the dataset with only line segments w/ kelp present 
print("Filtering dataset to presence features only...")
df_filt = df[df['kelp_presence']== 1]
fc_filt = "scratch.gdb\\kelp_only_1984"
df_filt.spatial.to_featureclass(location=fc_filt, overwrite=True)

# run the function 
print("Calculating abundance...")
ab = fns.calc_abundance_lines(abundance_containers, [fc_filt])
ab = ab.drop('fc_name', axis=1)
print("Abundance esults: ")
print(ab.head())

# combine 
print("Combining results...")
result = pd.merge(pres, ab, how="left", on=["SITE_CODE"])
print(result.head())

# export results ------------------------------------------------------


# save to csv in results folder
result.to_csv("kelp_data_synth_results\\seattle_1984_synth.csv")


fns.clear_scratch()