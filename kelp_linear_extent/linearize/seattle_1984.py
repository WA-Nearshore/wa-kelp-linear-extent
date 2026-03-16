# 1984 data analysis
# data from K:\kelp\projects\2024_westseattle_magnolia_1984_imagery\WestSeattleMagnolia1984_final.gdb

# 2026 update (no data update, just script improvement) = complete, 2026-03-16

# set environment -------------------------------------------------------

import sys
import os
import arcpy
import pandas as pd
from arcgis.features import GeoAccessor, GeoSeriesAccessor # these are used to create sedfs

# project root is the folder within which the entire kelp_linear_extent module is located (2 levels up from this file)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
print("Project working directory:")
print(PROJECT_ROOT)
sys.path.append(PROJECT_ROOT) # this lets the project function library be found as a module

import kelp_linear_extent.fns as fns # project function library

arcpy.env.overwriteOutput = True # overwrite outputs 

# set workspace to parent folder
fns.reset_ws()

# set up scratch workspace
SCRATCH_WS = fns.config_scratch()

# USER INPUT -----------------------------------------------------------

dataset_name = "WADNR_1984_Seattle_Imagery" # this will be appended to data records 
cov_cat_containers = os.path.join(PROJECT_ROOT, "LinearExtent.gdb\\abundance_containers")
fc = os.path.join(PROJECT_ROOT, "kelp_data_sources\\WestSeattleMagnolia1984\\WestSeattleMagnolia1984_final.gdb\\bull_kelp_1984_edits_reviewed")

# prep data ------------------------------------------------------------

print(f"Kelp data to be linearized: {fc}")

# convert to df
print("Converting to dataframe...")
df = pd.DataFrame.spatial.from_featureclass(fc)

# use SITE_NO field to derive appropriate SITE_CODE
df["SITE_CODE"] = "cps" + df["SITE_NO"].astype(str).str.zfill(4)

# filter down to surveyed segments
df = df[df["surveyed"] == 1]

# calculate presence --------------------------------------------

print("Calculating presence...")

pres = (
    df.groupby("SITE_CODE", as_index=False)
    .agg({"kelp_presence": "max"})
    .rename(columns={"kelp_presence": "presence"})
)
pres["year"] = "1984"
pres["source"] = dataset_name
print("Presence results:")
print(pres.head())

# calculate coverage category -----------------------------------------------

# create a filtered version of the dataset with only line segments w/ kelp present
print("Filtering dataset to presence features only...")
df_filt = df[df["kelp_presence"] == 1]
fc_filt = os.path.join(SCRATCH_WS, "kelp_only_1984")
df_filt.spatial.to_featureclass(location=fc_filt, overwrite=True)

# run the function
print("Calculating coverage category...")
cov_cat = fns.calc_cov_cat(cov_cat_containers, [fc_filt], kelp_geometry_type="line")
cov_cat = cov_cat.drop("fc_name", axis=1)
print("Abundance results: ")
print(cov_cat.head())

# combine
print("Combining results...")
result = pd.merge(pres, cov_cat, how="left", on=["SITE_CODE"])
print(result.head())

# export results ------------------------------------------------------

# save to csv in results folder
os.makedirs(f"{PROJECT_ROOT}\\kelp_data_linear_outputs", exist_ok=True)
out_results = os.path.join(PROJECT_ROOT, f"kelp_data_linear_outputs\\{dataset_name}_result.csv")
result.to_csv(out_results)
print(f"Saved as csv here: {out_results}")
 
fns.clear_scratch()
