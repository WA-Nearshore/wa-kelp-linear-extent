# Summarize Classified Imagery POLYGONS from Fixed Wing Aerial Program

# Classified Imagery from Fixed Wing Aerial Imagery Program 
# Copies of polygons brought down from the network on 2026-06-03
# Polygons all exported to a local gdb
# AOIs: compiled to a gdb from the shapefiles on network 

# Note: CHP, CYP, SMI were manually merged to one fc (AQR) because there is only 1 ortho tile index for all of them

# set up environment --------------------------------------

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

import kelp_linear_extent_code.fns as fns # noqa: E402 project function library

arcpy.env.overwriteOutput = True # overwrite outputs 

# set workspace to parent folder
fns.reset_ws()

# set up scratch workspace
SCRATCH_WS = fns.config_scratch()

# USER INPUT ----------------------------------------------------------

dataset_name = "WADNR_KAM"
years = ["2022"] # list years for which classified data is currently available (usually more yrs of AOIs available than classified data)
kelp_data_path =  os.path.join(PROJECT_ROOT, "kelp_data_sources\\fixed_wing_aerial_imagery")
containers = os.path.join(PROJECT_ROOT, "LinearExtent.gdb\\lines_and_containers\\kelp_containers_v3")
cov_cat_containers = os.path.join(PROJECT_ROOT, "LinearExtent.gdb\\lines_and_containers\\cov_cat_containers")

# prep data ---------------------------------------------------------------

arcpy.env.workspace = f"{kelp_data_path}\\classified_polygons.gdb"
kelp_fc_names = arcpy.ListFeatureClasses()
print("All available kelp fcs:")
for fc in kelp_fc_names: 
    print({fc})

# append parent file path
kelp_fcs_raw = [f"{kelp_data_path}\\classified_polygons.gdb\\{fc}" for fc in kelp_fc_names]
fns.reset_ws()

# dissolve into single layer by year
print("Prepping kelp fcs...")
kelp_fcs = []
for yr in years: 
    print(f"Filtering to kelp fcs for {yr}")
    # create a subsetted list of fcs for years
    fcs_for_year = []
    for fc in kelp_fcs_raw: 
        fc_desc = arcpy.Describe(fc) # get fc name
        if fc_desc.name[-4:] == yr: # if name ends in target year, add fc to list 
            fcs_for_year.append(fc)
    
    print(fcs_for_year)

    # merge into a single fc for year
    out_diss_yr = f"{SCRATCH_WS}\\kelp_{yr}"
    print(f"Merging all fcs for {yr} into one...")
    arcpy.management.Merge(fcs_for_year, out_diss_yr)
    kelp_fcs.append(out_diss_yr) # append to list

fns.reset_ws()

# get AOI fcs
print("Prepping AOIs...")
arcpy.env.workspace = f"{kelp_data_path}\\AOIs.gdb"
aoi_fc_names = arcpy.ListFeatureClasses()
print("These AOI fcs are available:")
for fc in aoi_fc_names:
    print(fc)

# filter to just years that we have data for 
print("Filtering AOIs to specified years...")
svy_bnds = []
for fc in aoi_fc_names:
    fc_desc = arcpy.Describe(fc)
    if fc_desc.name[-4:] in years:
        svy_bnds.append(fc)
        print(f"Added {fc_desc.name} to survey boundary list")

print("Survey boundary fcs:")
print(svy_bnds)
svy_bnds = [f"{kelp_data_path}\\AOIs.gdb\\{fc}" for fc in svy_bnds] # append parent file path

# zip into paired list
fc_list = zip(kelp_fcs, svy_bnds)

# calculate presence ------------------------------------------------------
pres_fcs = fns.calc_presence(fc_list, containers, variable_survey_area=True)

sdf_list = fns.df_from_fc(pres_fcs, dataset_name)

print("This is the structure of the sdfs:")
print(sdf_list[0].head())

# merge to one df
print("Combining to one dataframe")
presence = pd.concat(sdf_list)
print(f"Total records: {len(presence)}")

# calculate abundance ------------------------------------------------------
print("Calculating coverage category....")

cov_cat = fns.calc_cov_cat(cov_cat_containers, kelp_fcs)

# add the year col
cov_cat['year'] = cov_cat['fc_name'].str[-4:]
cov_cat = cov_cat.drop(columns=['fc_name'])
print("Reformatted cov_cat table:")
print(cov_cat.head())

# tidy and export ----------------------------------------------------------

print("Combining results to one dataframe")
results = pd.merge(presence, cov_cat, how="left", on=["SITE_CODE", "year"])
print(results.head())

# Write to csv
os.makedirs(f"{PROJECT_ROOT}\\kelp_data_linear_outputs", exist_ok=True)
out_results = os.path.join(PROJECT_ROOT, f"kelp_data_linear_outputs\\{dataset_name}_result.csv")
results.to_csv(out_results)
print(f"Saved as csv here: {out_results}")

# Clear scratch gdb to keep project size down
fns.clear_scratch()



