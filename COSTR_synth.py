# Summarize COSTR kelp data to linear extent

# COSTR data download date: 2024-06-17
# link: https://fortress.wa.gov/dnr/adminsa/gisdata/datadownload/kelp_canopy_strait_coast.zip

# set environment -----------------------------------------------------
import arcpy
import os
import sys
import arcpy.conversion
import pandas as pd
import numpy as np
from arcgis.features import GeoAccessor, GeoSeriesAccessor

sys.path.append(os.getcwd())

# import the project function library 
import fns

arcpy.env.overwriteOutput = True

fns.reset_ws()

# prep data --------------------------------------------------------------

# containers
containers = "LinearExtent.gdb\\kelp_containers_v2"

print(f"Using {containers} as container features")

# set workspace to gdb with kelp data sources
kelp_data_path = "kelp_data_sources\\kelp_canopy_strait_coast\\kelp_canopy_coast_strait.gdb"
arcpy.env.workspace = kelp_data_path

# load feature classes into list
costr_fcs = arcpy.ListFeatureClasses('kelp1*')  # add all fcs from 1900s
costr_fcs.extend(arcpy.ListFeatureClasses('kelp2*')) # add all fcs from 2000s

print("Datasets to be summarized: ")
print(costr_fcs)

# append path to fcs in list 
costr_fcs = [f"{kelp_data_path}\\{fc}" for fc in costr_fcs]

fns.reset_ws()

# clip containers
print("Clipping containers to survey area...")
containers_clip = "scratch.gdb\\containers_COSTR"
costr_bnd = f"{kelp_data_path}\\map_index"
arcpy.analysis.Clip(containers, costr_bnd, containers_clip)

print("Container fc clipped to " + costr_bnd.rsplit('\\', 1)[-1])

# calculate presence ----------------------------------------------------

fns.sum_kelp_within(costr_fcs, containers_clip)

# get list of summarize within output fcs
arcpy.env.workspace = os.path.join(os.getcwd(), "scratch.gdb")
sumwithin_fcs = arcpy.ListFeatureClasses('sum*')

sdf_list = fns.df_from_fc(sumwithin_fcs, "WADNR_COSTR")

fns.reset_ws()

print("This is the structure of the presence sdfs:")
print(sdf_list[1].head())

# merge to one df
presence = pd.concat(sdf_list)

print("All years of data have been merged to one df")
print(" ")
print(presence.head())

# calculate abundance ------------------------------------------------

print("Calculating abundance...")
abundance_containers = "LinearExtent.gdb\\abundance_containers"
abundance = fns.calc_abundance(abundance_containers, costr_fcs)

# add the year col
abundance['year'] = abundance['fc_name'].str[-4:]
abundance = abundance.drop(columns=['fc_name'])
print("Reformatted abundance table:")
print(abundance.head())

# compile and export --------------------------------------------------
results = pd.merge(presence, abundance, how="left", on=["SITE_CODE", "year"])

results.to_csv("kelp_data_synth_results\\COSTR_synth.csv")
print("Saved as csv here: kelp_data_synth_results\\COSTR_synth.csv")

# Clear scratch gdb to keep project size down
fns.clear_scratch()
