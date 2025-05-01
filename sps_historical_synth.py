# Integrate Berry 2021 data with this dataset
# Data source = "K:\kelp\projects\historical_comparison_sps_2018\spatial_data\final_gdb\bull_kelp_sps_1878_2017.gdb"

# set up env ----------------------------------------

import pandas as pd
import numpy as np
import arcpy
from arcgis import GeoAccessor, GeoSeriesAccessor

# load data -----------------------------------------

kelp_obs = "kelp_data_sources\\bull_kelp_sps_1878_2017.gdb\\kelp_all_obs"
kelp_df = pd.DataFrame.spatial.from_table(kelp_obs)

lines_fc = "LinearExtent.gdb\\all_lines_clean_v2"
lines_df = pd.DataFrame.spatial.from_featureclass(lines_fc)

print("")
print(kelp_df.head())

# QAQC -----------------------------------------------

## compare site_codes to ensure no mismatches
sps_codes = kelp_df['SITE_CODE'].unique()
line_codes = kelp_df['SITE_CODE'].unique()
diff_codes = list(set(sps_codes).difference(line_codes))
print(f"Non-matching codes: {diff_codes}")

# reformat ------------------------------------------

kelp_df["presence"] = kelp_df["kelp"].astype(int)
kelp_df["year"] = kelp_df["surveydate"].astype(int)
kelp_df["source"] = "Berry_et_al_2021"

out_table = kelp_df[['SITE_CODE', 'source', 'year', 'presence']]
print("Reformatted table:")
print(out_table.head())

# write out -----------------------------------------
out_table.to_csv('kelp_data_synth_results\\sps_historical.csv')
