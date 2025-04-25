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

print(kelp_df.head())

# reformat ------------------------------------------

kelp_df["presence"] = kelp

# write out -----------------------------------------