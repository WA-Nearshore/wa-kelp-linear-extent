# Combine linear extent data synthesis results and join to lines

# set environment -----------------------------------------------------------------------

import arcpy
import pandas as pd
from pathlib import Path
import os
from arcgis import GeoSeriesAccessor, GeoAccessor # noqa: F401

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

arcpy.env.overwriteOutput = True

# load data -------------------------------------------------------------------------------

# INPUTS
# Kelp data summarize within results tables
synth_folder = Path(PROJECT_ROOT) / "kelp_data_linear_outputs"

# linear extent fc
lines = os.path.join(PROJECT_ROOT, "LinearExtent.gdb//all_lines_clean_v2")

# metadata
most_rec_meta = os.path.join(PROJECT_ROOT, "kelp_reference//linear_extent_most_recent_v2.xml")
all_records_meta = os.path.join(PROJECT_ROOT, "kelp_reference//linear_extent_all_records.xml")

# OUTPUTS
OUT_PATH = os.path.join(PROJECT_ROOT, "kelp_data_compiled")
OUT_GDB = "kelp_data_compiled.gdb"
most_rec_fc = os.path.join(OUT_PATH, OUT_GDB, "most_recent")
all_records_fc = os.path.join(OUT_PATH, OUT_GDB, "all_records")

# create output gdb if needed
if not arcpy.Exists(os.path.join(OUT_PATH, OUT_GDB)):
    print("Creating output gdb")
    arcpy.management.CreateFileGDB(OUT_PATH, OUT_GDB)
else:
    print("Out gbd exists. Outputs may overwrite existing fcs.")

tbls = list(synth_folder.glob("*.csv"))
print("Synth results tables available:")
for t in tbls:
    print(t)

print(f"Using {lines} as line segment feature class")

# define functions -----------------------------------------------------------------------

def reset_ws():
    arcpy.env.workspace = os.getcwd()


def csv_to_pd(tbls):
    """
    convert linearized output csvs to a list of pd dataframes
    """
    pd.set_option("display.max_columns", 7)

    dfs = []

    for t in tbls:
        df = pd.read_csv(t)
        print(f"{t} converted to dataframe:")
        print(df.head())
        print(df.dtypes)
        print(f"Total records: {len(df)}")
        dfs.append(df)

    return dfs


def combine_results(synth_dfs, OUT_PATH):
    """
    combine list of pd dfs into most recent and all records .csvs  
    in folder specified with OUT_PATH
    """
    pd.set_option("display.max_rows", 7)
    #### Bind rows ####
    all_synth = pd.concat(synth_dfs)
    print("Joined results df: ")
    print(all_synth.head(5))

    # handle instances where presence and coverage_cat disagree
    # if presence == 0 and coverage_cat > 0, make coverage_cat 0
    # if presence == 1 and coverage_cat == 0, make coverage_cat 1
    all_synth["coverage_cat"] = all_synth.apply(
        lambda row: 0
        if row["presence"] == 0 and (pd.isna(row["coverage_cat"]) or row["coverage_cat"] > 0)
        else 1
        if row["presence"] == 1 and (row["coverage_cat"] == 0)
        else row["coverage_cat"],
        axis=1,
    )

    # drop extra column
    all_synth = all_synth.drop(["Unnamed: 0"], axis=1)

    # drop sum area hectares and sum length meters
    all_synth = all_synth.drop(["sum_Area_HECTARES"], axis=1)
    all_synth = all_synth.drop(["sum_Length_METERS"], axis=1)

    # drop rows where source is null
    all_synth = all_synth.dropna(subset=["source"], axis=0)

    # rename abundance
    all_synth.rename(columns={"coverage_cat": "coverage_category"}, inplace=True)

    # add the source_url field
    print("Adding the source_urls in from the file source_urls.csv")
    source_url = pd.read_csv(os.path.join(PROJECT_ROOT, "kelp_reference", "source_urls.csv"))
    all_synth = all_synth.merge(
        source_url[["source", "source_url"]], on="source", how="left"
    )

    # save this as the 'all_records" table.
    os.makedirs(OUT_PATH, exist_ok=True)
    all_synth.to_csv(os.path.join(OUT_PATH, "all_records.csv"))
    print("Compiled all results and written to csv: all_records.csv")
    print(f"Total records: {len(all_synth)}")

    #### Select most recent year for each site_code ####
    # identify non-numeric or NA rows
    non_numeric_years = all_synth[
        all_synth["year"].isna() | ~all_synth["year"].astype(str).str.isdigit()
    ]

    # print them
    print(non_numeric_years)

    # find most recent year for each SITE_CODE
    most_recent_year = all_synth.groupby("SITE_CODE")["year"].transform("max")

    # grab those rows
    most_recent = all_synth[all_synth["year"] == most_recent_year]

    # check if site_code is unique
    if not most_recent["SITE_CODE"].is_unique:
        dupes = most_recent[
        most_recent.duplicated("SITE_CODE", keep=False)
        ].sort_values("SITE_CODE")
        print(f"{len(dupes)} sites have more than 1 record for the most recent year:")
        print(dupes)
        print("Selecting source with maximum coverage...")
    else:
        print("All sites have unique records for most recent year")

    # count number of records for most recent year
    most_recent.loc[:, "n_records_most_rec"] = most_recent.groupby("SITE_CODE")[
        "SITE_CODE"
    ].transform("count")

    # for years with multiple records, select row with max proportional_presence
    most_recent["coverage_category"].fillna(
        -9999, inplace=True
    )  # replace NULL values with -9999 so the below code works
    most_rec_max = most_recent[
        most_recent["coverage_category"]
        == most_recent.groupby("SITE_CODE")["coverage_category"].transform("max")
    ]

    # Set index to site_code
    most_rec_max = most_rec_max.set_index("SITE_CODE")

    print("Preview of most recent year table:")
    print(most_rec_max.head(5))

    # write to a csv
    most_rec_max.to_csv(os.path.join(OUT_PATH, "most_recent.csv"))
    print("Written to csv: most_recent.csv")


def join_results_to_lines(tbl, lines, out_lines):
    """
    join compiled csvs to line features 
    """
    #### Join to line segments fc ####
    # note to self: need to delete the extra fields length_m and SITE_CODE_1 from the outputs
    # also: could use fieldmapping to set the field types more appropriately here

    print(f"Converting {tbl} and lines to pd dataframes...")
    # convert lines to a sdf
    sdf = pd.DataFrame.spatial.from_featureclass(lines)

    # load tbl to dataframe
    tbl_df = pd.read_csv(tbl)
    print(f"{len(tbl_df)} records in table")

    # join one-to-many
    print("Merging...")
    joined = pd.merge(sdf, tbl_df, how="outer", on="SITE_CODE")
    print(f"Resulting table has {len(joined)} records")
    print(joined.head())

    # write to feature class
    print("Writing to feature class...")
    joined.spatial.to_featureclass(location=out_lines, overwrite=True)
    print(f"Feature class created: {out_lines}")

    # Write csv to gdb table
    print("Writing to table...")

    # tidy output feature class
    print("Removing unnecessary fields...")
    arcpy.management.DeleteField(
        out_lines, ["length_m", "unnamed_01", "unnamed_0", "sum_Area_Hectares", "sum_Length_METERS"]
    )
    arcpy.management.AlterField(out_lines, "site_code", "SITE_CODE", "SITE_CODE")

def apply_metadata(feature_class, metadata_file_path):
    # Create a metadata object for the feature class
    metadata_object = arcpy.metadata.Metadata(feature_class)

    # Read the metadata from the XML file into the metadata object
    metadata_object.importMetadata(metadata_file_path)

    # Apply the loaded metadata to the feature class
    metadata_object.save()

    print(
        f"Metadata from file {metadata_file_path} has been successfully applied to {feature_class}."
    )

# create most recent  ------------------------------------------------------------------
reset_ws()

synth_dfs = csv_to_pd(tbls)

combine_results(synth_dfs, OUT_PATH)

join_results_to_lines(tbl=os.path.join(OUT_PATH, "most_recent.csv"), 
                      lines=lines, 
                      out_lines=most_rec_fc)

# create the all records dataset ----------------------------------------------------

join_results_to_lines(tbl=os.path.join(OUT_PATH, "all_records.csv"),
                      lines=lines,
                      out_lines=all_records_fc)

# Append metadata ----------------------------------------------------------------------

apply_metadata(most_rec_fc, most_rec_meta)
apply_metadata(all_records_fc, all_records_meta)

# that's it -------------------------------------------------------------------------------
print("Fin.")
