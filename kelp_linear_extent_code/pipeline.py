# Load modules
# 2026 notes: specify /linearize/ path

import subprocess

# All data sources should be copied into /kelp_data_sources folder
# Check the notes at the top of each script for any file naming info or pre-processing


def run_script(py_file):
    subprocess.run(["python", py_file])


# Historical/"one time" datasources, no updates anticipated
# No need to rerun unless lines/containers/source datasets have been editted since 06/2025

historical_sources = [
    "cps_sps_boat.py",
    "cps_uas.py",
    "shorezone.py",
    "seattle_1984.py",
    "sps_historical.py",
]

for script in historical_sources:
    print("----------------------------------")
    print(f"Running {script}...")
    try:
        run_script(f"linearize/{script}")
        print(f"{script} complete")
        print("----------------------------------")
    except Exception as e:
        print(f"Error occured while running {script}: {e}")

# Living datasources
# Rerun as updates occur
living_sources = [
    "costr_aqres.py",
    "dnr_kayak.py",
    "fixed_wing.py",
    "mrc_kayak.py",
    "samish_sji.py",
]

for script in living_sources:
    print("----------------------------------")
    print(f"Running {script}...")
    try:
        run_script(f"linearize/{script}")
        print(f"{script} complete")
        print("----------------------------------")
    except Exception as e:
        print(f"Error occured while running {script}: {e}")


# Run the join script
try:
    run_script("compile_linear_data.py")
    print("Analysis complete")
except Exception as e:
    print("!!!!!!!!!!!!!!! Unable to complete join script !!!!!!!!!!!!!!!")
    print(f"{e}")
