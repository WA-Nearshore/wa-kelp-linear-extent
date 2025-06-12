# Load modules 

import subprocess

# All data sources should be copied into /kelp_data_sources folder
# Check the notes at the top of each script for any file naming info or pre-processing 

def run_script(py_file):
    subprocess.run(["python", py_file])

# Historical/"one time" datasources, no updates anticipated
# No need to rerun unless lines/containers/source datasets have been editted since 06/2025

historical_sources = ["cps_sps_reformat.py", "cps_uas_synth.py", "shorezone_synth.py", "seattle_1984_synth.py", "sps_historical_synth.py"]

for script in historical_sources:
    print("----------------------------------")
    print(f"Running {script}...")
    try: 
        run_script(script)
        print(f"{script} complete")
        print("----------------------------------")
    except Exception as e:
        print(f"Error occured while running {script}: {e}")

# Living datasources
# Rerun as updates occur 
living_sources = ["COSTR_synth.py", "AQRES_synth.py", "DNR_kayak_synth.py", "fixedwing_poly_synth.py",
                  "mrc_kayak_synth.py", "samish_synth.py"]

for script in living_sources:
    print("----------------------------------")
    print(f"Running {script}...")
    try: 
        run_script(script)
        print(f"{script} complete")
        print("----------------------------------")
    except Exception as e:
        print(f"Error occured while running {script}: {e}")


# Run the join script
try:
    run_script("kelp_synth_join.py")
    print("Analysis complete")
except Exception as e:
    print("!!!!!!!!!!!!!!! Unable to complete join script !!!!!!!!!!!!!!!")
    print(f"{e}")