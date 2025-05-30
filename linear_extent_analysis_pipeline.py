# Load modules 

import subprocess

# All data sources should be copied into /kelp_data_sources folder

# Historical/"one time" datasources, no updates anticipated
# No need to rerun unless datasets have been editted since 03/2025
subprocess.run(["python", "COSTR_synth.py"])
subprocess.run(["python", "AQRES_synth.py"])
subprocess.run(["python", "cps_sps_reformat.py"])
subprocess.run(["python", "cps_uas_synth.py"])
subprocess.run(["python", "shorezone_synth.py"])


# Living datasources
# Rerun as updates occur 
subprocess.run(["python", "DNR_kayak_synth.py"]) # Annual updates anticipated
subprocess.run(["python", "fixedwing_poly_synth.py"]) # Annual updates anticipated
subprocess.run(["python", "mrc_kayak_synth.py"]) # Annual updates anticipated
subprocess.run(["python", "samish_synth.py"]) # Not sure update schedule


# Run the join script
subprocess.run(["python", "kelp_synth_join.py"])

# Linear Extent updated
print("Analysis complete")