# Load modules 

import subprocess

# All data sources should be copied into /kelp_data_sources folder

# Historical datasources, no updates anticipated
# No need to rerun unless datasets have been editted since 06/2024
subprocess.run(["python", "COSTR_synth.py"])
subprocess.run(["python", "AQRES_synth.py"])
subprocess.run(["python", "cps_sps_synth.py"])
subprocess.run(["python", "samish_synth.py"])

# Living datasources
# Rerun as updates occur 
subprocess.run(["python", "kayak_synth.py"]) # Annual updates anticipated
subprocess.run(["python", "fixedwing_synth.py"]) # Annual updates anticipated

# Run the join script
subprocess.run(["python", "kelp_synth_join.py"])

# Linear Extent updated
print("Analysis complete")