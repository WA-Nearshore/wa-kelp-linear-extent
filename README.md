# WA Floating Kelp Linear Extent Project #
#### Washington Department of Natural Resources #### 
#### Nearshore Habitat Program ####
[![DOI](https://zenodo.org/badge/903042774.svg)](https://doi.org/10.5281/zenodo.15742420)

This is the repository for all the analysis scripts that compile and synthesize a multitude of floating kelp spatial datasets into a cohesive synthesis of WA-statewide floating kelp linear extent. Raw kelp spatial data and intermediate outputs are not included in this script, but data is available upon request. The 2026 code and data update is currently underway. 

Scripts are authored by Gray McKenna.

More information about this project, including data access and User Guide, is available at https://experience.arcgis.com/experience/e03ea8b2a6574e4094230aff5e862626/ or at https://www.dnr.wa.gov/programs-and-services/aquatics/aquatic-science/nearshore-habitat-program

Contact nearshore@dnr.wa.gov with questions about usage of these scripts or this dataset.

Project Structure:  
ProjectFolder/  
&nbsp;├── kelp_linear-extent/  
&nbsp;&nbsp;├── linearize/ _this folder contains analysis scripts for individual data sources_  
&nbsp;&nbsp;├── compile_linear_data.py _this script compiles outputs from the linearize scripts_  
&nbsp;&nbsp;├── fns.py _this script contains functions and utilities used in linearize scripts_  
&nbsp;&nbsp;└── pipeline.py _this script runs the entire workflow, including all linearize scripts and the compilation script_  
&nbsp;├── kelp_data_sources/ _(files not included in repo) raw kelp spatial data_
&nbsp;└── kelp_data_linear_outputs/ _(files not included in repo) outputs from linearize scripts_

This figure demonstrates the workflow logic:
![Flow chart of analysis pathway](https://github.com/WA-Nearshore/wa-kelp-linear-extent/blob/main/analysis_pathway.jpg?raw=true)
