# WA Floating Kelp Linear Extent Project #
#### Washington Department of Natural Resources #### 
#### Nearshore Habitat Program ####
[![DOI](https://zenodo.org/badge/903042774.svg)](https://doi.org/10.5281/zenodo.15742420)

This is the repository for all the analysis scripts that compile and synthesize a multitude of floating kelp spatial datasets into a cohesive synthesis of WA-statewide floating kelp linear extent.
Scripts are authored by Gray McKenna.

More information about this project, including data access and User Guide, is available at https://experience.arcgis.com/experience/e03ea8b2a6574e4094230aff5e862626/ or at https://www.dnr.wa.gov/programs-and-services/aquatics/aquatic-science/nearshore-habitat-program

Contact nearshore@dnr.wa.gov with questions about usage of these scripts or this dataset.

This project is structured to first run data synthesis scripts for each individual input data source. Then a join script (kelp_synth_join.py) compiles all the results and produces the linear extent datasets. The pipeline script (linear_extent_analysis_pipeline.py) can run all the scripts in the appropriate order. This figure demonstrates the workflow logic:
![Flow chart of analysis pathway](https://github.com/WA-Nearshore/wa-kelp-linear-extent/blob/main/analysis_pathway.jpg?raw=true)
