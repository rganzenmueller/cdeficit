## Code Repository for "Humans have depleted global terrestrial carbon stocks by a quarter"

### Overview
This repository contains the code used for the analysis published in "Humans have depleted global terrestrial carbon stocks by a quarter".

For questions, please contact Raphael Ganzenmüller (r.ganzenmueller@lmu.de).

### Associated Publications
Ganzenmüller et al., Humans have depleted global terrestrial carbon stocks by a quarter, One Earth (2025), https://doi.org/10.1016/j.oneear.2025.101392

* **Data:** Ganzenmüller et al., Humans have depleted global terrestrial carbon stocks by a quarter [Data set], Zenodo (2025), https://doi.org/10.5281/zenodo.15777016

### Associated Input Data

The input data for this analysis are sourced from various publicly available datasets, or directly provided:

* [Copernicus Land Service]
* [Erb2018] (Data provided by Karl-Heinz Erb)
* [ESACCI-BIOMASS]
* [ESACCI-LC]
* [FAO Ecozones 2010]
* [Geomorpho90m]
* [GSOC]
* [HILDA+]
* [Huang2021]
* [Lesiv2022]
* [Mo2023]
* [Riggio2020]
* [Sanderman2017]
* [Soilgrids2017]
* [Soilgrids2020]
* [Spawn 2020]
* [TRENDY]
* [Walker2022]
* [WDPA]
* [WorldClim]


[Copernicus Land Service]: https://zenodo.org/communities/copernicus-land-cover/
[ESACCI-BIOMASS]: https://data.ceda.ac.uk/neodc/esacci/biomass/data/agb/maps/v5.01/netcdf/
[ESACCI-LC]: https://cds.climate.copernicus.eu/datasets/satellite-land-cover/
[FAO Ecozones 2010]: https://data.apps.fao.org/catalog/dataset/2fb209d0-fd34-4e5e-a3d8-a13c241eb61b/resource/63fcc575-6248-4fec-8211-1d971102ef64?inner_span=True
[Geomorpho90m]: https://doi.pangaea.de/10.1594/PANGAEA.899135
[GSOC]: https://data.apps.fao.org/glosis/
[HILDA+]: https://doi.pangaea.de/10.1594/PANGAEA.921846
[Huang2021]: https://doi.org/10.6084/m9.figshare.12199637.v1
[Lesiv2022]: https://doi.org/10.5281/zenodo.4541512
[Mo2023]: https://zenodo.org/records/10021968
[Riggio2020]: https://doi.org/10.25338/B80G7Z
[Sanderman2017]: https://github.com/whrc/Soil-Carbon-Debt
[Soilgrids2017]: https://files.isric.org/soilgrids/former/2017-03-10/data/
[Soilgrids2020]: https://files.isric.org/soilgrids/latest/data/
[Spawn 2020]: https://doi.org/10.3334/ORNLDAAC/1763
[TRENDY]: https://mdosullivan.github.io/GCB/
[Walker2022]: https://doi.org/10.7910/DVN/DSDDQK
[WDPA]: https://www.protectedplanet.net/en/thematic-areas/wdpa?tab=WDPA
[WorldClim]: https://www.worldclim.org/data/worldclim21.html

### Requirements
* Python: Version 3.12
* Python packages: see `environment.yml`
* CDO version 2.0.5 (https://mpimet.mpg.de/cdo)
 
### Project structure
```
.  
├── 01_prep                                   # Data preparation  
│   ├── regrid_high_res_v1_01.py              # Function for regridding global high resolution data  
│   ├── xgrid_utils.py                        # Function to calculate grid cell area  
│   ├── 01_01_prep_land_mask.ipynb            # Create land-sea mask from copernicus luc data  
│   ├── 01_02_prep_area_ha.ipynb              # Create array with grid cell areas of target grid  
│   ├── 01_xx_prep_copernicusluc.ipynb        # Prepare copernicus luc data  
│   ├── 01_xx_prep_esabio.ipynb               # Prepare esa cci biomass data  
│   ├── 01_xx_prep_esacciluc.ipynb            # Prepare esa cci luc data  
│   ├── 01_xx_prep_fao2010.ipynb              # Prepare ecozone data (fao global ecological zones)  
│   ├── 01_xx_prep_geom90m.ipynb              # Prepare geom90m data (amatulli)  
│   ├── 01_xx_prep_gsoc2022.ipynb             # Prepare gsoc soil data (fao)  
│   ├── 01_xx_prep_hilda.ipynb                # Prepare hilda+ luc data (winkler)  
│   ├── 01_xx_prep_huang2021.ipynb            # Prepare belowground biomass data (huang)  
│   ├── 01_xx_prep_lesiv2022.ipynb            # Prepare forest management data (lesiv)  
│   ├── 01_xx_prep_riggio2020.ipynb           # Prepare human influence data (riggio)  
│   ├── 01_xx_prep_soilgrids.ipynb            # Prepare soilgrids data (poggio, hengl)  
│   ├── 01_xx_prep_spawn.ipynb                # Prepare above and belowground biomass carbon data (spawn)  
│   ├── 01_xx_prep_wdpa.ipynb                 # Prepare protected areas data (wdpa)  
│   └── 01_xx_prep_worldclim.ipynb            # Prepare worldclim data (fick)  
│  
├── 02_dbase                                  # Database for random forest models  
│   ├── 02_01_prep_biomass_carbon.ipynb       # Prepare biomass carbon data  
│   ├── 02_02_prep_soil_carbon.ipynb          # Prepare soil carbon data  
│   ├── 02_03_prep_pot_training.ipynb         # Prepare potential training grid cells  
│   └── 02_04_prep_database.ipynb             # Create database for random forest models  
│  
├── 03_rf                                     # Application of random forest models  
│   ├── 03_01_best_params.ipynb               # Get parameters of best performing random forest models  
│   ├── 03_02_predict_rf.ipynb                # Predict carbon with random forest models  
│   ├── 03_03_predict_qrf.ipynb               # Predict carbon with quantile random forest models  
│   ├── 03_04_predictions2ds.ipynb            # Convert predictions to dataarray  
│   ├── 03_05_adjustments.ipynb               # Adjust random forest predictions to resolve internal inconsistencies  
│   ├── 03_06_importances.ipynb               # Get feature importance of random forest models  
│   └── 03_07_scores.ipynb                    # Calculate statistical metrics  
│  
├── 04_out                                    # Final output datasets  
│   └── 04_01_ds_out.ipynb                    # Create final output datasets  
│  
├── 05_prep_other                             # Data preparation for plots and tables  
│   ├── regrid_high_res_v1_01.py              # Function for regridding global high resolution data  
│   ├── xgrid_utils.py                        # Function to calculate grid cell area  
│   ├── 05_01_prep_fig_total_carbon.ipynb     # Prepare data for figure "total carbon barplot"  
│   ├── 05_02_prep_fig_ecozone.ipynb          # Prepare data for figure "ecozone"  
│   ├── 05_03_prep_fig_example.ipynb          # Prepare data for figure "example"  
│   ├── 05_04_01_prep_dgvm_for_regridd.ipynb  # Prepare data for figure "dgvm" - Prepare dgvm for regridding  
│   ├── 05_04_02_prep_other_for_regridd.ipynb # Prepare data for figure "dgvm" - Prepare other data for regridding  
│   ├── 05_04_03_prep_grid_area.ipynb         # Prepare data for figure "dgvm" - Prepare grid cell area data  
│   ├── 05_04_04_regridd_dgvm.ipynb           # Prepare data for figure "dgvm" - Regridd dgvms using CDO  
│   ├── 05_04_05_regridd_other.ipynb          # Prepare data for figure "dgvm" - Regridd other  
│   ├── 05_04_06_merge_all.ipynb              # Create dataset with all data at luh2 resolution  
│   ├── 05_04_07_calc_values_global.ipynb     # Calculate global carbon stocks for 1700 primary land area  
│   └── 05_04_08_calc_values_ecozone.ipynb    # Calculate ecozone carbon stocks for 1700 primary land area  
│  
├── 06_eval                                   # Plots and tables  
│   ├── 06_01_fig_total_carbon_barplot.ipynb  # Figure total carbon barplot - Fig 1  
│   ├── 06_02_fig_spatial_carbon_maps.ipynb   # Figure spatial carbon maps - Fig 2  
│   ├── 06_03_fig_ecozone_gabstract.ipynb     # Figures ecozone and graphical abstract - Fig 3 - Fig graphical abstract  
│   ├── 06_04_fig_spatial_examples_map.ipynb  # Figures spatial examples - Fig 4 - Fig S3  
│   ├── 06_05_fig_dgvm.ipynb                  # Figure dgvm - Fig 5
│   ├── 06_06_fig_uncertainties_maps.ipynb    # Figure uncertainties maps - Fig S1
│   ├── 06_07_fig_diff_other_maps.ipynb       # Figure carbon difference other studies - Fig S2
│   ├── 06_08_fig_dgvm_deficit_maps.ipynb     # Figures dgvm carbon deficit maps - Fig S4 - Fig S5
│   ├── 06_09_fig_dgvm_eco_barplot.ipynb      # Figure dgvm Deficit Ecosystem Barplot - Fig S6
│   ├── 06_10_fig_luh2_primary1700.ipynb      # Figure luh2 primary 1700 - Fig S7
│   ├── 06_11_fig_carbon_eco_scatter.ipynb    # Figure carbon ecosystem scatter - Fig S8
│   ├── 06_12_fig_latitude_carbon.ipynb       # Figure latitude carbon - Fig S9
│   ├── 06_13_fig_training_data_maps.ipynb    # Figure training data maps - Fig S10
│   ├── 06_14_fig_dgvm_forest_cover.ipynb     # Figure dgvm forest cover - Fig S11
│   ├── 06_15_fig_uncertainties_under.ipynb   # Figure uncertainties underlying carbon - Fig S12
│   ├── 06_16_fig_scatter_performance.ipynb   # Figures performance of random forest models - Fig S13 - Fig S14
│   ├── 06_17_fig_feature_importance.ipynb    # Figure feature importance - Fig S15
│   └── 06_18_numbers_for_publication.ipynb   # Calculate numbers for publication  
│  
├── environment.yml
├── README.md
└── LICENSE.md
```

### Licensing
This code is licensed under the **MIT License**.

MIT License

Copyright (c) 2025 Raphael Ganzenmüller

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
