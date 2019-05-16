#!/usr/bin/bash

# Cedar

#python experiments/notebooks/generate_figures.py experiments/data/json/cedar_batch_total_dedicated_nt_4d_2.json experiments/data/json/cedar_pilots8_total_dedicated_nt_4d_2.json experiments/data/json/cedar_pilots16_total_dedicated_nt_4d_2.json cedar
python experiments/notebooks/generate_figures.py experiments/data/json/cedar_batch_total_dedicated_nt_jobstart.json experiments/data/json/cedar_pilots8_total_dedicated_nt_jobstart.json experiments/data/json/cedar_pilots16_total_dedicated_nt_jobstart.json cedar

# Beluga

python experiments/notebooks/generate_figures.py experiments/data/json/batch_total_dedicated_beluga_nt.json experiments/data/json/pilots8_total_dedicated_beluga_nt.json experiments/data/json/pilots16_total_dedicated_beluga_nt.json beluga

