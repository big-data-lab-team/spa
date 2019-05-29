#!/usr/bin/bash

# Cedar

#python experiments/notebooks/generate_figures.py experiments/data/json/cedar_batch_total_dedicated_nt_4d_2.json experiments/data/json/cedar_pilots8_total_dedicated_nt_4d_2.json experiments/data/json/cedar_pilots16_total_dedicated_nt_4d_2.json cedar
python experiments/notebooks/generate_figures.py experiments/data/json/cedar_batch_may15.json experiments/data/json/cedar_pilots8_may15.json experiments/data/json/cedar_pilots16_may15.json cedar

# Beluga

python experiments/notebooks/generate_figures.py experiments/data/json/beluga_batch_may17.json experiments/data/json/beluga_pilots8_may17.json experiments/data/json/beluga_pilots16_may17.json beluga

