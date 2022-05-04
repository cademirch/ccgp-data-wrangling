#!/bin/bash
eval "$(conda shell.bash hook)"
today=$(date +"%Y-%m-%d")

conda activate snakemake
python3 /home/ubuntu/ccgp-data-wrangling/scripts/update_gsheet.py &> /home/ubuntu/ccgp-data-wrangling/logs/update_gsheet/${today}_log.txt