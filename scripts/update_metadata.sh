#!/bin/bash
eval "$(conda shell.bash hook)"
today=$(date +"%Y-%m-%d")

conda activate snakemake
python3 /home/ubuntu/ccgp-data-wrangling/scripts/update_metadata.py metadata &> /home/ubuntu/ccgp-data-wrangling/logs/update_metadata/${today}_log.txt