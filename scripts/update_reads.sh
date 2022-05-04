#!/bin/bash
eval "$(conda shell.bash hook)"
today=$(date +"%Y-%m-%d")

conda activate snakemake
python3 /home/ubuntu/ccgp-data-wrangling/update_reads.py &> /home/ubuntu/ccgp-data-wrangling/logs/update_reads/${today}_log.txt