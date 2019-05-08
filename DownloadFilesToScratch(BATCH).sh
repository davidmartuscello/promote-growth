#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --time=12:00:00
#SBATCH --mem=50GB
#SBATCH --job-name=bdap_loaddata
#SBATCH --mail-type=END
#SBATCH --mail-user=dm4350@nyu.edu
#SBATCH --output=slurm_%j.out

cd /scratch/dm4350/bdap/stack_exchange_data
wget -A 7z -c -r -l 1 -nd https://archive.org/details/stackexchange
