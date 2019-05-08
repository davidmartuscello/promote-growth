#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --time=12:00:00
#SBATCH --mem=50GB
#SBATCH --job-name=bdap
#SBATCH --mail-type=END
#SBATCH --mail-user=ttc290@nyu.edu
#SBATCH --output=slurm_%j.out

cd /scratch/ttc290/bdap/reddit_comments
wget -A bz2,xz,zst -c -r -l 1 -nd https://files.pushshift.io/reddit/comments/
