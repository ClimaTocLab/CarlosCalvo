#!/bin/bash

#SBATCH --job-name=run_thundeR
#SBATCH --qos=np
#SBATCH --nodes=7
#SBATCH --ntasks=896
#SBATCH --ntasks-per-node=128
#SBATCH --mem=200G
#SBATCH --time=2-00:00:00
#SBATCH --hint=nomultithread
#SBATCH --output=output_thundeR.out
#SBATCH --error=error_thundeR.error


set -vx

module purge

module load python3
module load conda
conda activate sounding_v2

python3 thundeR_parallel.py
