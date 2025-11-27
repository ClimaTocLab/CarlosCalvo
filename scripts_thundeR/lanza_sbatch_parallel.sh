#!/bin/bash

#SBATCH --job-name=run_thundeR
#SBATCH --qos=np
#SBATCH --nodes=1
#SBATCH --ntasks=64
#SBATCH --ntasks-per-node=64
#SBATCH --mem=50G
#SBATCH --time=2-00:00:00
#SBATCH --hint=nomultithread
#SBATCH --output=output_thundeR.out
#SBATCH --error=error_thundeR.error


set -vx

source .sounding/bin/activate

srun python3 thundeR_parallel.py
