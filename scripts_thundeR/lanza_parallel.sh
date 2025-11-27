#!/bin/bash

for var in {12..13}; do
#for var in "ps"; do
  echo "AROME TIMESTEP $var"
  sbatch lanza_sbatch_parallel.sh $var

done
