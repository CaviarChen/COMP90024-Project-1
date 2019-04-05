#!/bin/bash
#SBATCH --time=00:03:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
module load Python/3.6.4-intel-2017.u2
mpiexec python src/main.py melbGrid.json bigTwitter.json
