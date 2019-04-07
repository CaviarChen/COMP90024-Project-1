#!/bin/bash
#SBATCH -p physical
#SBATCH --time=00:03:00
#SBATCH --nodes=2
#SBATCH --ntasks=8
#SBATCH --ntasks-per-node=4
#SBATCH --cpus-per-task=1
module load Python/3.6.4-intel-2017.u2
time mpirun python src/main.py melbGrid.json bigTwitter.json
