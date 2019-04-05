#!/bin/bash
#SBATCH -p physical
#SBATCH --time=00:10:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
module load Python/3.6.4-intel-2017.u2
mpirun python src/main.py melbGrid.json smallTwitter.json
