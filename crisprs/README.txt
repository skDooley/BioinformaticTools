This module takes directories with .fasta, .fna, or .fa files and searches them for a crispr array using minCED and pilerCR. For adaptation, clone this folder and fix paths in scripts/GetCRISPRs.py to reflect your local repositories. Run using:
python scripts/GetCRISPRs.py

This will create all the commands that will be run for minCED and pilerCR. If you use a slurm submission system fon an HPC, use:
./scripts/LaunchCRISPR_find.sh
To submit all the jobs to the hpc. (NOTE: Modify Line 8 with todays date if you want the logs labeled with the date.)

Alternatively you can run this directly from the command line with LaunchCRISPR_find.sh by modifying line 8 to:
    bash $fname
and remove lines 10-17, all the scripts will run 1 by 1.
