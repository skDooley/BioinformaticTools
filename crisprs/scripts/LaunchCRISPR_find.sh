#!/bin/bash


for fname in /mnt/research/germs/shane/databases/crisprs/scripts/hpc_scripts/*.sb;
do
    #baseFileName=$(basename "$fname" .sb)
    echo $fname
    sbatch --job-name=CRISPR$batchn -A shadeash-colej --cpus-per-task=1 --mem=10G --output=/mnt/research/germs/shane/databases/crisprs/logs/CRISPRS_10_19_2020_run_$batchn.log --ntasks=1 --time=1:00:00 $fname

    numTotal=$(squeue -u $USER -O reason | grep -v REASON  | wc -l)
    total=0
    while [ $numTotal -gt 999 ] ; do
        sleep 10
        total=$((total + 10))
        echo "Sleeping. Wait Time: $total"
        numTotal=$(squeue -u $USER -O reason | grep -v REASON  | wc -l)
    done
done
