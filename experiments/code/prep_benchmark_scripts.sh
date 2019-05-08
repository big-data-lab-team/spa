#!/usr/bin/bash

for i in $(find . -regextype "egrep" -iregex './[0-9][0-9][0-9]+.*applogs'); do
	slurm_id=$(echo $i | sed -e 's/-applogs//g' -e 's/\.\///g')
	log=$(find . -regextype "egrep" -iregex "./logs/.*$slurm_id\.out")
	task=$(echo $log | sed -e 's/\.\/logs\///g' -e 's/-.*//g')
	out="$task-$slurm_id.txt"
	out_new="$task-$slurm_id.csv"

	echo "slurm_id: $slurm_id \t task: $task \t outfile: $out"
	grep -H '' $i/* > $out

	sed -e 's/:/,/g' -e 's/\.int.*txt//g' -e 's/.*applogs\///g' <$out >$out_new
	rm $out

done
