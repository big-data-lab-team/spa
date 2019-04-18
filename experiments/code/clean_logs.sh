#!/usr/bin/bash


for f in pilot_*
do
	launch_cmd=$(grep -i 'program to launch\|command to be executed' $f)
	if [[ $launch_cmd == *"batch"* ]]; then
		exec_mode=$(echo $launch_cmd | awk '{print $11}')
	else
		exec_mode=$(echo $launch_cmd | sed s/[][]//g | awk '{print $17}')
		if [[ $exec_mode == "16p1d" ]]; then exec_mode=$exec_mode-bb-inc; fi
	fi
	echo $(date -d "$(stat -c %y $f)" +"%s")
	cp $f ../data/logger-out/$(date -d "$(stat -c %y $f)" +"%s")-$exec_mode.out	
done
