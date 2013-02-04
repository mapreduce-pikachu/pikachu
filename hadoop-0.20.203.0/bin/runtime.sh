#!/bin/bash

#for ((i = 0; i < 10; i++))
#do

#RC=$(date)
#echo "Start ${RC}" >> runtime.out
#./run_wordcount.sh
#RC=$(date)
#echo "Stop ${RC}" >> runtime.out
#./script.sh
#sleep 5

#done

#echo "script" >> runtime.out
for ((i = 0; i < 2; i++))
do
rm /home/gandhir/whois.txt
./script.sh
mv /home/gandhir/mapper.txt /home/gandhir/mapper_$i.rohan
sleep 5
done
