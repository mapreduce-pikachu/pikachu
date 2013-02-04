#/bin/bash


ARG=$1
if [ $# -eq 0 ]
then
echo "./restart.sh 1 for only restart"
echo "./restart.sh 2 for restart + ant"
exit
fi

if [ $ARG -eq 1 ]
then
echo "pressed 1"
./mystop.sh
sleep 5
./mystart.sh
exit
fi

if [ $ARG -eq 2 ]
then
./mystop.sh
cd ..
ant
scp -r build sp13:~/Hadoop_Setup-socc/hadoop-0.20.203.0 >&temp
scp -r build sp14:~/Hadoop_Setup-socc/hadoop-0.20.203.0 >&temp
ssh sp13 "rm ~/Hadoop_Setup-socc/hadoop-0.20.203.0/logs/*"
ssh sp14 "rm ~/Hadoop_Setup-socc/hadoop-0.20.203.0/logs/*"
ssh sp13 "rm -rf ~/Hadoop_Setup-socc/hadoop-0.20.203.0/logs/userlogs/*"
ssh sp14 "rm -rf ~/Hadoop_Setup-socc/hadoop-0.20.203.0/logs/userlogs/*"
cd bin
./mystart.sh
exit
fi


