./hadoop dfs -rmr output
rm -f /export/gandhir/progCard.txt
rm -f /export/gandhir/reduceCard.txt
./hadoop jar ../../Rohan/selfjoin.jar org.myorg.SelfJoin -conf ../conf/myconf.xml -r 13 join-20 output
#cp ~/rohan.txt ~/systap/code/Results/state_machine/
