./hadoop dfs -rmr output
rm -f /export/gandhir/progCard.txt
./hadoop jar ../../Rohan/multiwc.jar org.myorg.MultiFileWordCount -conf ../conf/myconf.xml -m 224 -r 14 text-64 output
#cp ~/rohan.txt ~/systap/code/Results/state_machine/
