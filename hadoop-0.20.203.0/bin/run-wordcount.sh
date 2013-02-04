#rm export/gandhir/rohan.txt
./hadoop dfs -rmr output
rm -f /export/gandhir/progCard.txt
rm -f /export/gandhir/reduceCard.txt
#./hadoop jar ../../Rohan/wordcount.jar org.myorg.WordCount -conf ../conf/myconf.xml -D mapred.reduce.tasks=8 text-short output
#./hadoop jar ../../Rohan/wordcount.jar org.myorg.WordCount -conf ../conf/myconf.xml -D mapred.reduce.tasks=14 text output
./hadoop jar ../../Rohan/wordcount.jar org.myorg.WordCount -conf ../conf/myconf.xml -D mapred.reduce.tasks=10 text-20 output

#cp ~/rohan.txt ~/systap/code/Results/state_machine/
