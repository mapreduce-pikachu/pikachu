./hadoop dfs -rmr output
rm -f /export/gandhir/progCard.txt
rm -f /export/gandhir/reduceCard.txt
./hadoop jar ../../Rohan/invert-index.jar org.myorg.InvertedIndex -conf ../conf/myconf.xml -r 13 text-20 output
#cp ~/rohan.txt ~/systap/code/Results/state_machine/
