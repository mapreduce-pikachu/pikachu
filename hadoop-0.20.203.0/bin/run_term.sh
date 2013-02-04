./hadoop dfs -rmr output
rm -f /export/gandhir/progCard.txt
./hadoop jar ../../Rohan/term.jar org.myorg.TermVectorPerHost -conf ../conf/myconf.xml -r 9 text-short output
#./hadoop jar ../../Rohan/term.jar org.myorg.TermVectorPerHost -conf ../conf/myconf.xml -r 9 join-20 output
#cp ~/rohan.txt ~/systap/code/Results/state_machine/
