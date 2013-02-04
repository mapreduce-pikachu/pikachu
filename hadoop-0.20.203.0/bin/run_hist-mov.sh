./hadoop dfs -rmr output
rm -f /export/gandhir/progCard.txt
./hadoop jar ../../Rohan/histogram-mov.jar org.myorg.HistogramMovies -conf ../conf/myconf.xml -r 14 hist-short output
#cp ~/rohan.txt ~/systap/code/Results/state_machine/
