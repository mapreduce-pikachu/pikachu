./hadoop dfs -rmr rand-sort
rm -f /export/gandhir/progCard.txt
rm -f /export/gandhir/reduceCard.txt

#./hadoop jar ../../Rohan/sort.jar org.myorg.Sort -conf ../conf/myconf.xml -r 10 rand-short rand-sort 
./hadoop jar ../../Rohan/sort.jar org.myorg.Sort -conf ../conf/myconf.xml -r 14 rand rand-sort 

#./hadoop jar ../../Rohan/sort.jar org.myorg.Sort -conf ../conf/myconf.xml -r 14 rand-long* rand-sort 
