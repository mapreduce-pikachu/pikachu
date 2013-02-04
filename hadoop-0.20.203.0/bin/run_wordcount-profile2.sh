
for ((i=0;i<10;i++))
do
cd ~/Hadoop_Setup/hadoop-0.20.203.0/bin/
#cp ~/Hadoop_Setup/hadoop-0.20.203.0/conf/temp/mapred-site-$i.xml mapred-site.xml
#./mystop.sh
#sleep(2)
#./mystart.sh
ssh gandhir@sp13 '~/Hadoop_Setup/hadoop-0.20.203.0/bin/hotcloud/getStats/start.sh' &

./hadoop dfs -rmr output
rm -f /export/gandhir/progCard.txt
RC=$(date)
echo "Start $i $RC" >> profile.out
./hadoop jar ../../Rohan/wordcount.jar org.myorg.WordCount -conf ../conf/myconf.xml -D mapred.reduce.tasks=1 text1$i output
RC=$(date)
echo "Stop $i $RC" >> profile.out

ssh gandhir@sp13 '~/Hadoop_Setup/hadoop-0.20.203.0/bin/hotcloud/getStats/clean.sh' &
#ssh gandhir@sp13 'cd ~/Hadoop_Setup/hadoop-0.20.203.0/bin/hotcloud/getStats'
ssh gandhir@sp13 'mkdir /home/gandhir/Hadoop_Setup/hadoop-0.20.203.0/bin/hotcloud/getStats/result1\'$i
ssh gandhir@sp13 'mv /home/gandhir/Hadoop_Setup/hadoop-0.20.203.0/bin/hotcloud/getStats/mystat.r* /home/gandhir/Hadoop_Setup/hadoop-0.20.203.0/bin/hotcloud/getStats/'result1$i
ssh gandhir@sp13 'mv /home/gandhir/Hadoop_Setup/hadoop-0.20.203.0/bin/hotcloud/getStats/R.eps* /home/gandhir/Hadoop_Setup/hadoop-0.20.203.0/bin/hotcloud/getStats/'result1$i
ssh gandhir@sp13 'mv /export/gandhir/rohan.txt /home/gandhir/Hadoop_Setup/hadoop-0.20.203.0/bin/hotcloud/getStats/'result1$i
#mv ~/Hadoop_Setup/hadoop-0.20.203.0/bin/profile.out result$i
done
