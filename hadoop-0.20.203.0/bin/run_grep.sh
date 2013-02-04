./hadoop dfs -rmr /export/gandhir/hadoop/output
#strace -p 23365 -p 23187 -f -q >&../../../trace/strace_01
./hadoop jar ../../Rohan/Grep.jar org.myorg.Grep /export/gandhir/hadoop/input-grep /export/gandhir/hadoop/output DW
