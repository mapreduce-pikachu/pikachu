ssh gandhir@ds02 'rm -f /export/gandhir/rohan.txt' &
ssh gandhir@ds02 '/home/gandhir/systap/code/remote_script.sh' &
ssh gandhir@ds03 'rm -f /export/gandhir/rohan.txt' &
ssh gandhir@ds03 '/home/gandhir/systap/code/remote_script.sh' &
ssh gandhir@ds04 'rm -f /export/gandhir/rohan.txt' &
ssh gandhir@ds04 '/home/gandhir/systap/code/remote_script.sh' &
ssh gandhir@ds05 'rm -f /export/gandhir/rohan.txt' &
ssh gandhir@ds05 '/home/gandhir/systap/code/remote_script.sh' &
ssh gandhir@ds06 'rm -f /export/gandhir/rohan.txt' &
ssh gandhir@ds06 '/home/gandhir/systap/code/remote_script.sh' &
ssh gandhir@ds07 'rm -f /export/gandhir/rohan.txt' &
ssh gandhir@ds07 '/home/gandhir/systap/code/remote_script.sh' &
ssh gandhir@ds08 'rm -f /export/gandhir/rohan.txt' &
ssh gandhir@ds08 '/home/gandhir/systap/code/remote_script.sh' &
ssh gandhir@ds09 'rm -f /export/gandhir/rohan.txt' &
ssh gandhir@ds09 '/home/gandhir/systap/code/remote_script.sh' &
ssh gandhir@ds10 'rm -f /export/gandhir/rohan.txt' &
ssh gandhir@ds10 '/home/gandhir/systap/code/remote_script.sh' &
ssh gandhir@ds11 'rm -f /export/gandhir/rohan.txt' &
ssh gandhir@ds11 '/home/gandhir/systap/code/remote_script.sh' &
ssh gandhir@ds12 'rm -f /export/gandhir/rohan.txt' &
ssh gandhir@ds12 '/home/gandhir/systap/code/remote_script.sh' &
ssh gandhir@ds14 'rm -f /export/gandhir/rohan.txt' &
ssh gandhir@ds14 '/home/gandhir/systap/code/remote_script.sh' &
sleep 6
#/home/gandhir/systap/code/socket_codes/perl_server_state_machine_v4.pl &
RC=$(date)
echo "Start $RC" >> runtime.out
#./run_grep.sh 
./run_wordcount.sh 
RC=$(date)
echo "Stop $RC" >> runtime.out
ssh gandhir@ds02 /home/gandhir/systap/code/clean_up.sh &
ssh gandhir@ds03 /home/gandhir/systap/code/clean_up.sh &
ssh gandhir@ds04 /home/gandhir/systap/code/clean_up.sh &
ssh gandhir@ds05 /home/gandhir/systap/code/clean_up.sh &
ssh gandhir@ds06 /home/gandhir/systap/code/clean_up.sh &
ssh gandhir@ds07 /home/gandhir/systap/code/clean_up.sh &
ssh gandhir@ds08 /home/gandhir/systap/code/clean_up.sh &
ssh gandhir@ds09 /home/gandhir/systap/code/clean_up.sh &
ssh gandhir@ds10 /home/gandhir/systap/code/clean_up.sh &
ssh gandhir@ds11 /home/gandhir/systap/code/clean_up.sh &
ssh gandhir@ds12 /home/gandhir/systap/code/clean_up.sh &
ssh gandhir@ds14 /home/gandhir/systap/code/clean_up.sh &
