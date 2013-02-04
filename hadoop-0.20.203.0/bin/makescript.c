#include "stdio.h"
#include "string.h"
#include "stdlib.h"

int main ()
{
  int i = 0;
  int max_worker = 0;
  FILE *f1, *f2;
  char worker[15][10];
  int result;
  f1 = fopen ("script.sh", "w");
  f2 = fopen ("/home/gandhir/Hadoop_Setup/hadoop-0.20.203.0/conf/slaves", "r");

  //
  for (i = 0; i < 15; i++)
    {
      if ( (result = fscanf (f2, "%s", worker[i])) == EOF)
	break;
      fprintf (f1, "ssh gandhir@%s 'rm -f /export/gandhir/rohan.txt' &\n", worker[i]);
          fprintf (f1, "ssh gandhir@%s '/home/gandhir/systap/code/remote_script.sh' &\n", worker[i]);
      //      fprintf (f1, "ssh gandhir@%s 'sudo stap /home/gandhir/systap/code/syscall.stp -o /export/gandhir/trace_all' &\n", worker[i]);
      
    }
  //
  max_worker = i;
  //
  fprintf (f1, "sleep 6\n");
  fprintf (f1, "/home/gandhir/systap/code/socket_codes/perl_server_state_machine_v4.pl &\n");
  
  //

//  for (i = 0; i < max_worker; i++)
  //  fprintf (f1, "ssh gandhir@%s /home/gandhir/systap/code/process_for_master.pl &\n", worker[i]);
  fprintf (f1, "RC=$(date)\n");
  fprintf (f1, "echo \"Start $RC\" >> runtime.out\n");
    fprintf (f1, "./run_wordcount.sh \n");
 //
  fprintf (f1, "RC=$(date)\n");
  fprintf (f1, "echo \"Stop $RC\" >> runtime.out\n");
  //
  for (i = 0; i < max_worker; i++)
    fprintf (f1, "ssh gandhir@%s /home/gandhir/systap/code/clean_up.sh &\n", worker[i]);
  //
}
