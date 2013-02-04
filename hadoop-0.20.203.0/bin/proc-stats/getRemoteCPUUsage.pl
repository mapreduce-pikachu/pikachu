#! /usr/bin/perl -w

use warnings;
my @cpu;

open $filein, "</home/gandhir/Hadoop_Setup-socc/hadoop-0.20.203.0/conf/slaves" or die "cannnot open file";
open $filelog, ">/export/gandhir/couusage.log" or die "cannnot open file";

$count = 0;
while ($line = readline ($filein)){
    chomp($line);
    $node = -1;

#    print $line . "\n";
    if (index ($line, 'sp') != -1){
	$node = 0;
    }elsif (index ($line, 'ds') != -1){
	$node = 1;
    }

    if (($node != 1) && ($node != 0)){
	print $filelog  "No SP; No DS" . $line . "\n";
	exit;
    }




    $out = `ssh $line "cat /export/gandhir/rohan-cpu.txt | tail -n 1"`;
    chomp($out);
    @cpuUsage = split (" ", $out);
    $ratio = $cpuUsage[1] / $cpuUsage[0];

    if (!$cpu[$node]){
	$cpu[$node] = 0;    
	$count[$node] = 0;
    }

    $cpu[$node] = $cpu[$node] * $count[$node] + $ratio;
    print $filelog $cpu[$node] . " " . $ratio . " " .  $count[$node] . "\n";
    $count[$node]++;
    $cpu[$node] = $cpu[$node] / $count[$node];
    print $filelog $line . "\t" . $out . "\n";
}

printf("%.3f:%.3f\n", $cpu[0], $cpu[1]);
