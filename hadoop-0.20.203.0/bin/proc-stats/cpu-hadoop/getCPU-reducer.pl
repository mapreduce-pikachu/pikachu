#! /usr/bin/perl -w

use warnings;
use Class::Struct;
use Time::HiRes qw(time);

struct process =>
{
    prevCPU => '$',
    active => '$',
    blacklist => '$',
};


$MOVINGAVERAGESIZE = 100;
$totalReducers = @ARGV;

open $filelog, ">/export/gandhir/cpu-monitor-log" or die "cannot open file";

for ($i = 0; $i < @ARGV;$i++){

    $pid = $ARGV[$i];
    $dir = '/proc/' . $pid;
    print $filelog $dir . "\n";
    if (-d $dir){

	$pidTable[$i] = $pid;

	$userCPU = `cat /proc/$pid/stat | awk '{print \$14}'`; # Getting the user CPU
	chomp($userCPU);
	$sysCPU = `cat /proc/$pid/stat | awk '{print \$15}'`; # gettting the sys CPU
	chomp($sysCPU);
	$procCPU = $userCPU + $sysCPU; # Total process CPU

	$hash{$pid} = new process(prevCPU => $procCPU, active => 1, blacklist => 0);
    }
    else{
	$totalReducers--;
    }
    print $filelog $pid . "\n";
}

if (@pidTable == 0){
    print $filelog "Exiting\n";
    exit;
}

#Initialization on other variables start
$timeStart = time;
$timeNow = $timeStart;

#Getting the total CPU used;
$activeCPUCommand = `cat /proc/stat | grep cpu | head -n 1 | awk '{print \$2, \$3, \$4, \$5}'`; #User, sys, nice and idle cpu
chomp($activeCPUCommand);

@activeCPUFields = split(" ", $activeCPUCommand);
$totalCPUPrev = $activeCPUFields[0] + $activeCPUFields[1] + $activeCPUFields[2] + $activeCPUFields[3];

$count = 0;
for ($i = 0; $i < $MOVINGAVERAGESIZE * $totalReducers; $i++){
    $movingAverage[$i] = 0; # So that the elements are defined.
}
$maximum = 0;
open $f1, ">/export/gandhir/rohan2.txt" or die "cannot open f1";

#Initialization on other variables end

print $filelog "Initialization done\n";

while (($timeNow - $timeStart) < (6 * 60 * 10)){ # scanning the CPU until 60 mins OR process dies.
    $timeNow = time;
    sleep 2;

#Getting the total CPU used;
    $activeCPUCommand = `cat /proc/stat | grep cpu | head -n 1 | awk '{print \$2, \$3, \$4, \$5}'`; #User, sys, nice and idle cpu
    chomp($activeCPUCommand);

    @activeCPUFields = split(" ", $activeCPUCommand);
    $totalCPU = $activeCPUFields[0] + $activeCPUFields[1] + $activeCPUFields[2] + $activeCPUFields[3];
    $activeCPU = $totalCPU - $totalCPUPrev;
    $totalCPUPrev = $totalCPU;

#    print $filelog $timeNow . " " . $activeCPU . "\n";

    foreach $pid (@pidTable){
	$dir = '/proc/' . $pid;

	if (-d $dir){
	    $userCPU = `cat /proc/$pid/stat | awk '{print \$14}'`; # Getting the user CPU
	    chomp($userCPU);
	    $sysCPU = `cat /proc/$pid/stat | awk '{print \$15}'`; # gettting the sys CPU
	    chomp($sysCPU);
	    $idleCPU = `cat /proc/$pid/stat | awk '{print \$16}'`; # gettting the idle CPU
	    $procCPU = $userCPU + $sysCPU + $idleCPU; # Total process CPU

	    $cpuUsed = $procCPU - $hash{$pid}->prevCPU;
	    $cpuPercent = $cpuUsed / $activeCPU * 100;
#	    print $cpuPercent . "\n";
	    $current = $cpuPercent;

	    $movingAverage[$count % ($MOVINGAVERAGESIZE * $totalReducers)] = $cpuPercent;
	    $count = ($count + 1);
#	    print "Result: " . $count . " " . $cpuUsed . "\n";
	    $hash{$pid}->prevCPU($procCPU);
	}
	else{
	    $hash{$pid}->active = 0; # Not using it as of now. 
	    $totalReducers--;

	    if ($totalReducers <= 0){
		print $filelog "Exiting\n";
		exit;
	    }
	}
    }  #foreach

    $total = 0;

    if ($count < ($MOVINGAVERAGESIZE * $totalReducers)){
	$min_count = $count;
    }
    else{
	$min_count = $MOVINGAVERAGESIZE * $totalReducers;
    }

    for ($i = 0; $i < $min_count; $i++){
	$total += $movingAverage[$i];
    }
    $average = $total / $min_count;

    if (($timeNow - $timeStart) > 150){
	if ($maximum < $average){
	    $maximum = $average;
	}
    }

    printf $f1 ("%d\t%f\t%f\t%f\n", $timeNow - $timeStart, $maximum, $average, $cpuPercent);

    open $f2, ">/export/gandhir/rohan-cpu.txt" or die "cannot open f2";
    printf $f2 ("%f\t%f\n", $maximum, $average, $current);
    close $f2;
}

print $f1 "Shutting down\n";
close $f1;
