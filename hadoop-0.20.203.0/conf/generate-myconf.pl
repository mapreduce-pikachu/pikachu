#!/usr/bin/perl -w

use warnings;
my %fields = ();

my @node1;
my @node2 ;

#########################################################################################################
# These are constants - Do not change
$cStart = "<?xml version=\"1.0\"?>" . "\n" . "<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>" . "\n" . "<configuration>" . "\n";
$cEnd = "</configuration>" . "\n";
$cPropertyStart = "\t" . "<property>" . "\n";
$cPropertyEnd = "\t" . "</property>" . "\n\n";

$cNameStart = "\t\t" . "<name>";
$cNameEnd = "</name>" . "\n";

$cValueStart = "\t\t" . "<value>";
$cValueEnd = "</value>" . "\n";
# These are constants END

#########################################################################################################
# These are different fields. Arranges as name and values. DO CHANGE before generating.

$nodetype1 = 5;
$nodetype2 = 34; # For homogeneous cluster set nodetype2 to 0.

$reduceslotmachine1 = 2; 
$reduceslotmachine2 = 2;

$reduceslots1 = $nodetype1 * $reduceslotmachine1; # reduceslots = 2
$reduceslots2 = $nodetype2 * $reduceslotmachine2;  # reduceslots = 2

$istarazu = 0;
$tarazur = 1;

$reducedelay = 200;
$maxvirtualbins = ($reduceslots1 + $reduceslots2) * 10;

if ($istarazu == 1){
    $reducedelay = 20000;
    $maxvirtualbins = ($reduceslots1 * $tarazur + $reduceslots2) * 10;
}




open $fslaves, "<slaves" or die "cannot open slaves";
open $f1, ">nodetype1.node" or die "cannot open nodetype1";
open $f2, ">nodetype2.node" or die "cannot open nodetype2";

$count = 0;
while ($line = readline ($fslaves)){
    chomp($line);
    if ($count < $nodetype1){
	$node1[$count] = $line;
    }
    else{
	$node2[$count - $nodetype1] = $line;
    }
    $count++;
}

foreach $element (@node1){
    print $f1 $element . "\n";
}

foreach $element (@node2){
    print $f2 $element . "\n";
}

# Number of map slots on type1 node and type 2.
$fields{"numberMapSlots"} = "8,2"; 

# Each reducer slot.

$string = "";
for ($i = 0; $i < $nodetype1; $i++){
    for ($j = 0; $j < $reduceslotmachine1; $j++){
	$string = $string . $node1[$i] . ",";
    }
}

for ($i = 0; $i < $nodetype2; $i++){
    for ($j = 0; $j < $reduceslotmachine2; $j++){
	$string = $string . $node2[$i] . ",";
    }
}
$string = substr($string, 0, length($string) - 1);

$fields{"hostsForTasks"} = $string;

#"sp13.ecn.purdue.edu,sp13.ecn.purdue.edu,sp14.ecn.purdue.edu,sp14.ecn.purdue.edu,ds06.ecn.purdue.edu,ds06.ecn.purdue.edu,ds07.ecn.purdue.edu,ds07.ecn.purdue.edu,ds08.ecn.purdue.edu,ds08.ecn.purdue.edu,ds09.ecn.purdue.edu,ds09.ecn.purdue.edu,ds10.ecn.purdue.edu,ds10.ecn.purdue.edu"; 

#host reducer share -- Not used. but let it be there.
$fields{"hostsReducerShare"} = "0,0,1,1,2,2,3,3,4,4,5,5,6,6,7,7,8,8,9,9,10,10,11,11,12,12,13,13";

# Nodes to execute the map tasks
$string = "";
for ($i = 0; $i < $nodetype1; $i++){
    for ($j = 0; $j < 1; $j++){
	$string = $string . $node1[$i] . ",";
    }
}

for ($i = 0; $i < $nodetype2; $i++){
    for ($j = 0; $j < 1; $j++){
	$string = $string . $node2[$i] . ",";
    }
}
$string = substr($string, 0, length($string) - 1);

$fields{"mapsForTasks"} = $string;
#"sp13.ecn.purdue.edu,sp14.ecn.purdue.edu,ds06.ecn.purdue.edu,ds07.ecn.purdue.edu,ds08.ecn.purdue.edu,ds09.ecn.purdue.edu,ds10.ecn.purdue.edu";

# reducerDelay
$fields{"reducerDelay"} = $reducedelay;

# maxVirtualBins
$fields{"maxVirtualBins"} = $maxvirtualbins;

# nodeType1
$string = "";
for ($i = 0; $i < $nodetype1; $i++){
    for ($j = 0; $j < 1; $j++){
	$string = $string . $node1[$i] . ",";
    }
}

$string = substr($string, 0, length($string) - 1);

$fields{"nodeType1"} = $string;

#"sp13.ecn.purdue.edu,sp14.ecn.purdue.edu";

# nodeType2

$string = "";
for ($i = 0; $i < $nodetype2; $i++){
    for ($j = 0; $j < 1; $j++){
	$string = $string . $node2[$i] . ",";
    }
}
$string = substr($string, 0, length($string) - 1);

$fields{"nodeType2"} = $string;
#"ds06.ecn.purdue.edu,ds07.ecn.purdue.edu,ds08.ecn.purdue.edu,ds09.ecn.purdue.edu,ds10.ecn.purdue.edu";

# isTarazu
$fields{"isTarazu"} = $istarazu;

# total reducer slots on each TYPE of nodes
$fields{"totalReduceSlots"} = "$reduceslots1" . "," . "$reduceslots2";

# R for Tarazu
$fields{"tarazuR"} = $tarazur;

#########################################################################################################
# Code starts
open $fTemp, ">myconf.xml" or die "cannot open temp myconf file";

print "Remember to check: \n" . "isTarazu ($istarazu, $tarazur)\n" . "totalReduceSlots ($reduceslots1,$reduceslots2)\n" . "maxVirtualBins ($maxvirtualbins)\n" . "reducerDelay ($reducedelay)\n" . "nodeType1-2 ($nodetype1,$nodetype2)\n";

print $fTemp $cStart;

while (($key, $value) = each(%fields) ) {
    print $fTemp $cPropertyStart;
    print $fTemp $cNameStart;
    print $fTemp $key;
    print $fTemp $cNameEnd;

    print $fTemp $cValueStart;
    print $fTemp $value;
    print $fTemp $cValueEnd;

    print $fTemp $cPropertyEnd;

}
print $fTemp $cEnd;
