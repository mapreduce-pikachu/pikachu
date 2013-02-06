#!/usr/bin/perl -w

use warnings;

$remotescript = "/home/ubuntu/Hadoop_Setup-socc/hadoop-0.20.203.0/conf/remotemount.pl";

print Rohan . "\n";

`sudo umount /mnt`;
`sudo mount /dev/xvdb /export`;
`sudo chown ubuntu /export`;
`mkdir /export/gandhir`;
`mkdir /export/gandhir/hadoop`;

open $f1, "./new" or die "cannot open slaves";

while ($line = readline($f1)) {
    chomp($line);
    print $line . "\n";

    `ssh $line "$remotescript"`;

    # `ssh $line "sudo umount /mnt"`;
    # `ssh $line "sudo mount /dev/xvdb /export"`;
    # `ssh $line "sudo chown ubuntu /export"`;
    # `ssh $line "mkdir /export/gandhir"`;
    # `ssh $line "mkdir /export/gandhir/hadoop"`;
}
