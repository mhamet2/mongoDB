#!/usr/bin/perl

use strict;
use warnings;

my $CONFIG;
my $ERROR;
my $email_file;
my $today_backup;

my $pipe2;
my $pipe3;
my @pipe;

$ENV{PATH}  = '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'; 
$CONFIG =read_config();
our $VERSION = '0.01';

sub read_config
{
        (my $config_file = $0 ) =~ s/\.pl/\.config/;
        open(my $file,'<',$config_file)
                or die "Cannot open config file: $config_file: $!";
        my %config;
        while(my $line = <$file>)
        {
                next if $line =~ /^\s*#/;
                chomp($line);
                my ($key,$value) = split/=/,$line;
                $config{ $key } = $value if $key and $value;
        }
        return \%config;
}

############ MONGODB COMMANDS #################
my $stop_balancing =  "mongo --host $CONFIG->{HOST} --port $CONFIG->{MONGOS} --eval \"sh.stopBalancer()\"";
my $start_balancing = "mongo --host $CONFIG->{HOST} --port $CONFIG->{MONGOS} --eval \"sh.startBalancer()\"";
my $check_balancing = "mongo --quiet --host $CONFIG->{HOST} --port $CONFIG->{MONGOS} --eval \"sh.getBalancerState()\"";
my $start_snapshot ="lvcreate -L $CONFIG->{SNAPSIZE} -s -n mongo_backup $CONFIG->{LVMONGO}";
my $remove_snapshot ="lvremove -f " . "\$(lvs $CONFIG->{LVMONGO} --noheadings | awk \{'print\$2'\})"."\/mongo_backup";
my $mount_snapshot ="mount " . "\/dev\/\$(lvs $CONFIG->{LVMONGO} --noheadings | awk \{'print\$2'\})\/mongo_backup"." $CONFIG->{PMOUNT}";
my $unmount_snapshot ="umount " . "\/dev\/\$(lvs $CONFIG->{LVMONGO} --noheadings | awk \{'print\$2'\})"."\/mongo_backup";
#my $start_backup = "tar cfp " .  "$CONFIG->{BACK}" . "/" . "$today_backup.tar  $CONFIG->{PBACK}";
my $lock_shard = "db.fsyncLock()";
my $unlock_shard = "db.fsyncUnlock()";
#############################################

sub OpenMongoPipe{

 foreach (@_){ 
  my @test;
  open $test[$_], '|-','mongo','--host',$CONFIG->{HOST} ,'--port', $_ , or die "cannot pipe from mongo :$!";
  push @pipe, $test[$_];
  do_log("Opened pipe successfully to  $CONFIG->{HOST} on port $_ \n");
 } 
}

sub CloseMongoPipes{
 foreach (@pipe){
  close ($_); 
  do_log("Closed pipe successfully to  $CONFIG->{HOST} on port $_ \n");
 }
}

sub ToMongoPipe{
  $| =1;
  my $pip = shift;
  my $string = shift;
  print $pip "$string\n";
  $pip->autoflush(1);
}


sub LockShards{
 foreach (@pipe){
 ToMongoPipe($_,$lock_shard);
 }
}

sub UnLockShards{
 foreach (@pipe){
 ToMongoPipe($_,$unlock_shard);
 }
}

#sub GetDatabases{
#
# my @databases = `$get_databases`;
# print @databases;
# return @databases;
#}

sub GetBalancerState{

 my $balancerstate =`$check_balancing`;
 chomp ($balancerstate);
 do_log("Balancing state = $balancerstate\n");
 if (($balancerstate ne "true") and  ($balancerstate ne "false")){
   $ERROR = " Unable to read Balancing state";
 }
 return $balancerstate;
}

sub StopBalancing{

 my $stopbalancing = `$stop_balancing`;	
 if (GetBalancerState() ne "false"){
  $ERROR = "Unable to stop balancing";
 }
 else{
 do_log("Balancing stopped\n");
 }
}

sub StartBalancing{

 my $startbalancing = `$start_balancing`;
 if (GetBalancerState() ne "true"){
  $ERROR = "Unable to start balancing";
 }
 else{
 do_log("Balancing started\n");
 } 
}

sub DoBackupFS
{
    my @data = localtime(); 
    $today_backup = sprintf "mongodb_backup_%04d%02d%02d_%02d_%02d", $data[5]+1900,$data[4]+1,$data[3],$data[2],$data[1];
    my $start_backup = "tar cfp " .  "$CONFIG->{PBACK}" . "/" . "$today_backup.tar  $CONFIG->{PMOUNT}";
    my $startbackup=`$start_backup`;
    do_log("Created backup on  $CONFIG->{PBACK}/$today_backup.tar\n");
}

sub DoSnapshot
{
    do_log("Creating snapshot from  $CONFIG->{LVMONGO}/ as mongo_backup \n");
    system "$start_snapshot";
    if ($? != 0) {
    $ERROR="Snapshot $CONFIG->{LVMONGO} creation failed $? $!";
    }

}

sub MountSnapshot
{
    do_log("Mounting snapshot mongo_backup to  $CONFIG->{PMOUNT}/\n");
    system "$mount_snapshot";
    if ($? != 0) {
    $ERROR="Failed mounting the snapshot $? $!";
    }

}

sub UnmountSnapshot
{
    do_log("Unmounting snapshot mongo_backup\n");
    system "$unmount_snapshot";
    if ($? != 0) {
    $ERROR="Failed unmounting the snapshot $? $!";
    }
}

sub RemoveSnapshot
{
    do_log("Removing snapshot mongo_backup\n");
    system "$remove_snapshot";
    if ($? != 0) {
    $ERROR="Unable to remove snapshot $remove_snapshot $? $!";
    }

}

sub DoBackupLVM
{
    DoSnapshot();
    MountSnapshot();
    DoBackupFS();
    UnmountSnapshot();
    RemoveSnapshot();
}

sub CompressBackup
{
 system "gzip -9 $CONFIG->{PBACK}/$today_backup.tar";
 if (-s "$CONFIG->{PBACK}/$today_backup" . ".tar.gz") {
    do_log ("Compressed successfully $CONFIG->{PBACK}/$today_backup.tar\n");
 }
 else{     
    $ERROR = "Error compressing $CONFIG->{PBACK}/$today_backup.tar"; 
 }
}

###### MISC #####

sub CheckBackup
{
    #my $today_backup="logs/mongodb_log_backup_20160504.log";
    do_log("Testing the backup\n");
    system "/bin/tar -xvzf $CONFIG->{PBACK}/$today_backup.tar.gz -O > /dev/null" and $ERROR = "Error during the backup test $? $!";
    my $size = (-s $CONFIG->{PBACK} . '/' . $today_backup . '.tar.gz') / 1024 / 1024 / 1024;
    do_log("Backup size $size Gb \n");
    do_log("Backup test ok\n");
}

sub CheckErrors
{
    if( $ERROR )
    {
        do_log("Backup script has aborted due to: $ERROR");
        send_error_mail();
        exit;
    }
}

sub send_error_mail
{
    `$CONFIG->{MAIL} -s $CONFIG->{IDMAQUINA}"-MongoDB-ERROR" $CONFIG->{NOTIFY} < $email_file`;
     do_log("Sending ERROR mail ($ERROR)\n");
}

sub SendOkMail
{
     do_log("Sending OK mail\n");
    `$CONFIG->{MAIL} -s $CONFIG->{IDMAQUINA}"-MongoDB-OK" $CONFIG->{NOTIFY} < $email_file`;
}

sub do_log
{
        my $text = shift;
        my $data = localtime();
        my @data = localtime();
        my $logformat = sprintf "%04d%02d%02d", $data[5]+1900,$data[4]+1,$data[3];
        my $log_file = "$CONFIG->{PBACK_LOG}/mongodb_log_backup_$logformat.log";
        $email_file = $log_file;
        open(my $log,'>>',$log_file )
                or warn "Cant log to $log_file) : $!";
        print $log "[$data] $text";
}

sub ApplyRetention
{
      do_log("Applying retention. Deleting $CONFIG->{PBACK}/mongodb_backup_*_* older than $CONFIG->{RETENTION} day\n");
      system "find $CONFIG->{PBACK}/ -name \"mongodb_backup_*_*.gz\" -mtime +$CONFIG->{RETENTION} -exec rm  {} \\;";
      if ($? != 0) {
        $ERROR="Failed deleting old backups $? $!";
      }  

}



StopBalancing();
OpenMongoPipe($CONFIG->{RS0},$CONFIG->{RS1},$CONFIG->{CONFIGSERVER});
LockShards();
#DoBackup();
DoBackupLVM();
UnLockShards();
StartBalancing();
CompressBackup();
CheckBackup();
CloseMongoPipes();
ApplyRetention();
CheckErrors();
SendOkMail();
