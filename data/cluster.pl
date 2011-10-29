#!/usr/bin/perl -w

%clusters = ();
open(CIN, $ARGV[0]);
while (<CIN>) {
  chomp;
  @parts = split /\t/;
  $clusters{$parts[0]} = $parts[1];
}
close(CIN);

open(FIN, $ARGV[1]);
while (<FIN>) {
  chomp;
  $id = $_;
  $tokens = <FIN>;
  $labels = <FIN>;
  <FIN>;
  if (exists($clusters{$id})) {
    print $clusters{$id}, "\n";
  } else {
    print "##NULL##\n";
  }
  print $tokens;
  print $labels;
  print "\n";
}
