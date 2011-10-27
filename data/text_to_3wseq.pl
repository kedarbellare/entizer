#!/usr/bin/perl -w

while (<STDIN>) {
  $wline = <STDIN>;
  $lline = <STDIN>;
  <STDIN>;

  chomp($wline);
  @words = split(/\t/, $wline);
  print join("\t", @words), "\n";
}
