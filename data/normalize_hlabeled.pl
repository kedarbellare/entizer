#!/usr/bin/perl -w

open(FIN, $ARGV[0]);
while (<FIN>) {
  print $_;
  $words = <FIN>;
  print $words;
  $labels = <FIN>;
  chomp($labels);
  @labelarr = split(/\t/, $labels);
  # if labels contains only booktitle, then map them to title
  $contains_title = 0;
  for ($i = 0; $i <= $#labelarr; $i++) {
    if ($labelarr[$i] =~ m/[BI]-title/g) {
      $contains_title = 1;
    }
  }
  if ($contains_title == 0) {
    for ($i = 0; $i <= $#labelarr; $i++) {
      $labelarr[$i] =~ s/B-booktitle/B-title/g;
      $labelarr[$i] =~ s/I-booktitle/I-title/g;
    }
  }
  print join("\t", @labelarr), "\n";
  $empty = <FIN>;
  print $empty;
}
