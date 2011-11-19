#!/usr/bin/perl -w

# parameters to change
$maxIdsPerClust = 4;
$maxNumToks = 80;

# usage: filter_rexa_citations.pl rexa_citations.txt

%cluster_count = ();
open(FIN, $ARGV[0]);
while (<FIN>) {
	chomp;
	$cluster = $_;
	$tokline = <FIN>;
	$lblline = <FIN>;
	<FIN>; # empty
	chomp($tokline);
	chomp($lblline);
	@toks = split(/\t/, $tokline);
	@lbls = split(/\t/, $lblline);
	die "#toks != #lbls for:\n$tokline\n$lblline\n" unless scalar(@toks) == scalar(@lbls);
	for ($i = 0; $i <= $#lbls; $i++) {
		$lbls[$i] =~ s/-thesis$/-tech/g;
		$lbls[$i] =~ s/-series$/-tech/g;
		$lbls[$i] =~ s/B-note$/O/g;
		$lbls[$i] =~ s/I-note$/O/g;
	}
	if (scalar(@toks) <= $maxNumToks && (!exists($cluster_count{$cluster}) || $cluster_count{$cluster} < $maxIdsPerClust)) {
		print $cluster, "\n";
		print join("\t", @toks), "\n";
		print join("\t", @lbls), "\n";
		print "\n";
    if (!exists $cluster_count{$cluster}) {
      $cluster_count{$cluster} = 0;
    }
    $cluster_count{$cluster}++;
	}
}
close(FIN);
