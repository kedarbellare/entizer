#!/usr/bin/perl -w

# parameters to change
$maxIdsPerClust = 4;
$maxNumToks = 80;

# usage: filter_rexa_citations.pl rexa_clusters.txt rexa_citations.txt

%clust2ids = ();
open(CLIN, $ARGV[0]);
while (<CLIN>) {
	chomp;
	@clustid = split /\t/;
	$id = shift @clustid;
	$clust = shift @clustid;
	if (exists $clust2ids{$clust}) {
		$clust2ids{$clust} = $clust2ids{$clust} . "\t$id";
	} else {
		$clust2ids{$clust} = $id;
	}
}
close(CLIN);

%filterid2clust = ();
foreach $clust (keys %clust2ids) {
	@ids = split(/\t/, $clust2ids{$clust});
	for ($i = 0; $i <= $#ids && $i < $maxIdsPerClust; $i++) {
		$filterid2clust{$ids[$i]} = $clust;
	}
}

open(FIN, $ARGV[1]);
while (<FIN>) {
	chomp;
	$id = $_;
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
	if (scalar(@toks) <= $maxNumToks && exists($filterid2clust{$id})) {
		print $id, "\n";
		print join("\t", @toks), "\n";
		print join("\t", @lbls), "\n";
		print "\n";
	}
}
close(FIN);
