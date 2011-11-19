#!/usr/bin/perl -w

# usage: filter_rexa_records.pl rexa_records.txt

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
	if ($cluster ne "##NULL##") {
		print $cluster, "\n";
		print join("\t", @toks), "\n";
		print join("\t", @lbls), "\n";
		print "\n";
	}
}
close(FIN);
