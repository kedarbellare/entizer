#!/usr/bin/perl -w

%rcounts = ();
open(RIN, $ARGV[0]);
while (<RIN>) {
	chomp;
	@parts = split /\t/;
	$rcounts{$parts[1]} = $parts[0];
}
close(RIN);

%tcounts = ();
open(TIN, $ARGV[1]);
while (<TIN>) {
	chomp;
	@parts = split /\t/;
	$tcounts{$parts[1]} = $parts[0];
}
close(TIN);

open(RTIN, $ARGV[2]);
while (<RTIN>) {
	chomp;
	@parts = split /\t/;
	$count = shift(@parts);
	$rphr = shift(@parts);
	$tphr = shift(@parts);
	if (exists($rcounts{$rphr}) && exists($tcounts{$tphr})) {
		$rcount = $rcounts{$rphr};
		$tcount = $tcounts{$tphr};
		$pmi = log((1.0 * $count)/($rcount * $tcount));
		print "$pmi\t$rphr\t$tphr\n";
	}
}
close(RTIN);
