#!/usr/bin/perl
# jneubert, 2020-11-07

# Ad-hoc script for preparing swib participants data for institution
# reconsiliation against Wikidata and for aggregating the results and posting
# them into a SPARQL endpoint.

# If no WDQS-approved endpoint is available, the data can be injected by a
# values clause - see below.

# TODO: Split script into two parts
# 1) read conftool data and transform to Openrefine input
#    (taking into account prior results)
# 2) aggregate results and load into sparql endpoint

# TODO: Do not use CSV with '|' delimiter - it is used in real-world
# organization names. Better use TSV for input files, too.

# TODO: Replace string 'swib20' with a variable

use strict;
use warnings;
use utf8;

use Data::Dumper;
use File::Slurp;
use HTML::Template;
use IPC::Open2;
use JSON::XS;
use Path::Tiny;
use POSIX;
use Readonly;
use XML::LibXML;

binmode( STDOUT, ":utf8" );
$Data::Dumper::Sortkeys = 1;

Readonly my $INPUT_FILE => '../var/src/participants.xml';
Readonly my $REFINE_DIR => path('../var/org_refine');

#
# read ConfTool data from registered participants
#
my $parser = XML::LibXML->new();
my $doc    = $parser->parse_file($INPUT_FILE);

my @participants = $doc->getElementsByTagName('participant');

my ( %data, $participants_count );
foreach my $participant (@participants) {
  my $country  = $participant->findvalue('country');
  my $city     = $participant->findvalue('city');
  my $org_name = $participant->findvalue('organisation');

  # create a lookup key including country, to deal with possible
  # ambiguous org names
  my $key = "$country!$org_name";
  $data{$key}{count}++;
  $data{$key}{country}  = $country;
  $data{$key}{city}     = $city;
  $data{$key}{org_name} = $org_name;
  $participants_count++;
}

#
# read identified organizations (results from prior openrefine loops)
#
my ( %organization, %known_key );
my @refine_files = $REFINE_DIR->children(qr/^swib20_org\d([a-z])?\.tsv$/);
foreach my $refine_file (@refine_files) {
  my $lines = $refine_file->slurp_utf8;
  foreach my $line ( split( /\n/, $lines ) ) {
    my ( $key, $qid ) = split( /\t/, $line );
    next unless $qid;
    $organization{$qid}{count} += $data{$key}{count};
    $known_key{$key} = $qid;
    my ( $country, $name ) = split( '!', $key );
    push( @{ $organization{$qid}{names} }, $name );
  }
}
##print Dumper \%organization;

#
# create and exeecute sparql queries for endpoint update
#
# q&d, use econ_corp endpoint, which is allowed in a WDQS service clause.
# The endpoint can be updated only locally on the remote sparql_server
# (which must be accessible by key authentication). Via Perl's open2(), STDIN
# is used for data transmission.
my $remote_cmd =
    'ssh sparql_server curl --silent '
  . '-X POST -H \"Content-type: application/sparql-update\" '
  . '--data-binary @- http://localhost:3030/econ_corp/update';

my $delete_query = <<'EOF';
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX zbwext: <http://zbw.eu/namespaces/zbw-extensions/>

delete {
  ?item zbwext:swib20participants ?count .
}
where {
  ?item zbwext:swib20participants ?count .
}
EOF

# execute delete query remotely
my $child_in;
open2( undef, $child_in, $remote_cmd );
print $child_in $delete_query;
close $child_in;

my $insert_query = <<'EOF';
P+REFIX wd: <http://www.wikidata.org/entity/>
PREFIX zbwext: <http://zbw.eu/namespaces/zbw-extensions/>

insert {
  ?item zbwext:swib20participants ?count .
}
where {
  values ( ?item ?count ) {
EOF
my ( $unknown_count, $no_org_count );
foreach my $qid ( keys %organization ) {

  # skip not-yet-determined and undefined entities
  if ( $qid eq 'Q59496158' ) {
    $unknown_count += $organization{$qid}{count};
    next;
  }
  if ( $qid eq 'Q7883029' ) {
    $no_org_count += $organization{$qid}{count};
    next;
  }

  $insert_query .= "    ( wd:$qid $organization{$qid}{count} )\n";

# print line for a values clause in a static query, e.g.
# https://www.wikidata.org/wiki/User:Jneubert/SWIB_queries/SWIB_institutions_extended
  ##print "(  wd:$qid $organization{$qid}{count} )\n";
}
$insert_query .= <<'EOF';
  }
}
EOF

# execute insert query remotely
open2( undef, $child_in, $remote_cmd );
print $child_in $insert_query;
close $child_in;

#
# output unidentified organizations (for next openrefine loop)
#
# column structure
my $refine_input        = "key|country|city|name|bt|organisation\n";
my $not_yet_known_count = 0;
foreach my $key ( sort keys %data ) {
  next if $known_key{$key};
  $refine_input .= "$key|$data{$key}{country}|$data{$key}{city}"
    . "|$data{$key}{org_name}||$data{$key}{org_name}\n";
  $not_yet_known_count++;

}
path("../var/org_refine/swib20_org_input.csv")->spew_utf8($refine_input);

#
# output statistics
#
print "\n";
print $participants_count, " total participants\n";
## minus items which are not an organization
print scalar( keys %organization ) - 2,
  " institutions identified in Wikidata\n";
print $unknown_count, " institutions not (yet) in Wikidata\n";
print $no_org_count,  " no institution given\n";
print $not_yet_known_count,
" new institutions not yet known, in ../var/org_refine/swib20_org_input.csv\n";
print "\n";
print 'Not in Wikidata: ',
  join( '; ', @{ $organization{'Q59496158'}{names} } ), "\n";

