#!/bin/sh
## no critic (RequireUseStrict)
exec perl -wx $0 "$@"
if 0;
#!perl -w
#line 7

###############################################################################
# HP Autonomy
###############################################################################
#
# myway.pl
#
# A perl re-implementation of flyway, maintaining 100% compatibility with
# flyway schema files and tables whilst providing enhanced automatic operation
# and resilience.
#
###############################################################################

# FIXME: # {{{
#
# * flyway-compliant --check and --migrate currently do nothing: --migrate can
#   be assumed to be default, but --check should be treated as --dry-run?
#
# }}}

# TODO: # {{{
#
# * Roll-back and/or restore backups on failure;
#
# * In a Galera cluster, drop out of cluster prior to backup?
#
# * fork()/exec() pv when performing restorations, and check for failure.  If
#   so, show the last error from 'SHOW ENGINE INNODB STATUS';
#
# * When tokenising, check entry -> tokens -> tables -> db, and confirm it
#   exists (caching already seen databases);
#
# ? Incorporate Cloud modules to allow backup directly to platform object store
#   this may require splitting data into small (10MB) chunks and passing an
#   overall manifest on completion, depending on platform;
#
# Likely deprecated:
#
# - Enhance Percona SQLParser code to handle more statement types;
#
# - Add parser for GRANT, etc.;
#
# - Allow 'parse' dbdump option to tokenise backup?
#
# }}}

# Copyright 2014-2016 Stuart Shelton, Autonomy Systems Ltd. & Hewlett Packard
# Enterprise Company.
# Portions of this program are copyright 2010-2012 Percona Inc.
#
# Feedback and improvements are welcome.
#
# THIS PROGRAM IS PROVIDED "AS IS" AND WITHOUT ANY EXPRESS OR IMPLIED
# WARRANTIES, INCLUDING, WITHOUT LIMITATION, THE IMPLIED WARRANTIES OF
# MERCHANTIBILITY AND FITNESS FOR A PARTICULAR PURPOSE.
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation, version 2; OR the Perl Artistic License.  On UNIX and similar
# systems, you can issue `man perlgpl' or `man perlartistic' to read these
# licenses.
#
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc., 59 Temple
# Place, Suite 330, Boston, MA  02111-1307  USA.

{
package myway;

## no critic (ProhibitSubroutinePrototypes)

use 5.014; # ... so that push/pop/etc work on scalars (experimental).

use strict;
use warnings;

use re '/a';

no if ( $] >= 5.02 ), warnings => 'experimental::autoderef';
# ... because we should be able to push and splice via references, noting that
# this warning doesn't exist on earlier perl versions and so can't be
# unconditionally disabled.  Sigh.

# We have to enable and disable this module at run-time due to generating
# "Uncaught exception from user code" errors during certain DBI failures.
#use diagnostics;
# ... in actual fact, diagnostics causes more problems than it solves.  It does
# appear to be, in reality, quite silly.

use constant VERSION     =>  "1.4.0";

use constant TRUE        =>  1;
use constant FALSE       =>  0;

use constant DEBUG       => ( $ENV{ 'DEBUG' } or FALSE );
#use constant OLDSCHEMA  => ( $ENV{ 'OLDSCHEMA' } or FALSE );

use constant DEFDELIM    => ';';

use constant PORT        =>  3306;
use constant VERTICAPORT =>  5433;
use constant MARKER      => '`<<VERSION>>';

use constant LOGMAX      =>  3; # debug
use constant SQLRETRYMAX =>  3;
use constant SQLMAX      =>  15764; # 16299 is the experimental maximum length
				    # that MySQL accepts, and Vertica's limit
				    # is 535 characters less...

# Necessary non-default modules:
#
# * DBI::Format ( DBI::Shell, Text::Reform, IO::Tee )
# * File::Touch ()
# * File::Which ()
# * Data::GUID (Sub::Exporter, Params::Util, Data::OptList, Sub::Install, Data::UUID) - Only needed for Vertica
# * match::smart ( Sub::Infix )
# * Regexp::Common ()
# * Sort::Versions ()
#

use v5.10;
use match::smart;

use Cwd qw( getcwd realpath );
use DBI;
use DBI::Format;
use File::Basename;
use File::Copy;
use File::Glob qw( :glob );
use File::Path qw( make_path );
use File::Temp;
use File::Touch;
use File::Which;
use FileHandle;
use Getopt::Long qw( GetOptionsFromArray );
use Module::Loaded qw( is_loaded );
use Pod::Usage;
## no critic (ProhibitStringyEval)
#use Readonly;
#if( eval "require( Readonly::XS ); 1;" ) {
#	Readonly::XS -> import();
#}
use Regexp::Common;
use Scalar::Util qw( looks_like_number );
use Sort::Versions;
use Time::HiRes qw( gettimeofday tv_interval );

## no critic (ProhibitStringyEval)
if( eval "require( DBD::ODBC ); 1;" ) {
	DBD::ODBC -> import();
	if( eval "require( Data::GUID ); 1;" ) {
		Data::GUID -> import();
	}
}

use Data::Dumper;
#use Devel::StackTrace; # Core with Perl 5.20, apparently not present on Ubuntu
			# 14.04 LTS release.
#use if ( DEBUG ), 'Devel::Trace';


sub pdebug( $;$$ );
sub pcomment( $;$$ );
sub psql( $;$$ );
sub pnote( $;$$ );
sub pwarn( $;$$ );
sub pfail( $;$$ );
sub pfatal( $;$$ );

sub initstate();
sub compressquotes( $$ );
sub decompressquotes( $$ );
sub checkentry( $$;$$ );
sub pushstate( $$ );
sub pushentry( $$$$$;$ );
sub pushfragment( $$$;$$ );
sub processcomments( $$$;$ );
sub processline( $$;$$ );
sub processfile( $$;$$$$ );

sub dbopen( $$$$;$$ );
sub dbclose( ;$$$$ );

sub dbdump( $;$$$$ );
sub dbrestore( $$;$ );

sub verticasetsearchpath( $$;$ );
sub dbcheckconnection( $;$ );

sub sqldo( $$;$ );
sub sqlprepare( $$ );
sub sqlexecute( $$;$@ );
sub sqlgetvalue( $$;$ );
sub sqlgetvalues( $$;$ );

sub outputtable( $$;$ );
sub formatastable( $$$ );

sub databasegetinfo( $;$$$ );
sub metadatamigrateschema( $;$$$ );
sub metadataupdateflywaytable( $$$$$$ );

sub applyschema( $$$$;$ );

sub main( @ );


our $fatal   = "FATAL: ";
our $failed  = "FAIL:  ";
our $warning = "WARN:  ";
our $info    = "INFO:  ";


# XXX: Too many globals... :(

# Maintain a simple counter to check that we're not double-opening (or double-
# closing) DBI database connections.  There should be sufficient checks to
# detect this eventuality - but an additional counter does little harm...
our $dbconns = 0;

our $verbosity = 0;

# This simply tracks whether either of quiet or silent are specified, as a very
# simple time-saver to reduce the number of parameters which functions must be
# passed...
our $quietorsilent = FALSE;

# Currently, this determines whether we'll execute DROP statements - ideally,
# we'd allow this in developer environments but not in production... however,
# environment names are merely labels, and hold no additional meaning.
our $allowunsafe = FALSE;

# XXX: It would be nice if this wasn't a global, but the degree of separation
#      between where we could plausibly set this and where we use it are too
#      great to make this easily localisable...
our $tokenise = FALSE;

# XXX: We're seeing random disconnects after successful commands with Vertica
#      so this provides a temporary way to alert and attempt to reconnect...
#
# The logic here is that if retries is defined, which is usually the case, then
# standard error-checking is performed.  If retries is undefined then we return
# without further error-checking on the assumption that we've been called from
# *within* an error-checking function, and don't want to spiral off into
# infinity...
our $connection;
our $retries = 0;
# ... and, of course, Vertica is very special :(
our $searchpath;

# # Schema and related data... # {{{

# Changes in flyway release 4.0:
#  `version_rank` is removed;
#  `version` is no longer NOT NULL;
#  `checksum` is still not DEFAULT NULL (although this was a myway change);
#  The `version_rank` and `installed_rank` indices are removed;
#  `installed_rank` is now the PRIMARY KEY;
#  The `type` value 'INIT' is now 'BASELINE'.
#
our $flywayinit;
our $flywayinitdesc;
#if( OLDSCHEMA ) {
#	$flywayinit = 'INIT';
#	$flywayinitdesc = '<< Flyway Init >>';
#} else {
$flywayinit = 'BASELINE';
$flywayinitdesc = '<< Flyway Baseline >>';
#}

our $mywayhistoryname = 'myway_version_history';
our $mywayhistoryddl = <<DDL;
CREATE TABLE IF NOT EXISTS `$mywayhistoryname` (
    `id`		INT		UNSIGNED AUTO_INCREMENT   NOT NULL
  , `myway_version`	VARCHAR(15)	                          NOT NULL
  , `flyway_compatible`	VARCHAR(15)	                          NOT NULL
  , `type`		VARCHAR(20)	                          NOT NULL
  , `description`	VARCHAR(200)	                          NOT NULL
  , `migrated`		BOOLEAN		DEFAULT '0'               NOT NULL
  , PRIMARY KEY (`id`)
  , CONSTRAINT `${mywayhistoryname}_unique` UNIQUE (`myway_version`)
) ENGINE='InnoDB' DEFAULT CHARSET='ASCII';
DDL
#INSERT IGNORE INTO `$mywayhistoryname` (`myway_version`, `flyway_compatible`, `type`, `description`) VALUES('1.1.2', '2.1', 'INIT', '<< Flyway Init >>');
#INSERT IGNORE INTO `$mywayhistoryname` (`myway_version`, `flyway_compatible`, `type`, `description`) VALUES('1.2.0', '4.0', 'BASELINE', '<< Flyway Baseline >>');
our $verticamywayhistoryddl = <<DDL;
CREATE TABLE IF NOT EXISTS __SCHEMA__"$mywayhistoryname" (
    "id"		AUTO_INCREMENT
  , "myway_version"	VARCHAR(15)	                          NOT NULL
  , "flyway_compatible"	VARCHAR(15)	                          NOT NULL
  , "type"		VARCHAR(15)	                          NOT NULL
  , "description"	VARCHAR(200)	                          NOT NULL
  , "migrated"		BOOLEAN		DEFAULT FALSE             NOT NULL
  , PRIMARY KEY ("id") ENABLED
  , CONSTRAINT "${mywayhistoryname}_unique" UNIQUE ("myway_version") ENABLED
)
ORDER BY "id"
SEGMENTED BY HASH( "myway_version" ) ALL NODES;
DDL
#INSERT INTO __SCHEMA__"$mywayhistoryname" (`myway_version`, `flyway_compatible`, `type`, `description`) VALUES('1.1.2', '2.1', 'INIT', '<< Flyway Init >>');
#INSERT INTO __SCHEMA__"$mywayhistoryname" (`myway_version`, `flyway_compatible`, `type`, `description`) VALUES('1.2.0', '4.0', 'BASELINE', '<< Flyway Baseline >>');

our $flywaytablename = 'schema_version';
our $flywayddl;
our $verticaflywayddl;
#if( OLDSCHEMA ) {
#	$flywayddl = <<DDL;
#	CREATE TABLE IF NOT EXISTS `$flywaytablename` (
#	    `version_rank`	INT		DEFAULT NULL
#	  , `installed_rank`	INT		                          NOT NULL
#	  , `version`		VARCHAR(50)	                          NOT NULL
#	  , `description`	VARCHAR(200)	                          NOT NULL
#	  , `type`		VARCHAR(20)	                          NOT NULL
#	  , `script`		VARCHAR(1000)	                          NOT NULL
#	  , `checksum`		INT		DEFAULT NULL
#	  , `installed_by`	VARCHAR(100)	                          NOT NULL
#	  , `installed_on`	TIMESTAMP	DEFAULT CURRENT_TIMESTAMP NOT NULL
#	  , `execution_time`	INT		                          NOT NULL
#	  , `success`		BOOLEAN		                          NOT NULL
#	  , PRIMARY KEY                         (`version`)
#	  ,         KEY `schema_version_vr_idx` (`version_rank`)
#	  ,         KEY `schema_version_ir_idx` (`installed_rank`)
#	  ,         KEY `schema_version_s_idx`  (`success`)
#	) ENGINE='InnoDB' DEFAULT CHARSET='UTF8';
#DDL
#	$verticaflywayddl = <<DDL;
#	CREATE TABLE IF NOT EXISTS __SCHEMA__"$flywaytablename" (
#	    "version_rank"	INT		DEFAULT NULL
#	  , "installed_rank"	INT		                          NOT NULL
#	  , "version"		VARCHAR(50)	                          NOT NULL
#	  , "description"	VARCHAR(200)	                          NOT NULL
#	  , "type"		VARCHAR(20)	                          NOT NULL
#	  , "script"		VARCHAR(1000)	                          NOT NULL
#	  , "checksum"		INT		DEFAULT NULL
#	  , "installed_by"	VARCHAR(100)	                          NOT NULL
#	  , "installed_on"	TIMESTAMP	DEFAULT now()             NOT NULL
#	  , "execution_time"	INT		                          NOT NULL
#	  , "success"		BOOLEAN		                          NOT NULL
#	  , PRIMARY KEY ("version") ENABLED
#	)
#	ORDER BY "version", "version_rank", "installed_rank", "success"
#	SEGMENTED BY HASH( "version" ) ALL NODES;
#DDL
#	#ALTER TABLE __SCHEMA__"$flywaytablename" ADD CONSTRAINT "${flywaytablename}_pk" PRIMARY KEY ("version") ENABLED;
#} else {
$flywayddl = <<DDL;
CREATE TABLE IF NOT EXISTS `$flywaytablename` (
    `installed_rank`	INT		                          NOT NULL
  , `version`		VARCHAR(50)
  , `description`	VARCHAR(200)	                          NOT NULL
  , `type`		VARCHAR(20)	                          NOT NULL
  , `script`		VARCHAR(1000)	                          NOT NULL
  , `checksum`		INT		DEFAULT NULL
  , `installed_by`	VARCHAR(100)	                          NOT NULL
  , `installed_on`	TIMESTAMP	DEFAULT CURRENT_TIMESTAMP NOT NULL
  , `execution_time`	INT		                          NOT NULL
  , `success`		BOOLEAN		                          NOT NULL
  , CONSTRAINT `${flywaytablename}_pk` PRIMARY KEY (`installed_rank`)
  , KEY `${flywaytablename}_s_idx` (`success`)
) ENGINE='InnoDB' DEFAULT CHARSET='UTF8';
DDL
$verticaflywayddl = <<DDL;
CREATE TABLE IF NOT EXISTS __SCHEMA__"$flywaytablename" (
    "installed_rank"	INT		                          NOT NULL
  , "version"		VARCHAR(50)
  , "description"	VARCHAR(200)	                          NOT NULL
  , "type"		VARCHAR(20)	                          NOT NULL
  , "script"		VARCHAR(1000)	                          NOT NULL
  , "checksum"		INT		DEFAULT NULL
  , "installed_by"	VARCHAR(100)	                          NOT NULL
  , "installed_on"	TIMESTAMP	DEFAULT now()             NOT NULL
  , "execution_time"	INT		                          NOT NULL
  , "success"		BOOLEAN		                          NOT NULL
  , PRIMARY KEY ("version") ENABLED
)
ORDER BY "version"
SEGMENTED BY HASH( "version" ) ALL NODES;
DDL
#ALTER TABLE __SCHEMA__"$flywaytablename" ADD CONSTRAINT "${flywaytablename}_pk" PRIMARY KEY ("installed_rank") ENABLED;
#}

# Note that field-lengths in myway* tables are not arbitrary, but instead are
# sized to hold the maximum permissible value for the field-type, according to
# the appropraite standards - mostly POSIX.
#
# TIMESTAMP fields must be provided with a non-zero default value (or NULL)
# as the //second// and further TIMESTAMP fields will be implicitly created
# as DEFAULT 0, which breaks when NO_ZERO_DATE is in effect...
#
our $mywaytablename = 'myway_schema_version';
our $mywayddl = <<DDL;
CREATE TABLE IF NOT EXISTS `$mywaytablename` (
    `id`		CHAR(36)	                          NOT NULL
  , `dbuser`		CHAR(16)	                          NOT NULL
  , `dbhost`		CHAR(64)	                          NOT NULL
  , `osuser`		CHAR(32)	                          NOT NULL
  , `host`		CHAR(64)	                          NOT NULL
  , `sha1sum`		CHAR(40)	                          NOT NULL
  , `path`		VARCHAR(4096)	CHARACTER SET 'UTF8'      NOT NULL
  , `filename`		VARCHAR(255)	CHARACTER SET 'UTF8'      NOT NULL
  , `started`		TIMESTAMP
  , `sqlstarted`	TIMESTAMP	DEFAULT NULL                  NULL
  , `finished`		TIMESTAMP	DEFAULT NULL                  NULL
  , `status`		TINYINT		UNSIGNED
  , PRIMARY KEY (`id`)
) ENGINE='InnoDB' DEFAULT CHARSET='ASCII';
DDL
our $verticamywayddl = <<DDL;
CREATE TABLE IF NOT EXISTS __SCHEMA__"$mywaytablename" (
    "id"		CHAR(36)	                          NOT NULL
  , "dbuser"		CHAR(16)	                          NOT NULL
  , "dbhost"		CHAR(64)	                          NOT NULL
  , "osuser"		CHAR(32)	                          NOT NULL
  , "host"		CHAR(64)	                          NOT NULL
  , "sha1sum"		CHAR(40)	                          NOT NULL
  , "path"		VARCHAR(4096)	                          NOT NULL
  , "filename"		VARCHAR(255)	                          NOT NULL
  , "started"		TIMESTAMP
  , "sqlstarted"	TIMESTAMP	DEFAULT NULL                  NULL
  , "finished"		TIMESTAMP	DEFAULT NULL                  NULL
  , "status"		INT
  , PRIMARY KEY ("id") ENABLED
)
ORDER BY "started"
SEGMENTED BY HASH( "id" ) ALL NODES;
DDL

# Previously, `statement` was of type VARCHAR(16384) - but SQL statements may
# be as many bytes as specified by the 'max_allowed_packet' variable, which
# defaults to 16MB.  To cope with this without truncation, we must use a TEXT
# field-type.  It is possible that, since this occurance is rare, it would be
# more advantageous performance-wise to store two fields: one for 'short' lines
# (of up to 21844 (3-byte) UTF-8 characters) and one for lines that wouldn't
# fit into a maximally-sized VARCHAR()...
#
# Vertica has a maximum VARCHAR length of 65000 octets, or 16250 4-byte UTF-8
# characters.
#
# Both databases also limit the maximum size of any given row to 64k/65000
# octets respectively, so the actual maximum length is even shorter...
#
# With the schema below, the maximum size MySQL accepts is 5486 - which must
# be taken into account if this schema is further changed in the future.  Since
# Vertica's limit is 535 octets shorter than MySQL's, and a character may be up
# to 4 octets, we have to assume that the limit we need to impose is actually
# 134 characters shorter at 5352.
#
# Update: Schema-analysis tools complain about this table lacking a Primary
#         Key, and there is also a 'innodb_force_primary_key' option which (is
#         supposed to) fail if no PK is present...
#
our $mywayactionsname = 'myway_schema_actions';
our $mywayactionsddl;
#if( OLDSCHEMA) {
#	$mywayactionsddl = <<DDL;
#	CREATE TABLE IF NOT EXISTS `$mywayactionsname` (
#	    `schema_id`		CHAR(36)	                          NOT NULL
#	  , `started`		TIMESTAMP(6)	DEFAULT CURRENT_TIMESTAMP NOT NULL
#	  , `event`		VARCHAR(256)	                          NOT NULL
#	  , `statement`		LONGTEXT	CHARACTER SET 'UTF8'      NOT NULL
#	  , `line`		INT		UNSIGNED
#	  , `time`		DECIMAL(13,3)
#	  , `state`		CHAR(5)
#	  , INDEX        `${mywayactionsname}_schema_id_idx` (`schema_id`)
#	  , CONSTRAINT   `${mywayactionsname}_schema_id_${mywaytablename}_id`
#	    FOREIGN KEY (`schema_id`) REFERENCES `$mywaytablename` (`id`)
#	    ON DELETE CASCADE
#	) ENGINE='InnoDB' DEFAULT CHARSET='ASCII';
#DDL
#} else {
$mywayactionsddl = <<DDL;
CREATE TABLE IF NOT EXISTS `$mywayactionsname` (
    `id`		INT		UNSIGNED AUTO_INCREMENT   NOT NULL
  , `schema_id`		CHAR(36)	                          NOT NULL
  , `started`		TIMESTAMP(6)	DEFAULT CURRENT_TIMESTAMP NOT NULL
  , `event`		VARCHAR(256)	                          NOT NULL
  , `statement`		VARCHAR(@{[SQLMAX]})	CHARACTER SET 'UTF8MB4'
  , `statement_long`	LONGTEXT	CHARACTER SET 'UTF8MB4'
  , `line`		INT		UNSIGNED
  , `time`		DECIMAL(13,3)
  , `state`		CHAR(5)
  , PRIMARY KEY (`id`)
  , INDEX        `${mywayactionsname}_schema_id_idx` (`schema_id`)
  , CONSTRAINT   `${mywayactionsname}_schema_id_${mywaytablename}_id`
    FOREIGN KEY (`schema_id`) REFERENCES `$mywaytablename` (`id`)
    ON DELETE CASCADE
) ENGINE='InnoDB' DEFAULT CHARSET='ASCII';
DDL
#}
our $verticamywayactionsddl = <<DDL;
CREATE TABLE IF NOT EXISTS __SCHEMA__"$mywayactionsname" (
    "id"		AUTO_INCREMENT
  , "schema_id"		CHAR(36)	                          NOT NULL
  , "started"		TIMESTAMP(6)	DEFAULT CURRENT_TIMESTAMP NOT NULL
  , "event"		VARCHAR(256)	                          NOT NULL
  , "statement"		VARCHAR(@{[SQLMAX]})
  , "statement_long"	LONG VARCHAR
  , "line"		INT
  , "time"		DECIMAL(13,3)
  , "state"		CHAR(5)
  , PRIMARY KEY ("id") ENABLED
  , CONSTRAINT   "${mywayactionsname}_schema_id_${mywaytablename}_id"
    FOREIGN KEY ("schema_id") REFERENCES __SCHEMA__"$mywaytablename" ("id")
)
ORDER BY "schema_id"
SEGMENTED BY HASH( "schema_id" ) ALL NODES;
DDL

our $mywayprocsname = 'myway_stored_procedures';
our $mywayprocsddl = <<DDL;
CREATE TABLE IF NOT EXISTS `$mywayprocsname` (
    `id`		CHAR(36)	                          NOT NULL
  , `dbuser`		CHAR(16)	                          NOT NULL
  , `dbhost`		CHAR(64)	                          NOT NULL
  , `osuser`		CHAR(32)	                          NOT NULL
  , `host`		CHAR(64)	                          NOT NULL
  , `sha1sum`		CHAR(40)	                          NOT NULL
  , `path`		VARCHAR(4096)	                          NOT NULL
  , `filename`		VARCHAR(255)	                          NOT NULL
  , `version`		VARCHAR(50)
  , `description`	VARCHAR(200)
  , `type`		VARCHAR(20)
  , `started`		TIMESTAMP
  , `sqlstarted`	TIMESTAMP	DEFAULT NULL                  NULL
  , `finished`		TIMESTAMP	DEFAULT NULL                  NULL
  , `status`		TINYINT		UNSIGNED
  , PRIMARY KEY (`id`)
) ENGINE='InnoDB' DEFAULT CHARSET='ASCII';
DDL
our $verticamywayprocsddl = <<DDL;
CREATE TABLE IF NOT EXISTS __SCHEMA__"$mywayprocsname" (
    "id"		CHAR(36)	                          NOT NULL
  , "dbuser"		CHAR(16)	                          NOT NULL
  , "dbhost"		CHAR(64)	                          NOT NULL
  , "osuser"		CHAR(32)	                          NOT NULL
  , "host"		CHAR(64)	                          NOT NULL
  , "sha1sum"		CHAR(40)	                          NOT NULL
  , "path"		VARCHAR(4096)	                          NOT NULL
  , "filename"		VARCHAR(255)	                          NOT NULL
  , "version"		VARCHAR(50)
  , "description"	VARCHAR(200)
  , "type"		VARCHAR(20)
  , "started"		TIMESTAMP
  , "sqlstarted"	TIMESTAMP	DEFAULT NULL                  NULL
  , "finished"		TIMESTAMP	DEFAULT NULL                  NULL
  , "status"		INT
  , PRIMARY KEY ("id") ENABLED
)
ORDER BY "started"
SEGMENTED BY HASH( "id" ) ALL NODES;
DDL
# }}}


# Note that any data output using 'print' will (by default) go to stdout,
# whereas everything output by the four functions below will go to stderr (in
# long mode).
#
# If myway.pl is invoked by the accompanying applyschema.sh script, then all
# data written to stdout will be thrown away if '--quiet' is in effect (and,
# indeed, almost all output will be discarded if '--silent' is used).
#
# Therefore, use the functions below for any output which it is important that
# the user see, and reserve the use of print for general status updates, such
# as non-essential progress reporting.

sub pdebug( $;$$ ) { # {{{
	my( $text, $verbose, $short ) = @_;
	$verbose = $verbosity unless( defined( $verbose ) );

	# Output $text at levels: debug(3)
	#
	return( undef ) unless ( $short or DEBUG or ( $verbose > 2 ) );

	$text = '' unless( defined( $text ) and length( $text ) );
	chomp $text;

	our $debug = "DEBUG: ";
	$debug = '=>' if( defined( $short ) and $short );

	foreach my $line ( split /\n/, $text ) {
		chomp $line;

		if( not( length( $line ) ) ) {
			print( "\n" );
		} else {
			if( defined( $short ) ) {
				# Short debug output is used more for continuous progress
				# reporting than actual debug data...
				print( "$debug $line\n" );
			} else {
				my $firstword = ( split( /\s/, $line ) )[ 0 ];
				if( $firstword =~ m/^[A-Z]+:/ ) {
					$line =~ s/^$firstword //;
					$debug = ' ' x ( length( $debug ) - length ( $firstword ) - 1 ) . $firstword . ' ';
				}
				warn( "$debug $line\n" );
			}
		}
	}

	return( TRUE );
} # pdebug # }}}

sub pcomment( $;$$ ) { # {{{
	my( $text, $ignored, $_ignored ) = @_;

	$text = '' unless( defined( $text ) and length( $text ) );
	chomp $text;

	our $comment = ' >';

	foreach my $line ( split /\n/, $text ) {
		chomp $line;

		if( length( $line ) ) {
			print( "$comment $line\n" );
		} else {
			print( "\n" );
		}
	}

	return( TRUE );
} # pcomment # }}}

sub psql( $;$$ ) { # {{{
	my( $text, $ignored, $_ignored ) = @_;

	$text = '' unless( defined( $text ) and length( $text ) );
	chomp $text;

	our $sql = '->';

	foreach my $line ( split /\n/, $text ) {
		chomp $line;

		if( length( $line ) ) {
			print( "$sql $line\n" );
		} else {
			print( "\n" );
		}
	}

	return( TRUE );
} # psql # }}}

sub psim( $;$$ ) { # {{{
	my( $text, $ignored, $_ignored ) = @_;

	# $pretend is not in scope here :(
	#pdebug( "psim() called with \$pretend unset\n" ) unless( $pretend );

	$text = '' unless( defined( $text ) and length( $text ) );
	chomp $text;

	our $sim = 'S>';

	foreach my $line ( split /\n/, $text ) {
		chomp $line;

		if( length( $line ) ) {
			print( "$sim $line\n" );
		} else {
			print( "\n" );
		}
	}

	return( TRUE );
} # psql # }}}

sub pnote( $;$$ ) { # {{{
	my( $text, $verbose, $short ) = @_;
	$verbose = $verbosity unless( defined( $verbose ) );

	# Output $text at levels: debug(3), notice(2)
	#
	#return( undef ) unless( $short or ( $verbose > 1 ) );
	return( undef ) unless( $verbose > 1 );

	$text = '' unless( defined( $text ) and length( $text ) );
	chomp $text;

	our $note = "NOTICE:";
	$note = '*>' if( defined( $short ) and $short );

	foreach my $line ( split /\n/, $text ) {
		chomp $line;

		if( length( $line ) ) {
			warn( "$note $line\n" );
		} else {
			print( "\n" );
		}
	}

	return( TRUE );
} # pnote # }}}

sub pwarn( $;$$ ) { # {{{
	my( $text, $verbose, $short ) = @_;
	$verbose = $verbosity unless( defined( $verbose ) );

	# Output $text at levels: debug(3), notice(2), warn(1)
	#
	#return( undef ) unless( $short or ( $verbose > 0 ) );
	return( undef ) unless( $verbose > 0 );

	$text = '' unless( defined( $text ) and length( $text ) );
	chomp $text;

	our $warn = $warning;
	$warn = '!>' if( defined( $short ) and $short );

	foreach my $line ( split /\n/, $text ) {
		chomp $line;

		if( length( $line ) ) {
			warn( "$warn $text\n" );
		} else {
			print( "\n" );
		}
	}

	return( TRUE );
} # pwarn # }}}

sub pfail( $;$$ ) { # {{{
	my( $text, $ignored, $_ignored ) = @_;

	# Output $text regardless
	#

	$text = '' unless( defined( $text ) and length( $text ) );
	chomp $text;

	foreach my $line ( split /\n/, $text ) {
		chomp $line;

		if( length( $line ) ) {
			warn( "$failed $line\n" );
		} else {
			print( "\n" );
		}
	}

	return( TRUE );
} # pfail # }}}

sub pfatal( $;$$ ) { # {{{
	my( $text, $ignored, $_ignored ) = @_;

	# Output $text regardless
	#

	$text = '' unless( defined( $text ) and length( $text ) );
	chomp $text;

	foreach my $line ( split /\n/, $text ) {
		chomp $line;

		if( length( $line ) ) {
			warn( "$fatal $line\n" );
		} else {
			print( "\n" );
		}
	}

	return( TRUE );
} # pfatal # }}}


sub initstate() { # {{{

	# Known single-line comments
	my @slc = (
		  '#'
		,  qw( // -- )
	);
	# Known multi-line comment pairs
	my %mlc = (
		  '\/\*'	=> '\*\/'
		, '\{'		=> '\}'
	);

	# We can now rely on Regexp::Common's $RE{ quoted } match, rather than
	# having to handle quotes ourselves...

	## Known quotation symbols
	#my @quo = (
	#	  '`'
	#	, "'"
	#	, '"'
	#);

	my @str = ();

	my %state = (
		  'file'	=>	 undef	# Current filename, for logging purposes
		, 'symbol'	=>       undef	# Used to hold the closing characters of a multi-line comment or statement delimiter
		, 'depth'	=>       0	# Comment nesting depth
		, 'line'	=>	 0	# Line of input on which current entry starts
		, 'type'	=>	 undef	# Entry type: comment, statement, or fragment
		, 'entry'	=>	 undef	# Current entry contents
	);

	@{ $state{ 'slc' } } = @slc;
	%{ $state{ 'mlc' } } = %mlc;
	#@{ $state{ 'quo' } } = @quo;
	@{ $state{ 'str' } } = @str;

	return( \%state );
} # initstate # }}}

sub compressquotes( $$ ) { # {{{
	my( $line, $state ) = @_;

	# We can now rely on Regexp::Common's $RE{ quoted } match, rather than
	# having to handle quotes ourselves...

	#my @quo = @{ $state -> { 'quo' } };
	#my $quochanged = FALSE;
	#
	## Try to isolate any quoted text (to be expanded out later) so that we
	## aren't trying to comment-check user-data...
	##
	#for( my $index = 0 ; $index < scalar( @quo ) ; $index++ ) {
	#	my $character = $quo[ $index ];
	#	$quochanged = TRUE if( $line =~ s/(\\$character|$character$character)/__MW_QUO_${index}__/g );
	#}
	#pdebug( "  q Escaped-quote filtered line is now '$line'" ) if( $quochanged );

	# We're only going to attempt to handle quoted text which starts and
	# ends on the same line.  However, a string such as:
	#
	#     don't do this */ /* don't do this either
	#
	# ... is problematic because the central portion does appear to be a
	# valid quoted string.  If we were to process comments first and only
	# look for quoted strings outside of comments, then we risk thinking
	# that text within a quoted string such as:
	#
	#     mime-type: text/*
	#
	# ... is actually the start of a comment.  At least we can ensure that
	# apparent quotes are balanced before we consider them...
	#
	my $filteredline = $line;
	my $strchanged = FALSE;
	foreach my $match ( ( $line =~ m/$RE{ quoted }/g ) ) {
		my $index = scalar( @{ $state -> { 'str' } } );
		$filteredline =~ s/\Q$match\E/__MW_STR_${index}__/;
		push( $state -> { 'str' }, $match );
		$strchanged = TRUE;
		pdebug( "  q Replacing '$match' with '__MW_STR_${index}__' to give '$filteredline'" );
	}

	#pdebug( "  q Quote-reduced line is now '$filteredline'" ) if( $quochanged or $strchanged );
	pdebug( "  q Quote-reduced line is now '$filteredline'" ) if( $strchanged );

	return( $filteredline );
} # compressquotes # }}}

sub decompressquotes( $$ ) { # {{{
	my( $line, $state ) = @_;

	return( undef ) unless( defined( $line ) and length( $line ) );

	# This function must perform the exact opposite of compressquotes(),
	# above...

	my @str = @{ $state -> { 'str' } };
	my $strchanged = FALSE;

	if( scalar( @str ) ) {
		# For some reason, I can't initialise with scalar( @str ) and
		# then decrement with --$index?
		#
		for( my $index = ( scalar( @str ) - 1 ) ; $index >= 0 ; $index-- ) {
			my $match = $str[ $index ];
			if( defined( $match ) and length( $match ) ) {
				if( $line =~ s/__MW_STR_${index}__/$match/ ) {
					pdebug( "  Q Replaced '__MW_STR_${index}__' with '$match' to give '$line'" );
					$strchanged = TRUE;
					@{ $state -> { 'str' } }[ $index ] = undef;
				}
			}
		}
	}

	# We can now rely on Regexp::Common's $RE{ quoted } match, rather than
	# having to handle quotes ourselves...

	#my @quo = @{ $state -> { 'quo' } };
	#my $quochanged = FALSE;
	#
	#if( scalar( @quo ) ) {
	#	for( my $index = ( scalar( @quo ) - 1 ) ; $index >= 0 ; $index -- ) {
	#		my $character = $quo[ $index ];
	#
	#		# We don't store whether the character was backslash-
	#		# escaped or double-character escaped - let's default
	#		# to the former, on the basis of it being more
	#		# obivous...
	#		#
	#		if( $line =~ s/__MW_QUO_${index}__/\\$character/g ) {
	#			$quochanged = TRUE;
	#		}
	#	}
	#	pdebug( "  Q Escaped-quote filtered line is now '$line'" ) if( $quochanged );
	#}

	#pdebug( "  Q Quote-expanded line is now '$line'" ) if( $quochanged or $strchanged );
	pdebug( "  Q Quote-expanded line is now '$line'" ) if( $strchanged );

	return( $line );
} # decompressquotes # }}}

sub checkentry( $$;$$ ) { # {{{
	my( $data, $state, $description, $line ) = @_;

	$line = $. unless( defined( $line ) );

	if( defined( $state -> { 'entry' } ) and scalar( @{ $state -> { 'entry' } } ) ) {
		pdebug( "  * " . ( defined( $description ) ? $description . ' n' : 'N' ) . "esting error near line $line - " . scalar( @{ $state -> { 'entry' } } ) . " fragment(s) still defined - adding regardless" );
		my $count = 0;
		foreach my $entry ( @{ $state -> { 'entry' } } ) {
			$count++;
			pdebug( "--- \$state $count contains '$entry'" );
		}

		$state -> { 'entry' } = 'fragment';
		if( defined( $data -> { 'entries' } ) ) {
			push( $data -> { 'entries' }, { 'line' => $state -> { 'line' }, 'type' => $state -> { 'type' }, 'entry' => $state -> { 'entry' } } );
		} else {
			$data -> { 'entries' } = [ { 'line' => $state -> { 'line' }, 'type' => $state -> { 'type' }, 'entry' => $state -> { 'entry' } } ];
		}
		$state -> { 'line' } = 0;
		undef( $state -> { 'type' } );
		undef( $state -> { 'entry' } );

		$count = scalar( @{ $data -> { 'entries' } } );
		pdebug( "... we now have $count " . ( ( 1 == $count ) ? 'entry' : 'entries' ) );

		return( FALSE );
	}

	return( TRUE );
} # checkentry # }}}

sub pushentry( $$$$$;$ ) { # {{{
	my( $data, $state, $type, $entry, $description, $line ) = @_;

	if( $entry =~ m/__MW_(STR|L?TOK|LITERAL_QUOTE_)_/ ) {
		pwarn( "\n" );
		pwarn( "Unexpended token detected in `$entry`\n" );
		#my $trace = Devel::StackTrace -> new;
		#print( $trace -> as_string );
		die( "$fatal Tokenisation failed [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
	}

	$line = $. unless( defined( $line ) );

	pdebug( "  E Adding " . ( defined( $description ) ? lc( $description ) . ' ' : '' ) . "entry '$entry' of type '$type' from line $line ..." );

	checkentry( $data, $state, $description, $line );

	my $element;
	$element -> { 'line' } = $line;
	$element -> { 'type' } = $type;
	$element -> { 'entry' } = $entry;

	pdebug( "  E Pushing $type '$entry' ..." );
	if( defined( $data -> { 'entries' } ) ) {
		push( $data -> { 'entries' }, $element );
	} else {
		$data -> { 'entries' } = [ $element ];
	}

	my $count = scalar( @{ $data -> { 'entries' } } );
	pdebug( "... we now have $count " . ( ( 1 == $count ) ? 'entry' : 'entries' ) );

	return( $count );
} # pushentry # }}}

sub pushstate( $$ ) { # {{{
	my( $data, $state ) = @_;

	my $entries = "entries";
	my $count = 0;
	$count = scalar( @{ $state -> { 'entry' } } ) if( defined( $state -> { 'entry' } ) );
	$entries = "entry" if( 1 == $count );

	my $element;
	$element -> { 'line' } = $state -> { 'line' } if( defined( $state -> { 'line' } ) );
	$element -> { 'type' } = $state -> { 'type' } if( defined( $state -> { 'type' } ) );
	$element -> { 'entry' } = $state -> { 'entry' } if( defined( $state -> { 'entry' } ) );
	$element -> { 'tokens' } = $state -> { 'tokens' } if( defined( $state -> { 'tokens' } ) );

	if( defined( $data -> { 'entries' } ) ) {
		pdebug( "  T Adding $count $entries from \$state to \$data -> { 'entries' } ..." );

		push( $data -> { 'entries' }, $element );
	} else {
		pdebug( "  T Setting \$data -> { 'entries' } to $count $entries from \$state" );

		$data -> { 'entries' } = [ $element ];
	}

	$count = scalar( @{ $data -> { 'entries' } } );
	pdebug( "  T ... we now have $count " . ( ( 1 == $count ) ? 'entry' : 'entries' ) );

	return( $count );
} # pushstate # }}}

sub pushfragment( $$$;$$ ) { # {{{
	my( $state, $type, $entry, $description, $line ) = @_;

	if( $entry =~ m/__MW_(STR|L?TOK|LITERAL_QUOTE_)_/ ) {
		pwarn( "\n" );
		pwarn( "Unexpended token detected in `$entry`\n" );
		#my $trace = Devel::StackTrace -> new;
		#print( $trace -> as_string );
		die( "$fatal Tokenisation failed [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
	}

	$line = $. unless( defined( $line ) );

	pdebug( "  F Adding " . ( defined( $description ) ? $description . ' ' : '' ) . "fragment '$entry' of type '$type' from line $line ..." );

	if( defined( $state -> { 'entry' } ) ) {
		push( $state -> { 'entry' }, ( defined( $entry ) ? $entry : '' ) );
	} else {
		$state -> { 'line' } = $line;
		$state -> { 'type' } = $type;
		$state -> { 'entry' } = [ ( defined( $entry ) ? $entry : '' ) ];
	}

	my $count = scalar( @{ $state -> { 'entry' } } );

	# XXX: This is a bit too verbose - uncomment if debugging fragment
	#      processing...
	#
	#pdebug( "  F ... current fragment contains $count " . ( ( 1 == $count ) ? 'line' : 'lines' ) . ':' );
	#for( my $n = 0 ; $n < $count ; $n++ ) {
	#	pdebug( '  F ... ' . ( $n + 1 ) . ': ' . @{ $state -> { 'entry' } }[ $n ] );
	#}
	#pdebug( '  F ... EOF' );
	pdebug( "  F ... current fragment contains $count " . ( ( 1 == $count ) ? 'line' : 'lines' ) );

	return( $count );
} # pushfragment # }}}

sub processcomments( $$$;$ ) { # {{{
	my( $data, $line, $masterstate, $strict ) = @_;

	return( undef ) unless( defined( $masterstate -> { 'comments' } ) );

	my $state = $masterstate -> { 'comments' };

	my @slc = @{ $state -> { 'slc' } };
	my %mlc = %{ $state -> { 'mlc' } };

	$line = compressquotes( $line, $masterstate );

	# Handle comments

	if( 0 == $state -> { 'depth' } ) {
		pdebug( "  C  Comment depth == 0" );

		# Check for comments which are capable of extending over
		# multiple lines, but which start and end on this line alone.
		#
		foreach my $start ( keys( %mlc ) ) {
			my $end = $mlc{ $start };

			$start =~ s/\\//g;
			$end =~ s/\\//g;
			pdebug( "  C Checking for balanced multi-line comment between '$start' and '$end' ..." );

			foreach my $match ( ( $line =~ m/$RE{ balanced }{ -begin => $start }{ -end => $end }{ -keep }/g ) ) {
				pdebug( "  C ... matched on sub-string '$match'" );

				pushentry( $data, $state, 'comment', decompressquotes( $match, $masterstate ), "Single-line" );

				# Ensure that Hints don't leave a trailing
				# semi-colon (but also ensure that Hint-like
				# comments do always have the correct
				# terminator)...
				#$match .= DEFDELIM if( $match =~ m:^/\*!\d{5} (.+) \*/$: );
				# This should no longer happen, as hints don't
				# get passed to this code-path.

				$line =~ m/^(.*?)\Q$match\E(.*?)$/;
				my $pre = $1;
				my $post = $2;
				$line = ( defined( $pre ) ? $pre : '' ) . ( defined( $post ) ? $post : '' );
				pdebug( "  C * \$line is now '$line' ('$match' removed)" );
			}
			pdebug( "  C Matches complete - post-match \$line is '$line'" );
			pdebug( "  C Empty line - returning" ) unless( length( $line ) );
			return( undef ) unless( length( $line ) );
		}
		pdebug( "  C Filtered text is '$line'" );

		# Comments which open and close on the same line have now been
		# removed - so now check for the start of actual multi-line
		# comments...
		#
		MLC: foreach my $start ( keys( %mlc ) ) {
			my $regex = $start . '.*$';

			eval { qr/$regex/; 1 } or do {
				die( "$fatal Invalid regex '$regex'" . ( ( defined( $state -> { 'file' } ) and length( $state -> { 'file' } ) ) ? " in file '" . $state -> { 'file' } . "'" : '' ) . ": $@ [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
			};

			pdebug( "  C Checking for multi-line initial token '$regex' ..." );

			if( $line =~ m/$regex/ ) {
				pdebug( "  C Match on '$regex'" );

				$state -> { 'symbol' } = $mlc{ $start };
				my( $pre, $post ) = split( /$start/, $line );
				pdebug( "  C Sections are '$pre' & '$post'" );

				( my $filtered = $pre ) =~ s/\s+//g;
				if( defined( $filtered ) and length( $filtered ) ) {
					pdebug( "  C Processing text before comment '$pre' ..." );
					$line = ( processline( $data, $pre, $masterstate, $strict ) )[ 0 ];
				} else {
					pdebug( "  C Empty line, save for comment" );
					undef( $line );
				}

				$start =~ s/\\//g;
				pdebug( "  C Adding starting fragment '$start$post'" );

				checkentry( $data, $state, 'Multi-line' );

				$state -> { 'line' } = $.;
				$state -> { 'type' } = 'comment';
				$state -> { 'entry' } = [ $start . $post ];
				pdebug( "  C ... current comment contains " . scalar( @{ $state -> { 'entry' } } ) . " lines ..." );

				$state -> { 'depth' } ++;

				( $filtered = $line ) =~ s/\s+//g if( defined( $line ) );
				if( defined( $filtered ) and length( $filtered ) ) {
					pdebug( "  C Remaining text is '$line', depth " . $state -> { 'depth' } );
				} else {
					pdebug( "  C No remaining text, depth " . $state -> { 'depth' } );
					last MLC
				}
			}
		} # MLC

		SLC: foreach my $token ( @slc ) {
			my $regex = quotemeta( $token ) . '.*$';

			pdebug( "  C Checking for single-line token '$regex' ..." );
			if( length( $line ) and ( $line =~ m/$regex/ ) ) {
				( my $comment = $line ) =~ s/^.*?\Q$token\E/$token/;
				$line =~ s/$regex//g;

				checkentry( $data, $state, 'One-line' );

				if( defined( $comment ) ) {
					pdebug( "  C Found single-line comment '$comment', remaining text '$line'" );

					pushentry( $data, $state, 'comment', decompressquotes( $comment, $masterstate ), 'One-line' );
				}

				pdebug( "  C Processing line before comment '$line' ..." ) if( length( $line ) );

				( my $filtered = $line ) =~ s/\s+//g;
				if( defined( $filtered ) and length( $filtered ) ) {
					return( processline( $data, $line, $masterstate, $strict ) )[ 0 ];
				} else {
					return( undef );
				}
			}
		} # SLC


	} else { # ( 0 != $state -> { 'depth' } )
		die( "$fatal Logic error" . ( ( defined( $state -> { 'file' } ) and length( $state -> { 'file' } ) ) ? " in file '" . $state -> { 'file' } . "'" : '' ) . ' [' . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) unless( defined( $state -> { 'symbol' } ) );

		pdebug( "  C In a comment, depth " . $state -> { 'depth' } . " ..." );

		# Remove embedded multi-line comments on one
		# line...
		my $end = $state -> { 'symbol' };
		my %values = reverse( %mlc );
		my $start = $values{ $end };
		undef( %values );

		( my $ustart = $start ) =~ s/\\//g;
		( my $uend = $end ) =~ s/\\//g;

		unless( ( 1 == $state -> { 'depth' } ) and ( $line =~ m/$end/ ) ) {
			pushfragment( $state, 'comment', decompressquotes( $line, $masterstate ), 'mid-comment' );

			return( undef );
		}

		pdebug( "  C Checking '$line' for balanced multi-line comment between '$ustart' and '$uend' (depth " . $state -> { 'depth' } . ") ..." );
		foreach my $match ( ( $line =~ m/$RE{ balanced }{ -begin => $ustart }{ -end => $uend }{ -keep }/g ) ) {
			pdebug( "  C Matched sub-string '$match'" );

			$line =~ m/^(.*?)\Q$match\E(.*?)$/;
			my $pre = $1;
			my $post = $2;
			$line = ( defined( $pre ) ? $pre : '' ) . ( defined( $post ) ? $post : '' );
			pdebug( "  C \$line is now '$line' ('$match' removed)" );
		}
		pdebug( "  C Post-match \$line is '$line'" );

		return( undef ) unless( length( $line ) );

		# We're currently inside a multi-line comment,
		# which either contains one or more significant opening
		# comments or one or more significant closing comments...
		#
		if( $line =~ m/$start/ ) {
			pdebug( "  C Match on start '$start'" );

			# A nested comment starts here
			#
			my( $pre, $post ) = split( /$start/, $line );
			pdebug( "  C Sections are '$pre' & '$post'" );
			if( length( $pre ) ) {
				pdebug( "  C Processing text before nested comment '$pre' ..." );

				$line = ( processline( $data, $pre, $masterstate, $strict ) )[ 0 ];
			} else {
				pdebug( "  C Empty line" );

				undef( $line );
			}
			pdebug( "  C Processing text after nested comment '$post' ..." ) if( length( $post ) );

			processline( $data, $post, $masterstate, $strict ) if( length( $post ) );

			$state -> { 'depth' } ++;

			pdebug( "  C Remaining text is '$line', depth " . $state -> { 'depth' } ) if( defined( $line ) );

		} elsif( $line =~ m/$end/ ) {
			pdebug( "  C Match on end '$end'" );

			# Comment ends on this line
			if( not( ( $state -> { 'depth' } ) > 0 ) ) {
				pdebug( "  C Depth nesting error on line $." );
				$state -> { 'depth' } = 1;
			}
			$state -> { 'depth' } --;

			my( $pre, $post ) = split( /$end/, $line );

			pdebug( "  C Sections are '$pre' & '$post' (depth " . $state -> { 'depth' } . ")" );

			# $depth is already decremented above...
			#
			if( 0 == $state -> { 'depth' } ) {
				$end =~ s/\\//g;

				pushfragment( $state, 'comment', decompressquotes( ( defined( $pre ) ? $pre : '' ) . $end, $masterstate ), 'ending' );

				pdebug( "  C Pushing resultant comments array ..." );
				pushstate( $data, $state );
				$state -> { 'line' } = 0;
				undef( $state -> { 'type' } );
				undef( $state -> { 'entry' } );
			}

			if( length( $post ) ) {
				pdebug( "  C Processing text after comment '$post' ..." );
				$line = ( processline( $data, $post, $masterstate, $strict ) )[ 0 ];
			} else {
				pdebug( "  C Empty line" );
				undef( $line );
			}

			( my $filtered = $line ) =~ s/\s+//g if( defined( $line ) );
			if( defined( $filtered ) and length( $filtered ) ) {
				pdebug( "  C Remaining text is '$line', depth " . $state -> { 'depth' } );
			} else {
				pdebug( "  C No remaining text, depth " . $state -> { 'depth' } );
			}

		} else {
			pdebug( "  C Empty line - returning" );

			return( undef );
		}
	} # $state -> { 'depth' }

	pdebug( "  C End processing text for comments." );

	return( decompressquotes( $line, $masterstate ) );
} # processcomments # }}}

sub processline( $$;$$ ) { # {{{
	my( $data, $line, $state, $strict ) = @_;
	my $status = undef;

	my $walk;

	if( not( defined( $state ) ) ) {
		$state -> { 'comments' } = initstate();
		$state -> { 'statements' } = initstate();
	}
	if( not( defined( $state -> { 'comments' } -> { 'depth' } ) ) ) {
		$state -> { 'comments' } = initstate();
	}
	if( not( defined( $state -> { 'statements' } -> { 'depth' } ) ) ) {
		$state -> { 'statements' } = initstate();
	}

	return( undef, undef ) unless( defined( $line ) and length( $line ) );
	if( $line =~ m/^\s*$/ ) {
		pdebug( "  S Skipping blank line '$line' ..." );
		return( undef, undef );
	}

	# Previously, we were looking for lines where the entirity of the line
	# was a hint in order to handle the text as a hint rather than as a
	# comment - however, it transpires that hints can correctly appear
	# within a statement also, so we'll just have to return any hint-like
	# data as-is.  Hopefully this will be safe...
	#
	#if( $line =~ m#(^|\s+)/\*!\d{5} .+ \*/(\s+|;\s*$)# ) {
	#
	# Be less specific here (especially since we don't yet know what the
	# delimiter will be without shifting the code below around), at the
	# potential risk of not identifying especially unusual comments (which
	# the database engine should then be able to handle, in any case)...
	#
	if( $line =~ m#(^|\s+)/\*!\d{5} .+ \*/# ) {
		pdebug( "  * Not processing text with hint '$line' for comments ..." );
	} else {
		pdebug( "  * Start processing text '$line' ..." );

		$line = processcomments( $data, $line, $state, $strict );

		return( $line, undef ) unless( defined( $line ) );
	}

	pdebug( "  S Line '$line' should contain a statement..." );

	$walk = sub ( $$;$ ) { # {{{
		my( $element, $block, $vars ) = @_;

		my $type = ref( $element );

		if( 'HASH' eq $type ) {
			$walk -> ( $_, $block, $vars ) for( values( %$element ) );
		} elsif( 'ARRAY' eq $type ) {
			$walk -> ( $_, $block, $vars ) for( @$element );
		} elsif( ( 'SCALAR' eq $type ) or ( '' eq $type ) ) {
			$block -> ( \$_[ 0 ], $vars );
		} else {
			die( "$fatal Unknown type '$type' [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
		}
	}; # walk # }}}

	if( $line =~ m/^\s*delimiter\s+([^\s]+)\s*(.*)$/i ) {
		my $delim = $1;
		$line = $2;

		pdebug( "  * Changing statement delimiter to '$delim' ..." );
		$state -> { 'statements' } -> { 'symbol' } = $delim;

		if( not( $tokenise ) ) {
			$state -> { 'statements' } -> { 'tokens' } = undef;
		} else {
			my $sqlparser = SQLParser -> new();
			my $tokens;
			eval {
				$tokens = $sqlparser -> parse( "DELIMITER $delim" );
			};
			if( $@ ) {
				pfail( '' . $@ . "\n" ) if( length( $@ ) );
				$state -> { 'statements' } -> { 'tokens' } = undef;
			} else {
				$state -> { 'statements' } -> { 'tokens' } = $tokens;
			}
		}

		pushfragment( $state -> { 'statements' }, 'statement', "DELIMITER $delim", 'delimiter change' );
		pushstate( $data, $state -> { 'statements' } );
		$state -> { 'statements' } -> { 'depth' } = 0;
		$state -> { 'statements' } -> { 'line' } = 0;
		undef( $state -> { 'statements' } -> { 'type' } );
		undef( $state -> { 'statements' } -> { 'entry' } );

		return( undef, undef ) unless( length( $line ) );

		if( length( $line ) ) {
			return( ( processline( $data, $line, $state, $strict ) )[ 0 ], undef );
		} else {
			return( undef, undef );
		}
	}

	my $delim = DEFDELIM;
	$delim = $state -> { 'statements' } -> { 'symbol' } if( defined( $state -> { 'statements' } -> { 'symbol' } ) );

	#pdebug( "  S About to split line '$line' into individual items..." );

	# We have an issue where a lone delimiter will cause the entire
	# 'foreach' block below to be skipped, meaning that the statement
	# terminated by the delimiter on a line by itself is never processed.
	# Rather than duplicate a chunk of code for this case, we cheat and add
	# a second delimiter - an entirely safe operation - which causes
	# 'foreach' to process the intervening space character!
	my $trailingdelim = FALSE;
	if( $line =~ m/^\s*\Q$delim\E\s*$/ ) {
		pdebug( "  S Expanding line '$line' with trailing delimiter '$delim'..." );
		$line = $delim . ' ' . $delim;
		$trailingdelim = TRUE;
	}

	if( $line =~ m/^\s*$/ ) {
		pdebug( "  S Skipping blank line '$line' ..." );
		return( undef, undef );
	}

	# A quoted sting could contain $delim, in which case literal splits
	# on $delim will break things - so, unfortunately, it looks as if we're
	# going to have to tokenise the active string /again/ ...

	my @linereplacements;
	# Handle escaped quotes, which confuse parsing.
	( my $filteredline = $line ) =~ s/''/__MW_LITERAL_QUOTE__/g;

	foreach my $match ( ( $filteredline =~ m/$RE{ quoted }/g ) ) {
		# $RE{quoted} also captures back-ticks,
		# which we need to maintain...
		next if( $match =~ m/^\`.*\`$/ );

		if( not( length( $match ) > 2 ) ) {
			pdebug( "  S Line-quoted string `$match` from `$filteredline` (originally `$line`) is too short to process - skipping further processing of this token" );
			next;
		}

		my $qu = substr( $match, 0, 1 );
		my $te = substr( $match, -1, 1 );
		if( $qu ne $te ) {
			pdebug( "  S Found differing line-quoted string delimiters `$qu` and `$te` - skipping further processing of string `$match` from `$filteredline` (originally `$line`)" );
			next;
		} elsif( not( $qu =~ m/['"]/ ) ) {
			pwarn( "  S Token string delimiter `$qu` is not recognised - skipping further processing of string `$match` from `$filteredline` (originally `$line`)" );
			next;
		}
		# Keep external quotes...
		$match =~ s/^$qu//;
		$match =~ s/$te$//;

		my $index = scalar( @linereplacements );
		$filteredline =~ s/$qu\Q$match\E$te/${qu}__MW_LTOK_${index}__${te}/;
		push( @linereplacements, $match );
		pdebug( "  S Replacing line \`$match\` with \`__MW_LTOK_${index}__\` to give \`$filteredline\`" );
	}

	pdebug( "  \$ Splitting line `$filteredline` on `$delim` ..." );
	foreach my $item ( split( /\Q$delim\E/, $filteredline ) ) {

		if( $item =~ m/^\s*$/ ) {
			if( $trailingdelim ) {
				$trailingdelim = FALSE;
			} else {
				pdebug( "  S Skipping blank segment '$item' ..." );
				next;
			}
		}

		#pdebug( "  S Split item '$item' from line '$filteredline'..." );

		# perl didn't like '\s' here...
		my $term = "\Q$item\E[[:space:]]*\Q$delim\E";
		if( $filteredline =~ m/$term/ ) {
			# $item contains a complete SQL statement, or the end
			# of a previously started one...
			#
			#pdebug( "  S Line '$filteredline' contains delimiter '$delim' and so is a complete statement, or the end of a previously started statement..." );

			my( $pre, $post ) = split( /\Q$delim\E/, $filteredline, 2 );
			$pre =~ s/^\s+//; $pre =~ s/\s+$//;
			$post =~ s/^\s+//; $post =~ s/\s+$//;

			$walk -> ( $pre, sub {
				my( $strref, $varref ) = @_;

				return( undef ) if( not( length( ${ $strref } ) and ( ${ $strref } =~ m/__MW_(LTOK|LITERAL_QUOTE_)_/ ) ) );
				#warn "WWW: strref is '$strref', refers to '" . ${ $strref } . "'";

				my $original = ${ $strref };
				if( ${ $strref } =~ s/__MW_LITERAL_QUOTE__/''/g ) {
					pdebug( "  S Replaced \`__MW_LITERAL_QUOTE__\` from complete section \`$original\` with \`''\` to give \`${ $strref }\`" );
				}

				my @str = @{ $varref };
				#warn "WWW: walk read " . scalar( @str ) . " parameters";
				return( undef ) if( not( scalar( @str ) ) );

				for( my $index = ( scalar( @str ) - 1 ) ; $index >= 0 ; $index-- ) {
					my $match = $str[ $index ];
					$match =~ s/__MW_LITERAL_QUOTE__/''/g;
					#warn "WWW: walk read original string '$match'";
					#pdebug( "Checking `$match`($index) against `${ $strref }` ...\n", undef, TRUE );

					# FIXME: It may be possible that a token might expand to a string containing another token, in which case we intentionally break...

					if( defined( $match ) and length( $match ) ) {
						$original = ${ $strref };
						if( ${ $strref } =~ s/__MW_LTOK_${index}__/$match/ ) {
							pdebug( "  S Replaced \`__MW_LTOK_${index}__\` from complete section \`$original\` with \`$match\` to give \`${ $strref }\`" );
						}
					}
				}
			}, \@linereplacements );
			$walk -> ( $post, sub {
				my( $strref, $varref ) = @_;

				return( undef ) if( not( length( ${ $strref } ) and ( ${ $strref } =~ m/__MW_(LTOK|LITERAL_QUOTE_)_/ ) ) );
				#warn "WWW: strref is '$strref', refers to '" . ${ $strref } . "'";

				my $original = ${ $strref };
				if( ${ $strref } =~ s/__MW_LITERAL_QUOTE__/''/g ) {
					pdebug( "  S Replaced \`__MW_LITERAL_QUOTE__\` from follow-on section \`$original\` with \`''\` to give \`${ $strref }\`" );
				}

				my @str = @{ $varref };
				#warn "WWW: walk read " . scalar( @str ) . " parameters";
				return( undef ) if( not( scalar( @str ) ) );

				for( my $index = ( scalar( @str ) - 1 ) ; $index >= 0 ; $index-- ) {
					my $match = $str[ $index ];
					$match =~ s/__MW_LITERAL_QUOTE__/''/g;
					#warn "WWW: walk read original string '$match'";
					#pdebug( "Checking `$match`($index) against `${ $strref }` ...\n", undef, TRUE );

					# FIXME: It may be possible that a token might expand to a string containing another token, in which case we intentionally break...

					if( defined( $match ) and length( $match ) ) {
						$original = ${ $strref };
						if( ${ $strref } =~ s/__MW_LTOK_${index}__/$match/ ) {
							pdebug( "  S Replaced \`__MW_LTOK_${index}__\` from follow-on section \`$original\` with \`$match\` to give \`${ $strref }\`" );
						}
					}
				}
			}, \@linereplacements );

			pdebug( "  S Complete or follow-on sections are `$pre` & `$post`" );

			pushfragment( $state -> { 'statements' }, 'statement', ( defined( $pre ) ? $pre : '' ) . $delim, 'SQL ending' );

			my $command = join( ' ', @{ $state -> { 'statements' } -> { 'entry' } } );
			$walk -> ( $command, sub {
				my( $strref, $varref ) = @_;

				return( undef ) if( not( length( ${ $strref } ) and ( ${ $strref } =~ m/__MW_(TOK|LITERAL_QUOTE_)_/ ) ) );
				#warn "WWW: strref is '$strref', refers to '" . ${ $strref } . "'";

				my $original = ${ $strref };
				if( ${ $strref } =~ s/__MW_LITERAL_QUOTE__/''/g ) {
					pdebug( "  S Replaced \`__MW_LITERAL_QUOTE__\` from tokenised hash leaf value \`$original\` with \`''\` to give \`${ $strref }\`" );
				}

				my @str = @{ $varref };
				#warn "WWW: walk read " . scalar( @str ) . " parameters";
				return( undef ) if( not( scalar( @str ) ) );

				for( my $index = ( scalar( @str ) - 1 ) ; $index >= 0 ; $index-- ) {
					my $match = $str[ $index ];
					$match =~ s/__MW_LITERAL_QUOTE__/''/g;
					#warn "WWW: walk read original string '$match'";

					# FIXME: It may be possible that a token might expand to a string containing another token, in which case we intentionally break...

					if( defined( $match ) and length( $match ) ) {
						$original = ${ $strref };
						if( ${ $strref } =~ s/__MW_LTOK_${index}__/$match/ ) {
							pdebug( "  S Replaced \`__MW_LTOK_${index}__\` from tokenised hash leaf value \`$original\` with \`$match\` to give \`${ $strref }\`" );
						}
					}
				}
			}, \@linereplacements );
			pdebug( "  S Expanded command is '$command'." );

			if( $command =~ m/^USE\s+`?(.+?)`?$/i ) {
				if( defined( $strict ) and $strict ) {
					die( "$fatal Not parsing prohibited command '$command'" . ( ( defined( $state -> { 'file' } ) and length( $state -> { 'file' } ) ) ? " from file '" . $state -> { 'file' } . "'" : '' ) . ' [' . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
				} else {
					pwarn( "Not parsing prohibited command '$command'" . ( ( defined( $state -> { 'file' } ) and length( $state -> { 'file' } ) ) ? " from file '" . $state -> { 'file' } . "'" : '' ) . "\n", undef, TRUE );
				}

			} elsif( $command =~ m/^\s*\Q$delim\E\s*$/ ) {
				pdebug( "  S Not parsing lone delimiter from '$command', but pushing statements array ..." );
				pushstate( $data, $state -> { 'statements' } );

			} else {
				if( not( $tokenise ) ) {
					$state -> { 'statements' } -> { 'tokens' } = undef;
				} else {
					# We only parse SQL to determine affected
					# tables, so we can make this easier by
					# removing all quoted strings, so that the
					# easily-confused parser is less likely to
					# become flummoxed.
					my @replacements;

					# Handle escaped quotes, which confuse parsing.
					( my $filteredcommand = $command ) =~ s/''/__MW_LITERAL_QUOTE__/g;

					foreach my $match ( ( $filteredcommand =~ m/$RE{ quoted }/g ) ) {
						# $RE{quoted} also captures back-ticks,
						# which we need to maintain...
						next if( $match =~ m/^\`.*\`$/ );

						if( not( length( $match ) > 2 ) ) {
							pdebug( "  S Quoted string `$match` is too short to process - skipping further processing of this token" );
							next;
						}

						my $qu = substr( $match, 0, 1 );
						my $te = substr( $match, -1, 1 );
						if( $qu ne $te ) {
							pdebug( "  S Found differing quoted string delimiters `$qu` and `$te` - skipping further processing string `$match`" );
							next;
						} elsif( not( $qu =~ m/['"]/ ) ) {
							pwarn( "  S Token string delimiter `$qu` is not recognised - skipping further processing string `$match`" );
							next;
						}
						# Keep external quotes...
						$match =~ s/^$qu//;
						$match =~ s/$te$//;

						my $index = scalar( @replacements );
						$filteredcommand =~ s/$qu\Q$match\E$te/${qu}__MW_TOK_${index}__${te}/;
						push( @replacements, $match );
						pdebug( "  S Replacing \`$match\` with \`__MW_TOK_${index}__\` to give \`$filteredcommand\`" );
					}

					my $tokens;
					eval {
						#
						# SQL::Parser
						#
						#my $sqlparser = SQL::Parser -> new( 'ANSI' );
						#$sqlparser -> parse( $command );
						#$tokens = $sqlparser -> structure;

						#
						# SQL::Tokenizer
						#
						#$tokens= SQL::Tokenizer->tokenize( $command, TRUE );

						#
						# Percona SQLParser (included above)
						#
						my $sqlparser = SQLParser -> new();
						$tokens = $sqlparser -> parse( $filteredcommand, $delim );
					};
					if( $@ ) {
						( my $errortext = $@ ) =~ s/\.$//;
						chomp( $errortext );

						if( $errortext =~ m/(Cannot parse .* queries) .* line (\d+)$/ ) {
							if( $verbosity > 0 ) { # debug(3), notice(2), warn(1)
								pwarn( "$1" . ( ( defined( $state -> { 'file' } ) and length( $state -> { 'file' } ) ) ? " at '" . $state -> { 'file' } . "':$2" : '' ) . "\n", undef, TRUE );
							}
							$status = $1
						} else {
							if( defined( $strict ) and $strict ) {
								die( "\n$fatal " . $errortext . ( ( defined( $state -> { 'file' } ) and length( $state -> { 'file' } ) ) ? " in file '" . $state -> { 'file' } . "'" : '' ) . ' [' . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
							} else {
								pwarn( "$errortext" . ( ( defined( $state -> { 'file' } ) and length( $state -> { 'file' } ) ) ? " in file '" . $state -> { 'file' } . "'" : '' ) . "\n", undef, TRUE );
							}
						}
						$state -> { 'statements' } -> { 'tokens' } = undef;
					} else {
						$walk -> ( $tokens, sub {
							my( $strref, $varref ) = @_;

							return( undef ) if( not( length( ${ $strref } ) and ( ${ $strref } =~ m/__MW_TOK_/ ) ) );
							#warn "WWW: strref is '$strref', refers to '" . ${ $strref } . "'";

							my $original = ${ $strref };
							if( ${ $strref } =~ s/__MW_LITERAL_QUOTE__/''/g ) {
								pdebug( "  S Replaced \`__MW_LITERAL_QUOTE__\` from tokenised hash leaf value \`$original\` with \`''\` to give \`${ $strref }\`" );
							}

							my @str = @{ $varref };
							#warn "WWW: walk read " . scalar( @str ) . " parameters";
							return( undef ) if( not( scalar( @str ) ) );

							for( my $index = ( scalar( @str ) - 1 ) ; $index >= 0 ; $index-- ) {
								my $match = $str[ $index ];
								$match =~ s/__MW_LITERAL_QUOTE__/''/g;
								#warn "WWW: walk read original string '$match'";

								# FIXME: It may be possible that a token might expand to a string containing another token, in which case we intentionally break...

								if( defined( $match ) and length( $match ) ) {
									$original = ${ $strref };
									if( ${ $strref } =~ s/__MW_TOK_${index}__/$match/ ) {
										pdebug( "  S Replaced \`__MW_TOK_${index}__\` from tokenised hash leaf value \`$original\` with \`$match\` to give \`${ $strref }\`" );
									}
								}
							}
						}, \@replacements );

						$state -> { 'statements' } -> { 'tokens' } = $tokens;
						#print Data::Dumper -> Dump( [ $tokens ], [ qw( *tokens ) ] ) if DEBUG;
					}
				} # ( $tokenise )

				pdebug( "  S Pushing resultant statements array ..." );
				pushstate( $data, $state -> { 'statements' } );
			} # ( $command !~ m/^USE\s+`?(.+?)`?$/i ) and ( $command !~ m/^\s*\Q$delim\E\s*$/ )

			$state -> { 'statements' } -> { 'depth' } = 0;
			$state -> { 'statements' } -> { 'line' } = 0;
			undef( $state -> { 'statements' } -> { 'type' } );
			undef( $state -> { 'statements' } -> { 'entry' } );

			if( length( $post ) and not( $post eq $delim and not( length( $pre ) ) ) ) {
				pdebug( "  S Follow-on section is '$post'" );

				my( $newline, $newstatus ) = processline( $data, $post, $state, $strict );
				$newstatus = $status unless( defined( $newstatus ) );
				return( $newline, $newstatus );
			} else {
				return( undef, $status );
			}

		} else { # ( $filteredline =~ m/$term/ )
			# $item contains a SQL fragment, which may be the start
			# of a new statement...
			#

			$line =~ s/^\s+//;
			$line =~ s/\s+$//;

			# FIXME: This /should/ be handled when read back out...
			#
			#my $original = $line;
			#if( $line =~ s/__MW_LITERAL_QUOTE__/''/g ) {
			#	pdebug( "  S Replaced \`__MW_LITERAL_QUOTE__\` from fragment \`$original\` with \`''\` to give \`$line\`" );
			#}

			pdebug( "  S New statement or fragment is '$line'" );

			if( 0 == $state -> { 'statements' } -> { 'depth' } ) {
				$state -> { 'statements' } -> { 'type' } = 'statement';
			}
			pushfragment( $state -> { 'statements' }, 'statement', $line, 'SQL statement' );
			return( undef, undef );
		} # ( $filteredline !~ m/$term/ )
	} # foreach my $item ( split( /\Q$delim\E/, $filteredline ) )

	if( length( $line ) ) {
		return( $line, undef );
	} else {
		return( undef, undef );
	}
} # processline # }}}

sub processfile( $$;$$$$ ) { # {{{
	my( $data, $file, $marker, $substitution, $strict, $stopaftermetadata ) = @_;

	return( undef ) unless( defined( $file ) and length( $file ) and -r $file );

	my $state = initstate();
	my $validated = TRUE;
	my $status = undef;

	$state -> { 'file' } = $file;

	pdebug( "processfile() invoked on file '$file'" );

	open( my $handle, '<:encoding(UTF-8)', $file )
		or die( "$fatal Cannot open '$file' for read: $!\n" );

	LINE: while( my $line = <$handle> ) {
		next LINE unless( length( $line ) );

		chomp( $line );
		$line =~ s/\r$//;

		next LINE unless( length( $line ) );

		if( defined( $marker ) and length( $marker ) ) {
			$substitution = '' unless( defined( $substitution ) and length( $substitution ) );

			( my $original = $line ) =~ s/^\s+//;
			if( $line =~ s/$marker/$substitution/ ) {
				( my $new = $line ) =~ s/^\s+//;
				pnote( "Substituted '$marker' for '$substitution' in string '$original' to result in '$new'\n", undef, TRUE ) unless( $quietorsilent );
			}
		}
		# NB: $. contains the last-read line-number
		pdebug( "$. '$line'" );

		my $newstatus;
		( $line, $newstatus ) = processline( $data, $line, $state, $strict );
		$status = $newstatus if( defined( $newstatus ) );

		if( defined( $stopaftermetadata ) and $stopaftermetadata ) {
			# Each $entry[] consists of: %{ @entry (text of statement), $line, $type (comment|statement) }
			my $entry = @{ $data -> { 'entries' } }[ 0 ];
			if( exists( $entry -> { 'type' } ) ) {
				if( 'comment' eq $entry -> { 'type' } ) {
					pdebug( "Value '$line' left over after calling processline()" ) if( length( $line ) );
					pdebug( "Metadata read - skipping remainder of file" );
					last LINE;
				} else {
					pwarn( "First block read from '$file' does not appear to be a metadata comment - parsing entire file\n" );
					$stopaftermetadata = undef;
				}
			}
		}

		next LINE unless( length( $line ) );

		pdebug( "Value '$line' leftover after calling processline()" );
	} # LINE

	close( $handle );

	# It's invalid to leave a dangling statement without a terminating
	# delimiter... but mistakes happen.  Let's try to catch this instance.
	my $count;
	$count = scalar( @{ $state -> { 'statements'} -> { 'entry' } } ) if( defined( $state -> { 'statements' } -> { 'entry' } ) );
	if( $count ) {
		pwarn( $count . " lines of data are hanging without a delimiter!" );

		my $delim = DEFDELIM;
		$delim = $state -> { 'statements' } -> { 'symbol' } if( defined( $state -> { 'statements' } -> { 'symbol' } ) );
		pwarn( "Attempting to auto-correct by inserting '" . $delim . "' character ..." );

		my( $unused, $newstatus ) = processline( $data, $delim, $state, $strict );
		$status = $newstatus if( defined( $newstatus ) );
	}

	if( defined( $status ) and length( $status ) ) {
		pwarn( "Serious schema error: " . $status . "\n", undef, TRUE );
		pwarn( "Your schema may not apply as you intend.\n", undef, TRUE );
		$validated = FALSE;
	}

	return( $validated );
} # processfile # }}}


sub dbopen( $$$$;$$ ) { # {{{
	my( $dbh, $dsn, $user, $password, $strict, $options ) = @_;

	# A freshly defined, typeless $dbh may be passed on first use...
	die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": arg1 must be passed by reference (" . ref( $dbh ) . ")\n" ) unless( ref( $dbh ) =~ m/^(SCALAR|REF)$/ );
	# In actual fact, do we ever want to allow dbopen() to be called on a defined
	# database handle?
	die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": \${ arg1 } must be of type DBI::db (" . ref( ${ $dbh } ) . ")\n" ) unless( ref( ${ $dbh } ) =~ m/^(|DBI::db)$/ );
	pdebug( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": \${ arg1 } should be uninitialised (" . ref( ${ $dbh } ) . ")\n" ) unless( ref( ${ $dbh } ) eq '' );

	my $error = undef;

	# We handle errors ourselves, to RaiseError should not be set unless
	# debugging...
	#
	#$options = { RaiseError => 1, PrintError => 0 } unless( $options );
	$options = { RaiseError => 0, PrintError => 1 } unless( $options );

	# Ugh, now that we have Vertica support we need to parse certain values
	# out of the provided DSN :(
	if( not( defined ( $user ) ) or ( '' eq $user ) ) {
		if( $dsn =~ m/(?:uid|user(?:name)?)=([^;]+)(?:;|$)/i ) {
			$user = $1;
		}
	}
	if( not( defined ( $password ) ) or ( '' eq $password ) ) {
		if( $dsn =~ m/(?:pwd|pass(?:word)?)=([^;]+)(?:;|$)/i ) {
			$password = $1;
		}
	}

	$connection -> { 'dsn' }      = $dsn      if( defined( $dsn ) );
	$connection -> { 'user' }     = $user     if( defined( $user ) );
	$connection -> { 'password' } = $password if( defined( $password ) );
	$connection -> { 'strict' }   = $strict   if( defined( $strict ) );
	$connection -> { 'options' }  = $options  if( defined( $options ) );

	if( DEBUG ) {
		if( not( $dbconns ) ) {
			if( not( defined( $retries ) and ( $retries eq '0' ) ) ) {
				if( $retries ) {
					warn( "*** DEBUG: dbopen() called with \$dbconns = $dbconns during retry $retries\n" );
				} else {
					warn( "*** DEBUG: dbopen() called with \$dbconns = $dbconns during error recovery\n" );
				}
			} else {
				warn( "*** DEBUG: dbopen() called with \$dbconns = $dbconns\n" );
			}
		}
	}

	#disable diagnostics;
	${ $dbh } = DBI -> connect( $dsn, $user, $password, $options )
		or $error = "Cannot create connection with DSN '$dsn' (user='$user'" . ( ( DEBUG or ( $verbosity > 2 ) ) ? ", password='$password'" : '' ) . ", options='@{ [ %{ $options } ] }'): $DBI::errstr"; # debug(3)
	#enable diagnostics;

	if( defined( ${ $dbh } ) ) {
		$dbconns ++;
		warn( "*** DEBUG: dbopen(): \$dbconns is now $dbconns\n" ) if( DEBUG );

		if( ${ $dbh } -> { 'Driver' } -> { 'Name' } =~ m/mysql/i ) {
			${ $dbh } -> { 'InactiveDestroy' } = 1;

			my $mode = sqlgetvalue( $dbh, 'SELECT @@SESSION.sql_mode' );
			pdebug( "Initial sql_mode is '$mode'" . ( $strict ? ', strict mode required' : '' ) );

			if( $strict ) {
				# If we're not in strict-mode (by the loosest possible
				# definition) then enable it...
				#
				if( $mode !~ m/^.*,?(((STRICT_ALL_TABLES|STRICT_TRANS_TABLES),.*(STRICT_ALL_TABLES|STRICT_TRANS_TABLES))|TRADITIONAL),?.*$/i ) {
					$mode .= ( defined( $mode ) and length ( $mode ) ? ',' : '' ) . "TRADITIONAL";
					sqldo( $dbh, "SET SESSION sql_mode = '$mode'" );
				}

				# Now that we we have an (expanded) sql_mode set, remove
				# the problematic NO_ZERO_DATE option...
				# N.B.: This is different from NO_ZERO_IN_DATE <sigh>
				#
				$mode = sqlgetvalue( $dbh, 'SELECT @@SESSION.sql_mode' );
				( my $newmode = $mode ) =~ s/,?NO_ZERO_DATE,?/,/i;
				$newmode =~ s/,?TRADITIONAL,?/,/i;
				$newmode =~ s/^,//;
				$newmode =~ s/,$//;
				sqldo( $dbh, "SET SESSION sql_mode = '$newmode'" ) unless( $mode eq $newmode );

				# Also set InnoDB strict mode - which, thankfully, is
				# somewhat less complex...
				#
				$mode = sqlgetvalue( $dbh, 'SELECT @@SESSION.innodb_strict_mode' );
				if( not( defined( $mode ) ) or ( $mode eq 0 ) ) {
					sqldo( $dbh, "SET SESSION innodb_strict_mode = ON" );
				}
			} else {
				# We actually probably do need to deal with this
				# eventuality - what happens if strict mode is already
				# set?

				# Clear all modes, but only if a strict setting is
				# detected.
				#
				# XXX: It would be better to filter this list with more
				#      granularity...
				#
				if( $mode =~ m/^.*,?(((STRICT_ALL_TABLES|STRICT_TRANS_TABLES),.*(STRICT_ALL_TABLES|STRICT_TRANS_TABLES))|TRADITIONAL),?.*$/i ) {
					sqldo( $dbh, "SET SESSION sql_mode = ''" );
				}
				$mode = sqlgetvalue( $dbh, 'SELECT @@SESSION.innodb_strict_mode' );
				if( $mode eq 1 ) {
					sqldo( $dbh, "SET SESSION innodb_strict_mode = OFF" );
				}
			}

			pdebug( "Updated sql_mode is '" . sqlgetvalue( $dbh, 'SELECT @@SESSION.sql_mode' ) . "'" );
		}
	}

	return( $error );
} # dbopen # }}}

sub dbclose( ;$$$$ ) { # {{{
	my( $dbh, $message, $dbname, $notimestamp ) = @_;

	$message = "Complete" unless( defined( $message ) and length( $message ) );
	$message .= " (from line " . ( caller( 0 ) )[ 2 ] . ")" if( DEBUG or ( $verbosity > 2 ) ); # debug(3)

	if( defined( $dbh ) and $dbh ) {
		if( not( $dbconns eq 1 ) ) {
			warn( "*** DEBUG: dbclose() called with \$dbconns = $dbconns\n" ) if( DEBUG );
		}

		if( ref( $dbh ) eq 'DBI::db' ) {
			$dbh -> disconnect or die( "$fatal: DBI 'disconnect' failed [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]: $!\n$@\n" );
		} else {
			die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": arg1 must be passed by reference (" . ref( $dbh ) . ")\n" ) unless( 'REF' eq ref( $dbh ) );
			die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": \${ arg1 } must be of type DBI::db (" . ref( ${ $dbh } ) . ")\n" ) unless( 'DBI::db' eq ref( ${ $dbh } ) );

			${ $dbh } -> disconnect or die( "$fatal: DBI 'disconnect' failed [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]: $!\n$@\n" );
		}

		$dbconns --;
		warn( "*** DEBUG: dbclose(): \$dbconns is now $dbconns\n" ) if( DEBUG );

		if( not( $quietorsilent ) ) {
			pdebug( "\n" );
			pdebug( "$message - disconnected from " . ( defined( $dbname ) and length( $dbname ) ? $dbname . ' ' : '' ) . "database ...\n", undef, TRUE );
		}
	}

	# Clear global connection-tracking hash...
	$connection = undef;

	if( not( defined( $notimestamp ) ) ) {
		my( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) = localtime( time );
		$year += 1900;
		pdebug( "\n" );
		pdebug( sprintf( "%s finished at %04d/%02d/%02d %02d:%02d.%02d\n\n", $0, $year, $mon, $mday, $hour, $min, $sec ), undef, TRUE ) unless( $quietorsilent )
	}

	return( TRUE );
} # dbclose # }}}


sub dbdump( $;$$$$ ) { # {{{
	my( $auth, $objects, $destination, $filename, $variables ) = @_;

	# N.B.: If per-table or per-database output is required, then call
	#       dbdump() multiple times with different $objects and
	#       $destination...
	#

	return( undef ) unless( defined( $auth -> { 'user' } ) and length( $auth -> { 'user' } ) );
	return( undef ) unless( defined( $auth -> { 'password' } ) and length( $auth -> { 'password' } ) );
	return( undef ) unless( defined( $auth -> { 'host' } ) and length( $auth -> { 'host' } ) );

	my $user =     $auth -> { 'user' };
	my $password = $auth -> { 'password' };
	my $host =     $auth -> { 'host' };

	my( $compress, $transactional, $skipmeta, $skipdefiner, $extinsert );
	if( defined( $variables ) ) {
		return( FALSE ) unless( ref( $variables ) eq 'HASH' );

		#
		# Retrieve variable and settings values # {{{
		#

		$compress      = $variables -> { 'compress' }      if( exists( $variables -> { 'compress' } ) );
		$transactional = $variables -> { 'transactional' } if( exists( $variables -> { 'transactional' } ) );
		$skipmeta      = $variables -> { 'skipmeta' }      if( exists( $variables -> { 'skipmeta' } ) );
		$skipdefiner   = $variables -> { 'skipdefiner' }   if( exists( $variables -> { 'skipdefiner' } ) );
		$extinsert     = $variables -> { 'extinsert' }     if( exists( $variables -> { 'extinsert' } ) );

		# }}}
	}

	my $memorybackend;
	my $canskipdefiners = FALSE;
	my $port;
	$port = $auth -> { 'port' } if( defined( $auth -> { 'port' } ) and $auth -> { 'port' } );
	$port = PORT unless( defined( $port ) and $port );

	system( "mysqldump >/dev/null 2>&1" );
	if( $? < 0 ) {
		die( "$fatal Unable to execute `mysqldump` command: $!\n" );
	} else {
		my $rc      = $? >>   8;
		#my $signal = $? &  127;
		#my $core   = $? &  128;

		if( not( 0 == $rc ) ) {
			if( 127 == $rc ) {
				die( "$fatal Unable to locate `mysqldump` commandi($rc): $!\n" );
			} elsif( not( 1 == $rc ) ) {
				pwarn( "`mysqldump` returned unexpected status '$rc' when executed to determine availability" );
			}
		}
	}

	if( defined( $filename ) and length( $filename ) and -d $filename and ( not( defined( $destination ) ) or not( length( $destination ) ) ) ) {
		$destination = $filename;
		$filename = undef;
	}
	if( defined( $filename ) and length( $filename ) ) {
		if( defined( $destination ) and length( $destination ) ) {
			if( not( -d $destination ) ) {
				## no critic (ProhibitLeadingZeros)
				make_path( $destination, {
					  mode		=> 0775
					, verbose	=> FALSE
					, error		=> \my $errors
				} );
				if( scalar( @{ $errors } ) ) {
					foreach my $entry ( @{ $errors } ) {
						my( $dir, $message ) = %{ $entry };
						if( length( $message ) ) {
							pfail( "Error creating directory '$dir': $message\n" );
						} else {
							pfail( "make_path general error: $message\n" );
						}
					}
					return( undef );
				}
			}
		}
	} elsif( defined( $destination) and ( '' eq ref( $destination ) ) and length( $destination ) ) {

		# Destination is specified and is a string, presumably a
		# file-path...
		#
		if( -f $destination ) {
			( $filename, $destination ) = fileparse( $destination );
		} elsif( not( -d $destination ) ) {
			## no critic (ProhibitLeadingZeros)
			make_path( $destination, {
				  mode		=> 0775
				, verbose	=> FALSE
				, error		=> \my $errors
			} );
			if( scalar( @{ $errors } ) ) {
				foreach my $entry ( @{ $errors } ) {
					my( $dir, $message ) = %{ $entry };
					if( length( $message ) ) {
						pfail( "Error creating directory '$dir': $message\n" );
					} else {
						pfail( "make_path general error: $message\n" );
					}
				}
				return( undef );
			}
		}
	} elsif( 'SCALAR' eq ref( $destination ) ) {

		# Destination is a string-var into which we should read the
		# database dump...
		# Beware memory requirements of large dumps!
		#
		$memorybackend = $destination;

	} elsif( 'ARRAY' eq ref( $destination ) ) {

		# Destination is an array-var into which we should read the
		# lines of the database dump...
		# Beware memory requirements of large dumps!
		#
		$memorybackend = $destination;

	} else {

		# Without a destination specified, we create an output file in
		# the current directory...
		#
		$destination = realpath( getcwd() );
	}

	my $optdb = '';
	my $opttab = '';
	if( defined( $auth -> { 'database' } ) and length( $auth -> { 'database' } ) ) {

		# We know the database we're concerned with, so $objects gives
		# us the tables to backup...
		#
		$filename = $auth -> { 'database' } . ".sql" unless( defined( $filename ) and length( $filename ) );

		if( not( defined( $objects ) and length( $objects ) ) ) {
			my $db = $auth -> { 'database' };
			$opttab = "--ignore-table=$db.$flywaytablename " .
				  "--ignore-table=$db.$mywaytablename " .
				  "--ignore-table=$db.$mywayactionsname " .
				  "--ignore-table=$db.$mywayprocsname" if( $skipmeta );

		} elsif( ( '' eq ref( $objects ) ) or ( 'SCALAR' eq ref( $objects ) ) ) {
			$opttab = $objects;

		} elsif( 'ARRAY' eq ref( $objects ) ) {
			$opttab = '';
			foreach my $item ( @{ $objects } ) {
				$opttab .= " $item"
			}

		} elsif( 'HASH' eq ref( $objects ) ) {
			die( "$fatal Unsupported reference type 'HASH' for table parameter \$objects [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );

		} else {
			die( "$fatal Unknown reference type '" . ref( $objects ) . "' for table parameter \$objects [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
		}

		# mysqldump accepts '<database> [tables]' or
		# '--databases <databases>', but not a combination of the two...
		#
		if( defined( $auth -> { 'database' } ) and length( $auth -> { 'database' } ) ) {
		#	if( not( length( $opttab ) ) ) {
		#		$optdb = '--databases ' . $auth -> { 'database' };
		#	} else {
				$optdb = $auth -> { 'database' };
		#	}
		}
	} else {

		# We've not defined a database to backup, therefore $objects
		# contains a list of databases...
		#
		$opttab = '';

		if( not( defined( $objects ) and length( $objects ) ) ) {
			$optdb = "--all-databases";

			if( $skipmeta ) {
				pdebug( "\n" );
				pdebug( "Checking for databases on '$host' ...\n", undef, TRUE );

				my $databases;

				my $dbh;
				my $dsn = "DBI:mysql:host=$host;port=$port";
				my $error = dbopen( \$dbh, $dsn, $user, $password, FALSE );
				{
					die( "$fatal $error [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) if $error;

					$databases = sqlgetvalues( \$dbh, 'SHOW DATABASES' );
				}
				dbclose( \$dbh, undef, undef, TRUE );

				for my $db ( @{ $databases } ) {
					next if( $db eq 'information_schema' );
					next if( $db eq 'mysql' );
					next if( $db eq 'performance_schema' );
					$opttab .= "--ignore-table=$db.$flywaytablename " .
						   "--ignore-table=$db.$mywaytablename " .
						   "--ignore-table=$db.$mywayactionsname " .
						   "--ignore-table=$db.$mywayprocsname";
				}
			}

		} elsif( ( '' eq ref( $objects ) ) or ( 'SCALAR' eq ref( $objects ) ) ) {
			$optdb = "--databases $objects";
			my $db = $objects;
			$opttab = "--ignore-table=$db.$flywaytablename " .
				  "--ignore-table=$db.$mywaytablename " .
				  "--ignore-table=$db.$mywayactionsname " .
				  "--ignore-table=$db.$mywayprocsname" if( $skipmeta );

		} elsif( 'ARRAY' eq ref( $objects ) ) {
			$optdb = "--databases";
			foreach my $item ( @{ $objects } ) {
				$optdb .= " $item";
				my $db = $item;
				$opttab .= "--ignore-table=$db.$flywaytablename " .
					   "--ignore-table=$db.$mywaytablename " .
					   "--ignore-table=$db.$mywayactionsname " .
					   "--ignore-table=$db.$mywayprocsname" if( $skipmeta );
			}

		} elsif( 'HASH' eq ref( $objects ) ) {
			die( "$fatal Unsupported reference type 'HASH' for table parameter \$objects [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );

		} else {
			die( "$fatal Unknown reference type '" . ref( $objects ) . "' for table parameter \$objects [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
		}

		if( '' eq ref( $destination ) ) {
			if( not( defined( $filename ) and length( $filename ) ) ) {
				my $default = 'dump.sql';

				if( -e $destination . '/'. $default ) {
					die( "$fatal dbdump - No output filename specified and '$default' already exists\n" );
				} else {
					pwarn( "$warning dbdump - No output filename specified - using '$default'" );
					$filename = $default;
				}
			}
		}
	}

	my $optauth = "\"--user=$user\" \"--password=$password\" \"--host=$host\"";
	my $optdump;

	# TODO: Support Stored Procedure-only backups:
	# if( 'procedure' eq $mode ) {
	# 	$optdump = '--skip-opt -no-create-info --no-data'
	# 		 . '--no-create-db --routines';
	# } else
	if( defined( $transactional ) and $transactional ) {
		$optdump = '--skip-opt --add-drop-database --add-drop-table'
			 . ' --add-locks --allow-keywords --comments'
			 . ' --complete-insert --create-options'
			 . ' --disable-keys --dump-date --events --flush-logs'
			 . ' --flush-privileges --hex-blob'
			 . ' --include-master-host-port --no-autocommit'
			 . ' --order-by-primary --quick --quote-names'
			 . ' --routines --set-charset --single-transaction'
			 . ' --triggers --tz-utc'
			 ;
	} else {
		$optdump = '--skip-opt --add-drop-database --add-drop-table'
			 . ' --add-locks --allow-keywords --comments'
			 . ' --complete-insert --create-options'
			 . ' --disable-keys --dump-date --events --flush-logs'
			 . ' --flush-privileges --hex-blob'
			 . ' --include-master-host-port --lock-all-tables'
			 . ' --order-by-primary --quick --quote-names'
			 . ' --routines --set-charset --triggers --tz-utc'
			 ;
		# Let's assume that if we're doing a full backup in this way,
		# then we're either backing up from a master node, or that this
		# behaviour is likely what we need regardless...
		#
		# --master-data: Write binary log name and position to output;
		# --dump-slave:  Include CHANGE MASTER statement that provides
		#                master log position.
		#
		# By using argument '2' the CHANGE MASTER command is commented,
		# allowing tracking of where the backup was taken from without
		# mandating restoration to a slave.
		#
		pdebug( "\n" );
		pdebug( "Checking Master status of database on '$host' ...\n", undef, TRUE );

		my $master;

		my $dbh;
		my $dsn = "DBI:mysql:host=$host;port=$port";
		my $error = dbopen( \$dbh, $dsn, $user, $password, FALSE );
		{
			die( "$fatal $error [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) if $error;

			$master = sqlgetvalue( \$dbh, 'SELECT @@log_bin' );
		}
		dbclose( \$dbh, undef, undef, TRUE );

		if( 1 == $master ) {
			pnote( "\n" );
			pnote( "Database has bin-logging enabled - adding '--master-data' option\n", undef, TRUE );

			$optdump .= ' --master-data=2';
			#$optdump .= ' --dump-slave=2';

			# N.B. The '--gtid' option is present in MariaDB
			#      mysqldump 10.0.13 and above only (although
			#      earlier 10.1.x versions may also lack the
			#      option...)
			#
			# Here, we're relying on mysqldump's behviour that if
			# invoked with only a single recognised option then it
			# will output its help text and return '1', but if the
			# option is not recognised then it will output a much
			# more terse error and return '2'.  This is likely more
			# robust than assuming functionality based on versions
			# (see above), but is vulnerable to changes in
			# mysqldump itself...
			#
			system( "mysqldump --gtid >/dev/null 2>&1" );
			if( $? < 0 ) {
				die( "$fatal Unable to execute `mysqldump --gtid` command: $!\n" );
			} else {
				my $rc      = $? >>   8;
				#my $signal = $? &  127;
				#my $core   = $? &  128;

				if( 1 == $rc ) {
					pnote( "`mysqldump` supports Global Transaction IDs - adding '--gtid' option\n", undef, TRUE );
					$optdump .= ' --gtid';
				}
			}
		}
	}
	if( defined( $extinsert ) and $extinsert ) {
		$optdump .= ' --extended-insert';
	} else {
		$optdump .= ' --skip-extended-insert';
	}

	# See previous comment for discussion of the viability of this
	# approach...
	#
	if( not( $skipdefiner ) ) {
		# Skip the logic below - wanting to skip removal of definers
		# and having mysqldump do so for us actually reduces to the
		# same situation...
		$canskipdefiners = TRUE;
	} else {
		system( "mysqldump --skip-definer >/dev/null 2>&1" );
		if( $? < 0 ) {
			die( "$fatal Unable to execute `mysqldump --skip-definer` command: $!\n" );
		} else {
			my $rc      = $? >>   8;
			#my $signal = $? &  127;
			#my $core   = $? &  128;

			if( 1 == $rc ) {
				pnote( "`mysqldump` supports skipping DEFINER arguments to CREATE statements - adding '--skip-definer' option\n", undef, TRUE );
				$optdump .= ' --skip-definer';
				$canskipdefiners = TRUE;
			}
		}
	}

	if( defined( $verbosity ) and ( $verbosity > 0 ) ) { # debug(3), notice(2), warn(1)
		$optdump .= ' --verbose';
	}

	my $msg = 'Commencing database backup';
	$msg .= ' - there may be a delay while a GLOBAL READ LOCK is obtained' if( not( $transactional ) );
	pdebug( "\n" );
	pdebug( "$msg ...\n", undef, TRUE ) unless( $quietorsilent );
	pwarn( "\n" );
	pwarn( "Passing control to `mysqldump` from this point onwards.\n\n", undef, TRUE );

	# N.B.: We're not capturing STDERR in either instance...
	#
	if( ( 'SCALAR' eq ref( $memorybackend ) ) or ( 'ARRAY' eq ref( $memorybackend ) ) ) {
		pdebug( "Shell-command is: 'mysqldump $optauth $optdb $opttab $optdump'" );
		$memorybackend = qx/ mysqldump $optauth $optdb $opttab $optdump /;
		if( not( defined( $memorybackend ) ) ) {
			return( undef );
		} else {
			return( TRUE );
		}
	} else {
		my $output = ( defined( $destination ) and length( $destination ) ? $destination . '/' : '' ) . $filename;
		my $command = "mysqldump $optauth $optdb $opttab $optdump ";
		$command = "strace -vvfFtTs 128 -o \"${output}.strace\" $command" if( DEBUG );

		if( not( $canskipdefiners ) ) {
			pwarn( "`mysqldump` does not support skipping 'DEFINER' arguments - attempting to fix in-line...", LOGMAX, not( $quietorsilent ) );
			# We'll use hard-coded ' ' in place of \s or [:space:]
			# since this is what `mysqldump` outputs...
			my $fixdefiner = <<'EOF';
sed -u 's|\/\*\!50017 DEFINER=`[^`]*`@`[^`]*`\s*\*\/||g ; /^CREATE DEFINER=[^ ]\+ \(FUNCTION\|PROCEDURE\)/s| DEFINER=`[^`]*`@`[^`]*` | |g'
EOF
			chomp( $fixdefiner );
			$command .= ' | ' . $fixdefiner;
		}

		if( defined( $compress ) and length( $compress ) ) {
			if( 'gzip' eq $compress ) {
				$output .= '.gz';
				$command .= " | gzip -9cf - > \"$output\"";
			} elsif( ( 'lzma' eq $compress ) or ( 'xz' eq $compress ) ) {
				$output .= '.' . $compress;
				$command .= " | $compress -z6cf - > \"$output\"";
			} elsif( ( 'bzip2' eq $compress ) or ( '' eq $compress ) ) {
				$output .= '.bz2';
				$command .= " | bzip2 -z9cf - > \"$output\"";
			} else {
				die( "$fatal Unknown compression scheme '$compress'\n" );
			}
		} else {
			if( $canskipdefiners ) {
				$command .= "--result-file=\"$output\"";
			} else {
				$command .= " > \"$output\"";
			}
		}
		eval {
			touch( $output );
		};
		if( $@ ) {
			( my $error = $@ ) =~ s/ at .+ line \d+\.$//;
			die( "$fatal $error [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
		}

		pdebug( "Shell-command is: '$command'" );
		my $result = qx( $command 2>&1 );
		my( $rc, $sig, $core ) = ( $? >> 8, $? & 127, $? & 128 );
		pdebug( "Command completed with return-code $rc, signal $sig, core-dump $core" );
		pdebug( "Output:\n$result" );

		if( not( defined( $result ) ) ) {
			pfail( "Unable to launch external process: $!\n" );
			return( undef );
		}
		if( $rc ) {
			pfail( "Database dump failed: $rc\n" ) ;
			return( undef );
		}
		# Unfortunately, mysqldump appears to also throw errors but
		# then exit successfully :(
		if( ( $result =~ m/mysqldump:\s+Couldn.t execute'/ ) or ( $result =~ m/\s+\(\d{4}\)$/ ) ) {
			pfail( "Database dump failed: $rc\n$result\n" ) ;
			return( undef );
		}

		#return( $result );
		return( TRUE );
	}

	# Unreachable
	return( undef );
} # dbdump # }}}

sub dbrestore( $$;$ ) { # {{{
	my( $auth, $file, $progress ) = @_;

	return( undef ) unless( defined( $auth -> { 'user' } ) and length( $auth -> { 'user' } ) );
	return( undef ) unless( defined( $auth -> { 'password' } ) and length( $auth -> { 'password' } ) );
	return( undef ) unless( defined( $auth -> { 'host' } ) and length( $auth -> { 'host' } ) );
	return( undef ) unless( defined( $auth -> { 'port' } ) and length( $auth -> { 'port' } ) );
	return( undef ) unless( defined( $file ) );

	my( $user, $password, $host, $port, $database );
	$user = $auth -> { 'user' };
	$password = $auth -> { 'password' };
	$host = $auth -> { 'host' };
	$port = $auth -> { 'port' };
	$database = $auth -> { 'database' } if( defined( $auth -> { 'database' } ) and length( $auth -> { 'database' } ) );

	my $mysql = which( 'mysql' );

	die( "$fatal Cannot locate 'mysql' binary\n" ) unless( defined( $mysql ) and -x $mysql );
	die( "$fatal Cannot read file '$file'\n" ) unless( -r $file );

	my $decompress;
	if( $file =~ /\.bz2$/ ) {
		$decompress = 'bunzip2 -cdq';
	} elsif( $file =~ /\.gz$/ ) {
		$decompress = 'gunzip -cq';
	} elsif( $file =~ /\.xz$/ ) {
		$decompress = 'unxz -cdq';
	} elsif( $file =~ /\.lzma$/ ) {
		$decompress = 'unlzma -cdq';
	}
	pdebug( "Will decompress data with command '$decompress'" ) if( defined( $decompress) and length( $decompress ) );

	# To resolve bugs.mysql.com/69970, any instance of:
	#
	#   /*!40000 DROP DATABASE IF EXISTS `mysql`*/;
	#
	# needs to be replaced with:
	#
	#   /*!50106 SET @OLD_GENERAL_LOG=@@GENERAL_LOG*/;
	#   /*!50106 SET GLOBAL GENERAL_LOG=0*/;
	#   /*!50106 SET @OLD_SLOW_QUERY_LOG=@@SLOW_QUERY_LOG*/;
	#   /*!50106 SET GLOBAL SLOW_QUERY_LOG=0*/;
	#   /*!40000 DROP DATABASE IF EXISTS `mysql`*/;
	#   /*!50106 SET GLOBAL GENERAL_LOG=@OLD_GENERAL_LOG*/;
	#   /*!50106 SET GLOBAL SLOW_QUERY_LOG=@OLD_SLOW_QUERY_LOG*/;
	#
	# Update: We then hit bugs.mysql.com/58116 where the line:
	#
	#   /*!50106 SET GLOBAL SLOW_QUERY_LOG=@OLD_SLOW_QUERY_LOG*/;
	#
	# results in:
	#
	#   ERROR 1146 (42S02) at line 59529: Table 'mysql.slow_log' doesn't exist
	#
	# ... which means that the fnial statement needs to be re-inserted at
	# the end of the file, somehow <sigh>
	#

	my $fixdrop = <<'EOF';
sed -u '/^\/\*\!40000 DROP DATABASE IF EXISTS `mysql`\*\/;\?$/s|^.*$|/*!50106 SET @OLD_GENERAL_LOG=@@GENERAL_LOG*/;\n/*!50106 SET GLOBAL GENERAL_LOG=0*/;\n/*!50106 SET @OLD_SLOW_QUERY_LOG=@@SLOW_QUERY_LOG*/;\n/*!50106 SET GLOBAL SLOW_QUERY_LOG=0*/;\n/*!40000 DROP DATABASE IF EXISTS `mysql`*/;\n/*!50106 SET GLOBAL GENERAL_LOG=@OLD_GENERAL_LOG*/;\n/*!50106 SET GLOBAL SLOW_QUERY_LOG=@OLD_SLOW_QUERY_LOG*/;|'
EOF
	chomp( $fixdrop );
	my $verbose = '';
	if( not( $quietorsilent ) ) {
		if( $verbosity > 1 ) { # debug(3), notice(2)
			$verbose = '-v';
		}
		if( DEBUG or ( $verbosity > 2 ) ) { # debug(3)
			$verbose = '-v -v -v';
		}
	}
	my $command = '"' . $file . ( defined( $decompress ) ? '" | ' . $decompress : '"' );
	$command .= ' | ' . $fixdrop;
	$command .= ' | { ' . $mysql . " -u $user -p$password -h $host ";
	$command .= "'$database' " if( defined( $database ) );
	$command .= "$verbose " if( defined( $verbose ) );
	$command .= '2>&1 ; }';

	if( not( defined( $progress ) and ( $progress eq 'never' ) ) and which( 'pv' ) ) {
		my( $pv, $columns, $rows );
		$pv = 'pv';
		$columns = $rows = '';

		if( defined( $progress ) and ( lc( $progress ) eq 'always' ) ) {
			$pv='pv -f';
		}
		if( defined( $ENV{ 'COLUMNS' } ) and $ENV{ 'COLUMNS' } ) {
			$columns = ' -w ' . $ENV{ 'COLUMNS' };
		}
		if( defined( $ENV{ 'LINES' } ) and $ENV{ 'LINES' } ) {
			$rows = ' -H ' . $ENV{ 'LINES' };
		}

		$command = $pv . ' -e -p -t -r -a -b -c ' . $columns . $rows . ' -N "' . basename( $file ) . '" ' . $command;
	} else {
		pwarn( "Cannot locate 'pv' executable: only errors will be reported\n\n" ) unless( defined( $progress ) and ( $progress eq 'never' ) );

		$command = 'cat ' . $command;
	}
	pdebug( "Restore command is '$command'" );

	if( defined( $database ) ) {
		# I can't imagine that this will change any time soon, but I
		# guess it's not impossible that at some future time `mariadb`
		# is the system database... ?
		my $systemdb = 'mysql';

		pdebug( "\n" );
		pdebug( "Connecting to system database `$systemdb` ...\n", undef, TRUE );

		my $systemdsn = "DBI:mysql:database=$systemdb;host=$host;port=$port";
		my $systemdbh;

		my $systemerror = dbopen( \$systemdbh, $systemdsn, $user, $password );
		{
			die( "$fatal $systemerror [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) if $systemerror;

			pdebug( "Creating database `$database` if necessary ...\n", undef, TRUE );

			# FIXME: We really need to process the baseline file to
			#        determine any additional database options (such as the
			#        default character-set) and execute these here...
			die( "$fatal Database creation error [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) if( not( sqldo( \$systemdbh, "CREATE DATABASE IF NOT EXISTS `$database`" ) ) );
		}
		dbclose( \$systemdbh, 'Complete', 'system', TRUE );
	}

	system( $command );
	if( -1 == $? ) {
		die( "Failed to execute 'pv' in order to monitor data restoration: $!\n" );
	} else {
		return( $? >> 8 );
	}

	# Unreachable
	return( undef );
} # dbrestore # }}}


sub verticasetsearchpath( $$;$ ) { # {{{
	my( $dbh, $path, $user ) = @_;

	return( undef ) unless( defined( $dbh ) );
	die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": arg1 must be passed by reference (" . ref( $dbh ) . ")\n" ) unless( 'REF' eq ref( $dbh ) );
	die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": \${ arg1 } must be of type DBI::db (" . ref( ${ $dbh } ) . ")\n" ) unless( 'DBI::db' eq ref( ${ $dbh } ) );

	return( undef ) unless( defined( $path ) and length( $path ) );

	$user = ${ $dbh } -> { 'Username' } unless( defined( $user ) and length( $user ) );

	my $availableschema = sqlgetvalues( $dbh, "SELECT DISTINCT `table_schema` FROM `tables`" );
	if( defined( $user ) and not( qr/^$user$/ |M| \@{ $availableschema } ) ) {
		$user = undef;
	}
	if( not( $quietorsilent ) ) {
		pdebug( "\n" );
		pdebug( "Setting Vertica SEARCH_PATH to include `$path` ...\n", undef, TRUE );
	}
	my $setsearchpath = "SET SEARCH_PATH TO \"$path\", " . ( ( defined( $user ) and length( $user ) ) ? "\"$user\", " : '' ) . "PUBLIC, v_catalog, v_monitor, v_internal";

	if( sqldo( $dbh, $setsearchpath ) ) {
		$searchpath = $path;
		return( TRUE );
	}

	return( FALSE );
} # verticasetsearchpath # }}}

sub dbcheckconnection( $;$ ) { # {{{
	my( $dbh, $sth ) = @_;

	return( undef ) unless( defined( $dbh ) );
	die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": arg1 must be passed by reference (" . ref( $dbh ) . ")\n" ) unless( 'REF' eq ref( $dbh ) );
	die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": \${ arg1 } must be of type DBI::db (" . ref( ${ $dbh } ) . ")\n" ) unless( 'DBI::db' eq ref( ${ $dbh } ) );
	if( defined( $sth ) ) {
		die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": arg2 must be passed by reference (" . ref( $sth ) . ")\n" ) unless( 'REF' eq ref( $sth ) );
		die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": \${ arg2 } must be of type DBI::st (" . ref( ${ $sth } ) . ")\n" ) unless( 'DBI::st' eq ref( ${ $sth } ) );
	}

	my $statement;
	$statement = ${ $sth } -> { 'Statement' } if( defined( $sth ) );
	$statement = ${ $dbh } -> { 'Statement' } unless( defined( $statement ) );

	# When executed via ODBC against Vertica, ping() causes corruption of
	# the database handle which results in the current prepared statement
	# always appearing to be "SQLTables_PING" - but the statement stored in
	# any active statement handle is unaffected and with the current
	# implmenetation the correct statement still seems to be executed
	# regardless...
	#
	return( TRUE ) if( ${ $dbh } -> ping() );

	my $err = ${ $dbh } -> err();
	my $errstr = ${ $dbh } -> errstr();
	my $state = ${ $dbh } -> state();

	# Just in case we have anything cached...
	#
	${ $sth } -> finish() if( defined( $sth ) );
	${ $dbh } -> disconnect();

	$dbconns --;
	warn( "*** DEBUG: dbcheckconnection(): \$dbconns is now $dbconns\n" ) if( DEBUG );

	pdebug( "Database unexpectedly dropped connection (from " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . "))!\n", undef, TRUE );

	if( not( defined( $connection ) and ( $connection -> { 'dsn' } ) ) ) {
		die( "$fatal No prior connection string saved - attempting to re-use closed connection? [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
	}

	if( pdebug( "Attempting to reconnect using last connection string:\n", undef, TRUE ) ) {
		$Data::Dumper::Pad = '    ';
		$Data::Dumper::Varname = "connection";
		print Dumper $connection;
	}

	# We re-open the connection here and leave it hanging, replacing the
	# connection the caller originally opened (and must close itself)...
	#
	my $error = dbopen( $dbh, $connection -> { 'dsn' } , $connection -> { 'user' }, $connection -> { 'password' }, $connection -> { 'strict' }, $connection -> { 'options' } );
	pwarn( "BUG: Reconnection returned '$error'\n", undef, TRUE ) if $error;

	my $vendor = ${ $dbh } -> get_info( 17 );
	die( "$fatal Database connection did not specify a vendor after reconnect [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) unless( defined( $vendor ) and length( $vendor ) );

	if( ( lc( $vendor ) eq 'vertica database' ) and ( defined( $searchpath ) and length( $searchpath ) ) ) {
		verticasetsearchpath( \$dbh, $searchpath ) or die( "$fatal Unable to restore database connetion state [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
	}
	my $text = 'Database responding';
	my $savedretries = $retries;
	$retries = undef;
	sqldo( $dbh, "SELECT '($text) DIRECT'" ) or die( "$fatal Database remains unusable [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
	my $result = sqlgetvalue( $dbh, "SELECT '($text) PREPARED'" ) or die( "$fatal Database remains unusable [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
	$retries = $savedretries;
	die( "$fatal Database remains unusable ('$result' != '($text)*') [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) unless( $result =~ m/^\Q($text)\E/ );

	# XXX: set_err automatically triggers RaiseError/PritnError/PrintWarn if $err is set?
	#${ $dbh } -> set_err( $err, $errstr, $state );

	return( FALSE );
} # dbcheckconnection # }}}


sub sqldo( $$;$ ) { # {{{
	my( $dbh, $st, $force ) = @_;

	return( undef ) unless( defined( $dbh ) );
	die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": arg1 must be passed by reference (" . ref( $dbh ) . ")\n" ) unless( 'REF' eq ref( $dbh ) );
	die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": \${ arg1 } must be of type DBI::db (" . ref( ${ $dbh } ) . ")\n" ) unless( 'DBI::db' eq ref( ${ $dbh } ) );

	return( undef ) unless( defined( $st ) and length( $st ) );

	warn( "*** DEBUG: dbsql(): called while \$dbconns is $dbconns\n" ) if( DEBUG and defined( $dbconns ) and not( $dbconns eq 1 ) );

	if( not( defined( $retries ) and ( $retries eq '0' ) ) ) {
		if( $retries ) {
			pwarn( "\n" );
			pwarn( "Database connection failed $retries time(s) whilst trying to directly execute statement \"$st\"\n" );
		} else {
			pdebug( "\n" );
			pdebug( "Database connection failed whilst attempting to recover from previous failure\n" );
		}
	}

	if( $st =~ m/__MW_(STR|L?TOK|LITERAL_QUOTE_)_/ ) {
		pwarn( "\n" );
		pwarn( "Unexpended token detected in `$st`\n" );

		#my $trace = Devel::StackTrace -> new;
		#print( $trace -> as_string );

		return( FALSE );
	}

	if( $st =~ m/^\s*DROP\s+(?:TABLE|DATABASE)\s/i ) {
		if( $allowunsafe ) {
			pwarn( "\n" );
			pwarn( "Executing destructive SQL statement:\n", undef, TRUE );
			pwarn( "$st\n", undef, TRUE );
		} else {
			pwarn( "\n" );
			pwarn( "Refusing to execute prohibited SQL statement:\n", LOGMAX, TRUE );
			pwarn( "$st\n", LOGMAX, TRUE );
			if( $force ) {
				# Don't abort...
				return( TRUE );
			} else {
				return( undef );
			}
		}
	}

	# Apparently '17' (SQL_DBMS_NAME) canonically returns the database
	# instance vendor...
	my $vendor = ${ $dbh } -> get_info( 17 );
	die( "$fatal Database connection did not specify a vendor [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) unless( defined( $vendor ) and length( $vendor ) );
	if( lc( $vendor ) eq 'vertica database' ) {
		# Can't we all just agree to get along??
		$st =~ s/`/\"/g;
	}

	# Please note that the following debug statement uses double-quotes as
	# the one standard form of deliniating data which standard SQL doesn't
	# use (or, at least, mandate) - Vertica kinda ruins this... :(
	pdebug( "SQL: Sending to database: \"$st\"" );
	eval {
		my $result = ${ $dbh } -> do( $st );

		# We're now seeing 'CREATE TABLE IF NOT EXISTS' throwing a
		# warning, and then we abort with 'MySQL server has gone away'
		# ... which is weird :(
		#
		# Update: Setting 'InactiveDestroy' on ${ $dbh } seems to
		#         resolve this by enabling auto-reconnect.
		#
		if( not( defined( $result ) ) ) {
			die( "$fatal Error in 'sqldo' while processing SQL statement [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]:\n$st\n" . ( defined( ${ $dbh } -> errstr() ) ? ${ $dbh } -> errstr() . "\n" : '' ) );
		}
	};
	if( $@ ) {
		my $error = '[No error from driver]';
		$error = join( ' ', split( /\s*\n+\s*/, ${ $dbh } -> errstr() ) ) if( defined( ${ $dbh } -> errstr() ) );
		( my $errorstr = $@ ) =~ s/ at .+ line \d+\.$//;
		my $err = '[Unknown]';
		$err = ${ $dbh } -> err() if( defined( ${ $dbh } -> err() ) );

		pwarn( "\n" );
		pwarn( "Error in 'sqldo' while processing SQL statement (" . $err . ": '$error'):\n$st\n\n$errorstr\n" );

		if( not( ${ $dbh } -> state() ) or ( ${ $dbh } -> state() eq 'S1000' ) or ( ${ $dbh } -> state() eq '00000' ) ) {
			if( defined( ${ $dbh } -> errstr() ) and ${ $dbh } -> errstr() =~ m/ \(SQL-\d{5}\)$/ ) {
				pdebug( "Manually updating State from '" . ${ $dbh } -> state() . "' to '$1'\n" );

				# XXX: set_err automatically triggers RaiseError/PritnError/PrintWarn if $err is set?
				${ $dbh } -> set_err( ${ $dbh } -> err, ${ $dbh } -> errstr, $1 );
			} else {
				pdebug( "Driver has set useless state '" . ${ $dbh } -> state() . "' with no recoverable context\n" );
			}
		} else {
			pdebug( "Driver has set SQL state '" . ${ $dbh } -> state() . "'\n" );
		}
		if( ( ( $st =~ m/^\s*DROP\s/i ) or ( $st =~ m/^\s*ALTER\s(O(N|FF)LINE\s+)?(IGNORE\s+)?TABLE\s.*\sDROP\s/i ) ) and ${ $dbh } -> err() eq '1091' ) {
			# XXX: This simply seems to propagate the error to the
			#      next prepared statement, which will then fail.
			#
			#
			# If we're trying to DROP an item which doesn't exist,
			# then arguably the desired state has been reached, so
			# we shouldn't abort...
			my $lasterr = ${ $dbh } -> err();
			${ $dbh } -> set_err( undef, undef, undef );

			# XXX: Do we break after the following statement, even
			#      though the error should have been cleared?
			if( not( sqldo( $dbh, "SELECT '(Encountered error $lasterr)'" ) ) ) {
				die( "$fatal Trivial command following error failed [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
			}

			if( not( defined( $retries ) ) ) {
				return( TRUE );
			} else {
				$retries ++;
				if( not( dbcheckconnection( $dbh ) ) ) {
					return( FALSE ) unless( $retries <= SQLRETRYMAX );
					pwarn( "Retrying SQL statement \"$st\" after $retries failures ...\n", undef, TRUE );
					return( sqldo( $dbh, $st, $force ) );
				}
			}
			$retries = 0;

			return( TRUE );

		} else {
			my $err = ${ $dbh } -> err();
			my $errstr = ${ $dbh } -> errstr();
			my $state = ${ $dbh } -> state();

			if( not( defined( $retries ) ) ) {
				return( FALSE );
			} else {
				$retries ++;
				if( not( dbcheckconnection( $dbh ) ) ) {
					return( FALSE ) unless( $retries <= SQLRETRYMAX );
					pwarn( "Retrying SQL statement \"$st\" after $retries failures ...\n", undef, TRUE );
					return( sqldo( $dbh, $st, $force ) );
				}
			}
			$retries = 0;

			# Operation failed yet dbcheckconnection returned TRUE
			# indicating that the database connection is still
			# valid - perhaps indicating a syntax error?

			#my $error = join( ' ', split( /\s*\n+\s*/, $errstr ) );
			#pdebug( "Restoring state $state (\"$error\"); omitting error value '$err'" );
			## XXX: set_err automatically triggers RaiseError/PritnError/PrintWarn if $err is set?
			#${ $dbh } -> set_err( 0, $errstr, $state );

			return( FALSE );
		}
	} else {

		if( not( defined( $retries ) ) ) {
			return( TRUE );
		} else {
			$retries ++;
			if( not( dbcheckconnection( $dbh ) ) ) {
				return( FALSE ) unless( $retries <= SQLRETRYMAX );
				pwarn( "Retrying SQL statement \"$st\" after $retries failures ...\n", undef, TRUE );
				return( sqldo( $dbh, $st, $force ) );
			}
		}
		$retries = 0;

		return( TRUE );
	}

	# Unreachable
	return( undef );
} # sqldo # }}}

sub sqlprepare( $$ ) { # {{{
	my( $dbh, $st ) = @_;

	return( undef ) unless( defined( $dbh ) );
	die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": arg1 must be passed by reference (" . ref( $dbh ) . ")\n" ) unless( 'REF' eq ref( $dbh ) );
	die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": \${ arg1 } must be of type DBI::db (" . ref( ${ $dbh } ) . ")\n" ) unless( 'DBI::db' eq ref( ${ $dbh } ) );

	return( undef ) unless( defined( $st ) and length( $st ) );

	warn( "*** DEBUG: dbsql(): called while \$dbconns is $dbconns\n" ) if( DEBUG and defined( $dbconns ) and not( $dbconns eq 1 ) );

	if( $retries ) {
		pwarn( "\n" );
		pwarn( "Database connection failed $retries time(s) when preparing statement \"$st\"\n" );
	}

	if( $st =~ m/^\s*DROP\s+(?:TABLE|DATABASE)\s/i ) {
		if( $allowunsafe ) {
			pwarn( "\n" );
			pwarn( "Executing destructive SQL statement:\n", undef, TRUE );
			pwarn( "$st\n", undef, TRUE );
		} else {
			pwarn( "\n" );
			pwarn( "Refusing to execute prohibited SQL statement:\n", LOGMAX, TRUE );
			pwarn( "$st\n", LOGMAX, TRUE );

			# We'll abort if we hit this, but that's better than
			# dropping a production database object...
			return( undef );
		}
	}


	# Apparently '17' (SQL_DBMS_NAME) canonically returns the database
	# instance vendor...
	my $vendor = ${ $dbh } -> get_info( 17 );
	die( "$fatal Database connection did not specify a vendor [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) unless( defined( $vendor ) and length( $vendor ) );
	if( lc( $vendor ) eq 'vertica database' ) {
		# Can't we all just agree to get along??
		$st =~ s/`/\"/g;
	}


	pdebug( 'SQL: Preparing: "' . join( ' ', split( /\s*\n\s*/, $st ) ) . '"' );
	my $sth = ${ $dbh } -> prepare_cached( $st );
	if( $@ ) {
		my $error = '[No error from driver]';
		$error = join( ' ', split( /\s*\n+\s*/, ${ $dbh } -> errstr() ) ) if( defined( ${ $dbh } -> errstr() ) );
		( my $errorstr = $@ ) =~ s/ at .+ line \d+\.$//;

		pfail( "\n" );
		pfail( "Error in 'sqlprepare' while preparing SQL statement:\n$st\n\n$errorstr\n" . ( defined( $error ) and length( $error ) ? $error . "\n" : '' ) );

		if( not( ${ $dbh } -> state() ) or ( ${ $dbh } -> state() eq 'S1000' ) or ( ${ $dbh } -> state() eq '00000' ) ) {
			if( defined( ${ $dbh } -> errstr() ) and ${ $dbh } -> errstr() =~ m/ \(SQL-\d{5}\)$/ ) {
				pdebug( "Manually updating State from '" . ${ $dbh } -> state() . "' to '$1'\n" );

				# XXX: set_err automatically triggers RaiseError/PritnError/PrintWarn if $err is set?
				${ $dbh } -> set_err( ${ $dbh } -> err, ${ $dbh } -> errstr, $1 );
			} else {
				pdebug( "Driver has set useless state '" . ${ $dbh } -> state() . "' with no recoverable context\n" );
			}
		} else {
			pdebug( "Driver has set SQL state '" . ${ $dbh } -> state() . "'\n" );
		}

		if( not( defined( $retries ) ) ) {
			return( undef );
		} else {
			$retries ++;
			if( not( dbcheckconnection( $dbh, ( defined( $sth ) ? \$sth : undef ) ) ) ) {
				return( undef ) unless( $retries <= SQLRETRYMAX );
				pwarn( "Retrying SQL statement \"$st\" after $retries failures ...\n", undef, TRUE );
				return( sqlprepare( $dbh, $st ) );
			}
		}
		$retries = 0;

		# Operation failed yet dbcheckconnection returned TRUE
		# indicating that the database connection is still
		# valid - perhaps indicating a syntax error?

		return( undef );
	} else {
		# N.B.: $sth -> finish() must be called prior to the next SQL
		#       interaction!

		if( not( defined( $retries ) ) ) {
			return( $sth );
		} else {
			$retries ++;

			# FIXME: There's something odd going on here... within
			#        the last two commits, we've gone from never
			#        hitting this situation to always seeming to
			#        hit it when preparing the statement to update
			#        `myway_stored_procedures`, to always hitting
			#        it when preparing "SELECT @@SESSION.sql_mode",
			#        even though every execute statement first
			#        prepares the statement to run, and this code-
			#        path is unaltered.  The error-checking herein
			#        appears to prevent this from being more than
			#        a cosmetic issue, however...
			if( not( dbcheckconnection( $dbh, ( defined( $sth ) ? \$sth : undef ) ) ) ) {
				return( undef ) unless( $retries <= SQLRETRYMAX );
				pdebug( "Retrying SQL statement \"$st\" after $retries failures ...\n", undef, TRUE );
				return( sqlprepare( $dbh, $st ) );
			}
		}
		$retries = 0;

		return( $sth );
	}

	# Unreachable
	return( undef );
} # sqlprepare # }}}

sub sqlexecute( $$;$@ ) { # {{{
	my( $dbh, $sth, $st, @values ) = @_;

	return( undef ) unless( defined( $dbh ) );
	die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": arg1 must be passed by reference (" . ref( $dbh ) . ")\n" ) unless( 'REF' eq ref( $dbh ) );
	die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": \${ arg1 } must be of type DBI::db (" . ref( ${ $dbh } ) . ")\n" ) unless( 'DBI::db' eq ref( ${ $dbh } ) );

	warn( "*** DEBUG: dbsql(): called while \$dbconns is $dbconns\n" ) if( DEBUG and defined( $dbconns ) and not( $dbconns eq 1 ) );

	if( defined( $sth ) and $sth ) {
		pdebug( 'SQL: About to execute prepared query in statement handle provided ...' );
	} else {
		return( undef ) unless( defined( $st ) and length( $st ) );
		pdebug( 'SQL: Preparing to execute query: "' . join( ' ', split( /\s*\n+\s*/, $st ) ) . '"' );
		$sth = sqlprepare( $dbh, $st );
		return( undef ) unless( defined( $sth ) and $sth );
	}

	if( $retries ) {
		if( defined( $sth -> { 'Statement' } ) and length( $sth -> { 'Statement' } ) ) {
			pwarn( "\n" );
			pwarn( "Database connection failed $retries time(s) whilst executing prepared statement \"" . $sth -> { 'Statement' } . "\"\n" );
		} elsif( defined( $st ) and length( $st ) ) {
			pwarn( "\n" );
			pwarn( "Database connection failed $retries time(s) whilst executing statement \"$st\"\n" );
		} else {
			# Unreachable?
			pwarn( "\n" );
			pwarn( "Database connection failed $retries time(s) whilst executing unknown statement\n" );
		}
	}

	pdebug( 'SQL: Executing: "' . join( ' ', split( /\s*\n+\s*/, $sth -> { 'Statement' } ) ) . '"' ) if( defined( $sth -> { 'Statement' } ) );
	pdebug( 'SQL: Parameters: "' . join( '", "', grep defined, @values ) . '"' ) if( @values and scalar( @values ) );
	eval {
		my $result = $sth -> execute( @values );
		if( not( defined( $result ) ) ) {
			pfatal( 'SQL executed: "' . join( ' ', split( /\s*\n+\s*/, $sth -> { 'Statement' } ) ) . '"' ) if( defined( $sth -> { 'Statement' } ) );
			pfatal( 'SQL parameters: "' . join( '", "', grep defined, @values ) . '"' ) if( @values and scalar( @values ) );
			if( defined( ${ $dbh } -> errstr() ) ) {
				die( "$fatal SQL execution error [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]: " . join( ' ', split( /\s*\n+\s*/, ${ $dbh } -> errstr() ) ) . "\n" );
			} else {
				die( "$fatal SQL execution error [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
			}
		}
	};
	if( $@ ) {
		my $errstr = $@;

		my( $sthst, $stherr, $stherrstr, $sthstate );
		$sthst = $sth -> { 'Statement' } if( defined( $sth -> { 'Statement' } ) );
		$stherr = $sth -> err if( defined( $sth -> err ) );
		$stherrstr = $sth -> errstr if( defined( $sth -> errstr ) );
		$sthstate = $sth -> state if( defined( $sth -> state ) );

		eval {
			$sth -> finish();
		};
		$sth = undef;

		@values = map { defined( $_ ) ? $_ : '' } @values;

		if( defined( $st ) ) {
			warn( "\n" );
			warn( "$failed Error when processing SQL statement:\n$st\n" );
			warn( '       ... with parameters "' . join( '", "', @values ) . "\"\n" ) if( scalar( @values ) );
		} elsif( defined( $sthst ) ) {
			warn( "\n" );
			warn( "$failed Error when processing statement from handle:\n$sthst\n" );
			warn( '       ... with parameters "' . join( '", "', @values ) . "\"\n" ) if( scalar( @values ) );
		} elsif( @values and scalar( @values ) ) {
			warn( "\n" );
			warn( "$failed Error applying parameters \"" . join( '", "', @values ) . "\"\n" );
		}
		warn( "Error details:\n" );
		warn( "Statement:    '" . $sthst . "'\n" ) if( defined( $sthst ) );
		foreach my $value ( @values ) {
			warn( "Parameter:    " . $value . "\n" );
		}
		warn( "Error:        '" . $errstr . "'\n" );
		warn( "Error string: '" . ${ $dbh } -> errstr() . "'\n" ) if( defined( ${ $dbh } -> errstr() ) );
		warn( "State:        '" . ${ $dbh } -> state() . "'\n" ) if( defined( ${ $dbh } -> state() ) );
		warn( "\n" );
		warn( "Statement debug:\n" );
		warn( "Error:        '" . $stherr . "'\n" ) if( defined( $stherr ) );
		warn( "Error string: '" . $stherrstr . "'\n" ) if( defined( $stherrstr ) );
		warn( "State:        '" . $sthstate . "'\n" ) if( defined( $sthstate ) );

		if( not( ${ $dbh } -> state() ) or ( ${ $dbh } -> state() eq 'S1000' ) or ( ${ $dbh } -> state() eq '00000' ) ) {
			if( defined( ${ $dbh } -> errstr() ) and ${ $dbh } -> errstr() =~ m/ \(SQL-\d{5}\)$/ ) {
				pdebug( "Manually updating State from '" . ${ $dbh } -> state() . "' to '$1'\n" );

				# XXX: set_err automatically triggers RaiseError/PritnError/PrintWarn if $err is set?
				${ $dbh } -> set_err( ${ $dbh } -> err, ${ $dbh } -> errstr, $1 );
			} else {
				pdebug( "$warning Driver has set useless state '" . ${ $dbh } -> state() . "' with no recoverable context\n" );
			}
		}

		if( not( defined( $retries ) ) ) {
			return( undef );
		} else {
			$retries ++;
			if( not( dbcheckconnection( $dbh ) ) ) {
				return( undef ) unless( $retries <= SQLRETRYMAX );
				my $statement = $st;
				$statement = $dbh -> { 'Statement' } unless( defined( $st ) and length( $st ) );
				pwarn( "Retrying SQL statement \"$statement\" after $retries failures ...\n", undef, TRUE );
				return( sqlexecute( $dbh, $sth, $st, @values ) );
			}
		}
		$retries = 0;

		# Operation failed yet dbcheckconnection returned TRUE
		# indicating that the database connection is still
		# valid - perhaps indicating a syntax error?

		return( undef );

	} else {
		# N.B.: $sth -> finish() must be called prior to the next SQL
		#       interaction!

		if( not( defined( $retries ) ) ) {
			return( $sth );
		} else {
			$retries ++;
			if( not( dbcheckconnection( $dbh, ( defined( $sth ) ? \$sth : undef ) ) ) ) {
				return( undef ) unless( $retries <= SQLRETRYMAX );
				my $statement = $st;
				$statement = $sth -> { 'Statement' } unless( defined( $st ) and length( $st ) );
				pwarn( "Retrying SQL statement \"$statement\" after $retries failures ...\n", undef, TRUE );
				return( sqlexecute( $dbh, $sth, $st, @values ) );
			}
		}
		$retries = 0;

		return( $sth );
	}

	# Unreachable
	return( undef );
} # sqlexecute # }}}

sub sqlgetvalue( $$;$ ) { # {{{
	my( $dbh, $st, $column ) = @_;

	return( undef ) unless( defined( $dbh ) );
	die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": arg1 must be passed by reference (" . ref( $dbh ) . ")\n" ) unless( 'REF' eq ref( $dbh ) );
	die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": \${ arg1 } must be of type DBI::db (" . ref( ${ $dbh } ) . ")\n" ) unless( 'DBI::db' eq ref( ${ $dbh } ) );

	return( undef ) unless( defined( $st ) and length( $st ) );

	warn( "*** DEBUG: dbsql(): called while \$dbconns is $dbconns\n" ) if( DEBUG and defined( $dbconns ) and not( $dbconns eq 1 ) );

	$column = 0 unless( defined( $column ) and ( $column =~ m/^\d+$/ ) and ( $column >= 0 ) );

	my $response;

	my $sth = sqlexecute( $dbh, undef, $st );
	if( not( defined( $sth ) and $sth ) ) {
		my $errstr = ${ $dbh } -> errstr();
		pfail( "\n" );
		pfail( "Unable to create statement handle to execute '$st'" . ( defined( $errstr ) and length( $errstr ) ? ": " . $errstr : '' ) . "\n" );
	} else {
		while( my $ref = $sth -> fetchrow_arrayref() ) {
			$response = @{ $ref }[ $column ];
		}
		$sth -> finish();
		pdebug( "SQL: Result:    \"$response\"" ) if( defined( $response ) );
	}

	return( $response );
} # sqlgetvalue # }}}

sub sqlgetvalues( $$;$ ) { # {{{
	my( $dbh, $st, $column ) = @_;

	return( undef ) unless( defined( $dbh ) );
	die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": arg1 must be passed by reference (" . ref( $dbh ) . ")\n" ) unless( 'REF' eq ref( $dbh ) );
	die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": \${ arg1 } must be of type DBI::db (" . ref( ${ $dbh } ) . ")\n" ) unless( 'DBI::db' eq ref( ${ $dbh } ) );

	return( undef ) unless( defined( $st ) and length( $st ) );

	warn( "*** DEBUG: dbsql(): called while \$dbconns is $dbconns\n" ) if( DEBUG and defined( $dbconns ) and not( $dbconns eq 1 ) );

	$column = 0 unless( defined( $column ) and ( $column =~ m/^\d+$/ ) and ( $column >= 0 ) );

	my @response;

	my $sth = sqlexecute( $dbh, undef, $st );
	if( not( defined( $sth ) and $sth ) ) {
		my $errstr = ${ $dbh } -> errstr();
		pfail( "\n" );
		pfail( "Unable to create statement handle to execute '$st'" . ( defined( $errstr ) and length( $errstr ) ? ": " . $errstr : '' ) . "\n" );
	} else {
		while( my $ref = $sth -> fetchrow_arrayref() ) {
			push( @response, @{ $ref }[ $column ] );
			pdebug( 'SQL: Result:    "' . @{ $ref }[ $column ] . '"' ) if( defined( @{ $ref }[ $column ] ) );
		}
		$sth -> finish();
	}

	return( \@response );
} # sqlgetvalues # }}}


sub outputtable( $$;$ ) { # {{{
	my( $dbh, $st, $fh ) = @_;

	return( undef ) unless( defined( $dbh ) );
	die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": arg1 must be passed by reference (" . ref( $dbh ) . ")\n" ) unless( 'REF' eq ref( $dbh ) );
	die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": \${ arg1 } must be of type DBI::db (" . ref( ${ $dbh } ) . ")\n" ) unless( 'DBI::db' eq ref( ${ $dbh } ) );

	my $sth = sqlexecute( $dbh, undef, $st );
	if( not( defined( $sth ) and $sth ) ) {
		my $errstr = ${ $dbh } -> errstr();
		pfail( "\n" );
		pfail( "Unable to create statement handle to render table" . ( defined( $errstr ) and length( $errstr ) ? ": " . $errstr : '' ) . "\n" );
	} else {
		my $table = DBI::Format::PartBox -> new();
		if( defined( $fh ) ) {
			$table -> header( $sth, $fh );
		} else {
			$table -> header( $sth );
		}
		while( my $ref = $sth -> fetchrow_arrayref() ) {
			$table -> row( $ref );
		}
		$table -> trailer();
		$sth -> finish();
	}

	return( TRUE );
} # outputtable # }}}

sub formatastable( $$$ ) { # {{{
	my( $dbh, $st, $indent ) = @_;

	return( undef ) unless( defined( $dbh ) );
	return( undef ) unless( defined( $st ) and length( $st ) );
	$indent = '' unless( defined( $indent ) and length( $indent ) );

	die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": arg1 must be passed by reference (" . ref( $dbh ) . ")\n" ) unless( 'REF' eq ref( $dbh ) );
	die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": \${ arg1 } must be of type DBI::db (" . ref( ${ $dbh } ) . ")\n" ) unless( 'DBI::db' eq ref( ${ $dbh } ) );

	my( $read, $write ) = FileHandle::pipe;

	# If we're writing more than 8k(?) of data, the pipe will block until
	# it is read from and drained... but in the code below, this only
	# happens once the table is fully written, resulting in a deadlock :(
	#
	my $firstchildpidorzero = fork;
	die( "$fatal fork() failed [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]: $!\n" ) unless( defined( $firstchildpidorzero ) );

	if( 0 == $firstchildpidorzero ) {
		# Child process

		# Will likely fail on non-UNIX systems - perlport(1) declares
		# 'setpgrp' as unimplemented on 'Win32, VMS, RISC OS, VOS'.
		#
		eval {
			setpgrp( 0, 0 );
		};

		$read -> close();

		# We'll output the table (to our pipe) in the child process, so
		# that the parent retains all I/O.
		#
		outputtable( $dbh, $st, $write );
		$write -> close();

		exit( 0 );
	} else {
		# Parent process

		$write -> close();

		while( my $line = $read -> getline() ) {
			chomp( $line );
			print( $indent . $line . "\n" ) if( length( $line ) );
		}
		$read -> close();

		if( waitpid( $firstchildpidorzero, 0 ) > 0 ) {
			my( $rc, $sig, $core ) = ( $? >> 8, $? & 127, $? & 128 );

			if( $core ) {
				pfail( "\n" );
				pfail( "rendering process $firstchildpidorzero core-dumped\n" );
			} elsif( 9 == $sig ) {
				pwarn( "rendering process $firstchildpidorzero was KILLed\n" );
			} else {
				pwarn( "rendering process $firstchildpidorzero returned $rc" . ( $sig ? " after signal $sig" : '' ) ) unless( 0 == $rc );
			}
		} else {
			pwarn( "backup process $firstchildpidorzero disappeared" );
		}
	}

	return( TRUE );
} # formatastable # }}}


sub databasegetinfo( $;$$$ ) { # {{{
	my( $dbh, $db, $engine, $vschm ) = @_;

	return( undef ) unless( defined( $dbh ) );
	die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": arg1 must be passed by reference (" . ref( $dbh ) . ")\n" ) unless( 'REF' eq ref( $dbh ) );
	die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": \${ arg1 } must be of type DBI::db (" . ref( ${ $dbh } ) . ")\n" ) unless( 'DBI::db' eq ref( ${ $dbh } ) );

	if( not( defined( $db ) and length( $db ) ) ) {
		$db = sqlgetvalue( $dbh, "SELECT DATABASE()" );
	}

	# Apparently '17' (SQL_DBMS_NAME) canonically returns the database
	# instance vendor...
	if( not( defined( $engine ) and length( $engine ) ) ) {
		$engine = ${ $dbh } -> get_info( 17 );

		if( not( defined( $engine ) and length( $engine ) ) ) {
			# How did this happen??
			if( dbcheckconnection( $dbh ) ) {
				$engine = ${ $dbh } -> get_info( 17 );
			}

			if( not( defined( $engine ) and length( $engine ) ) ) {
				die( "Could not determine database engine (after two tries) [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
			}
		}

		$engine = lc( $engine );
		if( defined( $engine ) and length( $engine ) and ( 'vertica database' eq $engine ) ) {
			$engine = 'vertica';
		} elsif( not( defined( $engine ) and length( $engine ) and ( 'mysql' eq $engine ) ) ) {
			pfail( "\n" );
			pfail( "Database engine is neither Vertica nor MySQL\n" );

			return( FALSE );
		}
	}

	my $verticadb = '';
	if ( 'vertica' eq $engine ) {
		if( defined( $vschm ) and length( $vschm ) ) {
			$verticadb = "$vschm`.`";
		} elsif( defined( $db ) and length( $db ) ) {
			$verticadb = "$db`.`";
		}
	}

	return( $db, $engine, $verticadb );
} # databasegetinfo # }}}

sub metadatamigrateschema( $;$$$ ) { # {{{
	my( $dbh, $db, $vschm, $variables ) = @_;

	return( undef ) unless( defined( $dbh ) );
	die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": arg1 must be passed by reference (" . ref( $dbh ) . ")\n" ) unless( 'REF' eq ref( $dbh ) );
	die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": \${ arg1 } must be of type DBI::db (" . ref( ${ $dbh } ) . ")\n" ) unless( 'DBI::db' eq ref( ${ $dbh } ) );

	return( FALSE ) unless( ref( $variables ) eq 'HASH' );

	#
	# Retrieve variable and settings values # {{{
	#

	my( $engine, $pretend, $quiet, $silent );
	$engine    = $variables -> { 'engine' }     if( exists( $variables -> { 'engine' } ) );
	$pretend   = $variables -> { 'pretend' }    if( exists( $variables -> { 'pretend' } ) );
	$quiet     = $variables -> { 'quiet' }      if( exists( $variables -> { 'quiet' } ) );
	$silent    = $variables -> { 'silent' }     if( exists( $variables -> { 'silent' } ) );

	# }}}

	my $verticadb = '';
	( $db, $engine, $verticadb ) = databasegetinfo( $dbh, $db, $engine, $vschm ) or return( FALSE );

	if( not( $quiet or $silent ) ) {
		pdebug( "\n" );
		pdebug( "Validating metadata schema for database engine '$engine' ...\n", undef, TRUE );
	}

	#if( OLDSCHEMA ) {
	#	my $success = FALSE;

	#	if( not( 'vertica' eq $engine ) ) {
	#		my $st = "DESCRIBE `$flywaytablename`";
	#		my $sth = sqlexecute( $dbh, undef, $st );
	#		if( not( defined( $sth ) and $sth ) ) {
	#			my $errstr = $dbh -> errstr();
	#			pfail( "\n" );
	#			pfail( "Unable to create statement handle to execute '$st'" . ( defined( $errstr ) and length( $errstr ) ? ": " . $errstr : '' ) . "\n" );

	#			return( FALSE );
	#		} else {
	#			my $foundoldschema = FALSE;

	#			ROWS: while( my $ref = $sth -> fetchrow_arrayref() ) {
	#				my $field = @{ $ref }[ 0 ];

	#				if( ( $field eq 'version_rank' ) ) {
	#					$foundoldschema = TRUE;
	#					$success = TRUE if( sqldo( $dbh, "ALTER TABLE `$flywaytablename` MODIFY `version_rank` INT DEFAULT NULL" ) );
	#					last ROWS;
	#				}
	#			}

	#			$sth -> finish();

	#			if( not( $foundoldschema ) ) {
	#				pwarn( "\n" );
	#				pwarn( "'OLDSCHEMA' is a debug option, and should not be used on any database with new metadata\n" );
	#			}
	#		}
	#	} elsif ( 'vertica' eq $engine ) {
	#		# XXX: Should implement a check as per the above, but
	#		#      Vertica never worked well enough to perform any
	#		#      deployments with the previous schema...
	#		$success = TRUE if(
	#			sqldo( $dbh, "ALTER TABLE `$verticadb$flywaytablename` ALTER COLUMN `version` DROP NOT NULL" ) and
	#			sqldo( $dbh, "ALTER TABLE `$verticadb$flywaytablename` ALTER COLUMN `version` SET DEFAULT NULL" )
	#		)
	#	}

	#	if( $success ) {
	#		pwarn( "Compatibility update applied to table `$flywaytablename`\n", undef, TRUE );
	#		return( TRUE );
	#	} else {
	#		pwarn( "Compatibility update failed to update table `$flywaytablename`\n", undef, TRUE );
	#		return( FALSE );
	#	}
	#}

	my $tableexists;
	my $setmigrated = FALSE;
	if( 'vertica' eq $engine ) {
		$tableexists = sqlgetvalue( $dbh, "SELECT COUNT(*) FROM `tables` WHERE `table_schema` = '$vschm' AND `table_name` = '$mywayhistoryname'" );
	} else {
		$tableexists = sqlgetvalue( $dbh, "SELECT COUNT(*) FROM `information_schema`.`TABLES` WHERE `TABLE_SCHEMA` = '$db' AND `TABLE_NAME` = '$mywayhistoryname'" );
	}

	if( defined( $tableexists ) and $tableexists ) { # {{{
		pdebug( "History metadata table '$mywayhistoryname' exists ..." );

		if( not( sqlgetvalue( $dbh, "SELECT COUNT(*) FROM `$verticadb$flywaytablename`" ) ) ) {
			# This is a fresh install, since we have no
			# `schema_version` entries.
			# XXX: If we have a database which had '--init'
			#      run with a legacy schema but which was
			#      not further used, this will still break.
			$setmigrated = TRUE;
		}

		my $st;
		if( 'vertica' eq $engine ) {
			# Emulate MySQL 5.7 output - there don't appear to be
			# equivalents for `Key` and `Extra` (which are both
			# MySQL extensions)...
			$st = "SELECT `column_name` AS 'Field', `data_type` AS 'Type', `is_nullable` AS 'Null', null AS Key, `column_default` AS 'Default', null AS 'Extra' FROM `v_catalog`.`columns` WHERE `table_schema` = '$vschm' AND `table_name` = '$mywayhistoryname' ORDER BY `ordinal_position`";
		} else {
			$st = "DESCRIBE `$mywayhistoryname`";
		}
		my $sth = sqlexecute( $dbh, undef, $st );
		if( not( defined( $sth ) and $sth ) ) {
			my $errstr = $dbh -> errstr();
			pfail( "\n" );
			pfail( "Unable to create statement handle to execute '$st'" . ( defined( $errstr ) and length( $errstr ) ? ": " . $errstr : '' ) . "\n" );

			return( FALSE );
		} else {
			my $foundoldschema = FALSE;

			ROWS: while( my $ref = $sth -> fetchrow_arrayref() ) {
				my $field = @{ $ref }[ 0 ];

				# `active` existed for the older schema
				# which we've now replaced, but not in
				# the current one...
				if( ( $field eq 'active' ) ) {
					pdebug( "History metadata table '$mywayhistoryname' is in need of replacement ..." );

					$foundoldschema = TRUE;
					last ROWS;
				}
			}

			$sth -> finish();

			if( $foundoldschema ) {
				my $currentversion = sqlgetvalue( $dbh, "SELECT DISTINCT `myway_version` FROM `$verticadb$mywayhistoryname` WHERE `active` IS TRUE ORDER BY `myway_version` DESC LIMIT 1" );
				pdebug( "Legacy history metadata table had active version '$currentversion'" );

				if( $pretend ) {
					psim( "Would drop legacy history metadata table '$mywayhistoryname' ..." );
				} else {
					pdebug( "Dropping legacy history metadata table '$mywayhistoryname' ..." );

					my $flag = $allowunsafe;
					$allowunsafe = TRUE;
					sqldo( $dbh, "DROP TABLE `$mywayhistoryname`", TRUE );
					$allowunsafe = $flag;
				}

				$tableexists = undef;
			}
		}
	} # }}}

	if( not( defined( $tableexists ) and $tableexists ) ) { # {{{
		if( $pretend ) {
			psim( "\n" );
			psim( "Would ensure that history metadata table `$mywayhistoryname` table exists.\n" );
		} else {
			if( not( $quiet or $silent ) ) {
				pdebug( "\n" );
				pdebug( "Ensuring that history metadata table `$mywayhistoryname` exists ...\n", undef, TRUE );
			}

			my $ddl = $mywayhistoryddl;
			if( 'vertica' eq $engine ) {
				$ddl = $verticamywayhistoryddl;
				if( defined( $vschm ) and length( $vschm ) ) {
					$ddl =~ s/__SCHEMA__/\"$vschm\"./g;
				} elsif( defined( $db ) and length( $db ) ) {
					$ddl =~ s/__SCHEMA__/\"$db\"./g;
				} else {
					$ddl =~ s/__SCHEMA__//g;
				}
			}
			sqldo( $dbh, $ddl ) or die( "$fatal `$mywayhistoryname` table creation failed [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
		}
	} # }}}

	if( not( $pretend ) ) {
		# Populate history table...

		if( 'vertica' eq $engine ) {
			sqldo( $dbh, "INSERT INTO `$verticadb$mywayhistoryname` (`myway_version`, `flyway_compatible`, `type`, `description`) VALUES('1.1.2', '2.1', 'INIT', '<< Flyway Init >>')" ) unless( sqlgetvalue( $dbh, "SELECT COUNT(*) FROM `$verticadb$mywayhistoryname` WHERE `myway_version` = '1.1.2'" ) );
			sqldo( $dbh, "INSERT INTO `$verticadb$mywayhistoryname` (`myway_version`, `flyway_compatible`, `type`, `description`) VALUES('1.2.0', '4.0', 'BASELINE', '<< Flyway Baseline >>')" ) unless( sqlgetvalue( $dbh, "SELECT COUNT(*) FROM `$verticadb$mywayhistoryname` WHERE `myway_version` = '1.2.0'" ) );
			sqldo( $dbh, "INSERT INTO `$verticadb$mywayhistoryname` (`myway_version`, `flyway_compatible`, `type`, `description`) VALUES('1.4.0', '4.0', 'BASELINE', '<< Flyway Baseline >>')" ) unless( sqlgetvalue( $dbh, "SELECT COUNT(*) FROM `$verticadb$mywayhistoryname` WHERE `myway_version` = '1.4.0'" ) );
		} else {
			sqldo( $dbh, "INSERT IGNORE INTO `$mywayhistoryname` (`myway_version`, `flyway_compatible`, `type`, `description`) VALUES('1.1.2', '2.1', 'INIT', '<< Flyway Init >>')" );
			sqldo( $dbh, "INSERT IGNORE INTO `$mywayhistoryname` (`myway_version`, `flyway_compatible`, `type`, `description`) VALUES('1.2.0', '4.0', 'BASELINE', '<< Flyway Baseline >>')" );
			sqldo( $dbh, "INSERT IGNORE INTO `$mywayhistoryname` (`myway_version`, `flyway_compatible`, `type`, `description`) VALUES('1.4.0', '4.0', 'BASELINE', '<< Flyway Baseline >>')" );
		}
		sqldo( $dbh, "UPDATE `$verticadb$mywayhistoryname` SET `migrated` = TRUE WHERE myway_version = '1.1.2'" ) if( $setmigrated );
		sqldo( $dbh, "UPDATE `$verticadb$mywayhistoryname` SET `migrated` = TRUE WHERE myway_version = '1.2.0'" ) if( $setmigrated );

		# For future updates...
		#sqldo( $dbh, "UPDATE `$verticadb$mywayhistoryname` SET `migrated` = TRUE WHERE myway_version = '1.4.0'" );
		#sqldo( $dbh, "UPDATE `$verticadb$mywayhistoryname` SET `migrated` = TRUE WHERE myway_version = '1.5.0'" );
		# ... etc.
	} # ( not ( $pretend ) )

	my $mywayversion = sqlgetvalue( $dbh, "SELECT DISTINCT `myway_version` FROM `$verticadb$mywayhistoryname` ORDER BY `myway_version` DESC LIMIT 1" );
	my $flywayversion;
	my $flywaydescription;
	if( defined( $mywayversion ) and length( $mywayversion ) ) {
		$flywayversion = sqlgetvalue( $dbh, "SELECT DISTINCT `flyway_compatible` FROM `$verticadb$mywayhistoryname` WHERE `myway_version` = '$mywayversion' LIMIT 1" );
		$flywaydescription = sqlgetvalue( $dbh, "SELECT DISTINCT `type` FROM `$verticadb$mywayhistoryname` WHERE `myway_version` = '$mywayversion' LIMIT 1" );
	}

	pdebug( "Metadata: myway '" . ( defined( $mywayversion ) ? $mywayversion : '<not set>' ) . "', flyway '" . ( defined( $flywayversion ) ? $flywayversion : '<not set>' ) . "', init string '" . ( defined( $flywaydescription ) ? $flywaydescription : '<not set>' ) . "'" );

	my $oldversions;

	if( defined( $mywayversion ) and length( $mywayversion ) ) {
		my @sortedversions = sort { versioncmp( $a, $b ) } ( $mywayversion, VERSION );
		my $latest = pop( @sortedversions );
		if( not( $latest eq VERSION ) ) {
			die( "$fatal The metadata version '$latest' declared in `$mywayhistoryname` is more recent than can be understood by version " . VERSION . " of this tool - aborting in order to maintain data integrity [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
		}

		$oldversions = sqlgetvalues( $dbh, "SELECT DISTINCT `myway_version` FROM `$verticadb$mywayhistoryname` WHERE `migrated` IS NOT TRUE AND `myway_version` != '$mywayversion' ORDER BY `myway_version`" );
	} else {
		# XXX: This should never happen, but we've seen Vertica
		#      fail to execute the previous query... in which
		#      case it's entirely unclear what we should do.
		#      Luckily in this case, we should be able to
		#      filter below...
		$oldversions = sqlgetvalues( $dbh, "SELECT DISTINCT `myway_version` FROM `$verticadb$mywayhistoryname` WHERE `migrated` IS NOT TRUE ORDER BY `myway_version`" );
	}

	foreach my $oldversion ( @{ $oldversions } ) {
		if( not( defined( $mywayversion ) and length( $mywayversion ) ) ) {
			pfail( "\n" );
			pfail( "Unable to determine current version from database - migration may fail ...\n" );
			if( $oldversion eq VERSION ) {
				pfail( "\n" );
				pfail( "Skipping version '$oldversion' on the assumption that we can't migrate away from the current release ...\n" );
				next;
			}
		}

		my $flywayversion = sqlgetvalue( $dbh, "SELECT DISTINCT `flyway_compatible` FROM `$verticadb$mywayhistoryname` WHERE `myway_version` = '$mywayversion'" );
		my $flywayoldinit = sqlgetvalue( $dbh, "SELECT DISTINCT `type` FROM `$verticadb$mywayhistoryname` WHERE `myway_version` = '$oldversion'" );
		my $flywayinit = sqlgetvalue( $dbh, "SELECT DISTINCT `type` FROM `$verticadb$mywayhistoryname` WHERE `myway_version` = '$mywayversion'" );
		my $flywayolddescription = sqlgetvalue( $dbh, "SELECT DISTINCT `description` FROM `$verticadb$mywayhistoryname` WHERE `myway_version` = '$oldversion'" );
		my $flywaydescription = sqlgetvalue( $dbh, "SELECT DISTINCT `description` FROM `$verticadb$mywayhistoryname` WHERE `myway_version` = '$mywayversion'" );

		pdebug( "$oldversion -> $mywayversion($flywayversion): init($flywayoldinit=>$flywayinit) desc($flywayolddescription=>$flywaydescription)" );

		if( $pretend ) {
			if( not( $quiet or $silent ) ) {
				psim( "\n" );
				psim( "Would upgrade metadata schema from version '$oldversion' to version '$mywayversion' (compatible with flyway version '$flywayversion') ...\n", undef, TRUE );
			}
		} else {
			if( defined( $oldversion ) and ( '1.1.2' eq $oldversion ) ) { # {{{
				pdebug( "Upgrading metadata schema from version '$oldversion' to version '$mywayversion' (compatible with flyway version '$flywayversion') ...\n", undef, TRUE ) unless( $quiet or $silent );

				# We need to migrate from original schema to Flyway 4.x schema:
				#
				#  `version_rank` is removed;
				#  `version` is no longer NOT NULL;
				#  The `version_rank` and `installed_rank` indices are removed;
				#  `installed_rank` is now the PRIMARY KEY;
				#  The `type` value 'INIT' is now 'BASELINE'.
				#
				# ... and in addition we've added a PK to myway_schema_actions.

				my $continue = TRUE;

				if( 'vertica' eq $engine ) {
					# Vertica can't add AUTO_INCREMENT columns, so we'll
					# have to just continue without the new Primary Key.
					# This isn't necessarily an issue, as it was only added
					# to keep InnoDB happy... but it is now inconsistent :(
					#
					#if( not( sqldo( $dbh, "ALTER TABLE `$verticadb$mywayactionsname` ADD COLUMN `id` AUTO_INCREMENT" ) ) ) {
					#	pwarn( "Unable to update `$verticadb$mywayactionsname` table\n", undef, TRUE );
					#	return( FALSE );
					#}

					sqldo( $dbh, "ALTER TABLE `$verticadb$flywaytablename` DROP COLUMN `version_rank`" ) or goto SCHEMA_UPDATE_FAILED;
					sqldo( $dbh, "ALTER TABLE `$verticadb$flywaytablename` DROP CONSTRAINT `C_PRIMARY`" ) or goto SCHEMA_UPDATE_FAILED;
					sqldo( $dbh, "ALTER TABLE `$verticadb$flywaytablename` ADD CONSTRAINT `${flywaytablename}_pk` PRIMARY KEY (`installed_rank`)" ) or goto SCHEMA_UPDATE_FAILED;
					sqldo( $dbh, "ALTER TABLE `$verticadb$flywaytablename` ALTER COLUMN `version` DROP NOT NULL" ) or goto SCHEMA_UPDATE_FAILED;
					sqldo( $dbh, "UPDATE `$verticadb$flywaytablename` SET `type` = '$flywayinit' WHERE `type` = '$flywayoldinit'" ) or goto SCHEMA_UPDATE_FAILED;
					sqldo( $dbh, "UPDATE `$verticadb$flywaytablename` SET `description` = '$flywaydescription' WHERE `description` = '$flywayolddescription'" ) or goto SCHEMA_UPDATE_FAILED;
				} else {
					my $st = "DESCRIBE `$mywayactionsname`";
					my $sth = sqlexecute( $dbh, undef, $st );
					if( not( defined( $sth ) and $sth ) ) {
						my $errstr = $dbh -> errstr();
						pfail( "\n" );
						pfail( "Unable to create statement handle to execute '$st'" . ( defined( $errstr ) and length( $errstr ) ? ": " . $errstr : '' ) . "\n" );

						return( FALSE );
					} else {
						my $foundnewschema = FALSE;

						ROWS: while( my $ref = $sth -> fetchrow_arrayref() ) {
							my $field = @{ $ref }[ 0 ];

							if( ( $field eq 'id' ) ) {
								$foundnewschema = TRUE;
								last ROWS;
							}
						}

						$sth -> finish();

						if( $foundnewschema ) {
							pwarn( "\n" );
							pwarn( "Metadata not flagged as updated but schema alteration 'id' already applied - database may not be consistent\n" );
						} else {
							if( not( sqldo( $dbh, "ALTER TABLE `$mywayactionsname` ADD COLUMN `id` INT UNSIGNED AUTO_INCREMENT NOT NULL FIRST, ADD CONSTRAINT PRIMARY KEY (`id`)" ) ) ) {
								pwarn( "Unable to update `$mywayactionsname` table\n", undef, TRUE );
								return( FALSE );
							}
						}
					}

					if( not( sqldo( $dbh, "DROP TEMPORARY TABLE IF EXISTS `${flywaytablename}_backup`" ) ) ) {
						pwarn( "Dropping temporary table `${flywaytablename}_backup` failed\n", undef, TRUE );
						return( FALSE );
					}
					if( not( sqldo( $dbh, "CREATE TEMPORARY TABLE IF NOT EXISTS `${flywaytablename}_backup` LIKE `$flywaytablename`" ) ) ) {
						pwarn( "Creating temporary table `${flywaytablename}_backup` failed\n", undef, TRUE );
						return( FALSE );
					}
					if( not( sqldo( $dbh, "INSERT INTO `${flywaytablename}_backup` SELECT * FROM `$flywaytablename`" ) ) ) {
						pwarn( "Populating temporary table `${flywaytablename}_backup` failed\n", undef, TRUE );
						return( FALSE );
					}

					$st = "SHOW INDEX FROM `$flywaytablename`";
					$sth = sqlexecute( $dbh, undef, $st );
					if( not( defined( $sth ) and $sth ) ) {
						my $errstr = $dbh -> errstr();
						pfail( "\n" );
						pfail( "Unable to create statement handle to execute '$st'" . ( defined( $errstr ) and length( $errstr ) ? ": " . $errstr : '' ) . "\n" );

						$continue = FALSE;
					} else {
						my $foundidx = 0;

						ROWS: while( my $ref = $sth -> fetchrow_arrayref() ) {
							my $key = @{ $ref }[ 2 ];

							if( ( $key eq 'schema_version_vr_idx' ) ) {
								sqldo( $dbh, "DROP INDEX `schema_version_vr_idx` ON `$flywaytablename`" );
								$foundidx ++;
							} elsif( ( $key eq 'schema_version_ir_idx' ) ) {
								sqldo( $dbh, "DROP INDEX `schema_version_ir_idx` ON `$flywaytablename`" );
								$foundidx ++;
							}
						}

						$sth -> finish();

						$continue = FALSE unless( $foundidx == 2 );
					}

					if( $continue ) {
						my $st = "DESCRIBE `$flywaytablename`";
						my $sth = sqlexecute( $dbh, undef, $st );
						if( not( defined( $sth ) and $sth ) ) {
							my $errstr = $dbh -> errstr();
							pfail( "\n" );
							pfail( "Unable to create statement handle to execute '$st'" . ( defined( $errstr ) and length( $errstr ) ? ": " . $errstr : '' ) . "\n" );

							$continue = FALSE;
						} else {
							my $foundoldschema = FALSE;

							ROWS: while( my $ref = $sth -> fetchrow_arrayref() ) {
								my $field = @{ $ref }[ 0 ];

								if( ( $field eq 'version_rank' ) ) {
									$foundoldschema = TRUE;
									last ROWS;
								}
							}

							$sth -> finish();

							if( not( $foundoldschema ) ) {
								pwarn( "\n" );
								pwarn( "Metadata not flagged as updated but schema alteration 'version_rank' already applied - database may not be consistent\n" );
							} else {
								if( not( sqldo( $dbh, "ALTER TABLE `$flywaytablename` DROP COLUMN `version_rank`" ) ) ) {
									pwarn( "Unable to update `$flywaytablename` table\n", undef, TRUE );
									$continue = FALSE;
								}
							}
						}
					}
					if( $continue ) {
						sqldo( $dbh, "ALTER TABLE `$flywaytablename` DROP PRIMARY KEY, ADD CONSTRAINT `${flywaytablename}_pk` PRIMARY KEY (`installed_rank`)" ) or goto SCHEMA_UPDATE_FAILED;
						sqldo( $dbh, "ALTER TABLE `$flywaytablename` MODIFY `version` VARCHAR(50)" ) or goto SCHEMA_UPDATE_FAILED;
						sqldo( $dbh, "UPDATE `$flywaytablename` SET `type` = '$flywayinit' WHERE `type` = '$flywayoldinit'" ) or goto SCHEMA_UPDATE_FAILED;
						sqldo( $dbh, "UPDATE `$flywaytablename` SET `description` = '$flywaydescription' WHERE `description` = '$flywayolddescription'" ) or goto SCHEMA_UPDATE_FAILED;
					}
				}

				sqldo( $dbh, "UPDATE `$verticadb$mywayhistoryname` SET `migrated` = TRUE WHERE `myway_version` = '$oldversion' AND `migrated` IS FALSE" ) or die( "$fatal Populating '$mywayhistoryname' failed" . ( defined( $dbh -> errstr() ) ? " with: " . $dbh -> errstr() : '' ) . ' [' . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
			} # ( defined( $oldversion ) and ( '1.1.2' eq $oldversion ) ) # }}}
			elsif( defined( $oldversion ) and ( '1.2.0' eq $oldversion ) ) { # {{{

				# Migrate from v1.2.0 to v1.4.0:
				#
				# `statement` is renamed to `statement_long`;
				# `statement_long` has its NOT NULL constraint removed;
				# A new attribute, named `statement`, is created as a maximally-sized VARCHAR()
				#
				# This is primarily to support searching for
				# executed statements in Vertica, which does
				# not allow use of the LIKE condition on
				# LONG VARCHAR fields...

				if( 'vertica' eq $engine ) {
					sqldo( $dbh, "ALTER TABLE `$verticadb$mywayactionsname` RENAME COLUMN `statement` TO `statement_long`" ) or goto SCHEMA_UPDATE_FAILED;
					sqldo( $dbh, "ALTER TABLE `$verticadb$mywayactionsname` ALTER COLUMN `statement_long` DROP NOT NULL" ) or goto SCHEMA_UPDATE_FAILED;
					sqldo( $dbh, "ALTER TABLE `$verticadb$mywayactionsname` ADD COLUMN `statement` VARCHAR(" . SQLMAX . ")" ) or goto SCHEMA_UPDATE_FAILED;
				} else {
					sqldo( $dbh, "ALTER TABLE `$mywayactionsname` CHANGE COLUMN `statement` `statement_long` LONGTEXT CHARACTER SET 'UTF8MB4'" ) or goto SCHEMA_UPDATE_FAILED;
					sqldo( $dbh, "ALTER TABLE `$mywayactionsname` ADD COLUMN `statement` VARCHAR(" . SQLMAX . ") CHARACTER SET 'UTF8MB4' AFTER `event`" ) or goto SCHEMA_UPDATE_FAILED;
				}

				sqldo( $dbh, "UPDATE `$verticadb$mywayhistoryname` SET `migrated` = TRUE WHERE `myway_version` = '$oldversion' AND `migrated` IS FALSE" ) or die( "$fatal Populating '$mywayhistoryname' failed" . ( defined( $dbh -> errstr() ) ? " with: " . $dbh -> errstr() : '' ) . ' [' . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
			} # ( defined( $oldversion ) and ( '1.2.0' eq $oldversion ) ) # }}}
			#elsif( defined( $oldversion ) and ( '1.4.0' eq $oldversion ) ) { # {{{
			#} # ( defined( $oldversion ) and ( '1.4.0' eq $oldversion ) ) # }}}
		} # ( $pretend )
	}

	return( TRUE );

	SCHEMA_UPDATE_FAILED:
		# We could automatically restore the backup table, but
		# for now we want to be able to inspect what failed.
		# FIXME: Restore backup on failure
		pwarn( "Error applying schema updates" . ( 'vertica' eq $engine ? '' : " - original table exists as '${flywaytablename}_backup', please manually inspect the differences" ) . "\n", undef, TRUE );
		die( "$fatal Database state is inconsistent [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
} # metadatamigrateschema # }}}

sub metadataupdateflywaytable( $$$$$$ ) { # {{{
	my( $dbh, $db, $vschm, $pretend, $insertorupdate, $variables ) = @_;

	return( undef ) unless( defined( $dbh ) );
	die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": arg1 must be passed by reference (" . ref( $dbh ) . ")\n" ) unless( 'REF' eq ref( $dbh ) );
	die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": \${ arg1 } must be of type DBI::db (" . ref( ${ $dbh } ) . ")\n" ) unless( 'DBI::db' eq ref( ${ $dbh } ) );

	return( FALSE ) unless( ref( $variables ) eq 'HASH' );

	if( not( defined( $insertorupdate ) ) or not( $insertorupdate ) or ( $insertorupdate =~ m/^\s*update\s*$/i ) ) {
		$insertorupdate = FALSE;
	} elsif( not( looks_like_number( $insertorupdate ) ) and not( $insertorupdate =~ m/^\s*insert\s*$/i ) ) {
		die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": arg1 must undefined, FALSE, TRUE, 'INSERT' or 'UPDATE' - '$insertorupdate' is ambiguous\n" );
	} else {
		$insertorupdate = TRUE;
	}

	my $engine;
	my $verticadb;
	( $db, $engine, $verticadb ) = databasegetinfo( $dbh, $db, $engine, $vschm ) or return( FALSE );

	#
	# Retrieve variable and settings values # {{{
	#

	my( $tablename, $installedrank, $schmversion, $desc, $filetype, $schmfile, $checksum, $user, $timestamp, $duration, $status );
	$tablename     = $variables -> { 'tablename' }     if( exists( $variables -> { 'tablename' } ) );
	$installedrank = $variables -> { 'installedrank' } if( exists( $variables -> { 'installedrank' } ) );
	$schmversion   = $variables -> { 'schmversion' }   if( exists( $variables -> { 'schmversion' } ) );
	$desc          = $variables -> { 'desc' }          if( exists( $variables -> { 'desc' } ) );
	$filetype      = $variables -> { 'filetype' }      if( exists( $variables -> { 'filetype' } ) );
	$schmfile      = $variables -> { 'schmfile' }      if( exists( $variables -> { 'schmfile' } ) );
	$checksum      = $variables -> { 'checksum' }      if( exists( $variables -> { 'checksum' } ) );
	$user          = $variables -> { 'user' }          if( exists( $variables -> { 'user' } ) );
	#$timestamp    = $variables -> { 'timestamp' }     if( exists( $variables -> { 'timestamp' } ) );
	$duration      = $variables -> { 'duration' }      if( exists( $variables -> { 'duration' } ) );
	$status        = $variables -> { 'status' }        if( exists( $variables -> { 'status' } ) );

	return( undef ) unless( defined( $tablename ) and defined( $schmversion ) );
	$tablename .= '`' unless( $tablename =~ m/`$/ );
	$tablename = "`$tablename" unless( $tablename =~ m/^`/ );

	if( not( defined( $installedrank ) ) ) {
		my $availabletables;

		die( "$fatal Unable to determine database engine [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) unless( defined( $engine ) );

		if( 'vertica' eq $engine ) {
			$availabletables = sqlgetvalues( $dbh, "SELECT `table_name` FROM `tables` WHERE `table_schema` = '$vschm'" );
		} elsif( 'mysql' eq $engine ) {
			$availabletables = sqlgetvalues( $dbh, "SHOW TABLES" );
		}
		if( not( $availabletables and scalar( @{ $availabletables } ) ) ) {
			pwarn( "\n" );
			pwarn( "Unable to retrieve list of tables for database `$db`" . ( defined( ${ $dbh } -> errstr() ) ? ': ' . ${ $dbh } -> errstr() : '' ) . "\n" );
		}

		my $name = $tablename;
		$name = ( split( /\./, $tablename ) )[ -1 ] if( $tablename =~ m/\./ );
		$name =~ s/`//g;
		if( qr/^$name$/ |M| \@{ $availabletables } ) {
			$installedrank = sqlgetvalue( $dbh, "SELECT MAX(`installed_rank`) FROM $tablename" );

			if( defined( $installedrank ) and $installedrank =~ m/^\d+$/ and $installedrank >= 0 ) {
				$installedrank++;
			} else {
				$installedrank = 0;
			}
		} else {
			pwarn( "\n" );
			pwarn( "Unable to find table '$name' in database '$db' - unable to apply change" );

			return( FALSE );
		}
		pwarn( ( caller( 1 ) )[ 3 ] . " called " . ( caller( 0 ) )[ 3 ] . " with mandatory parameter \$installedrank undefined - defaulting to '$installedrank'", LOGMAX, TRUE );
	}
	if( not( defined( $desc ) and length( $desc ) ) ) {
		$desc = 'Not specified';
		pwarn( ( caller( 1 ) )[ 3 ] . " called " . ( caller( 0 ) )[ 3 ] . " with mandatory parameter \$desc undefined - defaulting to '$desc'", LOGMAX, TRUE );
	}
	if( not( defined( $filetype ) and length( $filetype ) ) ) {
		$filetype = 'SQL';
		pwarn( ( caller( 1 ) )[ 3 ] . " called " . ( caller( 0 ) )[ 3 ] . " with mandatory parameter \$filetype undefined - defaulting to '$filetype'", undef, TRUE );
	}
	if( not( defined( $schmfile ) and length( $schmfile ) ) ) {
		$schmfile = '/dev/null';
		pwarn( ( caller( 1 ) )[ 3 ] . " called " . ( caller( 0 ) )[ 3 ] . " with mandatory parameter \$schmfile undefined - defaulting to '$schmfile'", LOGMAX, TRUE );
	}
	if( not( defined( $user ) and length( $user ) ) ) {
		$user = 'Unknown';
		pwarn( ( caller( 1 ) )[ 3 ] . " called " . ( caller( 0 ) )[ 3 ] . " with mandatory parameter \$user undefined - defaulting to '$user'", LOGMAX, TRUE );
	}
	if( not( defined( $duration ) ) ) {
		$duration = 0;
		pwarn( ( caller( 1 ) )[ 3 ] . " called " . ( caller( 0 ) )[ 3 ] . " with mandatory parameter \$duration undefined - defaulting to '$duration'", undef, TRUE );
	}
	if( not( defined( $status ) ) ) {
		$status = 0;
		pwarn( ( caller( 1 ) )[ 3 ] . " called " . ( caller( 0 ) )[ 3 ] . " with mandatory parameter \$status undefined - defaulting to '$status'", undef, TRUE );
	}

	# }}}

	my $sth;

	sqldo( $dbh, "UNLOCK TABLES" ) unless( $pretend or ( 'vertica' eq $engine ) );

	if( $insertorupdate ) {
		$sth = sqlprepare( $dbh, <<SQL );
INSERT INTO $tablename (
    `installed_rank`
  , `version`
  , `description`
  , `type`
  , `script`
  , `checksum`
  , `installed_by`
  , `installed_on`
  , `execution_time`
  , `success`
) VALUES ( ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ? )
SQL
		die( "$fatal Unable to create flyway metadata insertion statement handle" . ( defined( ${ $dbh } -> errstr() ) ? ': ' . ${ $dbh } -> errstr() : '' ) . ' [' . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) unless( defined( $sth ) and $sth );
		if( not( $pretend ) ) {
			sqlexecute( $dbh, $sth, undef,
				  $installedrank
				, $schmversion
				, $desc
				, $filetype
				, $schmfile
				, $checksum
				, $user
				#  CURRENT_TIMESTAMP
				, $duration
				, $status
			) or die( "$fatal Updating '$tablename' with new record failed" . ( defined( $sth -> errstr() ) ? ': ' . $sth -> errstr() : ( defined( ${ $dbh } -> errstr() ) ? ": " . ${ $dbh } -> errstr() : '' ) ) . ' [' . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
		}
	} else { # not( $insertorupdate )
		$sth = sqlprepare( $dbh, <<SQL );
UPDATE $tablename SET
    `installed_rank` = ?
  , `description` = ?
  , `type` = ?
  , `script` = ?
  , `checksum` = ?
  , `installed_by` = ?
  , `installed_on` = CURRENT_TIMESTAMP
  , `execution_time` = ?
  , `success` = ?
WHERE `version` = ?
SQL
		die( "$fatal Unable to create updated tracking statement handle" . ( defined( $sth -> errstr() ) ? ': ' . $sth -> errstr() : '' ) . ' [' . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) unless( defined( $sth ) and $sth );
		if( not( $pretend ) ) {
			sqlexecute( $dbh, $sth, undef,
				  $installedrank
				, $desc
				, $filetype
				, $schmfile
				, $checksum
				, $user
				#  CURRENT_TIMESTAMP
				, $duration
				, $status
				, $schmversion
			) or die( "$fatal Updating '$tablename' with updated record failed" . ( defined( $sth -> errstr() ) ? ": " . $sth -> errstr() : ( defined( $dbh -> errstr() ) ? ": " . $dbh -> errstr() : '' ) ) . ' [' . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
		}
	}
	$sth -> finish();

	if( not( $pretend ) ) {
		pdebug( "Committing transaction data\n", undef, TRUE ) unless( $quietorsilent );
		sqldo( $dbh, "COMMIT" ) or die( "$fatal Failed to commit transaction [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
	}
} # metadataupdateflywaytable # }}}


sub applyschema( $$$$;$ ) { # {{{
	my( $file, $actions, $variables, $auth, $schmvirtual ) = @_;

	return( FALSE ) unless( ref( $actions ) eq 'HASH' );
	return( FALSE ) unless( ref( $variables ) eq 'HASH' );
	return( FALSE ) unless( not( defined( $schmvirtual ) ) or ( ( ref( $schmvirtual ) eq '' ) or ( ref( $schmvirtual ) eq 'SCALAR' ) ) );
	$schmvirtual = ${ $schmvirtual } if( ref( $schmvirtual ) eq 'SCALAR' );

	# TODO: Value written to metadata table on success
	my $status = 0;

	my $filenamematchesexpectedformat = sub( $;$ ) { # {{{
		my( $filename, $result ) = @_;

		die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": arg2 must be passed by reference (" . ref( $result ) . ")\n" ) unless( not( defined( $result ) ) or ( 'SCALAR' eq ref( $result ) ) );

		# This regex could result in strange things if we have no file-extension...
		if( $filename =~ m/^(?:V[[:xdigit:].]+__)?V[[:xdigit:].]+__(.*?)(?:\.(not-)?.*?)?(?:\.d[dmc]l)?(?:\..*?)?$/ ) {
			if( defined( $result ) ) {
				${ $result } = $1;
				return( TRUE );
			} else {
				return( $1 );
			}
		}
		if( defined( $result ) ) {
			${ $result } = undef;
		}
		return( undef );
	}; # $filenamematchesexpectedformat # }}}
	my $filenamecontainsvalidversionstring = sub( $;$ ) { # {{{
		my( $filename, $result ) = @_;

		die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": arg2 must be passed by reference (" . ref( $result ) . ")\n" ) unless( not( defined( $result ) ) or ( 'SCALAR' eq ref( $result ) ) );

		if( $filename =~ m/^(?:V[[:xdigit:].]+__)?V([[:xdigit:].]+)__/ ) {
			if( defined( $result ) ) {
				${ $result } = $1;
				return( TRUE );
			} else {
				return( $1 );
			}
		}
		if( defined( $result ) ) {
			${ $result } = undef;
		}
		return( undef );
	}; # $filenamecontainsvalidversionstring # }}}
	my $fileismigrationschema = sub( $;$$ ) { # {{{
		my( $filename, $previous, $target ) = @_;

		die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": arg2 must be passed by reference (" . ref( $previous ) . ")\n" ) unless( not( defined( $previous ) ) or ( 'SCALAR' eq ref( $previous ) ) );
		die( "LOGIC: " . ( caller( 1 ) )[ 3 ] . "(" . ( caller( 0 ) )[ 2 ] . ") -> " . ( caller( 0 ) )[ 3 ] . ": arg3 must be passed by reference (" . ref( $target ) . ")\n" ) unless( not( defined( $target ) ) or ( 'SCALAR' eq ref( $target ) ) );

		if( $filename =~ m/^V([[:xdigit:].]+)__V([[:xdigit:].]+)__/ ) {
			if( defined( $previous ) ) {
				${ $previous } = $1;
			}
			if( defined( $target ) ) {
				${ $target } = $2;
			}
			return( TRUE );
		}
		return( undef );
	}; # $fileismigrationschema # }}}

	#
	# Retrieve variable and settings values # {{{
	#

	# ... with Readonly or Readonly::XS:
	#if( exists( $actions -> { 'init' } ) ) {
	#	Readonly my $action_init = $actions -> { 'init' };
	#} else {
	#	my $action_init;
	#}
	#my( $action_migrate, $action_check );

	my( $action_init, $action_migrate, $action_check );
	$action_check   = $actions -> { 'check' }   if( exists( $actions -> { 'check' } ) );
	$action_init    = $actions -> { 'init' }    if( exists( $actions -> { 'init' } ) );
	$action_migrate = $actions -> { 'migrate' } if( exists( $actions -> { 'migrate' } ) );

	my( $tmpdir, $mode, $marker, $first, $backupdir, $strict );
	my( $skipmeta, $extinsert, $nobackup, $desc, $pretend, $progress );
	my( $quiet, $silent, $environment, $limit );
	my( $compat, $force );
	$backupdir   = $variables -> { 'backupdir' }   if( exists( $variables -> { 'backupdir' } ) );
	$compat      = $variables -> { 'compat' }      if( exists( $variables -> { 'compat' } ) );
	$desc        = $variables -> { 'desc' }        if( exists( $variables -> { 'desc' } ) );
	$environment = $variables -> { 'environment' } if( exists( $variables -> { 'environment' } ) );
	$extinsert   = $variables -> { 'extinsert' }   if( exists( $variables -> { 'extinsert' } ) );
	$first       = $variables -> { 'first' }       if( exists( $variables -> { 'first' } ) );
	$force       = $variables -> { 'force' }       if( exists( $variables -> { 'force' } ) );
	$limit       = $variables -> { 'limit' }       if( exists( $variables -> { 'limit' } ) );
	$marker      = $variables -> { 'marker' }      if( exists( $variables -> { 'marker' } ) );
	$mode        = $variables -> { 'mode' }        if( exists( $variables -> { 'mode' } ) );
	$pretend     = $variables -> { 'pretend' }     if( exists( $variables -> { 'pretend' } ) );
	$progress    = $variables -> { 'progress' }    if( exists( $variables -> { 'progress' } ) );
	$quiet       = $variables -> { 'quiet' }       if( exists( $variables -> { 'quiet' } ) );
	$silent      = $variables -> { 'silent' }      if( exists( $variables -> { 'silent' } ) );
	$skipmeta    = $variables -> { 'skipmeta' }    if( exists( $variables -> { 'skipmeta' } ) );
	$strict      = $variables -> { 'strict' }      if( exists( $variables -> { 'strict' } ) );
	$tmpdir      = $variables -> { 'tmpdir' }      if( exists( $variables -> { 'tmpdir' } ) );
	$nobackup    = $variables -> { 'unsafe' }      if( exists( $variables -> { 'unsafe' } ) );

	my( $dsn, $engine, $vschm );
	$dsn    = $variables -> { 'dsn' }    if( exists( $variables -> { 'dsn' } ) );
	$engine = $variables -> { 'engine' } if( exists( $variables -> { 'engine' } ) );
	$vschm  = $variables -> { 'vschm' }  if( exists( $variables -> { 'vschm' } ) );

	my( $user, $pass, $host, $port, $db );
	$user = $auth -> { 'user' }     if( exists( $auth -> { 'user' } ) );
	$pass = $auth -> { 'password' } if( exists( $auth -> { 'password' } ) );
	$host = $auth -> { 'host' }     if( exists( $auth -> { 'host' } ) );
	$port = $auth -> { 'port' }     if( exists( $auth -> { 'port' } ) );
	$db   = $auth -> { 'database' } if( exists( $auth -> { 'database' } ) );

	#
	# Perform additional validation
	#

	$first = FALSE unless( defined( $first ) and $first );

	my( $schmfile, $schmpath, $schmext, $filetype );
	if( not( defined( $file ) and length( $file ) ) ) {
		if( defined( $action_init ) ) {
			if( not( length( $action_init ) ) ) {
				die( "$fatal --init requires a parameter: Initial version not specified\n" );
			} elsif( ( -d $action_init ) or ( -r $action_init ) ) {
				if( $force ) {
					pwarn( "Initial version '$action_init' looks like a filesystem object - force-continuing\n", undef, TRUE );
				} else {
					if( $pretend ) {
						pwarn( "Initial version '$action_init' looks like a filesystem object - would abort unless forced\n", undef, TRUE );
					} else {
						die( "$fatal Initial version '$action_init' looks like a filesystem object - re-run with '--force' to proceed regardless\n" );
					}
				}
			} elsif( $action_init =~ m/^--/ ) {
				if( $force ) {
					pwarn( "Initial version '$action_init' looks like a follow-on argument - force-continuing\n", undef, TRUE );
				} else {
					if( $pretend ) {
						pwarn( "Initial version '$action_init' looks like a follow-on argument - would abort unless forced\n", undef, TRUE );
					} else {
						die( "$fatal Initial version '$action_init' looks like a follow-on argument - re-run with '--force' to proceed regardless\n" );
					}
				}
			}
		} else {
			die( "$fatal File name required [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
		}
	} else {
		die( "$fatal Cannot read from file '$file'\n" ) unless( defined( $file ) and -r $file );

		( $schmfile, $schmpath, $schmext ) = fileparse( realpath( $file ), qr/\.[^.]+/ );
		if( defined( $schmfile ) and not( defined( $desc ) and length( $desc ) ) ) {
			if( ( $desc = $filenamematchesexpectedformat -> ( $schmfile ) ) ) {
				$desc =~ s/_/ /g;
			}
		}
		$desc = 'No description' unless( defined( $desc ) and length( $desc ) );
		if( defined( $schmext ) and length( $schmext ) ) {
			$filetype = uc( $schmext );
			$filetype =~ s/\.//;

			if( not( $filetype =~ m/SQL/ ) ) {
				pwarn( "File type '$filetype' (from filename '$file') is not recognised\n", undef, TRUE );
				if( $force ) {
					pwarn( "Metadata may no longer be Flyway-compatible once this schema is applied - force-continuing\n", undef, TRUE );
				} else {
					if( $pretend ) {
						pwarn( "Metadata may no longer by Flyway-compatible if this schema is applied - would abort unless forced\n", undef, TRUE );
					} else {
						die( "$fatal Metadata may no longer be Flyway-compatible once this schema is applied - re-run with '--force' to proceed regardless\n" );
					}
				}
			}

			$schmfile = $schmfile . $schmext;
		} else {
			$filetype = 'Unknown';
		}
		if( $fileismigrationschema -> ( $schmfile ) ) {
			$filetype = 'JUMP';
		}
	} # }}}

	#
	# Tokenise and parse SQL statements from $file # {{{
	#

	my $invalid = FALSE;
	my $metadata = {};
	my $metafile;
	my $procedureversion;

	if( 'procedure' eq $mode ) { # {{{
		# In this case, we retrieve the previous/current-version logic
		# from the metadata file, and many files may be applied with
		# the same version.
		$metafile = dirname( $file ) . '/' . $db . '.metadata';
		die( "$fatal Cannot read metadata '$db.metadata' for file '$file'\n" ) unless( -s $metafile );
		pnote( "Using metadata file '$metafile'\n", undef, TRUE ) unless( $quiet or $silent );

		$invalid = $invalid | not( processfile( $metadata, $metafile, undef, undef, $strict, TRUE ) );
		die( "$fatal Metadata failed validation - aborting [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) if( $invalid );

		my $okay = TRUE;
		METADATA_ADJUST: foreach my $entry ( $metadata -> { 'entries' } ) {
			foreach my $statement ( @{ $entry } ) {
				if( 'comment' eq $statement -> { 'type' } ) {
					if( defined( $marker ) and length( $marker ) ) {
						if( 'ARRAY' eq ref( $statement -> { 'entry' } ) ) {
							foreach my $line ( @{ $statement -> { 'entry' } } ) {
								chomp( $line );
								if( $line =~ m/Target\s+version:\s+([^\s]+)\s*/i ) {
									$procedureversion = "_v${1}` ";
									$procedureversion =~ s/\./_/g;
									if( $first ) {
										if( $pretend ) {
											psim( "Would adjust Stored Procedure names with version string '$procedureversion' from metadata\n", undef, TRUE );
										} else {
											pnote( "Adjusting Stored Procedure names with version string '$procedureversion' from metadata\n", undef, TRUE ) unless( $quiet or $silent );
										}
									}
								} elsif( $line =~ m/Environment:\s+([^\s]+)\s*$/i ) {
									if( defined( $1 ) and length( $1 ) ) {
										foreach my $restriction ( split( /\s*[,\s]+\s*/, $1 ) ) {
											my( $invert, $match );
											$restriction =~ m/^\s*(!)?\s*([^\s]+)\s*$/;
											$invert = $1 if( defined( $1 ) and length( $1 ) );
											$match = $2 if( defined( $2 ) and length( $2 ) );
											if( not( defined( $match ) ) ) {
												pwarn( "Metadata directive '$line' is not valid\n", undef, TRUE );
												return( FALSE );
											}
											pdebug( "Read metadata environment restriction '" . ( defined( $invert ) ? $invert : '' ) . "$match'" );
											## no critic (ProhibitConditionalDeclarations)
											$environment = '' unless( defined( $environment ) );
											if(
											  ( ( defined( $invert ) and ( $invert eq '!' ) ) and ( lc( $match ) eq lc( $environment ) ) )
											  or
											  ( not( defined( $invert ) and ( $invert eq '!' ) ) and not( lc( $match ) eq lc( $environment ) ) )
											) {
												if( '' eq $environment ) {
													pwarn( "Metadata from file '$schmfile' only allows execution in environment '$match'\n", undef, TRUE );
												} else {
													pwarn( "Metadata from file '$schmfile' prohibits execution in environment '$environment'\n", undef, TRUE );
												}
												# Don't fail, as we could have multiple files for different
												# environments, all of which but one drop-out here...
												return( TRUE );
											}
											pdebug( "Validated metadata environment restriction '" . ( defined( $invert ) ? $invert : '' ) . "$match'" );
										}
									}
								}
							}
						}
					}
				} elsif( $okay ) {
					$okay = FALSE;
					pwarn( "Metadata file '$metafile' contains non-comment code which will be executed before procedure definitions are processed\n", undef, TRUE ) if( $first );
					last METADATA_ADJUST;
				}
			}
		}

		if( not( defined( $procedureversion ) and length( $procedureversion ) ) ) {
			$procedureversion = $1 if( dirname( $file ) =~ m/^(?:.*?)(_v\d+_\d+(_\d+)?)$/ );
			if( defined( $procedureversion ) ) {
				if( defined( $marker ) and length( $marker ) ) {
					$procedureversion .= '` ';
					if( $pretend ) {
						psim( "Would adjust Stored Procedure names with version string '$procedureversion' from path for file '$schmfile'\n", undef, TRUE );
					} else {
						pnote( "Adjusting Stored Procedure names with version string '$procedureversion' from path for file '$schmfile'\n", undef, TRUE ) unless( $quiet or $silent );
					}
				}
			} else {
				if( $force ) {
					pwarn( "Cannot determine Stored Procedure version string from metadata or path - removing versioning from file '$schmfile'\n", undef, TRUE );
				} else {
					if( $pretend ) {
						pwarn( "Cannot determine Stored Procedure version string from metadata or path for file '$schmfile' - would abort unless forced\n", undef, TRUE );
					} else {
						die( "$fatal Cannot determine Stored Procedure version string from metadata or directory '" . dirname( $file ) . "' for file '$schmfile' - aborting [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
					}
				}
			}
		}
		if( defined( $procedureversion ) and ( $procedureversion =~ m/_v(\d+_\d+)_\d+/ ) ) {
			my $version = $1;
			$procedureversion =~ m/_v\d+_\d+_(\d+)/;
			my $hotfix = $1;
			$procedureversion =~ s/_v${version}_${hotfix}/_v${version}/;
			$version =~ s/_/./g;
			pwarn( "Metadata contains hotfix version $hotfix to base version $version\n", undef, TRUE ) if( $first );
		}

	} # ( 'procedure' eq $mode ) # }}}

	my $data = {};

	if( not( 'procedure' eq $mode ) ) { # {{{
		$invalid = $invalid | not( processfile( $data, $file, undef, undef, $strict, TRUE ) );
		die( "$fatal Schema file '$file' failed validation - aborting [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) if( $invalid );

	} else {
		if( defined( $marker ) and length( $marker ) ) {
			# Process file metadata and content here, as we need to substitute $marker
			# for $procedureversion...
			$invalid = $invalid | not( processfile( $data, $file, $marker, $procedureversion, $strict ) );
			die( "$fatal Stored Procedure '$file' failed validation after substitution - aborting [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) if( $invalid );
		} else {
			$invalid = $invalid | not( processfile( $data, $file, undef, undef, $strict ) );
			die( "$fatal Stored Procedure '$file' failed validation - aborting [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) if( $invalid );
		}

		my @entries;

		if( $first ) {
			# Previously, we only processed the metadata as far as
			# reading the initial metadata.  If we're on the first
			# iteration, we now need to process the entire file in
			# order to discover any contents.
			$invalid = $invalid | not( processfile( $metadata, $metafile, undef, undef, $strict ) );
			die( "$fatal Metadata file '$metafile' failed validation - aborting [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) if( $invalid );
		} # else {
		#	# Each $entry[] consists of: %{ @entry (text of statement), $line, $type (comment|statement) }
		#	my @procedurecomments = ();
		#	foreach my $entry ( $metadata -> { 'entries' } ) {
		#		foreach my $statement ( @{ $entry } ) {
		#			if( not( 'statement' eq $statement -> { 'type' } ) ) {
		#				push( @procedurecomments, $statement );
		#			}
		#		}
		#
		#	}
		#	push( @entries, @procedurecomments );
		#	$metadata -> { 'entries' } = \@procedurecomments;
		#}

		foreach my $entry ( $metadata -> { 'entries' } ) {
			push( @entries, @{ $entry } );
		}
		foreach my $entry ( $data -> { 'entries' } ) {
			push( @entries, @{ $entry } );
		}

		$data -> { 'entries' } = \@entries;
	} # }}}

	pnote( "Finished pre-processing file '$file' ...\n", undef, TRUE ) unless( $quiet or $silent );

	# }}}

	#
	# Get list of databases and tables from the instance # {{{
	#

	my $uuid;
	my $availabledatabases;
	my $availabletables;
	my $safetorestore = FALSE;
	my $verticadb = '';

	if( not( $quiet or $silent ) ) {
		pdebug( "\n" );
		pdebug( "Connecting to database `$db` to gather state data ...\n", undef, TRUE );
	}

	$dsn = "DBI:mysql:database=$db;host=$host;port=$port" unless( defined( $dsn ) );
	my $dbh;
	my $error = dbopen( \$dbh, $dsn, $user, $pass, $strict );
	{
		die( "$fatal Gathering state: $error [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) if $error;

		if( 'vertica' eq $engine ) {
			my $path;
			if( defined( $vschm ) and length( $vschm ) ) {
				$path = $vschm;
			} elsif( defined( $db ) and length( $db ) ) {
				$vschm = $db;
				$path = $db;
			}
			if( defined( $path ) and length( $path ) ) {
				verticasetsearchpath( \$dbh, $searchpath, $user );
			}

			# Vertica has no UUID-generation capability...
			if( is_loaded( 'Data::GUID' ) ) {
				eval {
					my $guid = Data::GUID -> new;
					$uuid = $guid -> as_string;
				};
			}
			if( not( defined( $uuid ) and length( $uuid ) ) ) {
				$uuid = sqlgetvalue( \$dbh, "SELECT HASH( SYSDATE() )" );
			}
		} else {
			$uuid = sqlgetvalue( \$dbh, "SELECT UUID()" );
		}

		if( 'mysql' eq $engine ) {
			$availabledatabases = sqlgetvalues( \$dbh, "SHOW DATABASES" );
			die( "$fatal Unable to retrieve list of available databases" . ( defined( $dbh -> errstr() ) ? ': ' . $dbh -> errstr() : '' ) . ' [' . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) unless( scalar( @{ $availabledatabases } ) );
		}

		if( 'vertica' eq $engine ) {
			$availabletables = sqlgetvalues( \$dbh, "SELECT `table_name` FROM `tables` WHERE `table_schema` = '$vschm'" );
		} elsif( 'mysql' eq $engine ) {
			$availabletables = sqlgetvalues( \$dbh, "SHOW TABLES" );
		}
		if( not( scalar( @{ $availabletables } ) ) ) {
			pwarn( "\n" );
			pwarn( "Unable to retrieve list of tables for database `$db`" . ( defined( $dbh -> errstr() ) ? ': ' . $dbh -> errstr() : '' ) . "\n" );
		}

		$verticadb = "$vschm`.`" if( 'vertica' eq $engine );
		if( not( sqlgetvalue( \$dbh, "SELECT COUNT(*) FROM `$verticadb$flywaytablename` WHERE `success` IS TRUE" ) ) ) {
			$safetorestore = TRUE;
		}
	}
	dbclose( \$dbh, undef, undef, TRUE );

	# }}}

	#
	# Process metadata tokens and validate versions # {{{
	#

	# N.B. $action_init can be defined but empty, when myway.pl is invoked with
	#      '--init' without an argument.
	my $schminitversion = $action_init;	# The parameter to --init; may
						# often be set but empty;
	my $schmfileversion;			# Any version present in the
						# current filename;
	my $schmversion;			# The actual version we're
						# planning to deploy - ideally
						# from metadata (but overriden
						# by $schminitversion);
	my $schmprevious = undef;		# Metadata previous version;
	my $schmtarget = undef;			# Metadata target version;
	my $schmdescription = undef;		# Metadata description.

	# Capture version from schema filename, if present...
	$filenamecontainsvalidversionstring -> ( $schmfile, \$schmfileversion );

	# XXX: We don't want to stomp on $flywayinit entries... or indeed any
	#      other.  Is there a sensible value to use here?  Auto-create a
	#      UUID?
	if( defined( $schminitversion ) and length( $schminitversion ) ) {
		$schmversion = $schminitversion;
	} elsif( defined( $schmfileversion ) and length( $schmfileversion ) ) {
		$schmversion = $schmfileversion;
	} else { # not( defined( $schmfileversion ) and length( $schmfileversion ) )
		# Stored Procedures are also not versioned by filename...
		if( defined( $schminitversion ) or ( 'procedure' eq $mode ) ) {
			$schmversion = 0;
		} else {
			my( $s, $ms ) = gettimeofday();
			# We don't want to be able to hit '0' at exactly
			# midnight, as that would look like the above case...
			# Account for leap-seconds ;)
			$schmversion = 0 - ( ( $s * 1000 ) + $ms + ( ( 24 * 60 * 60 + 1 ) * 1000 ) );
		}
	}


	# Process the first (or first two: Stored Procedures may have the
	# shared metadata file prefixed) comment only, to determine whether we
	# want to apply the file in question... if so, we'll proceed to
	# re-process the whole file.
	#
	my $firstcomment = TRUE;
	my $secondcomment = undef;

	foreach my $entry ( $data -> { 'entries' } ) {
		foreach my $statement ( @{ $entry } ) {
			if( 'comment' eq $statement -> { 'type' } ) {
				if( 'ARRAY' eq ref( $statement -> { 'entry' } ) ) {
					#
					# Version-tracking data must be in an
					# array in the first comment - we don't
					# process data in following comments,
					# or any non-standard format with
					# multiple tracking items on different
					# lines.
					#
					# FIXME: We'll only consider single-
					#        line Description fields, too.
					#
					# N.B.   Stored Procedure metadata is
					#        prefixed with the shared
					#        metadata, so we need to
					#        process the first two comments
					#        in case both contain metadata
					#        directives...
					#
					if( defined( $firstcomment ) or ( ( 'procedure' eq $mode ) and defined( $secondcomment ) ) ) {
						my $restorefile = undef;

						foreach my $line ( @{ $statement -> { 'entry' } } ) {
							my $requiredengine;

							chomp( $line );
							if( $line =~ m/Description:\s+(.*)\s*$/i ) {
								$schmdescription = $1;
								pdebug( "Read metadata description '$schmdescription'" );
							} elsif( $line =~ m/Engine:\s+([^#]+)(?:#.*)?\s*$/i ) {
								my $requiredengine = $1;
								pdebug( "Read metadata engine requirement '$requiredengine' (database engine is '$engine')" );
								if( not( lc( $engine ) eq lc( $requiredengine ) ) ) {
									pwarn( "Metadata from file '$schmfile' requires database engine '$requiredengine' but the current connection reports a '$engine' back-end\n", undef, TRUE );
									return( FALSE );
								}
							} elsif( $line =~ m/Database:\s+([^\s]+)\s*/i ) {
								pdebug( "Read required database name '$1'" );
								if( not( lc( $db ) eq lc( $1 ) ) ) {
									pwarn( "Metadata from file '$schmfile' only allows deployment to database '$1', but current database is '$db'\n", undef, TRUE );
									return( FALSE );
								}
							} elsif( $line =~ m/Schema:\s+([^\s]+)\s*/i ) {
								pdebug( "Read required schema name '$1'" );
								if( not( defined( $requiredengine ) and ( 'vertica' eq lc( $requiredengine ) ) ) ) {
									pwarn( "The 'Schema' directive from file '$schmfile' is only valid when 'Engine' is specified and declared to be \"vertica\"\n", undef, TRUE );
									return( FALSE );
								}
								if( not( lc( $vschm ) eq lc( $1 ) ) ) {
									pwarn( "Metadata from file '$schmfile' only allows deployment to schema '$1', but current schema is '$vschm'\n", undef, TRUE );
									return( FALSE );
								}
							} elsif( $line =~ m/Previous\s+version:\s+([^\s]+)\s*/i ) {
								$schmprevious = $1;
								pdebug( "Read metadata previous version '$schmprevious'" );
							} elsif( $line =~ m/Target\s+version:\s+([^\s]+)\s*/i ) {
								$schmtarget = $1;
								pdebug( "Read metadata target version '$schmtarget'" );
							} elsif( $line =~ m/Environment:\s+([^\s]+)\s*$/i ) {
								if( defined( $1 ) and length( $1 ) ) {
									foreach my $restriction ( split( /\s*[,\s]+\s*/, $1 ) ) {
										my( $invert, $match );
										$restriction =~ m/^\s*(!)?\s*([^\s]+)\s*$/;
										$invert = $1 if( defined( $1 ) and length( $1 ) );
										$match = $2 if( defined( $2 ) and length( $2 ) );
										if( not( defined( $match ) ) ) {
											pwarn( "Metadata directive '$line' is not valid\n", undef, TRUE );
											return( FALSE );
										}
										pdebug( "Read metadata environment restriction '" . ( defined( $invert ) ? $invert : '' ) . "$match'" );
										## no critic (ProhibitConditionalDeclarations)
										$environment = '' unless( defined( $environment ) );
										if(
										  ( ( defined( $invert ) and ( $invert eq '!' ) ) and ( lc( $match ) eq lc( $environment ) ) )
										  or
										  ( not( defined( $invert ) and ( $invert eq '!' ) ) and not( lc( $match ) eq lc( $environment ) ) )
										) {
											if( '' eq $environment ) {
												pwarn( "Metadata from file '$schmfile' only allows execution in environment '$match'\n", undef, TRUE );
											} else {
												pwarn( "Metadata from file '$schmfile' prohibits execution in environment '$environment'\n", undef, TRUE );
											}
											# Don't fail, as we could have multiple files for different
											# environments, all of which but one drop-out here...
											return( TRUE );
										}
										pdebug( "Validated metadata environment restriction '" . ( defined( $invert ) ? $invert : '' ) . "$match'" );
									}
								}
							} elsif( $line =~ m/Restore:\s+([^#]+)(?:#.*)?\s*$/i ) {
								$restorefile = $1;
								pdebug( "Read metadata file-restore request for '$restorefile'" );
							}
						}

						if( defined( $schmprevious ) and not( ( $schmprevious =~ m/[\d.]+/ ) or ( $schmprevious =~ m#(?:na|n/a)#i ) ) ) {
							pnote( "Read dubious prior version '$schmprevious' from file '$schmfile'\n", undef, TRUE );
							$schmprevious = undef;
						}
						if( defined( $schmtarget ) and not( $schmtarget =~ m/[\d.]+/ ) ) {
							pnote( "Read dubious target version '$schmtarget' from file '$schmfile'\n", undef, TRUE );
							$schmtarget = undef;
						}

						#
						# Validate metadata version against file version and optional # {{{
						# specified target limit...
						#

						if( defined( $schminitversion ) and length( $schminitversion ) ) {
							# If we have an optional --init argument, check this against
							# the filename version (if set) and the metadata version...
							if( defined( $schmtarget ) ) {
								if( not( $schminitversion =~ m/^$schmtarget$/ ) ) {
									pwarn( "Baseline version '$schminitversion' differs from metadata version '$schmtarget' from file '$schmfile'\n", undef, TRUE );
								}
							}
							if( defined( $schmfileversion ) and length( $schmfileversion ) ) {
								if( not( $schminitversion =~ m/^$schmfileversion$/ ) ) {
									pwarn( "Baseline version '$schminitversion' differs from filename version '$schmfileversion' from file '$schmfile'\n", undef, TRUE );
								} elsif( defined( $schmtarget ) ) { # not( defined( $schmfileversion ) and length( $schmfileversion ) )
									pdebug( "Filename '$schmfile' lacks recognisable version component - not validating against baseline version '$schminitversion' or target verison '$schmtarget'\n" );
								}
							}
						} else { # not( defined( $schminitversion ) and length( $schminitversion ) )
							# --init was not specified, or had no value provided, so check
							# the filename version (if set) against the metadata version...
							if( defined( $schmtarget ) ) {
								if( defined( $schmfileversion ) and length( $schmfileversion ) ) {
									if( not( $schmfileversion =~ m/^$schmtarget$/ ) ) {
										pwarn( "Filename version '$schmfileversion' differs from metadata version '$schmtarget' from file '$schmfile'\n", undef, TRUE );
									}
								} else { # not( defined( $schmfileversion ) and length( $schmfileversion ) )
									pdebug( "Filename '$schmfile' lacks recognisable version component - not validating against target version '$schmtarget'\n" );

								}
							} else { # not( defined( $schmtarget ) )
								if( defined( $schmfileversion ) and length( $schmfileversion ) ) {
									pdebug( "Using filename version '$schmfileversion' from file '$schmfile' due to lack of metadata\n", undef, TRUE );
								} else {
									pfatal( "Could not discover target version from file metadata, filename '$schmfile', or command-line arguments\n" );
									return( FALSE );
								}
							}
						}

						if( defined( $schmtarget ) ) {
							if( defined( $schmfileversion ) and length( $schmfileversion ) ) {
								my( $previous, $target );
								if( $fileismigrationschema -> ( $schmfile, \$previous, \$target ) ) {
									if( not( ( defined( $schmprevious ) and ( $schmprevious eq $previous ) ) and ( $schmtarget eq $target ) ) ) {
										pwarn( "Migration schema filename declares transition from version '$previous' to version '$target', but file metadata describes the transition from '" . ( defined( $schmprevious ) ? "$schmprevious" : '<not specified>' ) . "' to '$schmtarget'\n", undef, TRUE );
									}
								}
							}
							if( not( $schmversion =~ m/^$schmtarget$/ ) ) {
								pwarn( "Current target version '$schmversion' differs from metadata target version '$schmtarget'\n", undef, TRUE ) unless( 'procedure' eq $mode );
							}
						} # defined( $schmtarget )

						if( defined( $schminitversion ) and length( $schminitversion ) ) {
							# Already set
							pdebug( "Using baseline version '$schmversion' as target, overriding any metadata and filename versions\n" );
						} elsif( defined( $schmtarget ) and length( $schmtarget ) ) {
							$schmversion = $schmtarget;
							pdebug( "Using metadata target version '$schmversion' as target, overriding any filename version\n" );
						} elsif( defined( $schmfileversion ) and length( $schmfileversion ) ) {
							# Already set
							pwarn( "Using filename version '$schmversion' as target, due to lack of valid metadata version\n" );
						} elsif( looks_like_number( $schmversion ) and ( $schmversion < 0 ) ) {
							pwarn( "Using synthesised version '$schmversion' as target, due to lack of valid metadata or filename version\n", LOGMAX );
						}

						if( defined( $limit ) and not( 'procedure' eq $mode ) ) {
							my( $scode, $schange, $sstep, $shotfix, $sother ) = ( $schmversion =~ m/^([[:xdigit:]]+)(?:\.(\d+)(?:\.(\d+)(?:\.(\d+))?)?)?(.*?)$/ );
							my( $lcode, $lchange, $lstep, $lhotfix, $lother ) = ( $limit =~ m/^([[:xdigit:]]+)(?:\.(\d+)(?:\.(\d+)(?:\.(\d+))?)?)?(.*?)$/ );

							if( not( defined( $scode ) and ( ( 0 == $scode ) or $scode ) ) ) {
								pwarn( "Could not determine major version number from target version '$schmversion'\n", undef, TRUE );
								$scode = 0;
							}
							if( defined( $sother ) and length( $sother ) ) {
								pwarn( "Target version '$schmversion' contains ignored element(s) '$sother'\n", undef, TRUE );
							}
							if( not( defined( $lcode ) and ( ( 0 == $lcode ) or $lcode ) ) ) {
								pwarn( "Could not determine major version number from specified limit '$limit'\n", undef, TRUE );
								$lcode = 0;
							}
							if( defined( $lother ) and length( $lother ) ) {
								pwarn( "Limit '$limit' contains ignored element(s) '$lother'\n", undef, TRUE );
							}

							$schange = 0 unless( defined( $schange ) and $schange );
							$sstep = 0 unless( defined( $sstep ) and $sstep );
							$shotfix = 0 unless( defined( $shotfix ) and $shotfix );
							$lchange = 0 unless( defined( $lchange ) and $lchange );
							$lstep = 0 unless( defined( $lstep ) and $lstep );
							$lhotfix = 0 unless( defined( $lhotfix ) and $lhotfix );

							my $sv = "$scode.$schange.$sstep.$shotfix";
							my $lv = "$lcode.$lchange.$lstep.$lhotfix";

							( my $sst = $sv ) =~ s/(?:\.0+)+$//;
							( my $sl = $lv ) =~ s/(?:\.0+)+$//;

							if( defined( $sst ) and defined ( $sl ) and not( $sst eq $sl ) ) {
								my @sortedversions = sort { versioncmp( $a, $b ) } ( $sst, $sl );
								my $latest = pop( @sortedversions );
								if( $latest eq $sst ) {
									if( $pretend ) {
										if( $force ) {
											pwarn( "Schema target version '$schmversion' is higher than specified target limit '$limit' for file '$schmfile' - would forcibly re-apply ...\n", undef, TRUE );
										} else {
											pwarn( "Schema target version '$schmversion' is higher than specified target limit '$limit' for file '$schmfile' - skipping ...\n", undef, TRUE );
											return( TRUE );
										}
									} else { # not( $pretend )
										if( $force ) {
											pwarn( "Schema target version '$schmversion' is higher than specified target limit '$limit' for file '$schmfile' - forcibly re-applying ...\n", undef, TRUE );
										} else {
											pwarn( "Schema target version '$schmversion' is higher than specified target limit '$limit' for file '$schmfile' - skipping ...\n\n", undef, TRUE );
											return( TRUE );
										}
									}
								}
							}
						} # }}}

						#die( "Aborting to prevent application of temporary version number '$schmversion' [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) if( looks_like_number( $schmversion ) and ( $schmversion < 0 ) );
						if( defined( $restorefile ) and length( $restorefile ) ) {
							if( not( defined( $schminitversion ) ) ) {
								pwarn( "'Restore' directive is only valid in a baseline file - ignoring ...\n", undef, TRUE ) unless( $first );
							} else {
								pnote( "Found restoration directive for file '$restorefile' ...\n", undef, TRUE );

								# $restorefile may be relative or absolute...
								if( -s ( realpath( "$schmpath/$restorefile" ) or realpath( $restorefile ) ) ) {
									if( -s realpath( "$schmpath/$restorefile" ) ) {
										$restorefile = realpath( "$schmpath/$restorefile" );
										pnote( "Using file location '$restorefile' ...\n", undef, TRUE );
									} else {
										$restorefile = realpath( $restorefile );
									}
								}
								if( not( -s $restorefile ) ) {
									if( not( $pretend ) ) {
										die( "$fatal Cannot locate file '$restorefile' from root or '$schmpath' directories\n" );
									} else {
										pwarn( "Could not locate file '$restorefile' from root or '$schmpath' directories, would abort\n", undef, TRUE );
									}
								} else {
									if( $pretend ) {
										# At this point, a fresh database will have metadata tracking tables
										# but no actual metadata, whereas an existing database (which has
										# accidentally had restore called upon it?) should be populated...
										if( $safetorestore ) {
											psim( "Would restore database from file '$restorefile' ...\n", undef, TRUE );
											return( \$schmversion );
										} else {
											if( $force ) {
												pwarn( "Would forcibly restore database from file '$restorefile', with risk of data-loss ...\n", LOGMAX, TRUE );
												return( TRUE );
											} else {
												pwarn( "Would refuse to overwrite populated database from file '$restorefile' to prevent data-loss\n" );
												return( FALSE );
											}
										}
									} else {
										if( not( $safetorestore ) ) {
											if( $force ) {
												pwarn( "Forcibly restoring database from file '$restorefile', with risk of data-loss ...\n", LOGMAX, TRUE );
											} else {
												pwarn( "Refusing to overwrite populated database from file '$restorefile' to prevent data-loss\n", LOGMAX, TRUE );
												return( FALSE );
											}
										}

										dbrestore( $auth, $restorefile, $progress );

										pdebug( "Connecting to database `$db` to migrate metadata if necessary and to determine the new schema version ...\n", undef, TRUE ) unless( $quiet or $silent );

										my $dbh;
										my $error = dbopen( \$dbh, $dsn, $user, $pass, $strict );
										{
											die( "$fatal Migrating version: $error [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) if $error;

											metadatamigrateschema( \$dbh, $db, $vschm, $variables );

											my $tableexists;
											if( 'vertica' eq $engine ) {
												$tableexists = sqlgetvalue( \$dbh, "SELECT COUNT(*) FROM `tables` WHERE `table_schema` = '$vschm' AND `table_name` = '$flywaytablename'" );
											} else {
												$tableexists = sqlgetvalue( \$dbh, "SELECT COUNT(*) FROM `information_schema`.`TABLES` WHERE `TABLE_SCHEMA` = '$db' AND `TABLE_NAME` = '$flywaytablename'" );
											}

											if( not( defined( $tableexists ) and $tableexists ) ) {
												pwarn( "Restored data appears to lack metadata tables - further migrations may fail\n", undef, TRUE );
												dbclose( \$dbh, undef, undef, TRUE );
												return( \$schmversion );

											} else {
												my $init = sqlgetvalue( \$dbh, "SELECT COUNT(*) FROM `$verticadb$flywaytablename` WHERE `success` IS TRUE" );
												if( not( defined( $init ) ) or ( 0 == $init ) ) {
													pwarn( "Restored data appears to lack metadata content - further migrations may fail\n", undef, TRUE );
													dbclose( \$dbh, undef, undef, TRUE );
													return( \$schmversion );

												} else {
													my $versions = sqlgetvalues( \$dbh, "SELECT DISTINCT `version` FROM `$verticadb$flywaytablename` WHERE `success` IS TRUE" );

													dbclose( \$dbh, undef, undef, TRUE );

													if( not( scalar( @{ $versions } ) ) ) {
														pwarn( "Restored data appears to lack metadata content - further migrations may fail\n", undef, TRUE );
														return( \$schmversion );
													}

													my @sortedversions = sort { versioncmp( $a, $b ) } @{ $versions };
													my $version = pop( @sortedversions );
													return( \$version );
												}
											}
										}

										# Unreachable
										dbclose( \$dbh, undef, undef, TRUE );

										die( 'LOGIC: Line ' . __LINE__ . " should never be reached  [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
									}
								}
							}
						}

						if( defined( $firstcomment ) ) {
							my $oldstatus = $status;
							my $greaterversionpresent = FALSE;

							my $tablename = $flywaytablename;
							my $statuscolumn = 'success';
							$status = '1';

							if( 'procedure' eq $mode ) {
								$tablename = $mywayprocsname;
								$statuscolumn = 'status';
								# For this to work, we /have/ to assume that filenames are consistent and
								# that they are always named '<function_name>.sql'...
								#
								$status = "'$status' AND filename = '$schmfile'";
							}

							my $installedversions;

							if( not( $quiet or $silent ) ) {
								pdebug( "\n" );
								pdebug( "Connecting to database `$db` to perform validation ...\n", undef, TRUE );
							}
							$dsn = "DBI:mysql:database=$db;host=$host;port=$port" unless( defined( $dsn ) );
							my $dbh;
							my $error = dbopen( \$dbh, $dsn, $user, $pass, $strict );
							{
								die( "$fatal Validating: $error [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) if $error;

								# We could sort in the database here, but I'm not sure "ORDER BY" would cope with the
								# various variations we might be trying to throw at it...
								#
								$installedversions = sqlgetvalues( \$dbh, "SELECT DISTINCT `version` FROM `$verticadb$tablename` WHERE `$statuscolumn` = $status" );
							}
							dbclose( \$dbh, undef, undef, TRUE );

							if( looks_like_number( $schmversion ) and ( $schmversion < 0 ) ) {
								pwarn( "Not validating installation chain for synthesised target version '$schmversion' assigned to file '$schmfile'\n", undef, TRUE );
							} elsif( defined( $tablename ) and not( qr/^$tablename$/ |M| \@{ $availabletables } ) ) {
								pwarn( "Metadata table `$tablename` does not exist - not validating target installation chain for file '$schmfile'\n", undef, TRUE );

							} else {
								# N.B. Logic change - previously, we were simply checking that the target version
								#      hadn't been applied to the database.  Now, we're checking that nothing
								#      newer than the target has been applied either.
								#
								my( $codeversion, $changeversion, $stepversion, $hotfixversion, $otherversion ) = ( $schmversion =~ m/^([[:xdigit:]]+)(?:\.(\d+)(?:\.(\d+)(?:\.(\d+))?)?)?(.*?)$/ );

								my $okay = TRUE;
								my $fresh = TRUE;

								if( scalar( @{ $installedversions } ) ) {
									if( qr/^$schmversion$/ |M| $installedversions ) {
										if( not( $first ) and ( 'procedure' eq $mode ) ) {
											# Duplicate installs are the norm for Stored Procedure installations, as each
											# definition is applied with the same metadata version.
											#
											$fresh = FALSE;
										} else {
											if( $pretend ) {
												if( $force or ( 'procedure' eq $mode ) ) {
													pwarn( '' . ( ( 'procedure' eq $mode ) ? 'Stored Procedure' : 'Schema' ) . " target version '$schmversion' has already been applied to this database - would forcibly re-apply ...\n", undef, TRUE );
												} else {
													pwarn( '' . ( ( 'procedure' eq $mode ) ? 'Stored Procedure' : 'Schema' ) . " target version '$schmversion' has already been applied to this database - skipping ...\n" , undef, TRUE);
													return( TRUE );
												}
											} else { # not( $pretend )
												if( $force or ( 'procedure' eq $mode ) ) {
													pwarn( '' . ( ( 'procedure' eq $mode ) ? 'Stored Procedure' : 'Schema' ) . " target version '$schmversion' has already been applied to this database - forcibly re-applying ...\n", undef, TRUE );
												} else {
													pwarn( "Schema target version '$schmversion' has already been applied to this database - skipping ...\n", undef, TRUE ) unless( $quiet or $silent );
													return( TRUE );
												}
											}
											$okay = FALSE;
											$fresh = FALSE;
										}
									} else {
										pdebug( "Schema target version '$schmversion' does not currently exist in database metadata ...\n" );

										my @sortedversions = sort { versioncmp( $a, $b ) } ( @{ $installedversions }, $schmversion );
										my $latest = pop( @sortedversions );
										if( $latest eq $schmversion ) {
											pdebug( "Schema target version '$schmversion' correctly falls after the most recently-applied schema version ...\n" );
										} else {
											# Our new version is not top of the list...

											my $updatemeta = FALSE;

											if( $pretend ) {
												if( $force or ( 'procedure' eq $mode ) ) {
													pwarn( '' . ( ( 'procedure' eq $mode ) ? 'Stored Procedure' : 'Schema' ) . " target version '$schmversion' is behind the highest successfully applied version '$latest' - would forcibly re-apply ...\n", undef, TRUE );
												} else {
													pwarn( '' . ( ( 'procedure' eq $mode ) ? 'Stored Procedure' : 'Schema' ) . " target version '$schmversion' is behind the highest successfully applied version '$latest' - skipping ...\n" , undef, TRUE);
													$updatemeta = TRUE;
												}
											} else { # not( $pretend )
												if( $force or ( 'procedure' eq $mode ) ) {
													pwarn( '' . ( ( 'procedure' eq $mode ) ? 'Stored Procedure' : 'Schema' ) . " target version '$schmversion' is behind the highest successfully applied version '$latest' - forcibly re-applying ...\n", undef, TRUE );
												} else {
													pwarn( "Schema target version '$schmversion' is behind the highest successfully applied version '$latest' - skipping ...\n", undef, TRUE ) unless( $quiet or $silent );
													$updatemeta = TRUE;
												}
											}

											if( $updatemeta ) {
												# Add metadata records here...

												my $variables = {};
												$variables -> { 'tablename' } =     "$verticadb$flywaytablename";
												$variables -> { 'installedrank' } =   undef;
												$variables -> { 'schmversion' } =    $schmversion;
												$variables -> { 'desc' } =           $schmdescription;
												$variables -> { 'filetype' } =       'SKIP';
												$variables -> { 'schmfile' } =       $schmfile;
												$variables -> { 'checksum' } =        undef;
												$variables -> { 'user' } =           $user;
												$variables -> { 'duration' } =        0;
												$variables -> { 'status' } =          1; # TRUE

												my $dbh;
												my $error = dbopen( \$dbh, $dsn, $user, $pass, $strict );
												{
													die( "$fatal Migration: $error [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) if $error;

													die( "$fatal Unable to write migration metadata record [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) unless( metadataupdateflywaytable( \$dbh, $db, $vschm, $pretend, TRUE, $variables ) );
												}
												dbclose( \$dbh, undef, undef, TRUE );

												return( TRUE );
											}

											$okay = FALSE;
											$fresh = FALSE;
										}
									}
								}

								# We only reach here if the file-version is new, or if
								# we're forcing re-installation.
								# XXX: ... or if we're in dry-run mode(?)

								my $latest;
								my $target;
								if( scalar( @{ $installedversions } ) ) {
									my @sortedversions = sort { versioncmp( $a, $b ) } ( @{ $installedversions } );
									$latest = pop( @sortedversions );
									@sortedversions = sort { versioncmp( $a, $b ) } ( @sortedversions, $schmversion );
									$target = pop( @sortedversions );
								}

								# The logic here is slightly non-obvious:
								#  $latest contains the highest version present
								#  and marked as successfully installed;
								#  $target will be equal to $latest if the file
								#  version is already installed, or will be
								#  equal to the file version if this is new.
								# Therefore, if the (inverse) condition below
								# evaluates to true then we've moved on.
								# $latest is used purely to track the installed
								# version.

								if( defined( $latest ) and defined( $target ) and not( $target eq $schmversion ) ) {
									# Latest installed version is beyond file version,
									# but we're in dry-run mode or forcing installation.

									if( $pretend ) {
										if( $force ) {
											pwarn( "Existing " . ( ( 'procedure' eq $mode ) ? 'Stored Procedure' : 'Schema' ) . " version '$latest' is greater than target '$schmversion'" . ( ( $quiet and ( 'procedure' eq $mode ) ) ? " for file '$schmfile'" : '' ) . ", and has already been applied to this database - would forcibly re-apply ...\n", undef, TRUE );
										} else {
											pwarn( "Existing " . ( ( 'procedure' eq $mode ) ? 'Stored Procedure' : 'Schema' ) . " version '$latest' is greater than target '$schmversion'" . ( ( $quiet and ( 'procedure' eq $mode ) ) ? " for file '$schmfile'" : '' ) . ", and has already been applied to this database - skipping ...\n", undef, TRUE );
											return( TRUE );
										}
									} else { # not( $pretend )
										if( $force ) {
											pwarn( "Existing " . ( ( 'procedure' eq $mode ) ? 'Stored Procedure' : 'Schema' ) . " version '$latest' is greater than target '$schmversion'" . ( ( $quiet and ( 'procedure' eq $mode ) ) ? " for file '$schmfile'" : '' ) . ", and has already been applied to this database - forcibly re-applying ...\n", undef, TRUE );
										} else {
											pwarn( "Existing " . ( ( 'procedure' eq $mode ) ? 'Stored Procedure' : 'Schema' ) . " version '$latest' is greater than target '$schmversion'" . ( ( $quiet and ( 'procedure' eq $mode ) ) ? " for file '$schmfile'" : '' ) . ", and has already been applied to this database - skipping ...\n", undef, TRUE ) unless( $quiet or $silent );
											return( TRUE );
										}
									}
									$greaterversionpresent = TRUE;
									$okay = FALSE;
								} elsif( defined( $hotfixversion ) and not( $hotfixversion =~ m/0+/ ) ) {
									# For this to work for Stored Procedures, we /have/
									# to assume that filenames are consistent and that
									# they are always named '<function_name>.sql'...
									#
									if( defined( $latest ) and ( $latest eq $schmversion ) ) {
										if( $pretend ) {
											if ( $force ) {
												pwarn( '' . ( ( 'procedure' eq $mode ) ? 'Stored Procedure' : 'Schema' ) . " hot-fix version '$schmversion'" . ( ( $quiet and ( 'procedure' eq $mode ) ) ? " for file '$schmfile'" : '' ) . " is already present - would forcibly re-apply ...\n", undef, TRUE );
											} else {
												pwarn( '' . ( ( 'procedure' eq $mode ) ? 'Stored Procedure' : 'Schema' ) . " hot-fix version '$schmversion'" . ( ( $quiet and ( 'procedure' eq $mode ) ) ? " for file '$schmfile'" : '' ) . " is already present - skipping ...\n", undef, TRUE );
												return( TRUE );
											}
										} else { # not( $pretend )
											if ( $force ) {
												pwarn( '' . ( ( 'procedure' eq $mode ) ? 'Stored Procedure' : 'Schema' ) . " hot-fix version '$schmversion'" . ( ( $quiet and ( 'procedure' eq $mode ) ) ? " for file '$schmfile'" : '' ) . " is already present - forcibly re-applying ...\n", undef, TRUE );
											} else {
												pwarn( '' . ( ( 'procedure' eq $mode ) ? 'Stored Procedure' : 'Schema' ) . " hot-fix version '$schmversion'" . ( ( $quiet and ( 'procedure' eq $mode ) ) ? " for file '$schmfile'" : '' ) . " is already present - skipping ...\n", undef, TRUE );
												return( TRUE );
											}
										}
										$okay = FALSE;
									} else {
										if( $pretend ) {
											pwarn( '' . ( ( 'procedure' eq $mode ) ? 'Stored Procedure' : 'Schema' ) . " hot-fix version '$schmversion' would be applied" . ( defined( $latest ) ? " over existing version '$latest'" : '' ) . ( ( $quiet and ( 'procedure' eq $mode ) ) ? " for file '$schmfile'" : '' ) . " ...\n", undef, TRUE );
										} else {
											pwarn( '' . ( ( 'procedure' eq $mode ) ? 'Stored Procedure' : 'Schema' ) . " hot-fix version '$schmversion' will be applied" . ( defined( $latest ) ? " over existing version '$latest'" : '' ) . ( ( $quiet and ( 'procedure' eq $mode ) ) ? " for file '$schmfile'" : '' ) . " ...\n", undef, TRUE );
										}
									}
								} elsif( $fresh ) { # and ( $first )
									pnote( '' . ( ( 'procedure' eq $mode ) ? 'Stored Procedure' : 'Schema' ) . " version '$schmversion'" . ( ( not( $quiet ) and ( 'procedure' eq $mode ) ) ? " from file '$schmfile'" : '' ) . " is a fresh install\n", undef, TRUE ) unless( $silent );
								} elsif( $first ) {
									pnote( '' . ( ( 'procedure' eq $mode ) ? 'Stored Procedure' : 'Schema' ) . " version '$schmversion'" . ( ( not( $quiet ) and ( 'procedure' eq $mode ) ) ? " from file '$schmfile'" : '' ) . " is a re-install\n", undef, TRUE ) unless( $silent );
								}
							}

							if( not( defined( $schmprevious ) ) or ( $schmprevious =~ m#(?:na|n/a)#i ) ) {
								pnote( "No previous version defined - not validating previous installation chain\n", undef, TRUE ) unless( $quiet or $silent );
								#die( "Cannot validate previous versions [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) unless( ( 'procedure' eq $mode ) or ( $schmprevious =~ m#(?:na|n/a)#i ) );

							} elsif( defined( $tablename ) and not( qr/^$tablename$/ |M| \@{ $availabletables } ) ) {
								pwarn( "Metadata table `$tablename` does not exist - not validating previous installation chain\n", undef, TRUE );
								#die( "Cannot validate previous versions [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );

							} else {
								# Ensure that we only consider successfully-applied versions (via $status)...
								#
								# N.B.: $status is pre-quoted
								#
								push( @{ $installedversions }, $schmvirtual ) if( defined( $schmvirtual ) );

								my $schmpreviousmatch = $schmprevious;
								if( $schmprevious =~ m/(?:\.0+)+$/ ) {
									$schmpreviousmatch =~ s/(\.0+)+$//;
									$schmpreviousmatch .= qr/(?:\.0+)+/;
								}

								if( $greaterversionpresent ) {
									pnote( "Not validating previous versions as target version has already been exceeded for file '$schmfile'\n", undef, TRUE );
								} else {

									if( scalar( @{ $installedversions } ) and defined( $schmpreviousmatch ) and ( qr/^$schmpreviousmatch$/ |M| $installedversions ) ) {
										if( 'procedure' eq $mode ) {
											pdebug( "Prior Stored Procedure definitions '$schmprevious' exist in myway metadata" );
										} else {
											pdebug( "Prior schema version '$schmprevious' correctly exists in flyway metadata" );
										}
									} else {
										if( 'procedure' eq $mode ) {
											# Stored Procedures should be entirely self-contained, so we can apply any
											# future version at any point, regardless of what is already present (with
											# and cleanup/migration performed by metadata commands).  Therefore, lacking
											# a previous version is not an issue, but we shouldn't allow installation of
											# older and duplicate definitions without '--force'.
											#
											pnote( "Prior Stored Procedure definitions '$schmprevious' have not been applied to this database\n", undef, TRUE );
										} else {
											if( $pretend ) {
												if( $force ) {
													pwarn( "Prior schema version '$schmprevious' has not been applied to this database - would forcibly apply ...\n", undef, TRUE );
												} else {
													pwarn( "Prior schema version '$schmprevious' has not been applied to this database - would abort.\n", undef, TRUE );
												}
											} else {
												if( $force ) {
													pwarn( "Prior schema version '$schmprevious' has not been applied to this database - forcibly applying ...\n", undef, TRUE );
												} else {
													die( "$fatal Prior schema version '$schmprevious' (required by '$schmfile') has not been applied to this database - aborting.\n" );
												}
											}
										} # not( 'procedure' eq $mode )
									} # not( scalar( @{ $installedversions } ) and defined( $schmpreviousmatch ) and ( qr/^$schmpreviousmatch$/ |M| $installedversions ) )
								} # not( $greaterversionpresent )
							} # defined( $schmprevious ) and( not( defined( $tablename ) and not( qr/^$tablename$/ |M| \@{ $availabletables } ) ) )

							$schmprevious = undef if( defined( $schmprevious ) and ( $schmprevious =~ m#(?:na|n/a)#i ) );

							$status = $oldstatus;
						} # defined( $firstcomment )

						if( defined( $firstcomment ) or ( ( 'procedure' eq $mode ) and defined( $secondcomment ) ) ) {
							if( defined( $firstcomment ) ) {
								$firstcomment = undef;
								$secondcomment = TRUE;
							} else {
								$secondcomment = undef;
							}
						}

						if( defined( $schmdescription ) and length( $schmdescription ) ) {
							if( defined( $desc ) ) {
								pnote( "Updating schema description from '$desc' to '$schmdescription'\n", undef, TRUE ) unless( $quiet or $silent );
							} else {
								pnote( "Updating schema description to '$schmdescription'\n", undef, TRUE ) unless( $quiet or $silent );
							}
							$desc = $schmdescription;
						}

						print( "\n" ) if( $verbosity and not( $quiet or $silent ) ); # debug(3), notice(2), warn(1)
					} # not( defined( $firstcomment ) or ( ( 'procedure' eq $mode ) and defined( $secondcomment ) ) )
				}
			}
		}
	} # }}}

	#
	# Open target database connection # {{{
	#

	if( not( $quiet or $silent ) ) {
		pdebug( "\n" );
		pdebug( "Connecting to database `$db` to migrate schema ...\n", undef, TRUE );
	}

	$dsn = "DBI:mysql:database=$db;host=$host;port=$port" unless( defined( $dsn ) );
	$error = dbopen( \$dbh, $dsn, $user, $pass, $strict );
	{
		die( "$fatal Application: $error [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) if $error;

		#
		# Populate tracking tables, if necessary # {{{
		#
		# $schmversion will be the metadata target version or a large negative
		# number.
		#

		my $installedrank;

		print( "\n" ) unless( defined( $schminitversion ) or $quiet or $silent );


		# We finally know that Stored Procedures are safe to apply!
		if( 'procedure' eq $mode ) {
			if( defined( $mywayprocsname ) and ( qr/^$mywayprocsname$/ |M| \@{ $availabletables } ) ) {
				my $uid = ( $ENV{ LOGNAME } or $ENV{ USER } or getpwuid( $< ) );
				my $oshost = qx( hostname -f );
				my $sum = qx( sha1sum \"$file\" );
				chomp( $oshost );
				chomp( $sum );
				$sum =~ s/\s+.*$//;

				my $sth = sqlprepare( \$dbh, <<SQL );
INSERT INTO `$mywayprocsname` (
  `id`
, `dbuser`
, `dbhost`
, `osuser`
, `host`
, `sha1sum`
, `path`
, `filename`
, `started`
) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, SYSDATE() )
SQL
# Currently unchanged values: `version`, `description`, `type`, `sqlstarted`, `finished`, `status`.
				die( "$fatal Unable to create tracking statement handle" . ( defined( $sth -> errstr() ) ? ': ' . $sth -> errstr() : '' ) . ' [' . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) unless( defined( $sth ) and $sth );
				if( not( $pretend ) ) {
					pdebug( "Inserting entry to `$mywayprocsname` for file '$schmfile'" );
					sqlexecute( \$dbh, $sth, undef,
						   $uuid
						,  $user
						,  $host
						,  $uid
						,  $oshost
						,  $sum
						,  $schmpath
						,  $schmfile

					);
				}
				$sth -> finish();
			}
		} else { # not( 'procedure' eq $mode )
			if( defined( $flywaytablename ) and not( qr/^$flywaytablename$/ |M| \@{ $availabletables } ) ) {
				pwarn( "Examined " . scalar( @{ $availabletables } ) . " tables:\n", undef, TRUE );
				print Data::Dumper -> Dump( [ $availabletables ], [ qw( *availabletables ) ] );
				if( $pretend ) {
					pwarn( "flyway metadata table `$flywaytablename` does not exist" . ( ( 'vertica' eq $engine ) ? " in schema '$vschm'" : '' ) . ".\n", undef, TRUE );
				} else {
					die( "$fatal flyway metadata table `$flywaytablename` does not exist" . ( ( 'vertica' eq $engine ) ? " in schema '$vschm'" : '' ) . ' [' . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
				}
			} else { # ( qr/^$flywaytablename$/ |M| \@{ $availabletables } )
				#
				# Write 'init' entry, if not already present # {{{
				#

				my $init = sqlgetvalue( \$dbh, "SELECT COUNT(*) FROM `$verticadb$flywaytablename` WHERE `success` IS TRUE" );
				if( defined( $init ) and ( 0 != $init ) ) {
					if( defined( $schminitversion ) ) {
						# FIXME: Later on, we decide that we don't trust the database to order arbitrary
						#        versions correctly, and so perform the operation manually ourselves.
						#        This should at least be made consistent...
						#
						my $versions = sqlgetvalues( \$dbh, "SELECT DISTINCT `version` FROM `$verticadb$flywaytablename` WHERE `success` IS TRUE" );
						if( scalar( @{ $versions } ) ) {
							my @sortedversions = sort { versioncmp( $a, $b ) } @{ $versions };
							my $version = pop( @sortedversions );
							pnote( "\n" );
							pnote( "flyway metadata table `$flywaytablename` is already initialised to version '$version'.\n", undef, TRUE );
							if( $force ) {
								if( $pretend ) {
									psim( "Would force re-initialisation to version '$schmversion'.\n", undef, TRUE );
								} else {
									pnote( "Forcing re-initialisation to version '$schmversion' ...\n", undef, TRUE );
									$init = 0;
								}
							} else {
								if( $pretend ) {
									pwarn( "Database `$db` has already been initialised to version '$version' - please use '--clear-metadata' to discard.\n", undef, TRUE );
								} else {
									#    "INFO:   xxx" - to match stdlib.sh widths from applyschema.sh
									die( "$info Database `$db` has already been initialised to version '$version' - please use '--clear-metadata' to discard.\n" );
								}
							}
						}
					}

				}
				# If force is active, we may have reset $init to 0...
				if( not( defined( $init ) ) or ( 0 == $init ) ) {
					$installedrank = sqlgetvalue( \$dbh, "SELECT MAX(`installed_rank`) FROM `$verticadb$flywaytablename`" );
					if( defined( $installedrank ) and $installedrank =~ m/^\d+$/ and $installedrank >= 0 ) {
						$installedrank++;
					} else {
						$installedrank = 0;
					}

					my $replacement;
					if( defined( $schmversion ) ) {
						$replacement = sqlgetvalue( \$dbh, "SELECT COUNT(*) FROM `$verticadb$flywaytablename` WHERE `version` = '$schmversion'" );
					}

					# Since flyway uses `version` alone as the primary key, we have no choice but to
					# over-write and previous data of the same version if the metadata is
					# re-initialised.  The myway metadata will still store a record of previous
					# actions, however.  The explicit version is not stored, but this can be found
					# by examining the file which was loaded.
					#
					# FIXME: There is no explicit link between a flyway entry and myway data.
					#
					my $variables = {};
					$variables -> { 'tablename' } =      "$verticadb$flywaytablename";
					$variables -> { 'installedrank' } =   $installedrank;
					$variables -> { 'schmversion' } =     $schmversion;
					$variables -> { 'desc' } =            $flywayinitdesc; # ( $schmdescription or $flywayinitdesc ) # <- We don't know this yet :(
					$variables -> { 'filetype' } =        $flywayinit;
					$variables -> { 'schmfile' } =      ( $schmfile or $flywayinitdesc );
					$variables -> { 'checksum' } =         undef;
					$variables -> { 'user' } =            $user;
					$variables -> { 'duration' } =         0;
					$variables -> { 'status' } =           1; # TRUE

					die( "$fatal Unable to write $flywayinit metadata record [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) unless( metadataupdateflywaytable( \$dbh, $db, $vschm, $pretend, ( defined( $replacement ) and $replacement =~ m/^\d+$/ and 0 == $replacement ), $variables ) );

					if( not( $pretend or $quiet or $silent ) ) {
						pnote( "\n" );
						pnote( "flyway metadata table `$flywaytablename` has been initialised to version '$schmversion':\n", undef, TRUE );
						# XXX: Elsewhere, we're sorting in-code to ensure
						#      correct ordering (e.g. 0.40 > 0.5) - but
						#      formatastable() takes a SQL statement as an
						#      argument, and MySQL lacks any version-sort
						#      function :(
						#
						# There doesn't appear to be any good in-database
						# solution... but I did see one suggestion of
						# padding data to four digits separated by decimal
						# points, and then abusing INET_ATON() to perform
						# a lexical sort :o
						#
						formatastable( \$dbh, "SELECT * FROM `$verticadb$flywaytablename` ORDER BY `version` DESC LIMIT 5", '   ' );
					}
				}
				if( defined( $schminitversion ) ) {
					# If --init was specified, return now that we've created
					# the database and necessary metadata tables.
					return( \$schmversion );
				}

				# }}}

				# We need to perform a version-sort here, because we
				# need to skip all files /less than or/ equal to the
				# baseline...
				#
				# Cases: {0, 0.3}; {0.1, 0.3}; {0.3, 0.3}; {0.3, 0.5}.
				#            ^^^         ^^^         ^^^    ^^^
				#
				my $versions = sqlgetvalues( \$dbh, "SELECT DISTINCT `version` FROM `$verticadb$flywaytablename` WHERE `success` IS TRUE AND ( `type` = '$flywayinit' OR `type` = 'INIT' )" );
				my @sortedversions;
				my $version;
				if( scalar( @{ $versions } ) ) {
					@sortedversions = sort { versioncmp( $a, $b ) } @{ $versions };
					$version = pop( @sortedversions );
				}
				if( not( defined( $version ) and length( $version ) ) ) {
					if( not( $force ) ) {
						die( "$fatal Database metadata is in need of migration or has not been initialised with this tool - please re-run with '--init' and the appropriate schema-file version number.\n" );
					} else {
						pwarn( "Database metadata is in need of migration or has not been initialised with this tool - will force-apply file '$schmfile' ...\n", undef, TRUE );
					}
				} else { # defined( $version ) and length( $version ) )
					my @sortedversions = sort { versioncmp( $a, $b ) } ( $version, $schmversion );
					my $latest = pop( @sortedversions );
					my $previous = pop( @sortedversions );
					if( not( $force ) ) {
						if( ( $schmversion eq $latest ) and ( $latest eq $previous ) ) {
							pdebug( "Skipping baseline file '$schmfile' ...\n", undef, TRUE ) unless( $quiet or $silent );
							dbclose( \$dbh, undef, undef, TRUE );
							return( TRUE );
						} elsif( $schmversion eq $previous ) {
							pdebug( "Skipping pre-initialisation file '$schmfile' ...\n", undef, TRUE ) unless( $silent );
							dbclose( \$dbh, undef, undef, TRUE );
							return( TRUE );
						}
					}
				} # defined( $version ) and length( $version ) )

				my $metadataversions = sqlgetvalues( \$dbh, "SELECT DISTINCT `version` FROM `$verticadb$flywaytablename` WHERE `success` IS TRUE" );
				if( defined( $schmversion ) and ( qr/^$schmversion$/ |M| $metadataversions ) ) {
					if( $pretend ) {
						if( $force ) {
							pwarn( "Schema version '$schmversion' has already been applied to this database - would forcibly re-apply ...\n", undef, TRUE );
						} else {
							pwarn( "Schema version '$schmversion' has already been applied to this database - skipping ...\n", undef, TRUE );
							dbclose( \$dbh, undef, undef, TRUE );
							return( TRUE );
						}
					} else { # not( $force )
						if( $force ) {
							pwarn( "Schema version '$schmversion' has already been applied to this database - forcibly re-applying ...\n", undef, TRUE );
						} else {
							pwarn( "Schema version '$schmversion' has already been applied to this database - skipping ...\n\n", undef, TRUE ) unless( $quiet or $silent );
							dbclose( \$dbh, undef, undef, TRUE );
							return( TRUE );
						}
					}
				}

				$installedrank = sqlgetvalue( \$dbh, "SELECT MAX(`installed_rank`) FROM `$verticadb$flywaytablename`" );
				if( defined( $installedrank ) and $installedrank =~ m/^\d+$/ and $installedrank >= 0 ) {
					$installedrank++;
				} else {
					$installedrank = 0;
				}

				{
				my $sth = sqlprepare( \$dbh, <<SQL );
INSERT INTO `$verticadb$mywaytablename` (
`id`
, `dbuser`
, `dbhost`
, `osuser`
, `host`
, `sha1sum`
, `path`
, `filename`
, `started`
) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, SYSDATE() )
SQL
# Unchanged: `sqlstarted`, `finished`, `status`.
				die( "$fatal Unable to create tracking statement handle" . ( defined( $sth -> errstr() ) ? ': ' . $dbh -> errstr() : '' ) . ' [' . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) unless( defined( $sth ) and $sth );
				my $uid = ( $ENV{ LOGNAME } or $ENV{ USER } or getpwuid( $< ) );
				my $oshost = qx( hostname -f );
				my $sum = qx( sha1sum \"$file\" );
				chomp( $oshost );
				chomp( $sum );
				$sum =~ s/\s+.*$//;
				if( not( $pretend ) ) {
					sqlexecute( \$dbh, $sth, undef,
						   $uuid
						,  $user
						,  $host
						,  $uid
						,  $oshost
						,  $sum
						,  $schmpath
						,  $schmfile

					);
				}
				$sth -> finish();
				}
			} # ( qr/^$flywaytablename$/ |M| \@{ $availabletables } )
		} # }}}

		if( not( 'procedure' eq $mode ) ) {
			# We've already read-in all Stored Procedure data, in order to
			# perform substitutions and to insert metadata - for schema
			# files, we finally read in the full file here...
			#
			$invalid = $invalid | not( processfile( $data, $file, undef, undef, $strict ) );
			die( "$fatal '$file' failed validation - aborting [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) if( $invalid );
		}

		#
		# Confirm data validity, and enumerate tables we need to backup # {{{
		#

		my @dumptables = ();
		my $dumpusers = FALSE;

		my $output;
		if( $pretend ) {
			$output = \&pfatal;
		} else {
			if( defined( $verbosity ) and $verbosity > 0 ) { # debug(3), notice(2), warn(1)
				# Log messages as (high-priority) warnings...
				$output = \&pwarn;
			} else {
				# Only log messages if DEBUG is set...
				$output = \&pdebug;
			}
		}

		# Check for non-deterministic index names and that we actually have
		# content, which requires that we've already called processfile() against
		# the whole file...
		my $statements = 0;
		foreach my $entry ( @{ $data -> { 'entries' } } ) {
			if( not( 'HASH' eq ref( $entry ) ) ) {
				pwarn( "\$entry has unexpected type '" . ref( $entry ) . "'\n" );
				$invalid = TRUE;
			} else {
				next unless( defined( $entry -> { 'type' } ) );
				next unless( 'statement' eq lc( $entry -> { 'type' } ) );

				#print Dumper $entry if( DEBUG );

				if( not( defined( $entry -> { 'entry' } ) ) ) {
					$output -> ( 'Unable to parse blank entry' );
					$invalid = TRUE;
					next;

				} else {
					my $text;
					my $texttype = ' ';
					if( 'ARRAY' eq ref( $entry -> { 'entry' } ) ) {
						$text = join( ' ', @{ $entry -> { 'entry' } } );
						$texttype = 'array ';
					} else {
						$text = $entry -> { 'entry' };
					}
					$text =~ s/^\s+//g; $text =~ s/\s+/ /g;
					$text =~ s/([\$\@\%])/\\$1/g;
					$text = qq($text);

					# MySQL uses indices internally to represent constraints, and so has odd syntax for the creation
					# of FOREIGN KEY constraints which allows the index which equates to the constraint to be specified
					# and, although this will generally operate equivalently to naming the constraint itself, there is
					# a semantic difference here.  At the same time, it is still valid (although unlikely to be be what
					# was intended) to specify an index name (only) and so we can only issue a warning when this occurs.
					if( $text =~ m/\sADD\s+(?:CONSTRAINT\s+)?FOREIGN\s+KEY\s*([^(][^()`,\s]+`?)[()`,\s]/i ) {
						pwarn( "index_name " . ( defined( $1 ) ? $1 : '' ) . " specified in place of CONSTRAINT - this is likely a bug:\n\n$text\n", 1 );
					}

					if( ( $text =~ m/[`' (]([^`' (]+_ibfk_\d+)[) '`]/ ) and not( $text =~ m/DROP\s+FOREIGN\s+KEY\s+`[^`]+_ibfk_\d+`/i ) ) {
						pwarn( "Auto-generated constraint name `$1` used as deterministic CONSTRAINT - this usage is deprecated:\n\n$text\n", 1 );
					}

					# Ensure that constraints are explicitly named, so that we can deterministically drop them later...
					if( ( $text =~ m/\sFOREIGN\s+KEY[\s(]/i ) and not( ( $text =~ m/\sCONSTRAINT\s+`[^`]+`\s+FOREIGN\s+KEY[\s(]/i ) or ( $text =~ m/DROP\s+FOREIGN\s+KEY\s/i ) ) ) {
						$output -> ( "Unwilling to create non-deterministic constraint from:\n\n$text\n" );
						$invalid = TRUE;
					}

					if( $tokenise and not( defined( $entry -> { 'tokens' } ) ) ) {
						# FIXME: Filter known edge-cases which the Parser fails to tokenise...
						if( 'mysql' eq $engine ) {
							if( not( $text =~ m/^((LOCK|UNLOCK|SET|CREATE\s+PROCEDURE|GRANT|TRUNCATE)\s+|\s*\/\*\!)/i ) ) {
								$output -> ( 'Unable to parse ' . $texttype . 'entry "' . $text . '"' );
								# FIXME: Don't abort simply because we hit something we can't parse...
								#$invalid = TRUE;
							}
						}

						# FIXME: Reinstate this once the Parser has full coverage
						#$invalid = TRUE;

						next;
					}
				}

				if( 'mysql' eq $engine ) {
					if( defined( $entry -> { 'tokens' } -> { 'type' } ) and defined( $entry -> { 'tokens' } -> { 'object' } ) ) {
						my $type = $entry -> { 'tokens' } -> { 'type' };
						my $object = $entry -> { 'tokens' } -> { 'object' };
						if( ( defined( $type ) and ( 'create' eq lc( $type ) ) ) and ( defined( $object ) and ( 'user' eq lc( $object ) ) ) ) {
							if( defined( $tmpdir ) and -d $tmpdir ) {
								$dumpusers = TRUE unless( -e $tmpdir . '/' . 'mysql.users.sql' );
							}
						}
					}
				}

				if( not( $nobackup ) and defined( $entry -> { 'tokens' } ) ) {
					foreach my $key ( keys( $entry -> { 'tokens' } ) ) {
						if( ref( $entry -> { 'tokens' } -> { $key } ) eq 'ARRAY' ) {
							foreach my $element ( @{ $entry -> { 'tokens' } -> { $key } } ) {
								if( ref( $element ) eq 'HASH' ) {
									foreach my $basekey ( keys( %{ $element } ) ) {
										# N.B.: This should never occur when ( $mode eq 'procedure' )...
										if( $basekey eq 'tbl' ) {
											my $table = $element -> { $basekey };

											# FIXME:  For input 'INSERT INTO `table`(`column1`,`column2`) VALUES ...',
											#         Parser output still contains the specified attributes :(
											# Update: Actually, it's worse - the parser thinks this is a huge inner-join.
											# Update: Now fixed, hopefully...
											#
											#$table =~ s/\([^\)]*\)//g;
											$table =~ s/`//g;

											if( defined( $table ) and ( not( scalar( @dumptables ) and ( qr/^$table$/ |M| \@dumptables ) ) ) ) {
												pdebug( "Adding table `$table` to backup list ...\n", undef, TRUE );
												push( @dumptables, $table );
											}
										}
									}
								}
							}
						}
					}
				}

				$statements++;
			}
		}
		if( not( $statements ) ) {
			if( not( defined( $schmprevious ) and defined( $schmtarget ) ) ) {
				pwarn( "No valid SQL statements found; metadata previous and target versions are not both defined\n", undef, TRUE );

				dbclose( \$dbh, undef, undef, TRUE );
				return( TRUE );

			} elsif( $tokenise ) {
				# Still issue a warning, but don't abort here - placeholder
				# schema should be allowed to fill gaps due to reorganisation.
				#
				if( defined( $schminitversion ) or ( $fileismigrationschema -> ( $schmfile ) ) ) {
					# Don't alert - baselines and migration schema
					# aren't supposed to have content...
				} elsif( $force ) {
					pwarn( "No valid SQL statements found in file '$schmfile', but forcing continued processing for now ...\n", undef, TRUE ) unless( $silent );

				} else {
					# OTOH, we do want to fail if we read a schema-file
					# which has accidentally been completely commented-out.
					# If a version really is intended to be skipped, either
					# a placeholder should be used, or the initial and
					# target versions should cover the gap.
					pwarn( "No valid SQL statements found in file '$schmfile'\n", undef, TRUE );

					dbclose( \$dbh, undef, undef, TRUE );
					return( FALSE );
				}
			}
		}
		if( $invalid ) {
			if( $pretend ) {
				pwarn( "SQL parsing failed for file '$schmfile' - continuing with valid statements only ..." );
			} else {
				if( defined( $verbosity ) and $verbosity > 0 ) { # debug(3), notice(2), warn(1)
					die( "$fatal SQL parsing failed for file '$schmfile'\n" );
				} else {
					die( "$fatal SQL parsing failed for file '$schmfile' - please re-execute with at least '--warn' level logging to display discovered issues\n" );
				}
			}
		}

		# }}}

		#
		# Perform backups # {{{
		#

		# TODO: Backup Stored Procedures also?
		if( $statements and not( ( 'procedure' eq $mode ) or $nobackup or( defined( $schminitversion ) ) ) ) {
			if( $dumpusers ) {
				# I can't imagine that this will change any time soon,
				# but I guess it's not impossible that at some future
				# time `maria` is the system database... ?
				my $systemdb = 'mysql';

				if( $pretend ) {
					psim( "\n" );
					psim( "User alterations detected - would back-up MySQL `users` tables.\n" );
				} else {
					if( not( qr/^$systemdb$/ |M| \@{ $availabledatabases } ) ) {
						pfail( "\n" );
						pfail( "`$systemdb` database does not appear to exist.  Detected databases were:\n" );
						foreach my $database ( @{ $availabledatabases } ) {
							warn( "\t$database\n" );
						}
						die( "$fatal Aborting\n" );
					}

					#
					# Populate list of system tables, for user-backup purposes
					#

					my $availablesystemtables;

					if( not( $quiet or $silent ) ) {
						pdebug( "\n" );
						pdebug( "Connecting to system database `$systemdb` ...\n", undef, TRUE );
					}

					my $systemdbh;
					my $systemdsn = "DBI:mysql:database=$systemdb;host=$host;port=$port";
					my $systemerror = dbopen( \$systemdbh, $systemdsn, $user, $pass, $strict );
					{
						die( "$fatal Getting databases: $systemerror [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) if $systemerror;

						$availablesystemtables = sqlgetvalues( $systemdbh, "SHOW TABLES" );
						if( not( scalar( @{ $availablesystemtables } ) ) ) {
							pwarn( "\n" );
							pwarn( "Unable to retrieve list of tables for database `$systemdb`" . ( defined( $systemdbh -> errstr() ) ? ': ' . $systemdbh -> errstr() : '' ) . "\n" );
						}
					}
					dbclose( \$systemdbh, undef, undef, TRUE );

					my @usertables = ( 'columns_priv', 'procs_priv', 'proxies_priv', 'tables_priv', 'user' );
					my @backuptables;
					my $showtables = FALSE;
					foreach my $table ( @usertables ) {
						if( qr/^$table$/ |M| \@{ $availablesystemtables } ) {
							push( @backuptables, $table );
						} else {
							pwarn( "\n" );
							pwarn( "`$table` table does not appear to exist in `$systemdb` database.\n" );
							$showtables = TRUE;
						}
					}
					if( $showtables ) {
						pwarn( "Detected databases were:\n" );
						foreach my $table ( @{ $availabletables } ) {
							warn( "\t$table\n" );
						}
						die( "$fatal Aborting\n" ) if( not( @backuptables ) or ( 0 == scalar( @backuptables ) ) );
					}

					pdebug( "\n" );
					pdebug( "User alterations detected - backing-up MySQL `user` and *`_priv` tables ...\n", undef, TRUE );

					my $auth = {
						  'user'	=> $user
						, 'password'	=> $pass
						, 'host'	=> $host
						, 'database'	=> $systemdb
					};
					my $options = {
						  'skipmeta'	=> $skipmeta
						, 'extinsert'	=> $extinsert
					};
					dbdump( $auth, \@backuptables, $tmpdir, "mysql.userpriv.$uuid.sql", $options ) or die( "$fatal Database backup failed - aborting\n" );

					pdebug( "\n" );
					pdebug( "MySQL table backups completed\n", undef, TRUE );
				}
			}
			if( scalar( @dumptables ) ) {

				# We could backup everything at once, but it's likely
				# more helpful to have individual files per table...
				#
				foreach my $table ( @dumptables ) {
					if( defined( $table ) and not( qr/^$table$/ |M| \@{ $availabletables } ) ) {
						pdebug( "Referenced table `$table` has not yet been created ...\n", undef, TRUE );
					} else {
						if( $pretend ) {
							psim( "\n" );
							psim( "Would back-up table `$table`.\n" );
						} else {
							pdebug( "\n" );
							pdebug( "Backing-up table `$table`...\n", undef, TRUE );

							my $auth = {
								  'user'	=> $user
								, 'password'	=> $pass
								, 'host'	=> $host
								, 'database'	=> $db
							};
							my $options = {
								  'skipmeta'	=> $skipmeta
								, 'extinsert'	=> $extinsert
							};
							dbdump( $auth, $table, $tmpdir, "$db.$table.$uuid.sql", $options ) or die( "$fatal Database backup failed - aborting\n" );

							pdebug( "\n" );
							pdebug( "Backup of `$db`.`$table` completed with UUID '$uuid'\n", undef, TRUE );
						}
					}
				}
			}
			# Populate regardless...
			#
			#if( $dumpusers or( scalar( @dumptables ) ) ) {
				if( $pretend ) {
					psim( "Would update myway timing metadata for invocation '$uuid' due to " . ( ( $dumpusers or( scalar( @dumptables ) ) ) ? "backups completed" : "SQL execution starting" ) . " ...\n" );
				} else {
					pdebug( "Commencing new transaction\n", undef, TRUE ) unless( $quiet or $silent );
					sqldo( \$dbh, "START TRANSACTION" ) or die( "$fatal Failed to start transaction [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
					pdebug( "Updating myway timing metadata for invocation '$uuid' due to " . ( ( $dumpusers or( scalar( @dumptables ) ) ) ? "backups completed" : "SQL execution starting" ) . " ...\n", undef, TRUE ) unless( $quiet or $silent );
					my $sql = "UPDATE `$verticadb$mywaytablename` SET `sqlstarted` = SYSDATE() WHERE `id` = '$uuid'";
					sqldo( \$dbh, $sql ) or die( "$fatal Closing statement execution failed [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );

					if( 'procedure' eq $mode ) {
						pdebug( "Committing transaction data\n", undef, TRUE ) unless( $quiet or $silent );
						sqldo( \$dbh, "COMMIT" ) or die( "$fatal Failed to commit transaction [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
					}
				}
			#}
		}

		# }}}

		my $schemastart = [ gettimeofday() ];

		my $delim = DEFDELIM;

		#
		# Check SQL statements for prohibited statements # {{{
		#
		# Pre-process tokenised statements in order to abort early (before any
		# changes have been applied) if we have any prohibited statements...
		#
		if( not( $allowunsafe ) ) {
			my $okay = TRUE;
			foreach my $entry ( $data -> { 'entries' } ) {
				foreach my $statement ( @{ $entry } ) {
					if( 'statement' eq $statement -> { 'type' } ) {
						foreach my $line ( @{ $statement -> { 'entry' } } ) {
							if( defined( $line ) and length( $line ) ) {
								if( $line =~ m/^\s*DROP\s+(?:TABLE|DATABASE)\s/i ) {
									if( $okay ) {
										pfatal( "\n" );
										pfatal( "Refusing to execute prohibited SQL statement:\n" );
										pfatal( "$line\n" );
										$okay = FALSE;
									}
									pwarn( "$line\n", undef, TRUE );
								}
							}
						}
					}
				}
			}

			if( not( $okay ) ) {
				dbclose( \$dbh, undef, undef, TRUE );
				return( FALSE );
			}
		} # }}}

		#
		# Process tokenised statements and apply changes # {{{
		my $state = FALSE;
		my $executed = 0;
		$firstcomment = TRUE;
		$secondcomment = undef;
		foreach my $entry ( $data -> { 'entries' } ) {
			foreach my $statement ( @{ $entry } ) {
				if( 'comment' eq $statement -> { 'type' } ) {
					if( 'ARRAY' eq ref( $statement -> { 'entry' } ) ) { # {{{
						if( defined( $firstcomment ) or ( ( 'procedure' eq $mode ) and defined( $secondcomment ) ) ) {
							if( defined( $firstcomment ) ) {
								$firstcomment = undef;
								$secondcomment = TRUE;
							} else {
								$secondcomment = undef;
							}
						} else { # not( defined( $firstcomment ) or ( ( 'procedure' eq $mode ) and defined( $secondcomment ) ) )
							foreach my $line ( @{ $statement -> { 'entry' } } ) {
								chomp( $line );
								pcomment( "$line\n" ) if( length( $line ) and ( $verbosity or not( $quiet or $silent ) ) ); # debug(3), notice(2), warn(1)
							}
						}
					} else { # not( 'ARRAY' eq ref( $statement -> { 'entry' } ) )
						my $line = $statement -> { 'entry' };
						chomp( $line );
						pcomment( "$line\n" ) if( length( $line ) and ( $verbosity or not( $quiet or $silent ) ) ); # debug(3), notice(2), warn(1)
					} # }}}
				} elsif( 'statement' eq $statement -> { 'type' } ) {
					if( defined( $statement -> { 'tokens' } -> { 'type' } ) and ( $statement -> { 'tokens' } -> { 'type' } ) ) { # {{{
						my $type = $statement -> { 'tokens' } -> { 'type' };
						my $laststatementwasddl = undef;

						if( $type =~ m/delete|delimiter|grant|insert|replace|select|update/i ) {
							if( $type =~ m/delimiter/i ) {
								$delim = $statement -> { 'tokens' } -> { 'delimiter' } if( defined( $statement -> { 'tokens' } -> { 'delimiter' } ) and length( defined( $statement -> { 'tokens' } -> { 'delimiter' } ) ) );
							}

							if( defined( $laststatementwasddl ) and not( $laststatementwasddl ) ) {
								# Last statement was not DDL, so we can still
								# roll-back.

							} elsif( not( 'procedure' eq $mode ) ) { # not( defined( $laststatementwasddl ) ) or $laststatementwasddl
								# Last statement was DDL, (or this is our
								# first statement) so start a new
								# transaction...

								if( $pretend ) {
									psim( "Would commence new transaction\n" );
								} else {
									pdebug( "Commencing new transaction\n", undef, TRUE ) unless( $quiet or $silent );
									sqldo( \$dbh, "START TRANSACTION" ) or die( "$fatal Failed to start transaction [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
								}
								$laststatementwasddl = FALSE;
							}
						} elsif( $type =~ m/call|alter|create|drop/i ) {
							if( defined( $laststatementwasddl ) ) {
								if( $laststatementwasddl ) {
									# Last statement was also DDL, so we
									# can't make use of transactions.

								} elsif( not( 'procedure' eq $mode ) ) { # not( $laststatementwasddl )
									# Last statement wasn't DDL, so end
									# that transaction...

									if( $pretend ) {
										psim( "Would commit transaction data\n" );
									} else {
										pdebug( "Committing transaction data\n", undef, TRUE ) unless( $quiet or $silent );
										sqldo( \$dbh, "COMMIT" ) or die( "$fatal Failed to commit transaction [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
									}
									$laststatementwasddl = TRUE
								}
							} else { # not( defined( $laststatementwasddl ) )
								# First statement is DDL, so no need to start
								# a transaction.

								$laststatementwasddl = TRUE;
							}

						#print Dumper $statement if( $type =~ m/create/i );

						#} elsif( $type =~ m/call|delimiter/i ) {
							# The first of these could do anything, the latter
							# nothing....

						} else {
							pwarn( "\n" );
							pwarn( "Unknown DDL/non-DDL statement type '$type'\n" );
						}
					}

					my $sql;
					foreach my $line ( @{ $statement -> { 'entry' } } ) {
						chomp( $line );
						$line =~ s/^\s+//;
						$line =~ s/\s+$//;
						if( $verbosity ) { # debug(3), notice(2), warn(1)
							psql( "$line\n" );
							foreach my $match ( ( $line =~ m/$RE{ balanced }{ -begin => '\/*' }{ -end => '*\/' }/g ) ) {
								if( $match =~ m:^/\*!\d{5} (.+) \*/$: ) { # / # <- Syntax highlight fail
									psql( "Hint: $1\n" );
								}
							}
						}

						# FIXME: Hack!!
						$line =~ s/\$\$\s*$//;

						# 'DELIMITER' isn't a reserved-word,
						# but /really/ should be...
						#if( not( $line =~ m/^\s*DELIMITER\s/i ) ) {
						if( not( $line =~ m/(?:^\s*|\Q$delim\E\s+)DELIMITER\s/i ) ) {
							$sql .= ' ' . $line;
						}
					}
					if( defined( $sql ) ) {
						$sql =~ s/^\s+//; $sql =~ s/\s+$//;
					}

					if( defined( $sql ) and length( $sql ) ) {
						my( $started, $start, $elapsed );
						if( $pretend ) {
							$executed++ if( $sql !~ m|^/\*!| );
						} else {
							my $realsql = $sql;

							if( $realsql =~ m/^\s*LOCK\s+TABLES/ ) {
								$realsql =~ s/;\s*$//;
								if( 'vertica' eq $engine ) {
									$realsql .= ", $vschm.$mywayactionsname";
								} elsif( 'mysql' eq $engine ) {
									$realsql .= ", $mywayactionsname WRITE";
								}
							}

							# This is a bit of a hack...
							$started = sqlgetvalue( \$dbh, "SELECT SYSDATE()" );

							$start = [ gettimeofday() ];
							if( sqldo( \$dbh, $realsql, $force ) ) {
								$elapsed = tv_interval( $start, [ gettimeofday() ] );
								$executed++ if( $realsql !~ m|^/\*!| );
								$state = $dbh -> state();

								if( $state ) {
									pwarn( "BUG: Successful execution resulted in state '" . $state . "' - resetting to zero\n", undef, TRUE );
									$state = FALSE;
								}
							} else {
								$elapsed = tv_interval( $start, [ gettimeofday() ] );
								$state = $dbh -> state();

								if( not( $state ) ) {
									$state = '00000';
									pwarn( "BUG: Unsuccessful execution resulted in undefined state - resetting to '$state'\n", undef, TRUE );
								}

								my $err = defined( $dbh -> err() ) ? $dbh -> err() : '???';
								my $errstr = defined( $dbh -> errstr() ) ? $dbh -> errstr() : 'Unknown error';
								pwarn( "Statement execution failed: $err ($state) '$errstr'\n", undef, TRUE );
								pwarn( "\"$realsql\"\n", undef, TRUE );

								pwarn( "Attempting to roll-back current transaction ...\n" );
								sqldo( \$dbh, 'ROLLBACK' ) or die( "$fatal Failed to rollback failed transaction [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n", undef, TRUE );

								pdebug( "Restarting transaction for metadata tracking purposes\n", undef, TRUE ) unless( $quiet or $silent );
								sqldo( \$dbh, 'START TRANSACTION' ) or die( "$fatal Failed to restart transaction [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
							}
						}

						if( not( 'procedure' eq $mode ) ) {
							# Vertica won't allow preparation of statements against
							# non-existent tables - this should only occur during
							# dry-run simulations...
							if( ( $engine eq 'vertica' ) and not( qr/^$mywayactionsname$/ |M| \@{ $availabletables } ) ) {
								pwarn( "Cannot prepare statements against tables which do not yet exist in Vertica\n", undef, TRUE );
							} else {
								my $counter;
								$counter = sqlgetvalue( \$dbh, "SELECT LAST_INSERT_ID()" ) unless( $engine eq 'vertica' );
								my $sth;
								# This is a bit tricky, since UTF-8 can be 1-byte, 2-byte
								# or 4-byte encoded, whereas MySQL uses a 3-byte encoding
								# scheme - but it does not appear to be clearly documented
								# when this applies.  MySQL also has a 'utf8mb4' encoding,
								# which appears to force 4-byte UTF-8 encoding, even for
								# characters representable in a shorter encoding...
								# ... in the end we take the Vertica limit of 65000 octets
								# and assume that this may need to hold 4-byte UTF-8 data,
								# resulting in a maximum character-count of 16250
								# characters, which is what length() counts.  However,
								# this is further reduced by maximum row-length limits
								# which apply to both databases.
								# This may result in some statements which would fit into
								# a VARCHAR() field being written into a LONGTEXT field
								# on either database, but this is still an edge-case, and
								# errs on the side of caution.
								#
								if( length( $statement -> { 'line' } ) >= SQLMAX ) {
									$sth = sqlprepare( \$dbh, <<SQL );
INSERT INTO `$verticadb$mywayactionsname` (
    `schema_id`
  , `started`
  , `event`
  , `statement_long`
  , `line`
  , `time`
  , `state`
) VALUES ( ?, ?, ?, ?, ?, ?, ? )
SQL
								} else {
									$sth = sqlprepare( \$dbh, <<SQL );
INSERT INTO `$verticadb$mywayactionsname` (
    `schema_id`
  , `started`
  , `event`
  , `statement`
  , `line`
  , `time`
  , `state`
) VALUES ( ?, ?, ?, ?, ?, ?, ? )
SQL
								}
								my $error = $dbh -> errstr();
								die( "$fatal Unable to create tracking statement handle" . ( defined( $error ) and length( $error ) ? ": " . $error . "\n" : '' ) . ' [' . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) unless( defined( $sth ) and $sth );
								if( not( $pretend ) ) {
									sqlexecute( \$dbh, $sth, undef,
										   $uuid
										,  $started
										, 'execute'
										,  $sql
										,  $statement -> { 'line' }
										,  $elapsed
										,  $state
									);
								}
								$sth -> finish();
								# $counter will be zero if no changes to AUTO_INCREMENT
								# values have occurred...
								if( defined( $counter ) and ( $counter > 0 ) ) {
									# Ensure that we're retrieving the same
									# value that subsequent calls will see...
									my $firstcounter = sqlgetvalue( \$dbh, "SELECT LAST_INSERT_ID()" );
									my $secondcounter = sqlgetvalue( \$dbh, "SELECT LAST_INSERT_ID()" );
									if( not( $firstcounter == $secondcounter ) ) {
										die( "$fatal LAST_INSERT_ID() unstable - aborting [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
									} elsif( not( $counter == $firstcounter ) ) {
										pwarn( "LAST_INSERT_ID() value has changed from '$counter' to '$firstcounter' - attempting to correct...\n", undef, TRUE ) if( DEBUG or $verbosity > 2 ); # debug(3)
										sqldo( \$dbh, "SELECT LAST_INSERT_ID($counter)" );
										my $newcounter = sqlgetvalue( \$dbh, "SELECT LAST_INSERT_ID()" );
										if( not( $counter == $newcounter ) ) {
											die( "$fatal Unable to correct LAST_INSERT_ID() return value [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
										}
									}
								}
							}
						} else { # ( 'procedure' eq $mode )
							if( not( $pretend ) ) {
								my $started = sqlgetvalue( \$dbh, "SELECT `sqlstarted` FROM `$mywayprocsname` WHERE `id` = '$uuid'" );
								if( not( defined( $started ) ) or ( 'NULL' eq $started ) ) {
									pdebug( "Updating myway timing metadata for stored procedure invocation '$uuid' due to creation commencing ...\n", undef, TRUE ) unless( $quiet or $silent );
									my $sql = "UPDATE `$mywayprocsname` SET `sqlstarted` = SYSDATE() WHERE `id` = '$uuid'";
									sqldo( \$dbh, $sql ) or die( "$fatal Statement execution failed [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
								}
							}
						}
					} # }}}
				} else {
					die( "$fatal Unknown statement type '" . $statement -> { 'type' } . "' [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
				}
				print( "\n" ) if( $verbosity and not( $quiet or $silent ) ); # debug(3), notice(2), warn(1)

				pdebug( "\$state is '$state' - falling through ...\n", undef, TRUE ) if( $state );
				last if( $state );
			} # foreach my $statement ( @{ $entry } )

			if( not( 'procedure' eq $mode ) ) {
				if( $pretend ) {
					psim( "Would commit transaction data\n" );
				} else {
					pdebug( "Committing transaction data\n", undef, TRUE ) unless( $quiet or $silent );
					sqldo( \$dbh, "COMMIT" ) or die( "$fatal Failed to commit transaction [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
				}
			}

			pdebug( "\$state is '$state' - falling through ...\n", undef, TRUE ) if( $state );
			last if( $state );
		} # foreach my $entry ( $data -> { 'entries' } ) # }}}

		#
		# Update metadata tables with timings and result # {{{
		#

		my $schemaelapsed = tv_interval( $schemastart, [ gettimeofday() ] );

		my $tablename = $mywaytablename;
		$tablename = $mywayprocsname if( 'procedure' eq $mode );

		if( $pretend ) {
			psim( "Would update myway metadata for invocation '$uuid' ...\n" );
			if( 0 == $executed ) {
				pwarn( "Statements executed is $executed - failing\n", undef, TRUE );
				$status = 0; # Failed
			} else {
				$status = 1; # Succeeded
			}
		} else {
			# FIXME: This is still pretty bare-bones...
			if( $state ) {
				pwarn( "\$state is '$state' - failing\n", undef, TRUE );
				$status = 0; # Failed
			} elsif( ( 0 == $executed ) and not( $fileismigrationschema -> ( $schmfile ) ) ) {
				pwarn( "statements executed is $executed - failing\n", undef, TRUE );
				$status = 0; # Failed
			} else {
				$status = 1; # Succeeded
			}

			pdebug( "Commencing new transaction\n", undef, TRUE ) unless( $quiet or $silent );
			sqldo( \$dbh, "START TRANSACTION" ) or die( "$fatal Failed to start transaction [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
			pdebug( "Updating myway metadata for " . ( $status ? '' : 'failed ' ) . "invocation '$uuid' ...\n", undef, TRUE ) unless( $quiet or $silent );
			my $sql = "UPDATE `$verticadb$tablename` SET `status` = '$status', `finished` = SYSDATE() WHERE `id` = '$uuid'";
			sqldo( \$dbh, $sql ) or die( "$fatal Closing statement execution failed [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );

			if( $state ) {
				if( not( $pretend ) ) {
					pdebug( "Committing transaction data\n", undef, TRUE ) unless( $quiet or $silent );
					sqldo( \$dbh, "COMMIT" ) or die( "$fatal Failed to commit transaction [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
				}

				dbclose( \$dbh, $status ? undef : 'Failed' );
				return( FALSE );
			}
		} # not( $pretend )

		if( defined( $tablename ) and not( qr/^$tablename$/ |M| \@{ $availabletables } ) ) {
			psim( "Would update " . ( 'procedure' eq $mode ? '' : 'flyway ' ) . "metadata with version '$schmversion' for '$schmfile', if it existed ...\n", undef, TRUE );
		} else {
			if( $pretend ) {
				psim( "Would update " . ( 'procedure' eq $mode ? '' : 'flyway ' ) . "metadata with version '$schmversion' for '$schmfile' ...\n" );
			} else {
				pdebug( "Updating " . ( 'procedure' eq $mode ? '' : 'flyway ' ) . "metadata with version '$schmversion' for '$schmfile' ...\n", undef, TRUE ) unless( $quiet or $silent );
			}
			if( not( 'procedure' eq $mode ) ) {
				my $replacement = sqlgetvalue( \$dbh, "SELECT COUNT(*) FROM `$verticadb$flywaytablename` WHERE `version` = '$schmversion'" );

				# Again, since `version` alone is the primary key, the `success` field is fairly
				# useless :(
				#
				my $variables = {};
				$variables -> { 'tablename' } =     "$verticadb$flywaytablename";
				$variables -> { 'installedrank' } =  $installedrank;
				$variables -> { 'schmversion' } =    $schmversion;
				$variables -> { 'desc' } =           $desc;
				$variables -> { 'filetype' } =       $filetype;
				$variables -> { 'schmfile' } =       $schmfile;

				# flyway appears to initialise the checksum value at
				# `version_rank`, then for each element of the table
				# multiplies this by 31 and adds the hashCode() value
				# associated with the item in question (or zero if null),
				# except for `execution_time` and `success`, which are
				# added directly.  This (large) value is then written to a
				# signed int(11) attribute in the database, which causes
				# the value to wrap.
				# I'm not even going to try to reproduce this scheme here...
				#
				# As of flyway-4.0, the above is still the case, except
				# now initially based on `installed_rank`
				#
				$variables -> { 'checksum' } =        0;
				$variables -> { 'user' } =           $user;
				$variables -> { 'duration' } =        int( $schemaelapsed + 0.5 ); # Round up
				$variables -> { 'status' } =         $status;

				die( "$fatal Unable to write metadata record [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) unless( metadataupdateflywaytable( \$dbh, $db, $vschm, $pretend, ( defined( $replacement ) and $replacement =~ m/^\d+$/ and 0 == $replacement ), $variables ) );

			} else { # ( 'procedure' eq $mode )
				my $sth = sqlprepare( \$dbh, <<SQL );
UPDATE `$mywayprocsname` SET
    `version` = ?
  , `description` = ?
  , `type` = ?
WHERE `id` = ?
SQL
# `finished` and `status` were UPDATEd just prior...
				die( "$fatal Unable to create tracking statement handle" . ( defined( $sth -> errstr() ) ? ': ' . $sth -> errstr() : '' ) . ' [' . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) unless( defined( $sth ) and $sth );
				if( not( $pretend ) ) {
					sqldo( \$dbh, "UNLOCK TABLES" ) unless( 'vertica' eq $engine );
					sqlexecute( \$dbh, $sth, undef,
						  $schmversion
						, $desc
						, $filetype
						, $uuid
					);
				}
				$sth -> finish();
			}
		} # not( defined( $tablename ) and not( qr/^$tablename$/ |M| \@{ $availabletables } ) ) # }}}

		if( not( $pretend ) ) {
			pdebug( "Committing transaction data\n", undef, TRUE ) unless( $quiet or $silent );
			sqldo( \$dbh, "COMMIT" ) or die( "$fatal Failed to commit transaction [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
		}
	}
	dbclose( \$dbh );

	# }}}

	if( 0 == $status ) {
		pwarn( "\$status == 0 - aborting\n", undef, TRUE );
		return( FALSE );
	} else {
		return( \$schmversion );
	}
} # applyschema # }}}


sub main( @ ) { # {{{
	my( @argv ) = @_;

	# Allow us to output UTF-8 data without warnings - I do hope you have
	# a Unicode-capable terminal...
	binmode( STDOUT, ":encoding(UTF-8)" );
	binmode( STDERR, ":encoding(UTF-8)" );

	my $port; # = PORT;
	my $marker = MARKER;
	my $odbcok = grep( /ODBC/i, DBI -> available_drivers );
	my $engine;
	my $verticadb = '';

	#
	# Populate command-line arguments and show help # {{{
	#

	my( $action_backup, $action_restore, $action_init );
	my( $action_migrate, $action_check );
	my( $mode, $dosub, $progress );
	my( $help, $showversion, $desc, @paths, $file, $nobackup, $keepbackups );
	my( $compat, $relaxed, $strict );
	my( $lock, $keeplock );
	my( $force, $clear, $compress, $small, $split, $skipmeta, $skipdefiner );
	my( $extinsert );
	my( $syntax, $odbcdsn, $user, $pass, $host, $db, $vschm );
	my( $environment, $limit );
	my( $pretend, $debug, $silent, $quiet, $notice, $verbose, $warn );

	# Pre-set negatable options which are on by default, but which can be
	# turned off...
	$skipdefiner = TRUE;

	my $ok = TRUE;
	my $getoptout = undef;

	my $missingoption = sub {
		my( $argument ) = @_;

		$getoptout = "value \"$argument\" has been specified with no preceeding option identifier";
		$ok = FALSE;
	}; # missingoption

	Getopt::Long::Configure( 'gnu_getopt' );
	Getopt::Long::Configure( 'no_bundling' );
	Getopt::Long::Configure( 'no_ignore_case' );
	eval { GetOptionsFromArray ( \@argv,
	  'b|backup:s'					=> \$action_backup
	,   'lock|lock-database|lock-databases!'	=> \$lock
	,   'keep-lock!'				=> \$keeplock
	,   'small|small-database|small-dataset'
	.   'trans|transaction|transactional'
	.   'single-transaction!'			=> \$small
	,   'compress:s'				=> \$compress
	,   'split|separate-files!'			=> \$split
	,   'skip-metadata!'				=> \$skipmeta
	,   'skip-definer!'				=> \$skipdefiner
	,   'extended-insert!'				=> \$extinsert

	, 'r|restore=s'					=> \$action_restore
	, 'i|init|baseline:s'				=> \$action_init
	, 'c|check!'					=> \$action_check

	,   'progress:s'				=> \$progress

	, 'o|mode=s'					=> \$mode
	,   'substitute|versionate|sub!'		=> \$dosub
	,   'marker=s'					=> \$marker

	, 'm|migrate!'					=> \$action_migrate
	, 's|schemata|directory|scripts=s{,}'		=> \@paths
	, 'f|file|filename|schema|script=s'		=> \$file
	,   'description=s'				=> \$desc
	,   'no-backup|no-backups!'			=> \$nobackup
	,   'keep-backup|keep-backups!'			=> \$keepbackups

	,   'clear-metadata!'				=> \$clear
	,   'force!'					=> \$force
	, 'a|allow-unsafe-operations|allow-unsafe!'	=> \$allowunsafe

	, 'P|port=i'					=> \$port

	,   'mysql-compat!'				=> \$compat
	,   'mysql-relaxed|relaxed!'			=> \$relaxed

	, 'y|syntax=s'					=> \$syntax
	,   'dsn|odbc:s'				=> \$odbcdsn
	, 'u|user|username=s'				=> \$user
	, 'p|pass|passwd|password=s'			=> \$pass
	, 'h|host=s'					=> \$host
	, 'd|db|database=s'				=> \$db
	,   'vertica-schema=s'				=> \$vschm

	, 'e|environment=s'				=> \$environment
	, 'l|limit|target-limit=s'			=> \$limit

	,   'dry-run!'					=> \$pretend
	,   'debug!'					=> \$debug
	,   'silent!'					=> \$silent
	, 'q|quiet!'					=> \$quiet
	, 'n|notice!'					=> \$notice
	, 'w|warn!'					=> \$warn
	, 'v|verbose+'					=> \$verbose
	,   'help|usage!'				=> \$help
	, 'V|version!'					=> \$showversion

	,   '<>'					=>  $missingoption
	#) or die( "$fatal Getopt::Long::GetOptions failed" . ( ( defined $@ and $@ ) ? ": " . $@ : "" ) . "\n" );
	); };
	if( $@ ) {
		$getoptout = $@ if( defined $@ and length( $@ ) );
		$ok = FALSE;
	}

	if( defined( $showversion ) and $showversion ) {
		print( basename( $0 ) . ' ' . VERSION . "\n" );
		exit( 0 );
	}

	$ok = FALSE if( ( defined( $file ) and $file ) and ( @paths and scalar( @paths ) ) );
	if( defined( $odbcdsn ) and $odbcdsn ) {
		$ok = FALSE unless( $odbcok );
	} else {
		$ok = FALSE unless( defined( $user ) and $user );
		$ok = FALSE unless( defined( $pass ) and $pass );

		$host = 'localhost' unless( defined( $host ) and $host );
	}


	if( defined( $mode ) and length( $mode ) ) {
		if( $mode =~ m/^\s*schema|schemata\s*$/i ) {
			$mode = 'schema';
		} elsif( $mode =~ m/^\s*(?:stored\s*)?proc(?:edure)?\s*$/i ) {
			$mode = 'procedure';
		} else {
			warn( "Option '--mode' must have value 'schema' or 'procedure'\n" );
			# Currently, this determines whether we'll execute DROP statements - ideally,
			# we'd allow this in developer environments but not in production... however,
			# environment names are merely labels, and hold no additional meaning.
			exit( 1 );
		}
	} else {
		$mode = 'schema';
	}

	if( defined( $progress ) and length( $progress ) ) {
		if( lc( $progress ) =~ m/^(always|auto|never)$/ ) {
			$progress = lc( $progress );
		} else {
			$progress = undef;
			$ok = FALSE;
		}
	} else {
		$progress = 'auto';
	}

	if( defined( $syntax ) and length( $syntax ) ) {
		if( $syntax =~ m/^\s*vertica\s*$/i ) {
			$syntax = 'vertica';
		} elsif( $syntax =~ m/^\s*mysql\s*$/i ) {
			$syntax = 'mysql';
		} else {
			warn( "Option '--syntax' must have value 'mysql' or 'vertica'\n" );
			exit( 1 );
		}
	} else {
		$syntax = 'mysql';
	}

	if( defined( $vschm ) and length( $vschm ) ) {
		$ok = FALSE if( not( $syntax eq 'vertica' ) );
		$nobackup = TRUE unless( defined( $nobackup ) );
	}

	if( not( defined( $odbcdsn ) and $odbcdsn ) ) {
		$ok = FALSE unless( $syntax eq 'mysql' );

		if( defined( $action_backup ) ) {
			$ok = FALSE if( defined( $db ) and defined( $lock ) );
			# TODO: Support backing-up Stored Procedures separately...
			$ok = FALSE if( 'procedure' eq $mode );
			$ok = FALSE if( defined( $pretend ) );
			$ok = FALSE if( defined( $clear ) );
		} elsif( defined( $action_restore ) ) {
			$ok = FALSE if( defined( $lock ) or defined( $keeplock ) );
			$ok = FALSE if( defined( $compress ) );
			$ok = FALSE if( defined( $small ) );
			$ok = FALSE if( defined( $split ) );
			$ok = FALSE if( defined( $skipmeta ) );
			$ok = FALSE if( defined( $extinsert ) );
			# TODO: Support restoring Stored Procedures only...
			$ok = FALSE if( 'procedure' eq $mode );
			$ok = FALSE if( defined( $pretend ) );
			$ok = FALSE if( defined( $clear ) );
			$ok = FALSE if( defined( $keepbackups ) );
		} else {
			$ok = FALSE unless( defined( $db ) and $db );
			$ok = FALSE if( defined( $lock ) or defined( $keeplock ) );
			$ok = FALSE if( defined( $compress ) );
			$ok = FALSE if( defined( $small ) );
			$ok = FALSE if( defined( $split ) );
			$ok = FALSE if( defined( $skipmeta ) );
			$ok = FALSE if( defined( $extinsert ) );
			$ok = FALSE if( defined( $dosub ) and not( 'procedure' eq $mode ) );
			if( not( defined( $action_init ) or defined( $clear ) ) ) {
				$ok = FALSE unless( ( defined( $file ) and $file ) or ( @paths and scalar( @paths ) ) );
			}
		}
	}

	# Set any binary parameters, so there's no further need to check that
	# they have been defined() ...
	if( defined( $clear ) and $clear ) {
		$clear = TRUE;
	} else {
		$clear = FALSE;
	}
	if( defined( $compat ) and $compat ) {
		$compat = TRUE;
	} else {
		$compat = FALSE;
	}
	if( defined( $force ) and $force ) {
		$force = TRUE;
	} else {
		$force = FALSE;
	}
	if( defined( $allowunsafe ) and $allowunsafe ) {
		$allowunsafe = TRUE;
	} else {
		$allowunsafe = FALSE;
	}
	if( defined( $keepbackups ) and $keepbackups ) {
		$keepbackups = TRUE;
	} else {
		$keepbackups = FALSE;
	}
	if( defined( $pretend ) and $pretend ) {
		$pretend = TRUE;
	} else {
		$pretend = FALSE;
	}
	if( defined( $quiet ) and $quiet ) {
		$quiet = TRUE;
		$quietorsilent = TRUE;
	} else {
		$quiet = FALSE;
	}
	if( defined( $relaxed ) and $relaxed ) {
		$relaxed = TRUE;
		$strict = FALSE;
	} else {
		$relaxed = FALSE;
		$strict = TRUE;
	}
	if( defined( $silent ) and $silent ) {
		$silent = TRUE;
		$quietorsilent = TRUE;
	} else {
		$silent = FALSE;
	}
	if( defined( $small ) and $small ) {
		$small = TRUE;
	} else {
		$small = FALSE;
	}
	if( defined( $split ) and $split ) {
		$split = TRUE;
	} else {
		$split = FALSE;
	}
	if( defined( $skipmeta ) and $skipmeta ) {
		$skipmeta = TRUE;
	} else {
		$skipmeta = FALSE;
	}
	if( defined( $skipdefiner ) and $skipdefiner ) {
		$skipdefiner = TRUE;
	} else {
		$skipdefiner = FALSE;
	}
	if( defined( $extinsert ) and $extinsert ) {
		$extinsert = TRUE;
	} else {
		$extinsert = FALSE;
	}
	if( defined( $nobackup ) and $nobackup ) {
		$nobackup = TRUE;
		$tokenise = FALSE;
	} else {
		$nobackup = FALSE;
		$tokenise = TRUE;
	}

	if( defined( $lock ) and $lock ) {
		$lock = TRUE;
	} else {
		$lock = FALSE;
	}
	if( defined( $keeplock ) and $keeplock ) {
		$keeplock = TRUE;
		$ok = FALSE unless( $lock );
	} else {
		$keeplock = FALSE;
	}

	if( defined( $dosub ) and $dosub ) {
		$dosub = TRUE;
	} else {
		$dosub = FALSE;
	}

	if( $pretend and $clear ) {
		$ok = FALSE;
	}
	if( $clear and not( $force ) ) {
		$ok = FALSE;
	}

	if( not( defined( $odbcdsn ) ) ) {
		if( not( defined( $port ) and $port > 0 ) ) {
			$port = PORT;
		}
	} else {
		my $debug = ( DEBUG or ( $verbosity > 2 ) ); # debug(3)

		my $output = FALSE;
		$output = TRUE if( not( $debug ) );

		$tokenise = FALSE;

		if( not( defined( $port ) and $port > 0 ) ) {
			if( $odbcdsn =~ m/Port=([^;]+)(?:;|$)/i ) {
				pdebug( "\n" ) unless( $output );
				pdebug( "Using port `$1` from supplied DSN ...\n", undef, TRUE ) unless( $quiet or $silent );
				$port = $1;
				$output = TRUE;
			} else {
				$port = VERTICAPORT;
			}
		}
		if( not( defined( $host ) and length( $host ) ) ) {
			if( $odbcdsn =~ m/ServerName=([^;]+)(?:|;$)/i ) {
				pdebug( "\n" ) unless( $output );
				pdebug( "Using host `$1` from supplied DSN ...\n", undef, TRUE ) unless( $quiet or $silent );
				$host = $1;
				$output = TRUE;
			} else {
				$ok = FALSE;
			}
		}
		if( not( defined( $user ) and length( $user ) ) ) {
			if( $odbcdsn =~ m/UserName=([^;]+)(?:;|$)/i ) {
				pdebug( "\n" ) unless( $output );
				pdebug( "Using username `$1` from supplied DSN ...\n", undef, TRUE ) unless( $quiet or $silent );
				$user = $1;
				$output = TRUE;
			} else {
				$ok = FALSE;
			}
		}
		if( not( defined( $pass ) and length( $pass ) ) ) {
			if( $odbcdsn =~ m/Password=([^;]+)(?:;|$)/i ) {
				pdebug( "\n" ) unless( $output );
				pdebug( "Using password `$1` from supplied DSN ...\n", undef, TRUE ) unless( $quiet or $silent );
				$pass = $1;
				$output = TRUE;
			} else {
				$ok = FALSE;
			}
		}
		if( not( defined( $db ) and length( $db ) ) ) {
			if( $odbcdsn =~ m/Database=([^;]+)(?:;|$)/i ) {
				pdebug( "\n" ) unless( $output );
				pdebug( "Using database `$1` from supplied DSN ...\n", undef, TRUE ) unless( $quiet or $silent );
				$db = $1;
			} else {
				$ok = FALSE;
				$output = TRUE;
			}
		}

		print( "\n" ) if( $output and( not( $debug ) ) );
	}

	undef( $user ) unless( defined( $user ) and length( $user ) );
	undef( $pass ) unless( defined( $pass ) and length( $pass ) );
	undef( $host ) unless( defined( $host ) and length( $host ) );
	undef( $db ) unless( defined( $db ) and length( $db ) );
	undef( $mode ) unless( defined( $mode ) and length( $mode ) );
	undef( $marker ) unless( $dosub and defined( $marker ) and length( $marker ) );
	undef( $file ) unless( defined( $file ) and length( $file ) );
	undef( $environment ) unless( defined( $environment ) and length( $environment ) );
	undef( $limit ) unless( defined( $limit ) and length( $limit ) );
	undef( @paths ) unless( @paths and scalar( @paths ) );

	if( defined( $odbcdsn ) ) {
		$ok = FALSE if( not( $odbcok ) );
		$ok = FALSE if( not( $vschm ) );
		$ok = FALSE if( $action_backup );
		$ok = FALSE if( $lock );
		$ok = FALSE if( $keeplock );
		$ok = FALSE if( $small );
		$ok = FALSE if( $compress );
		$ok = FALSE if( $split );
		$ok = FALSE if( $skipmeta );
		$ok = FALSE if( $extinsert );
		$ok = FALSE if( $action_restore );
		$ok = FALSE if( $dosub );
		$ok = FALSE if( $marker );
		$ok = FALSE if( not( $nobackup ) );
		$ok = FALSE if( $keepbackups );
		$ok = FALSE if( $compat );
		$ok = FALSE if( $relaxed );
	}

	if( ( defined( $help ) and $help ) or ( 0 == scalar( @ARGV ) ) ) {
		my $myway = basename( $0 );
		my $length = length( $myway ) + length( "Usage:  " );
		# vi: set colorcolumn=117: to place highlight after 80th column
		#	                               2         3         4         5         6         7         8
		#	                           67890123456789012345678901234567890123456789012345678901234567890
		if( defined( $odbcdsn ) or ( $syntax eq 'vertica' ) ) {
			print(       "Usage: $myway --dsn \"ODBC DSN\" [--database <schema>] ...\n" );
			print( ( " " x $length ) . "<--init [version]|[--migrate|--check] ...\n" );
			print( ( " " x $length ) . "<--scripts <directory>|--file <schema>> [[:syntax:]]\n" );
			print( ( " " x $length ) . "[--vertica-schema <schema name>]\n" );
			print( ( " " x $length ) . "[--clear-metadata] [--force] [--dry-run] [--silent] [--quiet]\n" );
			print( ( " " x $length ) . "[--notice] [--warn] [--debug]\n" );
			print( "\n" );
			print( ( " " x $length ) . "ODBC DSN example:  Driver={Vertica};Database=db;ServerName=localhost;Port=5433;UserName=x;Password=y\n" );
			print( "\n" );
			print( ( " " x $length ) . "                   ... where 'Vertica' is an entry in /etc/odbcinst.ini:\n" );
			print( "\n" );
			print( ( " " x $length ) . "                   [Vertica]\n" );
			print( ( " " x $length ) . "                   Description = HPE Vertica ODBC Driver\n" );
			print( ( " " x $length ) . "                   Driver = /opt/vertica/lib64/libverticaodbc.so\n" );
			print( "\n" );
			print( ( " " x $length ) . "syntax:            --syntax <mysql|vertica>\n" );
			print( "\n" );
			print( ( " " x $length ) . "--vertica-schema - Specify Vertica database/schema name\n" );
		} else {
			if( $odbcok ) {
			print(       "Usage: $myway <--username <user> --password <passwd> --host <node> ...\n" );
			print( ( " " x $length ) . " [--port <port>] --database <db>> ...\n" );
			print( ( " " x $length ) . "| <--dsn <dsn>> [[:syntax:]] ...\n" );
			} else {
			print(       "Usage: $myway -u <username> -p <password> -h <host> -d <database> [-P <port>]\n" );
			}
			print( ( " " x $length ) . "<--backup [directory] [:backup options:]|...\n" );
			print( ( " " x $length ) . " --restore <file> [:restore options:]|--init [version]>|...\n" );
			print( ( " " x $length ) . "[--migrate|--check] <--scripts <directory>|--file <schema>> ...\n" );
			print( ( " " x $length ) . "[--target-limit <version>] [[:mode:]] [[:syntax:]] ...\n" );
			print( ( " " x $length ) . "[--mysql-compat] [--no-backup|--keep-backup] ...\n" );
			print( ( " " x $length ) . "[--clear-metadata] [--force] [--dry-run] [--silent] [--quiet]\n" );
			print( ( " " x $length ) . "[--notice] [--warn] [--debug]\n" );
			print( "\n" );
			print( ( " " x $length ) . "backup options:   [--compress [:scheme:]] [--transactional]\n" );
			print( ( " " x $length ) . "                  [--lock [--keep-lock]] [--separate-files]\n" );
			print( ( " " x $length ) . "                  [--skip-metadata] [--no-skip-definer]\n" );
			print( ( " " x $length ) . "                  [--extended-insert]\n" );
			print( ( " " x $length ) . "scheme:           <gzip|bzip2|xz|lzma>\n" );
			print( "\n" );
			print( ( " " x $length ) . "restore options:  [--progress[=<always|auto|never>]]\n" );
			print( "\n" );
			print( ( " " x $length ) . "mode:              --mode <schema|procedure>\n" );
			print( ( " " x $length ) . "                  [--substitute [--marker <marker>]\n" );
			print( "\n" );
			if( $odbcok ) {
			print( ( " " x $length ) . "syntax:            --syntax <mysql|vertica>\n" );
			print( "\n" );
			}
			if( $syntax eq 'mysql' ) {
			print( ( " " x $length ) . "  * MySQL compatibility:\n" );
			print( "\n" );
			print( ( " " x $length ) . "--mysql-compat   - Required for MySQL prior to v5.6.4\n" );
			print( ( " " x $length ) . "--mysql-relaxed  - Do not operate in STRICT mode\n" );
			print( "\n" );
			}
			print( ( " " x $length ) . "  * Backup options:\n" );
			print( "\n" );
			print( ( " " x $length ) . "--no-backup      - Do not take backups before making changes\n" );
			print( ( " " x $length ) . "--keep-backup    - Copy backups to a local directory on exit\n" );
			print( "\n" );
			print( ( " " x $length ) . "--compress       - Compress backups [using <scheme> algorithm]\n" );
			print( ( " " x $length ) . "--transactional  - Don't lock transactional tables\n" );
			print( ( " " x $length ) . "--lock           - Lock instance for duration of backup\n" );
			print( ( " " x $length ) . "--keep-lock      - Keep lock for up to 24 hours after backup\n" );
			print( "\n" );
			print( ( " " x $length ) . "  * Stored Procedure options:\n" );
			print( "\n" );
			print( ( " " x $length ) . "--substitute     - Replace the string '" . ( ( defined( $marker ) and length( $marker ) ) ? $marker : MARKER ) . "' with version\n" );
			print( ( " " x $length ) . "                   number from stored procedure directory name\n" );
			print( ( " " x $length ) . "--marker         - Use string in place of '" . MARKER . "'\n" );
		}
		{
			print( "\n" );
			print( ( " " x $length ) . "  * Schema options:\n" );
			print( "\n" );
			print( ( " " x $length ) . "--allow-unsafe   - Allow DROP TABLE & DROP DATABASE statements\n" );
			print( "\n" );
			print( ( " " x $length ) . "  * Metadata options:\n" );
			print( "\n" );
			print( ( " " x $length ) . "--description    - Override description for a single schema file\n" );
			#print( ( " " x $length ) . "--set-version    - Override version for a single schema file\n" );
			print( "\n" );
			print( ( " " x $length ) . "--environment    - Specify environment for metadata filtering\n" );
			print( ( " " x $length ) . "--target-limit   - Specify required final schema version\n" );
			print( "\n" );
			print( ( " " x $length ) . "--clear-metadata - Remove all {my,fly}way metadata tables\n" );
			print( "\n" );
			print( ( " " x $length ) . "--force          - Allow a database to be re-initialised or\n" );
			print( ( " " x $length ) . "                   ignore previous and target versions when\n" );
			print( ( " " x $length ) . "                   applying schema files\n" );
			print( "\n" );
			print( ( " " x $length ) . "  * Output control:\n" );
			print( "\n" );
			print( ( " " x $length ) . "--warn           - Output additional warning messages only\n" );
			print( ( " " x $length ) . "--notice         - Output standard progress messages\n" );
			print( ( " " x $length ) . "--debug          - Output copious debugging statements\n" );
			print( "\n" );
			print( ( " " x $length ) . "--silent         - Output only fatal errors\n" );
			print( ( " " x $length ) . "--quiet          - Output only essential messages\n" );
			print( "\n" );
			print( ( " " x $length ) . "--dry-run        - Validate but do not execute schema SQL\n" );
			print( "\n" );
			print( "(N.B. '--dry-run' requires an initialised database to prepare statements against)\n" );
		}
		if( defined( $odbcdsn ) or ( $syntax eq 'vertica' ) ) {
			if( not( defined( $odbcok ) and $odbcok ) ) {
				pwarn( "\nCould not load DBD::ODBC module - ODBC functionality not available\n\n", LOGMAX );
			}
		}
		exit( 0 );
	} elsif( not( $ok ) ) {
		warn( "Mutually-exclusive arguments '--schema' and '--schemata' cannot both be specified\n" ) if( defined( $file ) and @paths );
		warn( "Mutually-exclusive arguments '--dry-run' and '--clear-metadata' cannot both be specified\n" ) if( $pretend and $clear );
		warn( "Mutually-exclusive arguments '--no-backup' and '--keep-backup' cannot both be specified\n" ) if( $nobackup and $keepbackups );
		warn( "Cannot specify argument '--compress' without option '--backup'\n" ) if( defined( $compress ) and not( defined( $action_backup ) ) );
		warn( "Cannot specify argument '--lock' or '--keep-lock' without option '--backup'\n" ) if( ( $lock or $keeplock ) and not( defined( $action_backup ) ) );
		warn( "Cannot specify argument '--separate-files' without option '--backup'\n" ) if( $split and not( defined( $action_backup ) ) );
		warn( "Cannot specify argument '--skip-metadata' without option '--backup'\n" ) if( $skipmeta and not( defined( $action_backup ) ) );
		warn( "Cannot specify argument '--no-skip-definer' without option '--backup'\n" ) if( not( $skipdefiner ) and not( defined( $action_backup ) ) );
		warn( "Cannot specify argument '--extended-insert' without option '--backup'\n" ) if( $extinsert and not( defined( $action_backup ) ) );
		warn( "Argument '--progress' must have value 'always', 'auto', or 'never'\n" ) unless( defined( $progress ) );
		warn( "Cannot specify argument '--keep-lock' without option '--lock'\n" ) if( $keeplock and not( $lock ) );
		warn( "Cannot specify argument '--clear-metadata' without option '--force'\n" ) if( $clear and not( $force ) );
		warn( "Cannot specify argument '--lock' with option '--database' (locks are global)\n" ) if( $lock and defined( $db ) );
		warn( "Cannot specify argument '--substitute' unless option '--mode' is 'procedure'\n" ) if( $dosub and not( 'procedure' eq $mode ) );
		if( defined( $odbcdsn ) and not( $odbcok ) ) {
			warn( "Argument '--dsn' specified but DBD::ODBC module not available\n" );
		}
		if( defined( $odbcdsn ) ) {
			warn( "Cannot specify argument '--backup' with option '--dsn'\n" ) if( defined( $action_backup ) );
			warn( "Cannot specify argument '--lock' with option '--dsn'\n" ) if( $lock );
			warn( "Cannot specify argument '--keep-lock' with option '--dsn'\n" ) if( $keeplock );
			warn( "Cannot specify argument '--transactional' with option '--dsn'\n" ) if( $small );
			warn( "Cannot specify argument '--compress' with option '--dsn'\n" ) if( defined( $compress ) );
			warn( "Cannot specify argument '--separate-files' with option '--dsn'\n" ) if( $split );
			warn( "Cannot specify argument '--skip-metadata' with option '--dsn'\n" ) if( $skipmeta );
			warn( "Cannot specify argument '--no-skip-definer' with option '--dsn'\n" ) if( not( $skipdefiner ) );
			warn( "Cannot specify argument '--extended-insert' with option '--dsn'\n" ) if( $extinsert );
			warn( "Cannot specify argument '--restore' with option '--dsn'\n" ) if( defined( $action_restore ) );
			warn( "Cannot specify argument '--mode' with option '--dsn'\n" ) if( not( $mode eq 'schema' ) );
			warn( "Cannot specify argument '--substitute' with option '--dsn'\n" ) if( $dosub );
			warn( "Cannot specify argument '--marker' with option '--dsn'\n" ) if( defined( $marker ) );
			warn( "Cannot specify argument '--backup' with option '--dsn'\n" ) if( not( $nobackup ) );
			warn( "Cannot specify argument '--keep-backup' with option '--dsn'\n" ) if( $keepbackups );
			warn( "Cannot specify argument '--mysql-compat' with option '--dsn'\n" ) if( $compat );
			warn( "Cannot specify argument '--mysql-relaxed' with option '--dsn'\n" ) if( $relaxed );
			warn( "Required argument '--vertica-schema' not specified\n" ) unless( defined( $vschm ) );
		} else {
			warn( "Argument '--syntax' must be 'mysql' unless using ODBC\n" ) unless( $syntax eq 'mysql' );
			warn( "Cannot specify argument '--vertica-schema' unless option '--syntax=vertica' is specified\n" ) if( defined( $vschm ) );
		}
		warn( "Required argument '--user' not specified\n" ) unless( defined( $user ) );
		warn( "Required argument '--password' not specified\n" ) unless( defined( $pass ) );
		warn( "Required argument '--host' not specified\n" ) unless( defined( $host ) );
		warn( "Required argument '--database' not specified\n" ) unless( defined( $db ) );
		warn( "Required argument '--schema' or '--schemata' not specified\n" ) unless( defined( $file ) or ( @paths ) or defined( $action_backup ) or defined( $action_restore ) or $clear );
		warn( "Command '--restore' takes only a filename as the single argument\n" ) if( defined( $action_restore ) );
		warn( "... additionally, Getopt failed with '$getoptout'\n" ) if( defined( $getoptout ) );
		exit( 1 );
	}

	if( defined( $odbcdsn ) ) {
		if( ( not( length( $odbcdsn ) ) ) ) {
			warn( "Invalid ODBC DSN: '$odbcdsn'\n" );
			exit( 1 );
		} elsif( $odbcdsn =~ m/[=;]/ ) {
			# A DSN can be a reference to a driver configuration in
			# /etc/odbc.ini or ~/.odbc.ini, and a set of parameters
			# which can be used stand-alone.  If the former case,
			# we can't override the provided values.  In the latter
			# we can overwrite or add any changes.  Unfortunately,
			# without parsing the ODBC configuration files we can't
			# be entirely sure which we have - but if the string
			# provided contains '=' or ';' characters, there's a
			# good chance that we have something we can add to...
			#
			pdebug( "\n" );
			pdebug( "Reconstructing ODBC DSN '$odbcdsn' ...\n", undef, TRUE ) unless( $quiet or $silent );
			if( not( $odbcdsn =~ m/Port=$port(;|$)/i ) ) {
				if( $odbcdsn =~ m/Port=([^;]+)(?:;|$)/i ) {
					$odbcdsn =~ s/(Port=)([^;]+)(;)?/$1$port$3/;
				} else {
					$odbcdsn .= ";Port=$port"
				}
			}
			if( not( $odbcdsn =~ m/ServerName=$host(;|$)/i ) ) {
				if( $odbcdsn =~ m/ServerName=([^;]+)(?:;|$)/i ) {
					$odbcdsn =~ s/(ServerName=)([^;]+)(;)?/$1$host$3/;
				} else {
					$odbcdsn .= ";ServerName=$host"
				}
			}
			if( not( $odbcdsn =~ m/UserName=$user(;|$)/i ) ) {
				if( $odbcdsn =~ m/UserName=([^;]+)(?:;|$)/i ) {
					$odbcdsn =~ s/(UserName=)([^;]+)(;)?/$1$user$3/;
				} else {
					$odbcdsn .= ";UserName=$user"
				}
			}
			if( not( $odbcdsn =~ m/Password=$pass(;|$)/i ) ) {
				if( $odbcdsn =~ m/Password=([^;]+)(?:;|$)/i ) {
					$odbcdsn =~ s/(Password=)([^;]+)(;)?/$1$pass$3/;
				} else {
					$odbcdsn .= ";Password=$pass"
				}
			}
			if( not( $odbcdsn =~ m/Database=$db(;|$)/i ) ) {
				if( $odbcdsn =~ m/Database=([^;]+)(?:;|$)/i ) {
					$odbcdsn =~ s/(Database=)([^;]+)(;)?/$1$db$3/;
				} else {
					$odbcdsn .= ";Database=$db"
				}
			}
			# Just in case...
			$odbcdsn =~ s/^;//g;
			$odbcdsn =~ s/;+/;/g;
			$odbcdsn =~ s/;$//g;
			pdebug( "Parameterised ODBC DSN is '$odbcdsn'\n", undef, TRUE ) unless( $quiet or $silent );
		}
	}

	# -s can be used on a directory in order to determine whether it is
	# empty...
	if( defined( $file ) and ( ( -d $file ) or not ( -s $file ) ) ) {
		warn( "File system object '$file' does not exist, is of zero length, or is not a regular file\n" );
		warn( "(Please use the '--scripts' option to specify multiple input files or directories)\n" );
		exit( 1 );
	}

	#if( OLDSCHEMA and( not( force ) ) ) {
	#	warn( "OLDSCHEMA is set in your environment.  This is a deprecated debug option and may\n" );
	#	warn( "only be used when --force is also enabled\n" );
	#	exit( 1 );
	#}

	if( @paths and scalar( @paths ) ) {
		# TODO: Support multiple descriptions for path invocations?

		warn( "Ignoring --description option '$desc' when invoked with --schemata\n" ) if( defined( $desc ) );
		warn( "Ignoring --description option '$desc' when invoked in Stored Procedure mode\n" ) if( defined( $mode ) and defined( $desc ) );
		$desc = undef;
	}

	$verbose = 1 if( defined( $warn ) and $warn and ( $verbosity < 1 ));
	$verbose = 2 if( defined( $notice ) and $notice and ( $verbosity < 2 ));
	$verbose = 3 if( defined( $debug ) and $debug and ( $verbosity < 3 ));
	$verbosity = $verbose if( defined( $verbose ) and ( $verbosity < $verbose )); # debug(3), notice(2), warn(1)

	# }}}

	#
	# Perform backup, if requested # {{{
	#

	my $auth = {
		  'user'	=> $user
		, 'password'	=> $pass
		, 'host'	=> $host
		, 'port'	=> $port
		, 'database'	=> $db
	};

	if( defined( $action_restore ) and length( $action_restore ) ) {
		if( dbrestore( $auth, $action_restore, $progress ) ) {
			die( "$fatal Datbase restoration failed for file '$action_restore'\n" );
		} else {
			exit( 0 );
		}
	}

	if( defined( $action_backup ) ) {
		# --backup may be used alone to trigger a backup, or as
		# --backup=/<dir> or --backup <dir> to specify a destination
		# directory - in which case the assigned value will be the
		# path, rather than '1'...
		#
		my $location;
		if( 1 ne $action_backup ) {
			$location = $action_backup;
			$action_backup = TRUE;
		}

		my $backupdsn;
		my $dbh;
		my $availabledatabases;
		my $availabletables;

		if( $lock and $keeplock ) {
			# The child must exit before the parent, so we'll fork,
			# lock tables and wait (for a long time) in the parent,
			# and then perform the backup in the child...
			#
			# ... unfortunately, this prevents us from (easily)
			# obtaining an exit status from the child process.
			#
			# ... so what we actually need to do is fork twice, and
			# have the parent call waitpid() on the (more likely to
			# exit) first child process running the backup.
			#
			my $firstchildpidorzero = fork;
			die( "$fatal fork() failed [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]: $!\n" ) unless( defined( $firstchildpidorzero ) );

			if( not( 0 == $firstchildpidorzero ) ) {
				# Parent process # {{{

				my $secondchildpidorzero = fork;
				die( "$fatal fork() failed [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]: $!\n" ) unless( defined( $secondchildpidorzero ) );

				if( 0 == $secondchildpidorzero ) {
					# Second child process # {{{

					eval {
						setpgrp( 0, 0 );
					};

					local $| = 1;

					if( defined( $db ) and length( $db ) ) {
						$backupdsn = "DBI:mysql:database=$db;host=$host;port=$port";
						if( not( $quiet or $silent ) ) {
							pdebug( "\n" );
							pdebug( "Connecting to database `$db` ...\n", undef, TRUE );
						}
					} else {
						$backupdsn = "DBI:mysql:host=$host;port=$port";
						if( not( $quiet or $silent ) ) {
							pdebug( "\n" );
							pdebug( "Connecting to database instance ...\n", undef, TRUE );
						}
					}

					my $error = dbopen( \$dbh, $backupdsn, $user, $pass, $strict );
					{
						die( "$fatal $error [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) if $error;

						if( not ( sqldo( \$dbh, "FLUSH TABLES WITH READ LOCK" ) ) ) {
							pwarn( "\n" );
							pwarn( "Failed to globally lock all instance databases' tables\n" );
						} else {
							pdebug( "\n" );
							pdebug( "All databases on host '$host' are now globally locked for 24 hours, or until\n", undef, TRUE );
							pdebug( "this process is interupted.\n", undef, TRUE );
							pdebug( "The backup process will continue concurrently.\n", undef, TRUE );
						}
						sqldo( \$dbh, "SELECT(SLEEP(86400))" );

						pdebug( "86400 seconds elapsed, dropping locks and disconnecting.\n", undef, TRUE );

						if( $dbh -> ping ) {
							sqldo( \$dbh, "UNLOCK TABLES" );
						}
					}
					dbclose( \$dbh, 'Backup complete' );

					exit( 0 );

					# End second child process # }}}
				}

				# Still the (original) parent process

				local $| = 1;

				my $rc;

				if( waitpid( $firstchildpidorzero, 0 ) > 0 ) {
					my( $sig, $core );

					( $rc, $sig, $core ) = ( $? >> 8, $? & 127, $? & 128 );

					if( $core ) {
						pfatal( "\n" );
						pfatal( "backup process $firstchildpidorzero core-dumped\n" );
						kill( -15, $secondchildpidorzero ) if( $secondchildpidorzero );
					} elsif( 9 == $sig ) {
						pwarn( "\n" );
						pwarn( "backup process $firstchildpidorzero was KILLed\n" );
						kill( -15, $secondchildpidorzero ) if( $secondchildpidorzero );
					} else {
						pwarn( "backup process $firstchildpidorzero returned $rc" . ( $sig ? " after signal $sig" : '' ) ) unless( 0 == $rc );

						pwarn( "\n", LOGMAX );
						pwarn( "All databases on host '$host' remain globally locked for 24 hours, or until\n", LOGMAX, TRUE );
						pwarn( "this process is terminated.\n", LOGMAX, TRUE );
					}
				} else {
					pwarn( "backup process $firstchildpidorzero disappeared" );
					kill( -15, $secondchildpidorzero ) if( $secondchildpidorzero );
				}

				if( waitpid( $secondchildpidorzero, 0 ) > 0 ) {
					my( $sig, $core );
					( $rc, $sig, $core ) = ( $? >> 8, $? & 127, $? & 128 );
					if( $core ) {
						pfatal( "\n" );
						pfatal( "lock process $secondchildpidorzero core-dumped\n" );
					} elsif( 9 == $sig ) {
						pwarn( "\n" );
						pwarn( "lock process $secondchildpidorzero was KILLed\n" );
					} else {
						pwarn( "lock process $secondchildpidorzero returned $rc" . ( $sig ? " after signal $sig" : '' ) );
					}
				} else {
					pwarn( "lock process $secondchildpidorzero disappeared" );
				}

				exit( $rc );

				# End original parent process # }}}
			}

			# First child process

			eval {
				setpgrp( 0, 0 );
			};

			# ... this now goes on to perform the
			# backup.
		}

		local $| = 1;

		if( defined( $db ) and length( $db ) ) {
			$backupdsn = "DBI:mysql:database=$db;host=$host;port=$port";
			if( not( $quiet or $silent ) ) {
				pdebug( "\n" );
				pdebug( "Connecting to database `$db` ...\n", undef, TRUE );
			}
		} else {
			$backupdsn = "DBI:mysql:host=$host;port=$port";
			if( not( $quiet or $silent ) ) {
				pdebug( "\n" );
				pdebug( "Connecting to database instance ...\n", undef, TRUE );
			}
		}

		my $success;

		my $error = dbopen( \$dbh, $backupdsn, $user, $pass, $strict );
		{
			die( "$fatal $error [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) if $error;

			if( defined( $db ) and length( $db ) ) {
				$availabletables = sqlgetvalues( \$dbh, "SHOW TABLES" );
			} else {
				$availabledatabases = sqlgetvalues( \$dbh, "SHOW DATABASES" );
			}

			if( not( $lock ) ) {
				dbclose( \$dbh, undef, undef, TRUE );
				$dbh = undef;
			} else { # $lock
				if( not( $keeplock ) ) {
					if( not ( sqldo( \$dbh, "FLUSH TABLES WITH READ LOCK" ) ) ) {
						dbclose( \$dbh, 'Failed' );
						$dbh = undef;

						# XXX: Abort if we've been
						#      asked to obtain a lock,
						#      but this has failed.

						pfail( "Unable to obtain global table lock\n" );

						exit( 1 );
					}
				} # else { # ( $keeplock )
					# We already have a lock from the
					# second child above.
					# $dbh is still valid...
				# }
			}

			# If $auth contains a 'database' entry, then parameter
			# two to dbdump(), $objects, specifies the tables to
			# back-up.  If not set, the entire database will be
			# backed-up.  If $auth does not provide a database,
			# then the entire instance will be backed-up.
			# If backing-up an entire instance, the resulting file
			# is written to $location.  If backing-up a single
			# database, then the backup is placed in a directory
			# named $location.  If no $location is specified, then
			# create the backup in the current directory.

			#if( defined( $location ) ) {
				my $options = {
					  'compress'		=> $compress
					, 'transactional'	=> $small
					, 'skipmeta'		=> $skipmeta
					, 'skipdefiner'		=> $skipdefiner
					, 'extinsert'		=> $extinsert
				};
				if( defined( $db ) and length( $db ) ) {
					if( not( $split ) or not( $availabletables and scalar( @{ $availabletables } ) ) ) {
						if( not( $availabletables and scalar( @{ $availabletables } ) ) ) {
							pwarn( "Unable to retrieve list of tables for database `$db`, backing up all tables to '$location' ...\n", undef, TRUE );
						}
						# $location is a filename ...
						$success = dbdump( $auth, undef, undef, $location, $options );
					} else {
						# Write per-table files to $location/ ...
						foreach my $table ( @{ $availabletables } ) {
							pnote( "Backing up table `$db`.`$table` to '$location/$db.$table.sql' ...\n", undef, TRUE );
							my $tablesuccess = dbdump( $auth, $table, $location, "$db.$table.sql", $options );
							$success += $tablesuccess;
							pwarn( "Table `$table` failed to backup: $tablesuccess\n", undef, TRUE ) if( not( $tablesuccess ) );
						}
					}
				} else {
					if( not( $split ) or not( $availabledatabases and scalar( @{ $availabledatabases } ) ) ) {
						if( not( scalar( @{ $availabledatabases } ) ) ) {
							pwarn( "Unable to retrieve list of databases for instance, backing up all databases to single file\n", undef, TRUE );
						}
						# $location is a directory ...
						$success = dbdump( $auth, undef, $location, undef, $options );
					} else {
						# Write per-database files to $location/ ...
						foreach my $database ( @{ $availabledatabases } ) {
							next if( qr/^$database$/ |M| [ 'information_schema', 'performance_schema' ] );
							pnote( "\n" );
							pnote( "Backing up database `$database` to '" . ( length( $location ) ? "$location/" : '' ) . "$database.sql' ...\n", undef, TRUE );
							my $databasesuccess = dbdump( $auth, $database, $location, "$database.sql", $options );
							if( defined( $databasesuccess ) and $databasesuccess ) {
								$success += $databasesuccess;
							} else {
								if( defined( $databasesuccess ) ) {
									pwarn( "Database `$database` failed to backup: $databasesuccess\n", undef, TRUE );
								} else {
									pwarn( "Database `$database` failed to backup\n", undef, TRUE );
								}
							}
						}
					}
				}
			#} else {
			#	$success = dbdump( $auth, undef, undef, undef, $options );
			#}

			if( $lock and not( $keeplock ) ) {
				# When ( $lock and $keeplock ), the second
				# child process holds the global lock, not us
				# (at this point, hopefully, the first child)

				if( $dbh -> ping ) {
					sqldo( \$dbh, "UNLOCK TABLES" );
				}
			}
		}
		dbclose( \$dbh, undef, undef, TRUE ) if( defined( $dbh ) );

		if( defined( $success) and $success ) {
			pnote( "Backup process completed successfully\n", LOGMAX, TRUE ) unless( $quietorsilent );

			exit( 0 );
		} else {
			pfail( "Backup process failed\n" );

			exit( 1 );
		}

		# Unreachable
		return( undef );
	}

	# }}}

	#
	# Determine file(s) to apply # {{{
	#

	my( @files, $target, $basepath );
	if( @paths and ( 1 == scalar( @paths ) ) ) {
		# Check for /path/*.sql invocations...

		my $path = $paths[ 0 ];
		my $pattern;

		if( -d $path ) {
			my $actualpath = realpath( $path );
			if( not( -d $actualpath ) ) {
				die( "$fatal Specified path '$path' does not appear to resolve to a directory\n" );
			}
			$path = $actualpath;
			$pattern = "*";

		} else {
			if( not( -s $path ) ) {
				die( "$fatal Object '$path' does not exist or cannot be read\n" );
			}
			my $pathprefix = dirname( realpath( $path ) );
			if( not( -d $pathprefix ) ) {
				die( "$fatal Apparent parent directory '$pathprefix' of file '$path' does not appear to resolve to a directory\n" );
			}
			$pattern = basename( $path );
			$path = $pathprefix;
		}

		$path = '.' unless( defined( $path ) and length( $path ) );
		$basepath = $path;
		@files = bsd_glob( $basepath . "/" . $pattern );
		if( scalar( @files ) ) {
			my @targetfiles;
			foreach my $file ( @files ) {
				push( @targetfiles, $file ) if( not( -d $file ) and -s $file );
			}
			$target = \@targetfiles;
		} else {
			die( "$fatal No files match pattern '$pattern' in directory '$basepath'\n" );
		}

	} elsif( @paths and scalar( @paths ) ) {
		$target = \@paths;

	} elsif( not( $clear ) ) {
		if( not( defined( $file ) and length( $file ) ) ) {
			die( "$fatal File name required [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) unless( defined( $action_init ) );
		} else {
			die( "$fatal Cannot read from file '$file'\n" ) unless( defined( $file ) and -r $file );
		}
	}

	if( defined( $target ) ) {
		# If we're applying Stored Procedures, we assume that
		# they are versioned by directory, and so we don't care
		# what they are called or what order they are applied
		# in, but they have to all be in the same place!
		# Note, however, that the adoption of symlinks causes problems
		# if we use 'realpath' indiscriminately...
		if( 'procedure' eq $mode ) {
			#my $path = realpath( @{ $target }[ 0 ] );
			#if( not( -d $path ) ) {
				#$path = dirname( $path );
				my $path = dirname( @{ $target }[ 0 ] );
				if( not( -d $path ) ) {
					die( "$fatal Cannot resolve directory name for path '" . @{ $target }[ 0 ] . "'\n" );
				}
			#}
			my $okay = TRUE;
			my $index = -1; # Will be incremented before use...
			my @newtarget = @{ $target };
			PROCFILE: foreach my $singlefile ( @{ $target } ) {
				$index ++;

				#my $singlepath = realpath( $singlefile );
				#if( not( -d $singlepath ) ) {
					#$singlepath = dirname( $singlepath );
					my $singlepath = dirname( $singlefile );
					if( not( -d $singlepath ) ) {
						$singlepath = undef;
						$okay = FALSE;
						pfatal( "\n" );
						pfatal( "Cannot resolve directory name for path '$singlefile'\n" );
					}
				#}
				if( defined( $singlepath ) and not( $singlepath eq $path ) ) {
					$okay = FALSE;
					pfatal( "\n" );
					pfatal( "Directory path for file '$singlefile' does not fall within detected common directory '$path'\n" );
				}

				if( basename( $singlefile ) =~ m/\.metadata$/ ) {
					pdebug( "Excluding metadata file '$singlefile' ..." );
					splice( @newtarget, $index, 1 );
					$index--;

					next PROCFILE;
				}
			}
			die( "$fatal No files found matching argument '@{ $target }'\n" ) unless( scalar( @newtarget ) );
			exit( 1 ) unless( $okay );
			$path = '.' unless( defined( $path ) and length( $path ) );
			$basepath = $path;
			@files = @newtarget;
		} else {
			my $shouldsort = TRUE;

			foreach my $index ( 0 .. scalar( @{ $target } ) ) {
				my $file = @{ $target }[ $index ];
				if( not( length( $file ) ) or ( $file =~ m/^\s*$/ ) ) {
					delete @{ $target }[ $index ];
				} else {
					( my $version = basename( $file ) ) =~ /^(?:V[[:xdigit:].]+__)?V([[:xdigit:].]+)__/;
					if( not( defined( $version ) and length( $version ) ) ) {
						pwarn( "\n" );
						pwarn( "Target '$file' has invalid name without an identifiable version\n" );
						pwarn( "Relying on filesystem sorting only - schema may be applied in an undefined order\n" );
						$shouldsort = FALSE;
					}
				}
			}
			if( $shouldsort ) {
				# FIXME: How to handle non-matching filenames here?  Match should be undef and so items pool to the top/bottom of the results?
				# Wrap versioncmp to better handle undef
				# parameters...
				my $vcmp = sub( $$ ) {
					my( $a, $b ) = @_;

					return( 0 ) unless( defined( $a ) or defined( $b ) );
					return( -1 ) if( not( defined( $a ) ) );
					return( 1 ) if( not( defined( $b ) ) );

					return( versioncmp( $a, $b ) );
				};
				@files = map { $_ -> [ 0 ] } sort { $vcmp -> ( $a -> [ 1 ], $b -> [ 1 ] ) } map { [ $_, basename( $_ ) =~ /V([[:xdigit:].]+)__/ ] } @{ $target };
			} else {
				@files = @{ $target };
			}
		}
	}

	if( 'procedure' eq $mode ) {
		if( not( defined( $basepath ) ) ) {
			die( "$fatal Base directory for Stored Procedures not defined\n" );
		} elsif( not -d $basepath ) {
			die( "$fatal Directory '$basepath' does not exist\n" );
		} elsif( not( -s $basepath . '/' . $db . '.metadata' ) ) {
			die( "$fatal Metadata file '" . $db . '.metadata' . "' for database `$db` is not present in directory '$basepath' or cannot be read\n" );
		}
	}

	if( not( $quiet or $silent ) ) {
		pdebug( "Processing " . ( 'procedure' eq $mode ? 'Stored Procedures' : 'schema files' ) . ":\n", undef, TRUE );
		foreach my $item ( @files ) {
			# Intentional spaces...
			pdebug( "  $item\n", undef, TRUE );
		}
		pdebug( "\n" );
	}

	my $tmpdir;
	if( not( $pretend ) and not( $nobackup ) ) {
		$tmpdir = File::Temp -> newdir() or die( "$fatal Temporary directory creation failed [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]: $!, $@\n" );
		pdebug( "Using temporary directory '$tmpdir' ..." );
	}

	my $backupdir;
	{
		my( $sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst ) = localtime( time );
		$year += 1900;
		pdebug( sprintf( "%s starting at %04d/%02d/%02d %02d:%02d.%02d\n\n", $0, $year, $mon, $mday, $hour, $min, $sec ), undef, TRUE ) unless( $quiet or $silent );

		my( $file, $path, $ext ) = fileparse( realpath( $0 ), qr/\.[^.]+/ );
		$backupdir = sprintf( "%s-backup.%04d%02d%02d.%02d%02d%02d", $file, $year, $mon, $mday, $hour, $min, $sec );
	}

	# }}}

	#
	# Create database, but only if --init is used # {{{
	#

	if( defined( $action_init ) and not( $pretend ) ) {
		if( not( $quiet or $silent ) ) {
			pdebug( "\n" );
			pdebug( "'--init' specified, ensuring that database `$db` exists ...\n", undef, TRUE );
		}
		my $dsn = ( defined( $odbcdsn ) ? 'DBI:ODBC:' . $odbcdsn : "DBI:mysql:host=$host;port=$port" );
		my $dbh;
		my $error = dbopen( \$dbh, $dsn, $user, $pass, $strict, { RaiseError => 0, PrintError => 0 } );
		{
			die( "$fatal $error\n" . ' ' x length( $fatal ) . " Is the database instance running?\n" ) if $error;

			if( $syntax eq 'mysql' ) {
				sqldo( \$dbh, "CREATE DATABASE IF NOT EXISTS `$db`" ) or die( "$fatal Failed to create database" . ( ( defined( $dbh -> errstr() ) and length( $dbh -> errstr() ) ) ? ': ' . $dbh -> errstr() : '' ) . ' [' . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
			} elsif( $syntax eq 'vertica' ) {
				if( defined( $vschm ) and length( $vschm ) ) {
					sqldo( \$dbh, "CREATE SCHEMA IF NOT EXISTS \"$vschm\"" ) or die( "$fatal Failed to create Vertica schema '$vschm'" . ( ( defined( $dbh -> errstr() ) and length( $dbh -> errstr() ) ) ? ': ' . $dbh -> errstr() : '' ) . ' [' . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
				} elsif( defined( $db ) and length( $db ) ) {
					sqldo( \$dbh, "CREATE SCHEMA IF NOT EXISTS \"$db\"" ) or die( "$fatal Failed to create Vertica schema '$db'" . ( ( defined( $dbh -> errstr() ) and length( $dbh -> errstr() ) ) ? ': ' . $dbh -> errstr() : '' ) . ' [' . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
				}
			}
		}
		dbclose( \$dbh, undef, undef, TRUE );
	}

	# }}}

	#
	# Open database connection to determine engine type and to ensure metadata tables exist # {{{
	#

	if( not( $quiet or $silent ) ) {
		pdebug( "\n" );
		pdebug( "Connecting to database `$db` to confirm existing version metadata ...\n", undef, TRUE );
	}

	my $dsn = ( defined( $odbcdsn ) ? 'DBI:ODBC:' . $odbcdsn : "DBI:mysql:database=$db;host=$host;port=$port" );
	my $dbversion;

	my $actions;
	$actions -> { 'check' }         = $action_check;
	$actions -> { 'init' }          = $action_init;
	$actions -> { 'migrate' }       = $action_migrate;

	my $variables;
	$variables -> { 'backupdir' }   = $backupdir;
	$variables -> { 'clear' }       = $clear;
	$variables -> { 'compat' }      = $compat;
	$variables -> { 'desc' }        = $desc;
	$variables -> { 'dsn' }         = $dsn;
	$variables -> { 'engine' }      =  undef;
	$variables -> { 'environment' } = $environment;
	$variables -> { 'extinsert' }   = $extinsert;
	$variables -> { 'first' }       =  TRUE;
	$variables -> { 'force' }       = $force;
	$variables -> { 'limit' }       = $limit;
	$variables -> { 'marker' }      = $marker if( $dosub );
	$variables -> { 'mode' }        = $mode;
	$variables -> { 'pretend' }     = $pretend;
	$variables -> { 'progress' }    = $progress;
	$variables -> { 'quiet' }       = $quiet;
	$variables -> { 'silent' }      = $silent;
	$variables -> { 'skipmeta' }    = $skipmeta;
	$variables -> { 'skipdefiner' } = $skipdefiner;
	$variables -> { 'strict' }      = $strict;
	$variables -> { 'tmpdir' }      = $tmpdir;
	$variables -> { 'unsafe' }      = $nobackup;
	$variables -> { 'vschm' }       = $vschm;

	my $dbh;
	my $error = dbopen( \$dbh, $dsn, $user, $pass, $strict, { RaiseError => 0, PrintError => 0 } );
	{
		die( "$fatal $error\n" . ' ' x length( $fatal ) . "(Databases will be auto-created on --init when not in dry-run mode)\n" ) if $error;

		# Apparently '17' (SQL_DBMS_NAME) canonically returns the database
		# instance vendor...
		$engine = lc( $dbh -> get_info( 17 ) );
		if( defined( $engine ) and length( $engine ) and ( 'vertica database' eq $engine ) ) {
			pdebug( "Successfully connected to Vertica database instance\n", undef, TRUE );
			$engine = 'vertica';

			my $path;
			if( defined( $vschm ) and length( $vschm ) ) {
				$verticadb = "$vschm`.`";
				$path = $vschm;
			} elsif( defined( $db ) and length( $db ) ) {
				$verticadb = "$db`.`";
				$path = $db;
			}
			verticasetsearchpath( \$dbh, $searchpath, $user ) if( defined( $path ) and length( $path ) )
		} elsif( defined( $engine ) and length( $engine ) and ( 'mysql' eq $engine ) ) {
			pdebug( "Successfully connected to MySQL database instance\n", undef, TRUE );
		} else {
			$engine = '' unless( defined( $engine ) and length( $engine ) );
			die( "$fatal Unknown database instance '$engine'\n" );
		}
		$variables -> { 'engine' } = $engine;

		#
		# Create {fl,m}yway metadata tables
		#

		# Under MySQL, the database is implicit and you have to 'USE' a
		# different database to perform DDL alterations.
		# Vertica strongly suggests always specifying the database (instance)
		# and schema (database) when deleting a table... although the (auto-
		# generated?) documentation also suggests specifying the column also,
		# which is hopefully simply an oversight...
		if( $clear ) { # and not( $pretend )
			my $flag = $allowunsafe;
			$allowunsafe = TRUE;
			if( 'procedure' eq $mode ) {
				sqldo( \$dbh, "DROP TABLE IF EXISTS `$mywayprocsname`", TRUE );
			} else {
				sqldo( \$dbh, "DROP TABLE IF EXISTS `$verticadb$flywaytablename`", TRUE );
				sqldo( \$dbh, "DROP TABLE IF EXISTS `$verticadb$mywayactionsname`", TRUE );
				sqldo( \$dbh, "DROP TABLE IF EXISTS `$verticadb$mywaytablename`", TRUE );
				sqldo( \$dbh, "DROP TABLE IF EXISTS `$verticadb$mywayhistoryname`", TRUE );
			}
			$allowunsafe = $flag;

			# We could continue here and reprovision the metadata tables...
			# but is this what a user would expect from clearing the meta-
			# data?
			# Ultimately, they'd still need to re-init the database - and
			# this action creates the tables in any case, so not doing so
			# here is simply a time-saving optimisation at best, and no
			# loss at worst.
			dbclose( \$dbh, "Metadata successfully cleared", $db );

			return( TRUE );
		}

		my @tables;
		if( 'vertica' eq $engine ) {
			@tables = (
				  { 'name' => 'Flyway', 'table' => $flywaytablename,  'ddl' => $verticaflywayddl,       'action' => "SELECT * FROM `$verticadb$flywaytablename` ORDER BY `version` DESC LIMIT 5" }
				, { 'name' => 'myway',  'table' => $mywaytablename,   'ddl' => $verticamywayddl,        'action' => "SELECT * FROM `$verticadb$mywaytablename` ORDER BY `started` DESC LIMIT 5" }
				, { 'name' => 'myway',  'table' => $mywayactionsname, 'ddl' => $verticamywayactionsddl, 'action' => "SELECT COUNT(*) FROM `$verticadb$mywayactionsname`" }
				, { 'name' => 'myway',  'table' => $mywayhistoryname, 'ddl' => $verticamywayhistoryddl, 'action' => "SELECT * FROM `$verticadb$mywayhistoryname` ORDER BY `myway_version` DESC" }
			);
		} else {
			@tables = (
				  { 'name' => 'Flyway', 'table' => $flywaytablename,  'ddl' => $flywayddl,       'action' => "SELECT * FROM `$flywaytablename` ORDER BY `version` DESC LIMIT 5" }
				, { 'name' => 'myway',  'table' => $mywaytablename,   'ddl' => $mywayddl,        'action' => "SELECT * FROM `$mywaytablename` ORDER BY `started` DESC LIMIT 5" }
				, { 'name' => 'myway',  'table' => $mywayprocsname,   'ddl' => $mywayprocsddl,   'action' => "SELECT * FROM `$mywayprocsname` ORDER BY `started` DESC LIMIT 5" }
				, { 'name' => 'myway',  'table' => $mywayactionsname, 'ddl' => $mywayactionsddl, 'action' => "SELECT COUNT(*) FROM `$mywayactionsname`" }
				, { 'name' => 'myway',  'table' => $mywayhistoryname, 'ddl' => $mywayhistoryddl, 'action' => "SELECT * FROM `$mywayhistoryname` ORDER BY `myway_version` DESC" }
			);
		}
		foreach my $table ( @tables ) {
			my $name   = $table -> { 'name'   };
			my $tname  = $table -> { 'table'  };
			my $ddl    = $table -> { 'ddl'    };
			my $action = $table -> { 'action' };

			# $compat can only be set if we're deploying to MySQL...
			if( $compat and $ddl =~ m/\stimestamp\([0-6]\)\s/i ) {
				pnote( "\n" );
				pnote( "Removing micro-second precision from definition for <mysql-5.6.4 compatibility ...\n", undef, TRUE );
				$ddl =~ s/(\stimestamp)\([0-6]\)(\s)/$1$2/gi;
			}

			if( $pretend ) {
				psim( "\n" );
				psim( "Would ensure that $name `$tname` table exists.\n" );
			} else {
				if( not( $quiet or $silent ) ) {
					pdebug( "\n" );
					pdebug( "Ensuring that $name `$tname` table exists ...\n", undef, TRUE );
				}
				if( 'vertica' eq $engine ) {
					if( defined( $vschm ) and length( $vschm ) ) {
						$ddl =~ s/__SCHEMA__/\"$vschm\"./g;
					} elsif( defined( $db ) and length( $db ) ) {
						$ddl =~ s/__SCHEMA__/\"$db\"./g;
					} else {
						$ddl =~ s/__SCHEMA__//g;
					}
				}
				# XXX: We don't filter for semicolons embedded within
				#      strings here as we should, we just ensure that
				#      none of the hard-coded SQL above includes such a
				#      thing...
				if( $ddl =~ m/[^\\];/ ) {
					foreach my $statement ( split( /;/, $ddl ) ) {
						if( defined( $statement ) and length( $statement ) and not ( $statement =~ m/^\s*$/ ) ) {
							sqldo( \$dbh, $statement ) or die( "$fatal Table creation failed [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
						}
					}
				} else {
					sqldo( \$dbh, $ddl ) or die( "$fatal Table creation failed [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
				}

				if( $quiet or $silent ) {
					eval {
						if( defined( $action ) and length( $action ) ) {
							sqldo( \$dbh, $action );
						} else {
							sqldo( \$dbh, "DESCRIBE `$tname`" ) if( 'mysql' eq $engine );
						}
					};
					if( $@ ) {
						if( $pretend ) {
							pwarn( "Table `$tname` does not exist, but will be created on a full run" );
						} else {
							die( "$fatal Essential meta-data table `$tname` is missing - cannot continue\n" );
						}
					}
				} else {
					eval {
						if( defined( $action ) and length( $action ) ) {
							formatastable( \$dbh, $action, '   ' );
						} else {
							formatastable( \$dbh, "DESCRIBE `$tname`", '   ' ) if( 'mysql' eq $engine );
							#formatastable( \$dbh, "SELECT * FROM `$tname`", '   ' );
						}
						print( "\n" );
					};
					if( $@ ) {
						if( $pretend ) {
							pwarn( "Table `$tname` does not exist, but will be created on a full run" );
						} else {
							die( "$fatal Essential meta-data table `$tname` is missing - cannot continue\n" );
						}
					}
				}

				# This all only applies to MySQL...
				#
				if( 'mysql' eq $engine ) {
					# Older myway.pl releases lacked a `sqlstarted`
					# attribute on metadata tables, and so could not
					# differentiate between when backups commenced and when
					# we actually started executing a SQL statement - let's fix this ;)
					#
					if( $tname eq $mywaytablename ) {
						my $st = "DESCRIBE `$tname`";
						my $sth = sqlexecute( \$dbh, undef, $st );
						if( not( defined( $sth ) and $sth ) ) {
							my $errstr = $dbh -> errstr();
							pfail( "\n" );
							pfail( "Unable to create statement handle to execute '$st'" . ( defined( $errstr ) and length( $errstr ) ? ": " . $errstr : '' ) . "\n" );
						} else {
							my $foundexecutionstarted = FALSE;

							while( my $ref = $sth -> fetchrow_arrayref() ) {
								my $field = @{ $ref }[ 0 ];
								my $type = @{ $ref }[ 1 ];

								# XXX: Hard-coded table structure :(
								if( $field eq 'sqlstarted' ) {
									$foundexecutionstarted = TRUE;
								}
							}

							if( not( $foundexecutionstarted ) ) {
								# XXX: Hard-coded SQL
								#
								eval {
									if ( sqldo( \$dbh, "ALTER TABLE `$tname` ADD COLUMN `sqlstarted` TIMESTAMP NULL DEFAULT NULL AFTER `started`" ) ) {
										pdebug( "Additional timing column for table `$tname` added\n", undef, TRUE );
									} else {
										pnote( "Additional timing column for table `$tname` could not be added\n", undef, TRUE );
									}
								};
							}

							$sth -> finish();
						}
					}

					# If we've dropped into MySQL compatibility mode
					# previously (as above) then revert the change now...
					#
					if( ( $tname eq $mywayactionsname ) and not( $compat ) ) {
						my $st = "DESCRIBE `$tname`";
						my $sth = sqlexecute( \$dbh, undef, $st );
						if( not( defined( $sth ) and $sth ) ) {
							my $errstr = $dbh -> errstr();
							pfail( "\n" );
							pfail( "Unable to create statement handle to execute '$st'" . ( defined( $errstr ) and length( $errstr ) ? ": " . $errstr : '' ) . "\n" );
						} else {
							my $foundoldstatementtype = FALSE;

							while( my $ref = $sth -> fetchrow_arrayref() ) {
								my $field = @{ $ref }[ 0 ];
								my $type = @{ $ref }[ 1 ];

								# XXX: Hard-coded table structure :(
								if( ( $field eq 'started' ) and ( $type =~ m/timestamp/i ) ) {
									if( lc( $type ) eq 'timestamp' ) {
										# XXX: Hard-coded SQL
										#
										if ( sqldo( \$dbh, "ALTER TABLE `$tname` MODIFY `started` TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP" ) ) {
											pdebug( "MySQL compatibility option on table `$tname` removed\n", undef, TRUE );
										} else {
											pnote( "MySQL compatibility option on table `$tname` could not be removed\n", undef, TRUE );
										}
									}
								} elsif( ( $field eq 'statement' ) and ( $type =~ m/varchar/i ) ) {
									# XXX: Hard-coded SQL
									#
									if ( sqldo( \$dbh, "ALTER TABLE `$tname` MODIFY `statement` LONGTEXT CHARACTER SET 'UTF8' NOT NULL" ) ) {
										pdebug( "Statement length limitations on table `$tname` removed\n", undef, TRUE );
									} else {
										pnote( "Statement length limitations on table `$tname` could not be removed\n", undef, TRUE );
									}
								}
							}

							$sth -> finish();
						}
					}
				}
			}
		}

		metadatamigrateschema( \$dbh, $db, $vschm, $variables );

		my $versions = sqlgetvalues( \$dbh, "SELECT DISTINCT `version` FROM `$verticadb$flywaytablename` WHERE `success` = '1'" );

		if( scalar( @{ $versions } ) ) {
			my @sortedversions = sort { versioncmp( $a, $b ) } @{ $versions };
			$dbversion = pop( @sortedversions );
		}
	}
	dbclose( \$dbh, undef, undef, TRUE );

	$engine = $variables -> { 'engine' };

	# }}}

	#
	# Apply schema file to database # {{{
	#

	my $version = undef;
	my $lastversion = undef;
	$lastversion = $dbversion if( defined( $dbversion ) );

	if( scalar( @files ) ) {
		my $totalfiles = scalar( @files );

		@files = ( shift( @files ) ) if( defined( $action_init ) );

		foreach my $item ( @files ) {
			if( not( -s $item ) ) {
				print( "\n" );
				if( not( -e $item ) ) {
					die( "$fatal File '$item' does not exist\n" );
				} elsif( not( -r $item ) ) {
					die( "$fatal File '$item' cannot be read\n" );
				} elsif( not( -s $item ) ) {
					die( "$fatal File '$item' is empty\n" );
				}
			} else {
				pnote( "Processing file '$item' ...\n", undef, TRUE ) unless( $quiet or $silent );
				eval {
					if( defined( $version ) ) {
						$version = applyschema( $item, $actions, $variables, $auth, $version );
					} else {
						$version = applyschema( $item, $actions, $variables, $auth );
					}
					if( not( defined( $version ) ) ) {
						$lastversion = undef;

						if( $pretend ) {
							die( "$fatal BUG: applyschema() returned undef during simulation [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
						}
					} elsif( ref( $version ) eq 'SCALAR' ) {
						$version = ${ $version };
						if( not( $version eq '__NOT_APPLIED__' ) ) {
							$lastversion = $version;
						}
						pnote( "This session now has base " . ( ( 'procedure' eq $mode ) ? 'Stored Procedure ' : '' ) . "version '$version'\n", undef, TRUE ) unless( $quiet or $silent );
					} elsif( ref( $version ) eq '' ) {
						if( not( $version ) ) {
							die( "$fatal Schema application failed to return a valid version [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
						}
						$version = '__NOT_APPLIED__';
					} else {
						pfatal( "\n" );
						pfatal( "applyschema() returned invalid data '$version':\n" );
						print Data::Dumper -> Dump( [ $version ], [ qw( *version ) ] );
						die( "$fatal applyschema() returned invalid response [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
					}
				};
				if( $@ ) {
					$lastversion = undef;

					if( $pretend ) {
						die( "$fatal BUG: applyschema() failed during simulation [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]: $@\n" );
					}

					my $error = $@;
					if( not( DEBUG or ( $verbosity > 2 ) ) ) { # debug(3)
						$error =~ s/ at .+ line \d+\.$//;
						$error = join( ' ', split( /\s*\n+\s*/, $error ) );
						chomp( $error );
						if( $error =~ m/[A-Z]+:\s/ ) {
							my $firstword = ( split( /\s/, $error ) )[ 0 ];
							$error =~ s/^$firstword\s+//;
						}
					}
					if( defined( $action_init ) ) {
						# Use $warning here rather than $fatal, as this is an expected
						# error when invoked from applyschema.sh (or, indeed, whenever
						# myway.pl is called with --init against an already-initialised
						# database)...
						#
						die( "\n$warning Failed to initialise from schema file '$item':\n" . ( ' ' x ( length( $fatal ) + 1 ) ) . "$error\n" );
					} else {
						die( "\n$fatal Error when applying schema file '$item':\n" . ( ' ' x ( length( $fatal ) + 1 ) ) . "$error\n" );
					}
				}
				$variables -> { 'first' } = FALSE if( $variables -> { 'first' } );
			}
		}

		# If we're only processing the baseline file (with possible
		# 'Restore' directive) then we need to check that we've reached
		# the intended version, if supplied.  Otherwise, the '--init'
		# step is performed separately from the remainder of the
		# migration and so we may be behind for now...
		# (This is thought to be less confusing than requiring separate
		# target-limit arguments for the initialisation versus the
		# migration, especially given that --init does not have to take
		# an argument itself)
		#
		if( defined( $action_init ) and ( not( defined( $limit ) ) or ( $totalfiles > 1 ) ) ) {
			# No action, we're good!
		} elsif( ( 'procedure' eq $mode ) or not( defined( $limit ) ) ) {
			# FIXME: We need to be able to specify Stored Procedure numbers separately...

			if( defined( $version ) and not( $version eq '__NOT_APPLIED__' ) ) {
				print( "$info Database " . ( ( $engine eq 'vertica' ) ? "schema $vschm" : "$db" ) . " is up to date at schema version '$version'\n" ) unless( $quiet or $silent );
			} elsif( defined( $lastversion ) ) {
				print( "$info Database " . ( ( $engine eq 'vertica' ) ? "schema $vschm" : "$db" ) . " is up to date at schema version '$lastversion'\n" ) unless( $quiet or $silent );
			} else {
				die( "$fatal Schema migration incomplete\n" );
			}
		} else { # defined( $limit ) and not( ( 'procedure' eq $mode ) ) and ( 1 == $totalfiles or not( defined( $action_init ) ) )
			if( defined( $version ) and ( $version eq '__NOT_APPLIED__' ) ) {
				if( defined( $lastversion ) ) {
					$version = $lastversion;
				} else {
					$limit =~ s/(?:\.0+)+$//;
					die( "$fatal Having processed " . scalar( @files ) . " file(s) (none of which were applied), database schema version is still behind target version '$limit'\n" );
				}
			}
			if( not( defined( $version ) ) ) {
				die( "$fatal Schema migration incomplete\n" );
			} elsif( $version eq $limit ) {
				print( "$info Database " . ( ( $engine eq 'vertica' ) ? "schema $vschm" : "$db" ) . " is up to date at schema version '$version'\n" ) unless( $quiet or $silent );
			} else {
				my( $vcode, $vchange, $vstep, $vhotfix ) = ( $version =~ m/^([[:xdigit:]]+)(?:\.(\d+)(?:\.(\d+)(?:\.(\d+))?)?)?$/ );
				my( $lcode, $lchange, $lstep, $lhotfix ) = ( $limit =~ m/^([[:xdigit:]]+)(?:\.(\d+)(?:\.(\d+)(?:\.(\d+))?)?)?$/ );
				$vchange = 0 unless( defined( $vchange ) and $vchange );
				$vstep = 0 unless( defined( $vstep ) and $vstep );
				$vhotfix = 0 unless( defined( $vhotfix ) and $vhotfix );
				$lchange = 0 unless( defined( $lchange ) and $lchange );
				$lstep = 0 unless( defined( $lstep ) and $lstep );
				$lhotfix = 0 unless( defined( $lhotfix ) and $lhotfix );

				my $sv = "$vcode.$vchange.$vstep.$vhotfix";
				my $lv = "$lcode.$lchange.$lstep.$lhotfix";

				( my $sst = $sv ) =~ s/(?:\.0+)+$//;
				( my $sl = $lv ) =~ s/(?:\.0+)+$//;

				if( $sst eq $sl ) {
					print( "$info Database " . ( ( $engine eq 'vertica' ) ? "schema $vschm" : "$db" ) . " is up to date at schema version '$version'\n" ) unless( $quiet or $silent );
				} else {
					my @sortedversions = sort { versioncmp( $a, $b ) } ( $sst, $sl );
					my $latest = pop( @sortedversions );
					if( $latest eq $sl ) {
						die( "$fatal Having processed " . scalar( @files ) . " file(s), database schema version '$sst' is still behind target version '$sl'\n" );
					} else {
						die( "$fatal Logic error - database schema version '$sst' is ahead of target version '$sl'\n" );
					}
				}
			}
		}
	} else {
		eval {
			die( "$fatal applyschema() failed [" . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" ) unless( applyschema( $file, $actions, $variables, $auth ) );
		};
		if( $@ ) {
			my $error;
			chomp( $error = $@ );
			die( ( ( defined( $error ) and length( $error ) ) ? "$error" : "$fatal applyschema() failed" ) . ' [' . ( caller( 0 ) )[ 3 ] . ':' . __LINE__ . "]\n" );
		}
	}

	if( not( 'procedure' eq $mode ) and defined( $tmpdir ) ) {
		if( $keepbackups ) {
			## no critic (ProhibitLeadingZeros)
			make_path( $backupdir, {
				  mode		=> 0775
				, verbose	=> FALSE
				, error		=> \my $errors
			} );
			if( scalar( @{ $errors } ) ) {
				foreach my $entry ( @{ $errors } ) {
					my( $dir, $message ) = %{ $entry };
					if( length( $message ) ) {
						pfail( "Error creating directory '$dir': $message\n" );
					} else {
						pfail( "make_path general error: $message\n" );
					}
				}
				return( undef );
			}

			pdebug( "\n" );
			pdebug( "Moving temporary backups to '$backupdir' ...\n", undef, TRUE );
			foreach my $file ( glob( qq( "${tmpdir}/*" ) ) ) {
				move( $file, $backupdir . '/' ) or pwarn( "Failed to move file '$file' to destination '$backupdir/': $@\n", LOGMAX );
			}
			pdebug( "Backups relocated\n", undef, TRUE );
		}
	}

	# }}}

	exit( 0 );
} # main # }}}

main( @ARGV );

1;

} # package myway;

{
# Include Percona SQLParser # {{{

# ###########################################################################
# SQLParser package $Revision$
# ###########################################################################

# Package: SQLParser
# SQLParser parses common MySQL SQL statements into data structures.
# This parser is MySQL-specific and intentionally meant to handle only
# "common" cases.  Although there are many limiations (like UNION, CASE,
# etc.), many complex cases are handled that no other free, Perl SQL
# parser at the time of writing can parse, notably subqueries in all their
# places and varieties.
#
# This package has not been profiled and since it relies heavily on
# mildly complex regex, so do not expect amazing performance.
#
# See SQLParser.t for examples of the various data structures.  There are
# many and they vary a lot depending on the statment parsed, so documentation
# in this file is not exhaustive.
#
# This package differs from QueryParser because here we parse the entire SQL
# statement (thus giving access to all its parts), whereas QueryParser extracts
# just needed parts (and ignores all the rest).

package SQLParser;

## no critic (ProhibitSubroutinePrototypes, ProhibitMutatingListFunctions)

use strict;
use warnings FATAL => 'all';

use English qw(-no_match_vars);

use constant MKDEBUG => $ENV{MKDEBUG} || 0;
use constant SQLDEBUG => $ENV{SQLDEBUG} || 0;

use constant DEFDELIM => ';';

# Used by improved parse_values
use Regexp::Common;

use Data::Dumper;
$Data::Dumper::Indent    = 1;
$Data::Dumper::Sortkeys  = 1;
$Data::Dumper::Quotekeys = 0;

sub new( $% );
sub parse( $$;$ );

sub parse_alter( $$ );
sub parse_call( $$ );
sub parse_create( $$ );
sub parse_delete( $$ );
sub parse_drop( $$ );
#sub parse_grant( $$ );
sub parse_insert( $$ ); # Alias of parse_replace
sub parse_select( $$ );
sub parse_update( $$ );

sub parse_add( $$ );
sub parse_change( $$ );
sub parse_character_set( $$ );
sub parse_columns( $$ );
sub parse_from( $$ ); # Alias of parse_into, parse_tables
sub parse_group_by( $$ );
sub parse_having( $$ );
sub parse_identifier( $$$ );
sub parse_identifiers( $$ );
sub parse_limit( $$ );
sub parse_modify( $$ );
sub parse_order_by( $$ );
sub parse_set( $$ );
sub parse_table_reference( $$ );
sub parse_values( $$ );
sub parse_where( $$ );

sub clean_query( $$ );
sub is_identifier( $$ );
sub normalize_keyword_spaces( $$ );
sub remove_functions( $$ );
sub remove_subqueries( $$ );
sub remove_using_columns( $$ );
sub replace_function( $$ );
sub set_Schema( $$ );
sub split_unquote( $$$ );
sub _parse_clauses( $$ );
sub _parse_csv( $$$ );
sub _parse_query( $$$$$ );
sub _is_constant( $$ );
sub _d;

# Basic identifers for database, table, column and function names.
my $quoted_ident   = qr/`[^`]+`/;
# '' is a valid constant...
#my $constant_ident   = qr/'[^']+'/;
my $constant_ident   = qr/'[^']*'/;
#my $unquoted_ident = qr/
#	\@{0,2}         # optional @ or @@ for variables
#	\w+             # the ident name
#	(?:\s*\([^\)]*\))? # optional function params
#/x;
my $unquoted_ident = qr/
	\@{0,2}         # optional @ or @@ for variables
	\w+             # the ident name
	(?:\s*$RE{ balanced }{ -parens => '()' })? # optional function params
/x;

my $ident_alias = qr/
  \s+                                 # space before alias
  (?:(AS)\s+)?                        # optional AS keyword
  ((?>$quoted_ident|$unquoted_ident)) # alais
/xi;

#my $function_ident = qr/
#	\s*
#	(
#		(?:\b\w+|`\w+`)\s*    # function name
#		\(                    # opening parenthesis
#		[^\)]*                # function args, if any
#		\)                    # closing parenthesis
#	)
#/x;
my $function_ident = qr/
	(
		(?:\b\w+|`\w+`)\s*    # function name
		$RE{ balanced }{ -parens => '()' }
	)
/x;

# A table is identified by 1 or 2 identifiers separated by a period
# and optionally followed by an alias.  See parse_table_reference()
# for why an optional index hint is not included here.
my $table_ident = qr/(?:
	((?:(?>$quoted_ident|$unquoted_ident)\.?){1,2}) # table
	(?:$ident_alias)?                               # optional alias
)/xo;

# A column is identified by 1 to 3 identifiers separated by periods
# and optionally followed by an alias.
my $column_ident = qr/(?:
	((?:(?>$quoted_ident|$constant_ident|$function_ident|$unquoted_ident|\*)\.?){1,3}) # column
	(?:$ident_alias)?                                  # optional alias
)/xo;

my %ignore_function = (
	NOT   => 1,
	IN    => 1,
	INDEX => 1,
	KEY   => 1,
);

# Sub: new # {{{
#   Create a SQLParser object.
#
# Parameters:
#   %args - Arguments
#
# Optional Arguments:
#   Schema - <Schema> object.  Can be set later by calling <set_Schema()>.
#
# Returns:
#   SQLParser object
sub new( $% ) {
	my ( $class, %args ) = @_;
	my $self = {
		  %args
		,  delimiter => DEFDELIM
	};
	return( bless( $self, $class ) );
} # new # }}}

# Sub: parse # {{{
#   Parse a SQL statment.   Only statements of $allowed_types are parsed.
#   This sub recurses to parse subqueries.
#
# Parameters:
#   $query - SQL statement
#
# Returns:
#   A complex hashref of the parsed SQL statment.  All keys and almost all
#   values are lowercase for consistency.  The struct is roughly:
#   (start code)
#   {
#     type       => '',     # one of $allowed_types
#     clauses    => {},     # raw, unparsed text of clauses
#     <clause>   => struct  # parsed clause struct, e.g. from => [<tables>]
#     keywords   => {},     # LOW_PRIORITY, DISTINCT, SQL_CACHE, etc.
#     functions  => {},     # MAX(), SUM(), NOW(), etc.
#     select     => {},     # SELECT struct for INSERT/REPLACE ... SELECT
#     subqueries => [],     # pointers to subquery structs
#   }
#   (end code)
#   It varies, of course, depending on the query.  If something is missing
#   it means the query doesn't have that part.  E.g. INSERT has an INTO clause
#   but DELETE does not, and only DELETE and SELECT have FROM clauses.  Each
#   clause struct is different; see their respective parse_CLAUSE subs.
sub parse( $$;$ ) {
	my ( $self, $query, $delim ) = @_;
	return unless $query;

	$delim = DEFDELIM unless( defined( $delim ) and length( $delim ) );

	#MKDEBUG && _d('Query:', $query);

	# Only these types of statements are parsed.
	my $allowed_types = qr/(?:
		 ALTER
		|CALL
		|CREATE
		|DELETE
		|DELIMITER
		|DROP
		|INSERT
		|REPLACE
		|SELECT
		|UPDATE
	)/xi;
		#|GRANT

	# Flatten and clean query.
	$query = $self->clean_query($query);

	# Remove first word, should be the statement type.  The parse_TYPE subs
	# expect that this is already removed.
	my $type;
	if ( $query =~ s/^(\w+)\s+// ) {
		$type = lc $1;
		MKDEBUG && _d('Query type:', $type);
		die "Cannot parse " . uc($type) . " queries"
			unless $type =~ m/$allowed_types/i;
	}
	elsif( $query eq $delim ) {
		# This is a bit of a hack to catch fragments after hints which
		# may have been passed through this far...
		return( undef );
	}
	else {
		die "Query '$query' does not begin with a word";  # shouldn't happen
	}

	$query = $self->normalize_keyword_spaces($query);

	MKDEBUG && _d('Normalised query:', $type, $query);

	if ( 'delimiter' eq $type ) {
		my @terms = split( /\s+/, $query );
		$self->{delimiter} = shift( @terms );
		my $struct;
		$struct->{type} = $type;
		$struct->{delimiter} = $self->{delimiter};
		$struct->{unknown} = join( ' ', @terms );
		return $struct;
	}

	# If query has any subqueries, remove/save them and replace them.
	# They'll be parsed later, after the main outer query.
	my @subqueries;
	if ( $query =~ m/(\(\s*SELECT\s+)/i ) {
		MKDEBUG && _d('Removing subqueries');
		@subqueries = $self->remove_subqueries($query);
		$query      = shift @subqueries;
	}
	elsif ( $type eq 'create' && $query =~ m/\s+SELECT/ ) {
		# XXX: create PROCEDURE may contain SELECT sub-queries, but
		#      it cannot be assumed that they will continue to the end
		#      of the line.  Delimiter-parsing in this instance is not
		#      trivial, as the active delimiter may differ from ';',
		#      which may also appear (without delimiting) in a quoted
		#      string...
		MKDEBUG && _d('CREATE..SELECT');
		#($subqueries[0]->{query}) = $query =~ m/\s+(SELECT\s+.+)/;
		#$query =~ s/\s+SELECT\s+.+//;
		# XXX: Let's give is a try anyway...
		($subqueries[0]->{query}) = $query =~ m/\s+(SELECT\s+[^;]+)/;
		$query =~ s/\s+SELECT\s+[^;]+//;
	}

	# Parse raw text parts from query.  The parse_TYPE subs only do half
	# the work: parsing raw text parts of clauses, tables, functions, etc.
	# Since these parts are invariant (e.g. a LIMIT clause is same for any
	# type of SQL statement) they are parsed later via other parse_CLAUSE
	# subs, instead of parsing them individually in each parse_TYPE sub.
	my $parse_func = "parse_$type";
	my $struct     = $self->$parse_func($query);
	if ( !$struct ) {
		MKDEBUG && _d($parse_func, 'failed to parse query');
		return;
	}
	$struct->{type} = $type;
	$self->_parse_clauses($struct);
	# TODO: parse functions

	if ( @subqueries ) {
		MKDEBUG && _d('Parsing subqueries');
		foreach my $subquery ( @subqueries ) {
			my $subquery_struct = $self->parse($subquery->{query});
			@{$subquery_struct}{keys %$subquery} = values %$subquery;
			push @{$struct->{subqueries}}, $subquery_struct;
		}
	}

	MKDEBUG && _d('Query struct:', Dumper($struct));
	return $struct;
} # parse # }}}

# Functions to handle top-level SQL elements

sub parse_alter( $$ ) { # {{{
	my ( $self, $query ) = @_;

	my $keywords = qr/(ONLINE|OFFLINE|IGNORE)/i;
	my ( $type, @query ) = split( /\s+/, $query );
	$query = join( ' ', @query );

	if( $type =~ m/TABLE/i ) {
		$query =~ s/^\s*TABLE\s+//i;

		#my $clauses = qr/(ADD(?:\s+(?:COLUMN|INDEX|KEY|CONSTRAINT|(?:CONSTRAINT\s+)?(?:(?:PRIMARY|FOREIGN)\s+KEY|UNIQUE\s+(?:INDEX|KEY))|(?:FULLTEXT|SPATIAL)\s+(?:INDEX|KEY)))?|(?:ALTER|CHANGE|MODIFY)(?:\s+COLUMN)?|DROP(?:\s+(?:COLUMN|INDEX|(?:(?:PRIMARY|FOREIGN)\s+)?KEY))?|(?:DIS|EN)ABLE\s+KEYS|RENAME\s+(?:TO|AS)?|ORDER\s+BY|CONVERT\s+TO\s+CHARACTER\s+SET|(?:DEFAULT\s+)?CHARACTER\s+SET(?:\s+=)?|(?:DISCARD|IMPORT)\s+TABLESPACE|(?:ADD|DROP|COALESCE|REORGANISE|ANALYSE|CHECK|OPTIMIZE|REBUILD|REPAIR)\s+PARTITION|PARTITION\s+BY|REMOVE\s+PARTITIONING)/i;
		my $clauses = qr/(ADD|(?:ALTER|CHANGE|MODIFY)|DROP|(?:DIS|EN)ABLE\s+KEYS|RENAME|ORDER\s+BY|CONVERT\s+TO\s+CHARACTER\s+SET|(?:DEFAULT\s+)?CHARACTER\s+SET|(?:DISCARD|IMPORT)\s+TABLESPACE|(?:ADD|DROP|COALESCE|REORGANISE|ANALYSE|CHECK|OPTIMIZE|REBUILD|REPAIR)\s+PARTITION|PARTITION\s+BY|REMOVE\s+PARTITIONING)/i;
		return $self->_parse_query($query, $keywords, 'tables', $clauses);

	}
} # parse_alter # }}}

sub parse_call( $$ ) { # {{{
	my ($self, $query) = @_;
	my ($name) = $query =~ m/
		(\S+)(?:\s*\(.*\)\s*)?
	/xi;
	$name =~ s/['"]//g;
	$name =~ s/\(\s*\)$//;
	return {
		object  => 'procedure',
		name    => $name,
		unknown => undef,
	};
} # parse_call # }}}

sub parse_create( $$ ) { # {{{
	my ($self, $query) = @_;
	return unless $query;

	# FIXME: This function will only really parse 'CREATE TABLE', and even
	#        then it doesn't give much information :(

	my ($obj, $name) = $query =~ m/
		(\S+)\s+
		(?:IF\s+NOT\s+EXISTS\s+)?
		(\S+)\s*.*$
	/xi;
	$name =~ s/['"]//g;
	$name =~ s/\(\s*\)$//;
	$name =~ s/[;(]$//;
	my $struct = {
		object  => lc $obj,
		name    => $name,
		unknown => undef,
	};

	if( lc( $obj ) eq 'procedure' ) {
		$query =~ s/\sBEGIN\s/ BEGIN; /gi;
		$query =~ s/\sEND( IF)\s/ END$1; /gi;
		$query =~ s/\sTHEN\s/ THEN; /gi;
		MKDEBUG && _d('Filtered query:', $query);
		my @subqueries = split( /;/, DEFDELIM . $query . DEFDELIM );
		for( my $n = 0 ; $n < scalar( @subqueries ) ; $n++ ) {
			my $subquery = $subqueries[ $n ];
			if( 0 == $n ) {
				$subquery = '';
			} elsif( 1 == $n ) {
				$subquery =~ s/^.*?BEGIN//;
				MKDEBUG && _d('Filtered initial sub-query:', $subquery);
			} elsif( scalar( @subqueries ) - 1 == $n ) {
				$subquery =~ s/END.*?$//;
				MKDEBUG && _d('Filtered final sub-query:', $subquery);
			}
			$subquery =~ s/^\s+//;
			$subquery =~ s/\s+$//;
			if( length( $subquery ) ) {
				MKDEBUG && _d('Parsing CREATE PROCEDURE sub-query:', $subquery);
				my $subquery_struct;
				eval {
					$subquery_struct = $self->parse($subquery);
				};
				if( $@ ) {
					$subquery_struct = { unknown => $subquery };
				}
				push @{$struct->{subqueries}}, $subquery_struct;
			}
		}
	}

	MKDEBUG && _d('Create struct:', Dumper($struct));
	return $struct;
} # parse_create # }}}

sub parse_delete( $$ ) { # {{{
	my ( $self, $query ) = @_;
	if ( $query =~ s/FROM\s+//i ) {
		my $keywords = qr/(LOW_PRIORITY|QUICK|IGNORE)/i;
		my $clauses  = qr/(FROM|WHERE|ORDER\s+BY|LIMIT(?:\s+\d+))/i;
		return $self->_parse_query($query, $keywords, 'from', $clauses);
	}
	else {
		die "DELETE without FROM: $query";
	}
} # parse_delete # }}}

sub parse_drop( $$ ) { # {{{
	my ($self, $query) = @_;

	# Keywords are expected to be at the start of the query, so these
	# that appear at the end are handled separately.  Afaik, SELECT are
	# the only statements with optional keywords at the end.  These
	# also appear to be the only keywords with spaces instead of _.
	my @keywords;
	my $final_keywords = qr/(RESTRICT|CASCADE)/i;
	1 while $query =~ s/\s+$final_keywords/(push @keywords, $1), ''/gie;

	my $struct;

	my $delimiter = $self->{delimiter};
	$delimiter = DEFDELIM unless( defined( $delimiter ) and length( $delimiter ) );
	( my $terms = $query ) =~ s/\s*\Q$delimiter\E\s*$//;
	#( my $terms = $query ) =~ s/\s*$delimiter\s*$//;
	$terms =~ s/IF\s+EXISTS//;

	my( $type, @objects ) = split( /\s+/, $terms );
	if( $type =~ m/TEMPORARY/i ) {
		if( not( shift( @objects ) =~ m/TABLE/i ) ) {
			die "TEMPORARY without TABLE: $query";
		} else {
			$struct->{keywords}->{temporary} = 1;
			$type = 'TABLE';
		}
	}
	if( 'LOGFILE' eq uc( $type ) ) {
		$type .= ' ' . shift( @objects );
		if( not( 'LOGFILE GROUP' eq $type ) ) {
			die "LOGFILE without GROUP: $query";
		}
	}

	$struct->{object}  = lc( $type );

	if( uc( $type ) =~ m/DATABASE|EVENT|FUNCTION|PROCEDURE|SERVER|TRIGGER/ ) {
		if( scalar( @objects ) > 1 ) {
			die "DROP " . uc( $type ) . " supports only one parameter: $query";
		}
		$struct->{name} = shift( @objects );

	} elsif( uc( $type ) eq 'INDEX' ) {
		# Handle 'ON tbl_name' plus optional 'ALGORITHM [=] {DEFAULT|INPLACE|COPY} | LOCK [=] {DEFAULT|NONE|SHARED|EXCLUSIVE} ...'
		my $name = shift( @objects );

		# XXX: Statements of the form:
		#          ALTER TABLE `foo` DROP INDEX `bar`
		#      ... will now invoke this code-path, so DROP without ON
		#      is valid iff we're part of an ALTER query. Unfortunately
		#      there's no way to tell whether this is the case at this.
		#
		if( scalar( @objects ) and not( 'ON' eq shift( @objects ) ) ) {
			die "DROP " . uc( $type ) . " without ON: $query";
		}

		my $tbl = shift( @objects );
		$struct->{name} = $self->parse_identifier('column', $name);
		$struct->{tbl} = $self->parse_identifier('table', $tbl) if( defined( $tbl ) );

		while( scalar( @objects ) ) {
			my $term = shift( @objects );
			if( not( $term =~ m/ALGORITHM|LOCK/i ) ) {
				die "DROP " . uc( $type ) . " unrecognised parameter $term: $query";
			}
			my $argument = shift( @objects );
			if( defined( $argument ) and ( '=' eq $argument ) ) {
				$argument = shift( @objects );
			}
			if( defined( $argument ) ) {
				$struct->{keywords}->{ lc( $term ) } = $argument;
			} else {
				die "Parameter $term requires an argument: $query";
			}
		}

	} elsif( uc( $type ) =~ m/LOGFILE GROUP|TABLESPACE/ ) {
		# Handle optional 'ENGINE [=] engine_name'
		$struct->{name} = shift( @objects );

		while( scalar( @objects ) ) {
			my $term = shift( @objects );
			if( not( $term =~ m/ENGINE/i ) ) {
				die "DROP " . uc( $type ) . " unrecognised parameter $term: $query";
			}
			my $argument = shift( @objects );
			if( defined( $argument ) and ( '=' eq $argument ) ) {
				$argument = shift( @objects );
			}
			if( defined( $argument ) ) {
				$struct->{keywords}->{ lc( $term ) } = $argument;
			} else {
				die "Parameter $term requires an argument: $query";
			}
		}

	} else {
		if( 1 == scalar( @objects ) ) {
			$struct->{name} = shift( @objects );
		} else {
			# TODO: Are commas in quoted object names valid?
			my $names = split( /,\s+/, join( ' ', @objects ) );
			$struct->{name} = $names;
		}
	}

	$struct->{unknown} = undef;

	# Add final keywords, if any.
	map { s/ /_/g; $struct->{keywords}->{lc $_} = 1; } @keywords;

	return $struct;
} # parse_drop # }}}

#sub parse_grant( $$ ) { # {{{
#	my ($self, $query) = @_;
#
#	my $keywords = qr/(
#		 ALL
#		|ALL\s+PRIVILEGES
#		|ALTER
#		|ALTER\s+ROUTINE
#		|CREATE
#		|CREATE\s+ROUTINE
#		|CREATE\s+TEMPORARY\s+TABLES
#		|CREATE\s+USER
#		|CREATE\s+VIEW
#		|DELETE
#		|DROP
#		|EVENT
#		|EXECUTE
#		|FILE
#		|GRANT\s+OPTION
#		|INDEX
#		|INSERT
#		|LOCK\s+TABLES
#		|PROCESS
#		|REFERENCES
#		|RELOAD
#		|RECPLICATION\s+CLIENT
#		|REPLICATION\s+SLAVE
#		|SELECT
#		|SHOW\s+DATABASES
#		|SHOW\s+VIEW
#		|SHUTDOWN
#		|SUPER
#		|TRIGGER
#		|UPDATE
#		|USAGE
#	)/xi;
#	my $clauses  = qr/(ON|TO|REQUIRE|AND|WITH)/i;
#
#	return $self->_parse_query($query, $keywords, 'grants', $clauses);
#} # parse_grant # }}}

sub parse_insert( $$ ) { # {{{
	my ( $self, $query ) = @_;
	return unless $query;

	MKDEBUG && _d('Parsing INSERT/REPLACE', $query);

	my $struct = {};

	my $delimiter = $self->{delimiter};
	$delimiter = DEFDELIM unless( defined( $delimiter ) and length( $delimiter ) );
	$query =~ s/\s*\Q$delimiter\E\s*$//;
	#$query =~ s/\s*$delimiter\s*$//;

	# Save, remove keywords.
	my $keywords   = qr/(LOW_PRIORITY|DELAYED|HIGH_PRIORITY|IGNORE)/i;
	1 while $query =~ s/$keywords\s+/$struct->{keywords}->{lc $1}=1, ''/gie;

	if ( $query =~ m/ON DUPLICATE KEY UPDATE (.+)/i ) {
		my $values = $1;
		die "No values after ON DUPLICATE KEY UPDATE: $query" unless $values;
		$struct->{clauses}->{on_duplicate} = $values;
		MKDEBUG && _d('Clause: on duplicate key update', $values);

		# This clause can be confused for JOIN ... ON in INSERT-SELECT queries,
		# so we remove the ON DUPLICATE KEY UPDATE clause after extracting its
		# values.
		$query =~ s/\s+ON DUPLICATE KEY UPDATE.+//;
	}

	# Parse INTO clause.  Literal "INTO" is optional.
#	if ( my @into = ($query =~ m/
#				(?:INTO\s+)?            # INTO, optional
#				(`[^`]+`|[^\s(]+?)\s*   # table ref
#				(\([^)]+\)\s+)?         # column list, optional
#				(VALUES?|SET|SELECT)\s* # start of next caluse
#			/xgci)
#	) {

	( my $string = $query ) =~ s/^\s*INTO\s+//;

	my $tbl;
	if( $string =~ m/^\s*`([^`]+)`/ and defined( $1 ) and length( $1 ) ) {
		$tbl = $1;
		MKDEBUG && _d('Found quoted table name', $tbl);
		$string =~ s/^\s*`\Q$tbl\E`\s*//;
	} elsif( $string =~ m/^\s*([^\s(]+?)[\s(]/ and defined( $1 ) and length( $1 ) ) {
		$tbl = $1;
		MKDEBUG && _d('Found table name', $tbl);
		$string =~ s/^\s*\Q$tbl\E\s*//;
	} else {
		die "INSERT/REPLACE without table: $query";
	}
	$struct->{clauses}->{into} = $tbl;
	MKDEBUG && _d('Clause: into', $tbl, ', string', $string);

	if( $string =~ m/^\s*(\(.*\))\s+(?:VALUES?|SET|SELECT)\s*/ ) {
		my @input = split( //, $1 );
		my @output;
		my $escaped = 0;
		my $quoted = 0;
		my $bra = 0;
		my $ket = 0;

		EXTRACT: foreach my $character ( @input ) {
			push( @output, $character );

			if( $character eq ')' and not( $quoted ) and ( $ket <= $bra ) ) {
				last EXTRACT;
			} else {
				if( $character eq "'" ) {
					if( not( $escaped ) ) {
						$quoted = not( $quoted );
					}
				} elsif( $character eq '(' ) {
					$bra++;
				} elsif( $character eq ')' ) {
					$ket++;
				}
				if( $character eq '\\' ) {
					$escaped = 1;
				} else {
					$escaped = 0;
				}
			}
		}
		my $cols = join( '', @output );
		$cols =~ s/^\(//;
		$cols =~ s/\)$//;
		if ( $cols ) {
			$struct->{clauses}->{columns} = $cols;
			MKDEBUG && _d('Clause: columns', $cols);

			$string =~ s/^\s*\(\s*\Q$cols\E\s*\)\s*//;
			#$string =~ s/^\s*\(\s*$cols\s*\)\s*//;
		} else {
			# Insert into no columns!?
			# Can apparently be used to create a new ID in an
			# auto-increment column of a table...
			$string =~ s/^\s*\(\s*\)\s*//;
		}
	}

	my @components = split( /\s+/, $string );
	my $next_clause = lc( shift( @components ) );  # VALUES, SET or SELECT
	die "INSERT/REPLACE without clause after table: $query"
		unless $next_clause;
	$next_clause = 'values' if $next_clause eq 'value';
	my ($values) = ($string =~ m/^\s*\Q$next_clause\E\s*(.*)$/i);
	#my ($values) = ($string =~ m/^\s*$next_clause\s*(.*)$/i);
	die "INSERT/REPLACE without values: $query" unless $values;
	$struct->{clauses}->{$next_clause} = $values;
	MKDEBUG && _d('Clause:', $next_clause, $values);

	# Save any leftovers.  If there are any, parsing missed something.
	($struct->{unknown}) = ($string =~ m/^\s*\Q$next_clause\E\s*\Q$values\E\s*(.*)$/i);
	#($struct->{unknown}) = ($string =~ m/^\s*\Q$next_clause\E\s*$values\s*(.*)$/i);
	#($struct->{unknown}) = ($string =~ m/^\s*$next_clause\s*$values\s*(.*)$/i);

#	if ( my @into = ($query =~ m/
#				(?:INTO\s+)?            # INTO, optional
#				(.+?)\s+                # table ref
#				(\([^\)]+\)\s+)?        # column list, optional
#				(VALUE.?|SET|SELECT)\s+ # start of next caluse
#			/xgci)
#	) {
#		my $tbl  = shift @into;  # table ref
#		$struct->{clauses}->{into} = $tbl;
#		MKDEBUG && _d('Clause: into', $tbl);
#
#		my $cols = shift @into;  # columns, maybe
#		if ( $cols ) {
#			$cols =~ s/[\(\)]//g;
#			$struct->{clauses}->{columns} = $cols;
#			MKDEBUG && _d('Clause: columns', $cols);
#		}
#
#		my $next_clause = lc(shift @into);  # VALUES, SET or SELECT
#		die "INSERT/REPLACE without clause after table: $query"
#			unless $next_clause;
#		$next_clause = 'values' if $next_clause eq 'value';
#		my ($values) = ($query =~ m/\G(.+)/gci);
#		die "INSERT/REPLACE without values: $query" unless $values;
#		$struct->{clauses}->{$next_clause} = $values;
#		MKDEBUG && _d('Clause:', $next_clause, $values);
#	}
#
#	# Save any leftovers.  If there are any, parsing missed something.
#	($struct->{unknown}) = ($query =~ m/\G(.+)/);

	return $struct;
} # parse_insert
{
	# Suppress warnings like "Name "SQLParser::parse_set" used only once:
	# possible typo at SQLParser.pm line 480." caused by the fact that we
	# don't call these aliases directly, they're called indirectly using
	# $parse_func, hence Perl can't see their being called a compile time.
	no warnings;
	# INSERT and REPLACE are so similar that they are both parsed
	# in parse_insert().
	*parse_replace = \&parse_insert;
} # }}}

sub parse_select( $$ ) { # {{{
	my ( $self, $query ) = @_;

	# Keywords are expected to be at the start of the query, so these
	# that appear at the end are handled separately.  Afaik, SELECT are
	# the only statements with optional keywords at the end.  These
	# also appear to be the only keywords with spaces instead of _.
	my @keywords;
	my $final_keywords = qr/(FOR\s+UPDATE|LOCK\s+IN\s+SHARE\s+MODE)/i;
	1 while $query =~ s/\s+$final_keywords/(push @keywords, $1), ''/gie;

	my $keywords = qr/(
		 ALL
		|DISTINCT
		|DISTINCTROW
		|HIGH_PRIORITY
		|STRAIGHT_JOIN
		|SQL_SMALL_RESULT
		|SQL_BIG_RESULT
		|SQL_BUFFER_RESULT
		|SQL_CACHE
		|SQL_NO_CACHE
		|SQL_CALC_FOUND_ROWS
	)/xi;
	my $clauses = qr/(
		 FROM
		|WHERE
		|GROUP\s+BY
		|HAVING
		|ORDER\s+BY
		|LIMIT(?:\s+\d+)
		|PROCEDURE
		|INTO\s+OUTFILE
	)/xi;
	my $struct = $self->_parse_query($query, $keywords, 'columns', $clauses);

	# Add final keywords, if any.
	map { s/ /_/g; $struct->{keywords}->{lc $_} = 1; } @keywords;

	return $struct;
} # parse_select # }}}

sub parse_update( $$ ) { # {{{
	my ( $self, $query ) = @_;

	my $keywords = qr/(LOW_PRIORITY|IGNORE)/i;
	my $clauses  = qr/(SET|WHERE|ORDER\s+BY|LIMIT(?:\s+\d+))/i;

	return $self->_parse_query($query, $keywords, 'tables', $clauses);
} # parse_update # }}}

# Functions to handle SQL components

# Sub: parse_add # {{{
# GROUP BY {col_name | expr | position} [ASC | DESC], ... [WITH ROLLUP]
sub parse_add( $$ ) {
	my ( $self, $add ) = @_;
	return unless $add;
	MKDEBUG && _d('Parsing ADD', $add);

	# Parse the identifers.
	my $idents = $self->parse_identifiers( $self->_parse_csv($add) );

	return $idents;
} # parse_add # }}}

sub parse_change( $$ ) { # {{{
	my ( $self, $change ) = @_;
	# TODO
	return $change;
} # parse_change # }}}

sub parse_character_set( $$ ) { # {{{
	my ( $self, $character_set ) = @_;
	# TODO
	return $character_set;
} # parse_character_set # }}}

sub parse_columns( $$ ) { # {{{
	my ( $self, $cols ) = @_;
	MKDEBUG && _d('Parsing columns list:', $cols);

	my @cols;
	pos $cols = 0;
	while (pos $cols < length $cols) {
MKDEBUG && _d("At position '" . ( ( pos $cols ) or 0 ) . "' of ", length $cols);
		if ($cols =~ m/\G\s*(__SQ\d+__)\s*(?>,|\Z)/gcxo) {
warn "SQL DEBUG: (4) " .
	( defined( $1 ) ? "\$1(db_tbl_col) is '$1'" : '' ) .
	( defined( $2 ) ? ", \$2(unused) is '$2'" : '' ) .
	( defined( $3 ) ? ", \$3(unused) is '$3'" : '' ) .
	( defined( $4 ) ? ", \$4(unused) is '$4'" : '' ) .
	( defined( $5 ) ? ", \$4(unused) is '$5'" : '' ) .
	"." if SQLDEBUG;
			MKDEBUG && _d("Passing-through expression with compressed element \"$1\"");
			my $col_struct = { expr => $1, (), () };
			push @cols, $col_struct;
		}
		# XXX: Looking at it, the alias/col/tbl hash is lacking either
		#      the col(umn) or the alias name, because only three
		#      values are stored :(
		# We see:
		# jobs.id AS id -> { alias => 'AS', col => 'id', tbl => 'jobs' }
		# ... so explicit_alias is missing, and $2 is in $3.
		#
		elsif ($cols =~ m/\G\s*($RE{ balanced }{ -parens => '()' })\s*(?>,|\Z)/gcxo) {
			my ($select_expr) = $1;
warn "SQL DEBUG: (5) " .
	( defined( $1 ) ? "\$1(db_tbl_col) is '$1'" : '' ) .
	( defined( $2 ) ? ", \$2(unused) is '$2'" : '' ) .
	( defined( $3 ) ? ", \$3(unused) is '$3'" : '' ) .
	( defined( $4 ) ? ", \$4(unused) is '$4'" : '' ) .
	( defined( $5 ) ? ", \$4(unused) is '$5'" : '' ) .
	"." if SQLDEBUG;
			# See comments for $function_ident(2) below
			MKDEBUG && _d("Cannot fully parse expression \"$select_expr\"");
			my $col_struct = { expr => $select_expr, (), () };
			push @cols, $col_struct;
		}
		elsif ($cols =~ m/\G\s*$column_ident\s*(?>,|\Z)/gcxo) {
warn "SQL DEBUG: (1) " .
	( defined( $1 ) ? "\$1(db_tbl_col) is '$1'" : '' ) .
	( defined( $2 ) ? ", \$2(unused) is '$2'" : '' ) .
	( defined( $3 ) ? ", \$3(as) is '$3'" : '' ) .
	( defined( $4 ) ? ", \$4(alias) is '$4'" : '' ) .
	( defined( $5 ) ? ", \$4(unused) is '$5'" : '' ) .
	"." if SQLDEBUG;
			#my ($db_tbl_col, $as, $alias) = ($1, $2, $3); # XXX
			my ($db_tbl_col, $as, $alias) = ($1, $3, $4); # XXX
			#MKDEBUG && _d("column identifier:", Dumper(\$db_tbl_col));
			my $ident_struct = $self->parse_identifier('column', $db_tbl_col);
			#MKDEBUG && _d("resulting column identifier struct:", Dumper(\$ident_struct));
			if (defined $ident_struct) {
				$alias =~ s/`//g if defined $alias and length $alias;
				my $col_struct = {
					%$ident_struct,
					($as    ? (explicit_alias => 1)      : ()),
					($alias ? (alias          => $alias) : ()),
				};
				push @cols, $col_struct;
			}
		}
		# Furthermore, if the LHS of a SELECT statement is actually a
		# function-call rather than an alias at all, then we need to
		# handle that differently (but only if the other approaches
		# have failed to match)...
		# Update: Moved to position 2
		elsif ($cols =~ m/\G\s*$function_ident\s*(?>,|\Z)/gcxo) {
			my ($select_expr) = $1;
warn "SQL DEBUG: (2) " .
	( defined( $1 ) ? "\$1(db_tbl_col) is '$1'" : '' ) .
	( defined( $2 ) ? ", \$2(as) is '$2'" : '' ) .
	( defined( $3 ) ? ", \$3(alias) is '$3'" : '' ) .
	( defined( $4 ) ? ", \$4(unused) is '$4'" : '' ) .
	( defined( $5 ) ? ", \$4(unused) is '$5'" : '' ) .
	"." if SQLDEBUG;
			# There's no obvious way to represent this in the
			# current structure, which is predecated upon having a
			# concrete identifier as a root element.  Having said
			# this, the expression is still represented in
			# { 'clauses' } -> { 'columns' } (although not as an
			# alias/col/tbl hash) so perhaps this is okay...
			MKDEBUG && _d("Cannot fully parse expression \"$select_expr\"");
			my $col_struct = { expr => $select_expr, (), () };
			push @cols, $col_struct;
		}
		# This can occur when, for example, the LHS of a SELECT
		# statement's alias definition is an expression rather
		# than a simple column-reference...
		elsif ($cols =~ m/\G\s*(.+?)$ident_alias\s*(?>,|\Z)/gcxo) {
			my ($select_expr, $as, $alias) = ($1, $2, $3); # XXX
warn "SQL DEBUG: (3) " .
	( defined( $1 ) ? "\$1(db_tbl_col) is '$1'" : '' ) .
	( defined( $2 ) ? ", \$2(as) is '$2'" : '' ) .
	( defined( $3 ) ? ", \$3(alias) is '$3'" : '' ) .
	( defined( $4 ) ? ", \$4(unused) is '$4'" : '' ) .
	( defined( $5 ) ? ", \$4(unused) is '$5'" : '' ) .
	"." if SQLDEBUG;
			$alias =~ s/`//g if $alias;
			# There's no obvious way to represent this in the
			# current structure, which is predecated upon having a
			# concrete identifier as a root element.  Having said
			# this, the expression is still represented in
			# { 'clauses' } -> { 'columns' } (although not as an
			# alias/col/tbl hash) so perhaps this is okay...
			MKDEBUG && _d("Cannot fully parse expression \"" . $select_expr . ( defined( $as ) ? ' ' . $as . ' ' : ' ' ) . $alias . "\"");
			my $col_struct = {
				expr => $select_expr,
				($as    ? (explicit_alias => 1)      : ()),
				($alias ? (alias          => $alias) : ()),
			};
			push @cols, $col_struct;
		}
		elsif ($cols =~ m/\G\s*(.+?)(.*)\s*(?>,|\Z)/gcxo) {
			my ($select_expr) = $1;
			MKDEBUG && _d("Cannot parse expression \"$select_expr\"");
			my $col_struct = { expr => $select_expr, (), () };
			push @cols, $col_struct;
		}
		else {
			die "Column ident match on '$cols' failed";  # shouldn't happen
		}
	}

	return \@cols;
} # parse_columns # }}}

# Sub: parse_from # {{{
#   Parse a FROM clause, a.k.a. the table references.  Does not handle
#   nested joins.  See http://dev.mysql.com/doc/refman/5.1/en/join.html
#
# Parameters:
#   $from - FROM clause (with the word "FROM")
#
# Returns:
#   Arrayref of hashrefs, one hashref for each table in the order that
#   the tables appear, like:
#   (start code)
#   {
#     name           => 't2',  -- table's real name
#     alias          => 'b',   -- table's alias, if any
#     explicit_alias => 1,     -- if explicitly aliased with AS
#     join  => {               -- if joined to another table, all but first
#                              -- table are because comma implies INNER JOIN
#       to        => 't1',     -- table name on left side of join, if this is
#                              -- LEFT JOIN then this is the inner table, if
#                              -- RIGHT JOIN then this is outer table
#       type      => '',       -- left, right, inner, outer, cross, natural
#       condition => 'using',  -- on or using, if applicable
#       columns   => ['id'],   -- columns for USING condition, if applicable
#       ansi      => 1,        -- true of ANSI JOIN, i.e. true if not implicit
#                              -- INNER JOIN due to following a comma
#     },
#   },
#   {
#     name => 't3',
#     join => {
#       to        => 't2',
#       type      => 'left',
#       condition => 'on',     -- an ON condition is like a WHERE clause so
#       where     => [...]     -- this arrayref of predicates appears, see
#                              -- <parse_where()> for its structure
#     },
#   },
#  (end code)
sub parse_from( $$ ) {
	my ( $self, $from ) = @_;
	return unless $from;
	MKDEBUG && _d('Parsing FROM/INTO/TABLES', $from);

	# Extract the column list from USING(col, ...) clauses else
	# the inner commas will be captured by $comma_join.
	my $using_cols;
	($from, $using_cols) = $self->remove_using_columns($from);

	my $funcs;
	($from, $funcs) = $self->remove_functions($from);

	# Table references in a FROM clause are separated either by commas
	# (comma/theta join, implicit INNER join) or the JOIN keyword (ansi
	# join).  JOIN can be preceded by other keywords like LEFT, RIGHT,
	# OUTER, etc.  There must be spaces before and after JOIN and its
	# keywords, but there does not have to be spaces before or after a
	# comma.  See http://dev.mysql.com/doc/refman/5.5/en/join.html
	my $comma_join = qr/(?>\s*,\s*)/;
	my $ansi_join  = qr/(?>
	  \s+
	  (?:(?:INNER|CROSS|STRAIGHT_JOIN|LEFT|RIGHT|OUTER|NATURAL)\s+)*
	  JOIN
	  \s+
	)/xi;

	my @tbls;     # all table refs, a hashref for each
	my $tbl_ref;  # current table ref hashref
	my $join;     # join info hahsref for current table ref
	foreach my $thing ( split /($comma_join|$ansi_join)/io, $from ) {
		# We shouldn't parse empty things.
		die "Error parsing FROM clause" unless $thing;

		# Strip leading and trailing spaces.
		$thing =~ s/^\s+//;
		$thing =~ s/\s+$//;
		MKDEBUG && _d('Table thing:', $thing);

		if ( $thing =~ m/\s+(?:ON|USING)\s+/i ) {
			MKDEBUG && _d("JOIN condition");
			# This join condition follows a JOIN (comma joins don't have
			# conditions).  It includes a table ref, ON|USING, and then
			# the value to ON|USING.
			my ($tbl_ref_txt, $join_condition_verb, $join_condition_value)
				= $thing =~ m/^(.+?)\s+(ON|USING)\s+(.+)/i;

			$tbl_ref = $self->parse_table_reference($tbl_ref_txt);

			$join->{condition} = lc $join_condition_verb;
			if ( $join->{condition} eq 'on' ) {
				# The value for ON can be, as the MySQL manual says, is just
				# like a WHERE clause.
				$join->{where} = $self->parse_where($join_condition_value, $funcs);
			}
			else { # USING
				# Although calling parse_columns() works, it's overkill.
				# This is not a columns def as in "SELECT col1, col2", it's
				# a simple csv list of column names without aliases, etc.
				$join->{columns} = $self->_parse_csv(shift @$using_cols);
			}
		}
		elsif ( $thing =~ m/(?:,|JOIN)/i ) {
			# A comma or JOIN signals the end of the current table ref and
			# the begining of the next table ref.  Save the current table ref.
			if ( $join ) {
				$tbl_ref->{join} = $join;
			}
			push @tbls, $tbl_ref;
			MKDEBUG && _d("Complete table reference:", Dumper($tbl_ref));

			# Reset vars for the next table ref.
			$tbl_ref = undef;
			$join    = {};

			# Next table ref becomes the current table ref.  It's joined to
			# the previous table ref either implicitly (comma join) or explicitly
			# (ansi join).
			$join->{to} = $tbls[-1]->{tbl};
			if ( $thing eq ',' ) {
				$join->{type} = 'inner';
				$join->{ansi} = 0;
			}
			else { # ansi join
				my $type = $thing =~ m/^(.+?)\s+JOIN$/i ? lc $1 : 'inner';
				$join->{type} = $type;
				$join->{ansi} = 1;
			}
		}
		else {
			# First table ref and comma-joined tables.
			$tbl_ref = $self->parse_table_reference($thing);
			MKDEBUG && _d('Table reference:', Dumper($tbl_ref));
		}
	}

	# Save the last table ref.  It's not completed in the loop above because
	# there's no comma or JOIN after it.
	if ( $tbl_ref ) {
		if ( $join ) {
			$tbl_ref->{join} = $join;
		}
		push @tbls, $tbl_ref;
		MKDEBUG && _d("Complete table reference:", Dumper($tbl_ref));
	}

	return \@tbls;
} # parse_from
{
	no warnings;  # Why? See details below parse_insert
	*parse_into   = \&parse_from;
	*parse_tables = \&parse_from;
} # }}}

# Sub: parse_group_by # {{{
# GROUP BY {col_name | expr | position} [ASC | DESC], ... [WITH ROLLUP]
sub parse_group_by( $$ ) {
	my ( $self, $group_by ) = @_;
	return unless $group_by;
	MKDEBUG && _d('Parsing GROUP BY', $group_by);

	# Remove special "WITH ROLLUP" clause so we're left with a simple csv list.
	my $with_rollup = $group_by =~ s/\s+WITH ROLLUP\s*//i;

	# Parse the identifers.
	my $idents = $self->parse_identifiers( $self->_parse_csv($group_by) );

	$idents->{with_rollup} = 1 if $with_rollup;

	return $idents;
} # parse_group_by # }}}

sub parse_having( $$ ) { # {{{
	my ( $self, $having ) = @_;
	# TODO
	return $having;
} # parse_having # }}}

sub parse_identifier( $$$ ) { # {{{
	my ( $self, $type, $ident ) = @_;
	return unless defined $type && length $type && defined $ident && length $ident;
	MKDEBUG && _d("Parsing", $type, "identifier:", $ident);

	if ( $ident =~ m/^\w+\(/ ) {  # Function like MIN(col)
		my ($func, $expr) = $ident =~ m/^(\w+)\(([^\)]*)\)/;
		MKDEBUG && _d('Function', $func, 'arg', $expr);
		return { col => $ident } unless $expr;  # NOW()
		return { col => $expr } if( $expr =~ m/,/ ); # FIXME: Multiple arguments, which can't be trivially split as below...
		$ident = $expr;  # col from MAX(col)
	}

	my %ident_struct;
	my @ident_parts = map { s/`//g; $_; } split /[.]/, $ident;
	if ( @ident_parts == 3 ) {
		@ident_struct{qw(db tbl col)} = @ident_parts;
		#MKDEBUG && _d($type, "identifier 3 parts:", Dumper(\%ident_struct));
	}
	elsif ( @ident_parts == 2 ) {
		my @parts_for_type = $type eq 'column' ? qw(tbl col)
							    : $type eq 'table'  ? qw(db  tbl)
							    : die "Invalid identifier type: $type";
		@ident_struct{@parts_for_type} = @ident_parts;
		#MKDEBUG && _d($type, "identifier 2 parts:", Dumper(\%ident_struct));
	}
	elsif ( @ident_parts == 1 ) {
		my $part = $type eq 'column' ? 'col' : 'tbl';
		@ident_struct{($part)} = @ident_parts;
		#MKDEBUG && _d($type, "identifier 1 part:", Dumper(\%ident_struct));
	}
	else {
		die "Invalid number of parts '" . scalar( @ident_parts ) . "' in $type reference: $ident\n" . Dumper(\@ident_parts);
	}

	if ( $self->{Schema} ) {
		if ( $type eq 'column' && (!$ident_struct{tbl} || !$ident_struct{db}) ) {
			my $qcol = $self->{Schema}->find_column(%ident_struct);
			if ( $qcol && scalar( @$qcol ) == 1 ) {
				@ident_struct{qw(db tbl)} = @{$qcol->[0]}{qw(db tbl)};
			}
		}
		elsif ( !$ident_struct{db} ) {
			my $qtbl = $self->{Schema}->find_table(%ident_struct);
			if ( $qtbl && scalar( @$qtbl ) == 1 ) {
				$ident_struct{db} = $qtbl->[0];
			}
		}
	}

	MKDEBUG && _d($type, "return identifier struct:", Dumper(\%ident_struct));
	return \%ident_struct;
} # parse_identifier # }}}

# Sub: parse_identifiers # {{{
#   Parse an arrayref of identifiers into their parts.  Identifiers can be
#   column names (optionally qualified), expressions, or constants.
#   GROUP BY and ORDER BY specify a list of identifiers.
#
# Parameters:
#   $idents - Arrayref of indentifiers
#
# Returns:
#   Arrayref of hashes with each identifier's parts, depending on what kind
#   of identifier it is.
sub parse_identifiers( $$ ) {
	my ( $self, $idents ) = @_;
	return unless $idents;
	MKDEBUG && _d("Parsing identifiers");

	my @ident_parts;
	foreach my $ident ( @$idents ) {
		MKDEBUG && _d("Identifier:", $ident);
		my $parts = {};

		if ( $ident =~ s/\s+(ASC|DESC)\s*$//i ) {
			$parts->{sort} = uc $1;  # XXX
		}

		if ( $ident =~ m/^\d+$/ ) {      # Position like 5
			MKDEBUG && _d("Positional ident");
			$parts->{position} = $ident;
		}
		elsif ( $ident =~ m/^\w+\(/ ) {  # Function like MIN(col)
			MKDEBUG && _d("Expression ident");
			my ($func, $expr) = $ident =~ m/^(\w+)\(([^\)]*)\)/;
			$parts->{function}   = uc $func;
			$parts->{expression} = $expr if $expr;
		}
		else {                           # Ref like (table.)column
			MKDEBUG && _d("Table/column ident");
			my ($tbl, $col)  = $self->split_unquote($ident);
			$parts->{table}  = $tbl if $tbl;
			$parts->{column} = $col;
		}
		push @ident_parts, $parts;
	}

	return \@ident_parts;
} # parse_identifiers # }}}

# Sub: parse_limit # {{{
# [LIMIT {[offset,] row_count | row_count OFFSET offset}]
sub parse_limit( $$ ) {
	my ( $self, $limit ) = @_;
	return unless $limit;
	my $struct = {
		row_count => undef,
	};
	if ( $limit =~ m/(\S+)\s+OFFSET\s+(\S+)/i ) {
		$struct->{explicit_offset} = 1;
		$struct->{row_count}       = $1;
		$struct->{offset}          = $2;
	}
	else {
		my ($offset, $cnt) = $limit =~ m/(?:(\S+),\s+)?(\S+)/i;
		$struct->{row_count} = $cnt;
		$struct->{offset}    = $offset if defined $offset;
	}
	return $struct;
} # parse_limit # }}}

sub parse_modify( $$ ) { # {{{
	my ( $self, $modify ) = @_;
	# TODO
	return $modify;
} # parse_modify # }}}

# Sub: parse_order_by # {{{
# [ORDER BY {col_name | expr | position} [ASC | DESC], ...]
sub parse_order_by( $$ ) {
	my ( $self, $order_by ) = @_;
	return unless $order_by;
	MKDEBUG && _d('Parsing ORDER BY', $order_by);
	my $idents = $self->parse_identifiers( $self->_parse_csv($order_by) );
	return $idents;
} # parse_order_by # }}}

sub parse_set( $$ ) { # {{{
	my ( $self, $set ) = @_;
	MKDEBUG && _d("Parse SET", $set);
	return unless $set;
	my $vals = $self->_parse_csv($set);
	return unless $vals && scalar(@$vals);

	my @set;
	foreach my $col_val ( @$vals ) {
		# Do not remove quotes around the val because quotes let us determine
		# the value's type.  E.g. tbl might be a table, but "tbl" is a string,
		# and NOW() is the function, but 'NOW()' is a string.
		my ($col, $val)  = $col_val =~ m/^([^=]+)\s*=\s*(.+)/;
		my $ident_struct = $self->parse_identifier('column', $col);
		my $set_struct   = {
			%$ident_struct,
			value => $val,
		};
		MKDEBUG && _d("SET:", Dumper($set_struct));
		push @set, $set_struct;
	}
	return \@set;
} # parse_set # }}}

# Sub: parse_table_reference # {{{
# Parse a table ref like "tbl", "tbl alias" or "tbl AS alias", where
# tbl can be optionally "db." qualified.  Also handles FORCE|USE|IGNORE
# INDEX hints.  Does not handle "FOR JOIN" hint because "JOIN" here gets
# confused with the "JOIN" thing in parse_from().
sub parse_table_reference( $$ ) {
	my ( $self, $tbl_ref ) = @_;
	return unless $tbl_ref;
	MKDEBUG && _d('Parsing table reference:', $tbl_ref);
	my %tbl;

	# First, check for an index hint.  Remove and save it if present.
	# This can't be included in the $table_ident regex because, for example,
	# `tbl` FORCE INDEX (foo), makes FORCE look like an implicit alias.
	if ( $tbl_ref =~ s/
			\s+(
				(?:FORCE|USE|INGORE)\s
				(?:INDEX|KEY)
				\s*\([^\)]+\)\s*
			)//xi)
	{
		$tbl{index_hint} = $1;
		MKDEBUG && _d('Index hint:', $tbl{index_hint});
	}

	if ( $tbl_ref =~ m/$table_ident/ ) {
		my ($db_tbl, $as, $alias) = ($1, $2, $3); # XXX
		my $ident_struct = $self->parse_identifier('table', $db_tbl);
		$alias =~ s/`//g if $alias;
		@tbl{keys %$ident_struct} = values %$ident_struct;
		$tbl{explicit_alias} = 1 if $as;
		$tbl{alias}          = $alias if $alias;
	}
	else {
		die "Table ident match failed";  # shouldn't happen
	}

	return \%tbl;
} # parse_table_reference # }}}

# Sub: parse_vaules # {{{
# Parses the list of values after, e.g., INSERT tbl VALUES (...), (...).
# Does not currently parse each set of values; it just splits the list.
sub parse_values( $$ ) {
	my ( $self, $values ) = @_;
	return unless $values;

	my $vals;
	if( $values =~ m/^\s*\(.*\)\s*$/ ) {
		my @multivals;
		foreach my $match ( ( $values =~ m/$RE{ balanced }{ -begin => '(' }{ -end => ')' }/g ) ) {
			$match =~ s/^\s*\(\s*//;
			$match =~ s/\s*\)\s*$//;

			MKDEBUG && _d('parse_values: Multi-value item', $match );
			my $items = $self->_parse_csv(
				$match,
				quoted_values => 1,
				remove_quotes => 0,
			);
			#push( @multivals, @{ $items } );
			push( @multivals, $items );
		}
		$vals = \@multivals;
	} else {
		MKDEBUG && _d('parse_values: Simple list', $values );
		$vals = $self->_parse_csv(
			$values,
			quoted_values => 1,
			remove_quotes => 0,
		);
	}
	return $vals;
} # parse_values # }}}

# Sub: parse_where # {{{
# This is not your traditional parser, but it works for simple to rather
# complex cases, with a few noted and intentional limitations.  First,
# the limitations:
#
#   * probably doesn't handle every possible operator (see $op)
#   * doesn't care about grouping with parentheses
#   * not "fully" tested because the possibilities are infinite
#
# It works in four steps; let's take this WHERE clause as an example:
#
#   i="x and y" or j in ("and", "or") and x is not null or a between 1 and 10 and sz="this 'and' foo"
#
# The first step splits the string on and|or, the only two keywords I'm
# aware of that join the separate predicates.  This step doesn't care if
# and|or is really between two predicates or in a string or something else.
# The second step is done while the first step is being done: check predicate
# "fragments" (from step 1) for operators; save which ones have and don't
# have at least one operator.  So the result of step 1 and 2 is:
#
#   PREDICATE FRAGMENT                OPERATOR
#   ================================  ========
#   i="x                              Y
#   and y"                            N
#   or j in ("                        Y
#   and", "                           N
#   or")                              N
#   and x is not null                 Y
#   or a between 1                    Y
#   and 10                            N
#   and sz="this '                    Y
#   and' foo"                         N
#
# The third step runs through the list of pred frags backwards and joins
# the current frag to the preceding frag if it does not have an operator.
# The result is:
#
#   PREDICATE FRAGMENT                OPERATOR
#   ================================  ========
#   i="x and y"                       Y
#                                     N
#   or j in ("and", "or")             Y
#                                     N
#                                     N
#   and x is not null                 Y
#   or a between 1 and 10             Y
#                                     N
#   and sz="this 'and' foo"           Y
#                                     N
#
# The fourth step is similar but not shown: pred frags with unbalanced ' or "
# are joined to the preceding pred frag.  This fixes cases where a pred frag
# has multiple and|or in a string value; e.g. "foo and bar or dog".
#
# After the pred frags are complete, the parts of these predicates are parsed
# and returned in an arrayref of hashrefs like:
#
#   {
#     predicate => 'and',
#     column    => 'id',
#     operator  => '>=',
#     value     => '42',
#   }
#
# Invalid predicates, or valid ones that we can't parse,  will cause
# the sub to die.
sub parse_where( $$ ) {
	my ( $self, $where, $functions ) = @_;
	return unless $where;
	MKDEBUG && _d("Parsing WHERE", $where);

	# Not all the operators listed at
	# http://dev.mysql.com/doc/refman/5.1/en/non-typed-operators.html
	# are supported.  E.g. - (minus) is an op but does it ever show up
	# in a where clause?  "col-3=2" is valid (where col=5), but we're
	# not interested in weird stuff like that.
	my $op_symbol = qr/
		(?:
		 <=(?:>)?
		|^
		|~
		|<(?:<|>)?
		|=
		|>(?:>|=)?
		|-
		|:=
		|!(?:=)?
		|\|(?:\|)?
		|\/
		|\*
		|\+
		|&(?:&)?
		|%
		|AND
		|BINARY
		|CASE
		|DIV
		|MOD
		|NOT
		|OR
		|XOR
	)/xi;
	my $op_verb = qr/
		(?:
			 (?:(?:NOT\s+)?LIKE)
			|(?:IS(?:\s+NOT\s*)?)
			|(?:(?:NOT\s+)?BETWEEN)
			|(?:(?:NOT\s+)?IN)
			|(?:(?:NOT\s+)?EXISTS)
			|(?:(?:NOT\s+)?(?:REGEXP|RLIKE))
			|(?:SOUNDS\s+LIKE)
		)
	/xi;
	my $op_pat = qr/
	(
		(?>
			 (?:$op_symbol)  # don't need spaces around the symbols, e.g.: col=1
			|(?:\s+$op_verb) # must have space before verb op, e.g.: col LIKE ...
		)
	)/x;

	# Step 1 and 2: split on and|or and look for operators.
	my $offset = 0;
	my $pred   = "";
	my @pred;
	my @has_op;
	while ( $where =~ m/\b(and|or)\b/gi ) {
		my $pos = (pos $where) - (length $1);  # pos at and|or, not after

		$pred = substr $where, $offset, ($pos-$offset);
		push @pred, $pred;
		push @has_op, $pred =~ m/$op_pat/o ? 1 : 0;

		$offset = $pos;
	}
	# Final predicate fragment: last and|or to end of string.
	$pred = substr $where, $offset;
	push @pred, $pred;
	push @has_op, $pred =~ m/$op_pat/o ? 1 : 0;
	MKDEBUG && _d("Predicate fragments:", Dumper(\@pred));
	MKDEBUG && _d("Predicate frags with operators:", @has_op);

	# Step 3: join pred frags without ops to preceding pred frag.
	my $n = scalar @pred - 1;
	for my $i ( 1..$n ) {
		$i   *= -1;
		my $j = $i - 1;  # preceding pred frag

		# Two constants in a row, like "TRUE or FALSE", are a special case.
		# The current pred ($i) will not have an op but in this case it's
		# not a continuation of the preceding pred ($j) so we don't want to
		# join them.  And there's a special case within this special case:
		# "BETWEEN 1 AND 10".  _is_constant() strips leading AND or OR so
		# 10 is going to look like an independent constant but really it's
		# part of the BETWEEN op, so this whole special check is skipped
		# if the preceding pred contains BETWEEN.  Yes, parsing SQL is tricky.
		next if $pred[$j] !~ m/\s+between\s+/i  && $self->_is_constant($pred[$i]);

		if ( !$has_op[$i] ) {
			$pred[$j] .= $pred[$i];
			$pred[$i]  = undef;
		}
	}
	MKDEBUG && _d("Predicate fragments joined:", Dumper(\@pred));

	# Step 4: join pred frags with unbalanced ' or " to preceding pred frag.
	for my $i ( 0..@pred ) {
		$pred = $pred[$i];
		next unless defined $pred;
		my $n_single_quotes = ($pred =~ tr/'//);
		my $n_double_quotes = ($pred =~ tr/"//);
		if ( ($n_single_quotes % 2) || ($n_double_quotes % 2) ) {
			if( defined( $pred[$i + 1] ) ) {
				$pred[$i]     .= $pred[$i + 1];
				$pred[$i + 1]  = undef;
			} else {
				MKDEBUG && _d("Predicate fragments cannot be balanced on quotes:", Dumper(\@pred));
			}
		}
	}
	MKDEBUG && _d("Predicate fragments balanced:", Dumper(\@pred));

	# Parse, clean up and save the complete predicates.
	my @predicates;
	foreach my $pred ( @pred ) {
		next unless defined $pred;
		$pred =~ s/^\s+//;
		$pred =~ s/\s+$//;
		my $conj;
		if ( $pred =~ s/^(and|or)\s+//i ) {
			$conj = lc $1;
		}
		my ($col, $op, $val) = $pred =~ m/^(.+?)$op_pat(.+)$/o;
		if ( !$col || !$op ) {
			if ( $self->_is_constant($pred) ) {
				$val = lc $pred;
			}
			else {
				die "Failed to parse predicate \"$pred\" from WHERE condition \"$where\"";
			}
		}

		# Remove whitespace and lowercase some keywords.
		if ( $col ) {
			$col =~ s/\s+$//;
			$col =~ s/^\(+//;  # no unquoted column name begins with (
		}
		if ( $op ) {
			$op  =  lc $op;
			$op  =~ s/^\s+//;
			$op  =~ s/\s+$//;
		}
		$val =~ s/^\s+//;

		# No unquoted value ends with ) except FUNCTION(...)
		if ( ($op || '') !~ m/IN/i && $val !~ m/^\w+\([^\)]+\)$/ ) {
			$val =~ s/\)+$//;
		}

		if ( $val =~ m/NULL|TRUE|FALSE/i ) {
			$val = lc $val;
		}

		if ( $functions ) {
			$col = shift @$functions if $col =~ m/__FUNC\d+__/;
			$val = shift @$functions if $val =~ m/__FUNC\d+__/;
		}

		push @predicates, {
			predicate => $conj,
			left_arg  => $col,
			operator  => $op,
			right_arg => $val,
		};
	}

	return \@predicates;
} # parse_where # }}}

# Helper functions

# Sub: clean_query # {{{
#   Remove spaces, flatten, and normalize some patterns for easier parsing.
#
# Parameters:
#   $query - SQL statement
#
# Returns:
#   Cleaned $query
sub clean_query( $$ ) {
	my ( $self, $query ) = @_;
	return unless $query;

	# Whitespace and comments.
	$query =~ s/^\s*--.*$//gm;  # -- comments
	$query =~ s/\s+/ /g;        # extra spaces/flatten
	$query =~ s!/\*.*?\*/!!g;   # /* comments */
	$query =~ s/^\s+//;         # leading spaces
	$query =~ s/\s+$//;         # trailing spaces

	return $query;
} # clean_query # }}}

# Sub: is_identifier # {{{
#   Determine if something is a schema object identifier.
#   E.g.: `tbl` is an identifier, but "tbl" is a string and 1 is a number.
#   See <http://dev.mysql.com/doc/refman/5.1/en/identifiers.html>
#
# Parameters:
#   $thing - Name of something, including any quoting as it appears in a query.
#
# Returns:
#   True of $thing is an identifier, else false.
sub is_identifier( $$ ) {
	my ( $self, $thing ) = @_;

	# Nothing is not an ident.
	return 0 unless $thing;

	# Tables, columns, FUNCTIONS(), etc. cannot be 'quoted' or "quoted"
	# because that would make them strings, not idents.
	return 0 if $thing =~ m/\s*['"]/;

	# Numbers, ints or floats, are not identifiers.
	return 0 if $thing =~ m/^\s*\d+(?:\.\d+)?\s*$/;

	# Keywords are not identifiers.
	return 0 if $thing =~ m/^\s*(?>
		 NULL
		|DUAL
	)\s*$/xi;

	# The column ident really matches everything: db, db.tbl, db.tbl.col,
	# function(), @@var, etc.
	return 1 if $thing =~ m/^\s*$column_ident\s*$/;

	# If the thing isn't quoted and doesn't match our ident pattern, then
	# it's probably not an ident.
	return 0;
} # }}}

# Sub: normalize_keyword_spaces # {{{
#   Normalize spaces around certain SQL keywords.  Spaces are added and
#   removed around certain SQL keywords to make parsing easier.
#
# Parameters:
#   $query - SQL statement
#
# Returns:
#   Normalized $query
sub normalize_keyword_spaces( $$ ) {
	my ( $self, $query ) = @_;

	# Add spaces between important tokens to help the parse_* subs.
	$query =~ s/\b(VALUE(?:S)?)\(/$1 (/i;
	$query =~ s/\bON\(/on (/gi;
	$query =~ s/\bUSING\(/using (/gi;

	# Start of (SELECT subquery).
	$query =~ s/\(\s+SELECT\s+/(SELECT /gi;

	return $query;
} # normalize_keyword_spaces # }}}

sub remove_functions( $$ ) { # {{{
	my ($self, $clause) = @_;
	return unless $clause;
	MKDEBUG && _d('Removing functions from clause:', $clause);
	my @funcs;
	$clause =~ s/$function_ident/replace_function($1, \@funcs)/eg;
	MKDEBUG && _d('Function-stripped clause:', $clause, Dumper(\@funcs));
	return $clause, \@funcs;
} # }}}

# Sub: remove_subqueries # {{{
# Remove subqueries from query, return modified query and list of subqueries.
# Each subquery is replaced with the special token __SQn__ where n is the
# subquery's ID.  Subqueries are parsed and removed in to out, last to first;
# i.e. the last, inner-most subquery is ID 0 and the first, outermost
# subquery has the greatest ID.  Each subquery ID corresponds to its index in
# the list of returned subquery hashrefs after the modified query.  __SQ2__
# is subqueries[2].  Each hashref is like:
#   * query    Subquery text
#   * context  scalar, list or identifier
#   * nested   (optional) 1 if nested
# This sub does not handle UNION and it expects to that subqueries start
# with "(SELECT ".  See SQLParser.t for examples.
sub remove_subqueries( $$ ) {
	my ( $self, $query ) = @_;

	# FIXME: This function broadly does the correct thing (including
	#        the handling of SQL statements with multiple nested subqueries
	#        which aren't nested within each other) - but still appears to
	#        leave un-expanded __SQ{x}__ tokens in the resultant struct :(
	# FIXME: The last subquery seems to have the remainder of the query
	#        appended to it :(

	# Find starting pos of all subqueries.
	my @start_pos;
	while ( $query =~ m/(\(\s*SELECT\s+)/gi ) {
		my $pos = (pos $query) - (length $1);
		push @start_pos, $pos;
	}

	# Starting with the inner-most, last subquery, find ending pos of
	# all subqueries.  This is done by counting open and close parentheses
	# until all are closed.  The last closing ) should close the ( that
	# opened the subquery.  No sane regex can help us here for cases like:
	# (select max(id) from t where col in(1,2,3) and foo='(bar)').
	@start_pos = reverse @start_pos;
	my @end_pos;
	for my $i ( 0 .. $#start_pos ) {
		my $closed = 0;
		pos $query = $start_pos[$i];
		while ( $query =~ m/([\(\)])/cg ) {
			my $c = $1;
			$closed += ($c eq '(' ? 1 : -1);
			last unless $closed;
		}
		push @end_pos, pos $query;
	}

	# Replace each subquery with a __SQn__ token.
	my @subqueries;
	my $len_adj = 0;
	my $n    = 0;
	for my $i ( 0 .. $#start_pos ) {
		MKDEBUG && _d('Query:', $query);

		my $outerfound = 0;
		my $struct   = {};
		my $token    = '__SQ' . $n . '__';

MKDEBUG && _d("SQ: Using token '$token'");
		# Adjust len for next outer subquery.  This is required because the
		# subqueries' start/end pos are found relative to one another, so
		# when a subquery is replaced with its shorter __SQn__ token the end
		# pos for the other subqueries decreases.  The token is shorter than
		# any valid subquery so the end pos should only decrease.
		for my $j ( $i .. ( $#start_pos - 1 ) ) {
			next if( $outerfound );
MKDEBUG && _d("SQ: Iteration '$j' ($i to " . ( $#start_pos - 1 ) . ")");

			my $outer_start = $start_pos[$j + 1];
			my $outer_end   = $end_pos[$j + 1];
MKDEBUG && _d("SQ: Outer start $outer_start, Outer end $outer_end");

			if (    $outer_start && ($outer_start < $start_pos[$i])
				  && $outer_end   && ($outer_end   > $end_pos[$i]) ) {
				MKDEBUG && _d("Subquery $n nested within outer subquery " . ( $j + 1 ) );

				$len_adj = 0;
				for my $k ( 0 .. ( $i - 1 ) ) {
MKDEBUG && _d("SQ: Iteration '$k' (0 to " . ( $i - 1 ) . ")");
					my $inner_start = $start_pos[$k];
					my $inner_end   = $end_pos[$k];
MKDEBUG && _d("SQ: Inner start $inner_start, Inner end $inner_end");

					if (    $inner_start && ($inner_start > $start_pos[$i])
						  && $inner_end   && ($inner_end   < $end_pos[$i]) ) {
						my $inner_len = $inner_end - $inner_start;
						MKDEBUG && _d("Subquery $n contains nested subquery $k of length $inner_len");
						$len_adj += $inner_len - length $token;
					}
				}

				$struct->{nested} = $i + 1;
				$outerfound = 1;
			}
			MKDEBUG && _d("Nested subquery $n has total adjustment $len_adj");
		}
		if( not( $outerfound ) ) {
			MKDEBUG && _d("Subquery $n not nested");

			$len_adj = 0;
			#for my $k ( 0 .. ( $i - 1 ) ) {
			#	my $inner_start = $start_pos[$k];
			#	my $inner_end   = $end_pos[$k];
			#
			#	if (    $inner_start && ($inner_start > $start_pos[$i])
			#		  && $inner_end   && ($inner_end   < $end_pos[$i]) ) {
			#		my $inner_len = $inner_end - $inner_start;
			#		MKDEBUG && _d("Subquery $n contains nested subquery $k of length $inner_len");
			#		$len_adj += $inner_len - length $token;
			#	}
			#}
			#MKDEBUG && _d("Top-level subquery $n has total adjustment $len_adj");

			if ( $subqueries[-1] && $subqueries[-1]->{nested} ) {
				MKDEBUG && _d("Outermost subquery");
			}
		}
MKDEBUG && _d('SQ: start_pos:', Dumper(\@start_pos));
MKDEBUG && _d('SQ: end_pos:', Dumper(\@end_pos));
MKDEBUG && _d('SQ: subqueries:', Dumper(\@subqueries));

		my $len    = $end_pos[$i] - $start_pos[$i] - $len_adj;
		MKDEBUG && _d("Subquery $n start " . $start_pos[$i] .
				', orig end ' . $end_pos[$i] . ', adj ' .
				$len_adj . ', adj end ' . ( $start_pos[$i] + $len ) .
				', len ' . $len . '.' );
		my $subquery = substr($query, $start_pos[$i], $len, $token);
		MKDEBUG && _d("Subquery $n:", $subquery);

		# Get subquery context: scalar, list or identifier.
		if ( $query =~ m/(?:=|>|<|>=|<=|<>|!=|<=>)\s*$token/ ) {
			$struct->{context} = 'scalar';
		}
		elsif ( $query =~ m/\b(?:IN|ANY|SOME|ALL|EXISTS)\s*$token/i ) {
			# Add ( ) around __SQn__ for things like "IN(__SQn__)"
			# unless they're already there.
			if ( $query !~ m/\($token\)/ ) {
				$query =~ s/$token/\($token\)/;
				$len_adj -= 2 if $struct->{nested};
			}
			$struct->{context} = 'list';
		}
		else {
			# If the subquery is not preceded by an operator (=, >, etc.)
			# or IN(), EXISTS(), etc. then it should be an indentifier,
			# either a derived table or column.
			$struct->{context} = 'identifier';
		}
		MKDEBUG && _d("Subquery $n context:", $struct->{context});

		# Remove ( ) around subquery so it can be parsed by a parse_TYPE sub.
		$subquery =~ s/^\s*\(//;
		$subquery =~ s/\s*\)\s*$//;

		# Save subquery to struct after modifications above.
		$struct->{query} = $subquery;
		push @subqueries, $struct;
		$n++;
	}

	return $query, @subqueries;
} # }}}

sub remove_using_columns( $$ ) { # {{{
	my ($self, $from) = @_;
	return unless $from;
	MKDEBUG && _d('Removing cols from USING clauses');
	my $using = qr/
		\bUSING
		\s*
		\(
			([^\)]+)
		\)
	/xi;
	my @cols;
	$from =~ s/$using/push @cols, $1; "USING ($#cols)"/eg;
	MKDEBUG && _d('FROM:', $from, Dumper(\@cols));
	return $from, \@cols;
} # }}}

sub replace_function( $$ ) { # {{{
	my ($func, $funcs) = @_;
	my ($func_name) = $func =~ m/^(\w+)/;
	if ( !$ignore_function{uc $func_name} ) {
		my $n = scalar @$funcs;
		push @$funcs, $func;
		return "__FUNC${n}__";
	}
	return $func;
} # }}}

sub set_Schema( $$ ) { # {{{
	my ( $self, $sq ) = @_;
	$self->{Schema} = $sq;
	return;
} # }}}

# Sub: split_unquote # {{{
#   Split and unquote a table name.  The table name can be database-qualified
#   or not, like `db`.`table`.  The table name can be backtick-quoted or not.
#
# Parameters:
#   $db_tbl     - Table name
#   $default_db - Default database name to return if $db_tbl is not
#                 database-qualified
#
# Returns:
#   Array: unquoted database (possibly undef), unquoted table
sub split_unquote( $$$ ) {
	my ( $self, $db_tbl, $default_db ) = @_;
	$db_tbl =~ s/`//g;
	my ( $db, $tbl ) = split(/[.]/, $db_tbl);
	if ( !$tbl ) {
		$tbl = $db;
		$db  = $default_db;
	}
	return ($db, $tbl);
} # }}}

# Sub: _parse_clauses # {{{
#   Parse raw text of clauses into data structures.  This sub recurses
#   to parse the clauses of subqueries.  The clauses are read from
#   and their data structures saved into the $struct parameter.
#
# Parameters:
#   $struct - Hashref from which clauses are read (%{$struct->{clauses}})
#             and into which data structs are saved (e.g. $struct->{from}=...).
sub _parse_clauses( $$ ) {
	my ( $self, $struct ) = @_;
	# Parse raw text of clauses and functions.
	foreach my $clause ( keys %{$struct->{clauses}} ) {
		# Rename/remove clauses with space in their names, like ORDER BY.
		if ( $clause =~ m/\s+/ ) {
			(my $clause_no_space = $clause) =~ s/\s+/_/g;
			$struct->{clauses}->{$clause_no_space} = $struct->{clauses}->{$clause};
			delete $struct->{clauses}->{$clause};
			$clause = $clause_no_space;
		}

		# XXX: Hack to work around LIMIT gaining first argument bug
		$clause =~ s/_\d+$//;

		my $parse_func     = "parse_$clause";
		$struct->{$clause} = $self->$parse_func($struct->{clauses}->{$clause});

		if ( $clause eq 'select' ) {
			MKDEBUG && _d('Parsing subquery clauses');
			$struct->{select}->{type} = 'select';
			$self->_parse_clauses($struct->{select});
		}
	}
	return;
} # _parse_clauses # }}}

# Sub: _parse_csv # {{{
# Split any comma-separated list of values, removing leading
# and trailing spaces.
sub _parse_csv( $$$ ) {
	my ( $self, $vals, %args ) = @_;
	return unless $vals;

	MKDEBUG && _d("Parsing values:", $vals);

	my @vals;
	if ( $args{quoted_values} ) {
MKDEBUG && _d("Parsing values:", Dumper(\%args));
		# If the vals are quoted, then they can contain commas, like:
		# "hello, world!", 'batman'.  If only we could use Text::CSV,
		# then I wouldn't write yet another csv parser to handle this,
		# but Maatkit doesn't like package dependencies, so here's my
		# light implementation of this classic problem.
		my $quote_char   = '';
		VAL:
		foreach my $val ( split(',', $vals) ) {
			MKDEBUG && _d("Next value:", $val);
			# If there's a quote char, then this val is the rest of a previously
			# quoted and split value.
			if ( $quote_char ) {
				MKDEBUG && _d("Value is part of previous quoted value");
				# split() removed the comma inside the quoted value,
				# so add it back else "hello, world" is incorrectly
				# returned as "hello world".
				$vals[-1] .= ",$val";

				# Quoted and split value is complete when a val ends with the
				# same quote char that began the split value.
				if ( $val =~ m/[^\\]*$quote_char$/ ) {
					if ( $args{remove_quotes} ) {
						$vals[-1] =~ s/^\s*$quote_char//;
						$vals[-1] =~ s/$quote_char\s*$//;
					}
					MKDEBUG && _d("Previous quoted value is complete:", $vals[-1]);
					$quote_char = '';
				}

				next VAL;
			}

			# Start of new value so strip leading spaces but not trailing
			# spaces yet because if the next check determines that this is
			# a quoted and split val, then trailing space is actually space
			# inside the quoted val, so we want to preserve it.
			$val =~ s/^\s+//;

			# A value is quoted *and* split (because there's a comma in the
			# quoted value) if the vale begins with a quote char and does not
			# end with that char.  E.g.: "foo but not "foo".  The val "foo is
			# the first part of the split value, e.g. "foo, bar".
			if ( $val =~ m/^(['"])/ ) {
				MKDEBUG && _d("Value is quoted");
				$quote_char = $1;  # XXX
				if ( $val =~ m/.$quote_char$/ ) {
					MKDEBUG && _d("Value is complete");
					$quote_char = '';
					if ( $args{remove_quotes} ) {
						$vals[-1] =~ s/^\s*$quote_char//;
						$vals[-1] =~ s/$quote_char\s*$//;
					}
				}
				else {
					MKDEBUG && _d("Quoted value is not complete");
				}
			}
			else {
				$val =~ s/\s+$//;
			}

			# Save complete value (e.g. foo or "foo" without the quotes),
			# or save the first part of a quoted and split value; the rest
			# of such a value will be joined back above.
			MKDEBUG && _d("Saving value", ($quote_char ? "fragment" : ""));
			push @vals, $val;
		}
	}
	else {
		my $filteredline = $vals;
		my $strchanged = 0; # FALSE
		my $index = 0;
		my @savedterms;
		foreach my $match ( ( $vals =~ m/$RE{ balanced }{ -parens => '()' }/g ) ) {
			$filteredline =~ s/\Q$match\E/__MW_STR_${index}__/;
			MKDEBUG && _d("Replacing '$match' with '__MW_STR_${index}__' to give '$filteredline'");
			$index++;
			$savedterms[ $index ] = $match;
			$strchanged = 1; # TRUE
		}

		if( !( $strchanged ) ) {
			@vals = map { s/^\s+//; s/\s+$//; $_ } split(',', $vals);
		} else {
			MKDEBUG && _d("Quote-reduced line is now:", $filteredline) if( $strchanged );

			my @compressedvals = map { s/^\s+//; s/\s+$//; $_ } split(',', $filteredline);

			foreach my $val ( @compressedvals ) {
				while( $val =~ m/__MW_STR_(\d+)__/ ) {
					my $position = $1;
					if( $position >= $index ) {
						die( "Read placeholder string '$position' beyond maximum seen '$index'\n" );
					}
					my $replacement = $savedterms[ $position ];
					$replacement = '' unless( defined( $replacement ) and length( $replacement ) );
					$val =~ s/__MW_STR_${position}__/$replacement/;
					MKDEBUG && _d("Replacing '__MW_STR_${position}__' with '$replacement' to give '$val'");
				}
				push( @vals, $val );
			}
		}
	}
MKDEBUG && _d("Parsed values:", Dumper(\@vals));

	return \@vals;
}
{
	no warnings;  # Why? See same line above.
	*parse_on_duplicate = \&_parse_csv;
} # }}}

# Sub: _parse_query # {{{
#    This sub is called by the parse_TYPE subs except parse_insert.
#    It does two things: remove, save the given keywords, all of which
#    should appear at the beginning of the query; and, save (but not
#    remove) the given clauses.  The query should start with the values
#    for the first clause because the query's first word was removed
#    in parse().  So for "SELECT cols FROM ...", the query given here
#    is "cols FROM ..." where "cols" belongs to the first clause "columns".
#    Then the query is walked clause-by-clause, saving each.
#
# Parameters:
#   $query        - SQL statement with first word (SELECT, INSERT, etc.) removed
#   $keywords     - Compiled regex of keywords that can appear in $query
#   $first_clause - First clause word to expect in $query
#   $clauses      - Compiled regex of clause words that can appear in $query
#
# Returns:
#   Hashref with raw text of clauses
sub _parse_query( $$$$$ ) {
	my ( $self, $query, $keywords, $first_clause, $clauses ) = @_;
	return unless $query;
	my $struct = {};

	# Save, remove keywords.
	1 while $query =~ s/(?:^|\s+)(?:$RE{quoted}\s+)?$keywords(?:\s+$RE{quoted})?\s+/$struct->{keywords}->{lc $1}=1, ''/gie;

	# Go clausing.
	my @clause = grep { defined $_ }
		($query =~ m/\G(.+?)(?:(?:^|\s+)$clauses(?:\s+|$)|\Z)/gci);

	my $clause = $first_clause,
	my $value  = shift @clause;
	$struct->{clauses}->{$clause} = $value;
	MKDEBUG && _d('Clause:', $clause, $value);

	# All other clauses.
	while ( @clause ) {
		$clause = shift @clause;
		$value  = shift @clause;
		$struct->{clauses}->{lc $clause} = $value;
		MKDEBUG && _d('Clause:', $clause, $value);
	}

	($struct->{unknown}) = ($query =~ m/\G(.+)/);

	return $struct;
} # _parse_query # }}}

# Sub: _is_constant # {{{
# Returns true if the value is a constant.  Constants are TRUE, FALSE,
# and any signed number.  A leading AND or OR keyword is removed first.
sub _is_constant( $$ ) {
	my ( $self, $val ) = @_;
	return 0 unless defined $val;
	$val =~ s/^\s*(?:and|or)\s+//;
	return
		$val =~ m/^\s*(?:TRUE|FALSE)\s*$/i || $val =~ m/^\s*-?\d+\s*$/ ? 1 : 0;
} # }}}

sub _d { # {{{
	my ($package, undef, $line) = caller 0;
	@_ = map { (my $temp = $_) =~ s/\n/\n# /g; $temp; }
		  map { defined $_ ? $_ : 'undef' }
		  @_;
	print STDERR "# $package:$line $PID ", join(' ', @_), "\n";
} # _d # }}}

# ###########################################################################
# End SQLParser package
# ###########################################################################

# }}}
}

# vi: set filetype=perl syntax=perl commentstring=#%s foldmarker=\ {{{,\ }}} foldmethod=marker colorcolumn=80 so=3:
