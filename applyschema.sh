#!/bin/bash

# stdlib.sh should be in /usr/local/lib/stdlib.sh, which can be found as
# follows by scripts located in /usr/local/{,s}bin/...
declare std_LIB='stdlib.sh'
type -pf 'dirname' >/dev/null 2>&1 || function dirname() { : ; }
# shellcheck disable=SC2153
for std_LIBPATH in							\
	"$( dirname -- "${BASH_SOURCE:-${0:-.}}" )"			\
	'.'								\
	"$( dirname -- "$( type -pf "${std_LIB}" 2>/dev/null )" )"	\
	"$( dirname -- "${BASH_SOURCE:-${0:-.}}" )/../lib"		\
	'/usr/local/lib'						\
	 ${FPATH:+${FPATH//:/ }}					\
	 ${PATH:+${PATH//:/ }}
do
	if [[ -r "${std_LIBPATH}/${std_LIB}" ]]; then
		break
	fi
done
unset -f dirname

# Attempt to use colourised output if the environment indicates that this is
# an appropriate choice...
[[ -n "${LS_COLORS:-}" ]] && \
	export STDLIB_WANT_COLOUR="${STDLIB_WANT_COLOUR:-1}"

# We want the non if-then-else functionality here - the third element should be
# executed if either of the first two fail...
#
# N.B. The shellcheck 'source' option is only valid with shellcheck 0.4.0 and
#      later...
#
# shellcheck disable=SC1091,SC2015
# shellcheck source=/usr/local/lib/stdlib.sh
[[ -r "${std_LIBPATH}/${std_LIB}" ]] && source "${std_LIBPATH}/${std_LIB}" || {
	# shellcheck disable=SC2154
	echo >&2 "FATAL:  Unable to source ${std_LIB} functions: ${?}${std_ERRNO:+ (ERRNO ${std_ERRNO})}"
	exit 1
}

std_DEBUG="${DEBUG:-0}"
std_TRACE="${TRACE:-0}"

# std_RELEASE was only added in release 1.3, and vcmp appeared immediately
# after in release 1.4...
if [[ "${std_RELEASE:-1.3}" == "1.3" ]] || std::vcmp "${std_RELEASE}" -lt "2.0.0"; then
	die "stdlib is too old - please update '${std_LIBPATH}/${std_LIB}' to at least v2.0.0"
elif std::vcmp "${std_RELEASE}" -lt "2.0.4"; then
        warn "stdlib is outdated - please update '${std_LIBPATH}/${std_LIB}' to at least v2.0.4"
fi

SCRIPT='myway.pl'
COMPATIBLE='1.4.0'
VALIDATOR='validateschema.sh'

# We want to be able to debug applyschema.sh without debugging myway.pl...
if [[ -n "${MYDEBUG:-}" ]]; then
	DEBUG="${MYDEBUG:-0}"
else
	unset DEBUG
fi

# Override `die` to return '2' on fatal error...
function die() { # {{{
	if [[ -n "${*:-}" ]]; then
		if [[ 'function' == "$( type -t std::colour )" ]]; then
			std_DEBUG=1 std::log >&2 "$( std::colour 'FATAL: ' )" "${*}"
		else
			std_DEBUG=1 std::log >&2 'FATAL: ' "${*}"
		fi
	fi
	std::cleanup 2

	# Unreachable
	return 1
} # die # }}}

function lock() { # {{{
	local lockfile="${1:-/var/lock/${NAME}.lock}"

	mkdir -p "$( dirname "${lockfile}" )" 2>/dev/null || exit 1

	if ( set -o noclobber ; echo "${$}" >"${lockfile}" ) 2>/dev/null; then
		std::garbagecollect "${lockfile}"
		return ${?}
	else
		return 1
	fi

	# Unreachable
	return 128
} # lock # }}}

# shellcheck disable=SC2155
function main() { # {{{
	local myway="$( std::requires --path "${SCRIPT}" )"

	local truthy='^(on|y(es)?|true|1)$'
	local falsy='^(off|n(o)?|false|0)$'
	#local silentfilter='^((Useless|Use of|Cannot parse|!>) |\s*$)'

	local actualpath filename validator
	local lockfile="/var/lock/${NAME}.lock"
	[[ -w /var/lock ]] || lockfile="${TMPDIR:-/tmp}/${NAME}.lock"

	# Sigh...
	[[ -d '/opt/vertica/bin' ]] && export PATH="${PATH:+${PATH}:}/opt/vertica/bin"

	${myway} --help >/dev/null 2>&1 || die "${myway} is failing to" \
		"execute - please confirm that all required perl modules are" \
		"available"
	# Alternatively...
	# eval "$( std::requires --path "perl" ) -c ${myway}" || die "${myway} is failing to compile - please confirm that all required perl modules are available"

	local version="$( ${myway} --version 2>/dev/null | rev | cut -d' '  -f 1 | rev | cut -d'.' -f 1-3 )"
	if [[ "${version}" != "${COMPATIBLE}" ]]; then
		die "$( basename "${0}" ) is compatible only with ${SCRIPT} version ${COMPATIBLE} - found version ${version} at '${myway}'"
	fi

	local -i novsort=0
	if ! sort -V <<<'' >/dev/null 2>&1; then
		warn 'Version sort unavailable - Stored Procedure load-order' \
		     'cannot be guaranteed.'
		warn 'Press ctrl+c now to abort ...'
		sleep 5
		warn 'Proceeding ...'
		novsort=1
	fi

	local arg schema db vdb dblist clist
	local -l progress='auto'
	local -i dryrun=0 cache=0 memcache=1 quiet=0 silent=0 keepgoing=0 force=0 validate=1
	local -a extra=()
	while [[ -n "${1:-}" ]]; do
		arg="${1}"
		case "${arg}" in
			-c|--conf|--config)
				shift
				if [[ -z "${1:-}" ]]; then
					die "Option ${arg} requires an argument"
				elif [[ ! -r "${1}" ]]; then
					die "Path ${1} cannot be read"
				else
					filename="${1}"
				fi
				;;
			-d|--database|--databases|-o|--only)
				shift
				if [[ -z "${1:-}" ]]; then
					die "Option ${arg} requires an argument"
				elif [[ "${1}" =~ ^-- ]]; then
					die "Argument '${1}' to option ${arg} looks like an option... aborting"
				else
					dblist="${dblist:+,}${1}"
				fi
				;;
			-f|--force)
				force=1
				;;
			-h|--help)
				export std_USAGE='[--config <file>] [--schema <path>] [ [--databases <database>[,...]] | [--clusters <cluster>[,...]] ] [--cache-results] [--no-memory-cache] [--dry-run] [--quiet|--silent] [--no-wrap] [--keep-going] [--force] [--no-validate] [--progress=<always|auto|never>] | [--locate <database>]'
				std::usage
				;;
			-k|--keep-going|--keepgoing)
				keepgoing=1
				;;
			-l|--locate|--whereis|--server|--host)
				shift
				if [[ -z "${1:-}" ]]; then
					die "Option ${arg} requires an argument"
				elif [[ "${1}" =~ ^-- ]]; then
					die "Argument '${1}' to option ${arg} looks like an option... aborting"
				else
					db="${1}"
				fi
				;;
			-m|--nomem|--nomemorycache|--no-memory-cache)
				memcache=0
				;;
			-n|--nowrap|--no-wrap)
				export STDLIB_WANT_WORDWRAP=0
				;;
			-p|--progress|--progress=*)
				if grep -Fq '=' <<<"${arg}"; then
					progress="$( cut -d'=' -f 2- <<<"${arg}" )"
					arg="$( cut -d'=' -f 1- <<<"${arg}" )"
				else
					shift
					progress="${1:-}"
				fi
				if [[ -z "${progress:-}" ]]; then
					die "Option ${arg} requires an argument"
				elif [[ "${progress}" =~ ^-- ]]; then
					die "Argument '${progress}' to option ${arg} looks like an option... aborting"
				elif ! [[ "${progress}" =~ always|auto|never ]]; then
					die "Argument to option ${arg} must have value 'always', 'auto', or 'never': '${progress}' is not recognised... aborting"
				fi
				;;
			-q|--quiet)
				quiet=1
				;;
			-r|--cache|--cacheresults|--cache-results)
				cache=1
				;;
			-s|--schema|--schemata|--directory|--scripts)
				shift
				if [[ -z "${1:-}" ]]; then
					die "Option ${arg} requires an argument"
				elif [[ ! -d "${1}" ]]; then
					die "Directory ${1} does not exist"
				else
					actualpath="${1}"
				fi
				;;
			-u|--cluster|--clusters)
				shift
				if [[ -z "${1:-}" ]]; then
					die "Option ${arg} requires an argument"
				elif [[ "${1}" =~ ^-- ]]; then
					die "Argument '${1}' to option ${arg} looks like an option... aborting"
				else
					clist="${clist:+,}${1}"
				fi
				;;
			   --no-validate)
				validate=0
				;;
			   --dry-run|--verify)
				dryrun=1
				;;
			   --silent)
				silent=1
				;;
			   --)
				shift
				while [[ -n "${1:-}" ]]; do
					extra+=( "${1}" )
					shift
				done
				;;
			   *)
				die "Unknown argument '${arg}'"
				;;
		esac
		shift
	done

	if [[ -n "${dblist:-}" && -n "${clist:-}" ]]; then
		die 'Options --databases and --clusters are mutually exclusive'
	fi

	filename="$( std::findfile -app 'dbtools' -name 'schema.conf' -dir '/etc' ${filename:+-default "${filename}"} )"
	if [[ ! -r "${filename}" ]]; then
		die 'Cannot read configuration file, please use --config to specify location'
	else
		debug "Using configuration file '${filename}' ..."
	fi

	local defaults="$( std::getfilesection "${filename}" 'DEFAULT' | sed -r 's/#.*$// ; /^[^[:space:]]+\.[^[:space:]]+\s*=/s/\./_/' | grep -Ev '^\s*$' | sed -r 's/\s*=\s*/=/' )"
	local hosts="$( std::getfilesection "${filename}" 'CLUSTERHOSTS' | sed -r 's/#.*$// ; /^[^[:space:]]+\.[^[:space:]]+\s*=/s/\./_/' | grep -Ev '^\s*$' | sed -r 's/\s*=\s*/=/' )"
	local databases="$( std::getfilesection "${filename}" 'DATABASES' | sed -r 's/#.*$// ; /^[^[:space:]]+\.[^[:space:]]+\s*=/s/\./_/' | grep -Ev '^\s*$' | sed -r 's/\s*=\s*/=/' )"

	[[ -n "${databases:-}" ]] || die "No databases defined in '${filename}'"

	debug "DEFAULTs:\n${defaults}\n"
	debug "CLUSTERHOSTS:\n${hosts}\n"
	debug "DATABASES:\n${databases}\n"

	# The 'grep' build used by gitbash doesn't support '-m'!
	# (... but does, surprisingly, apparently support '-P')
	local mgrep='grep -m 1'
	${mgrep} 'x' <<<'x' >/dev/null 2>&1 || mgrep='grep'

	if [[ -n "${db:-}" ]]; then
		local name="$( ${mgrep} -o "^${db}$" <<<"${databases}" )"
		[[ -n "${name:-}" ]] || die "Database '${db}' not defined in '${filename}'"

		local details="$( std::getfilesection "${filename}" "${name}" | sed -r 's/#.*$// ; /^[^[:space:]]+\.[^[:space:]]+\s*=/s/\./_/' | grep -Ev '^\s*$' | sed -r 's/\s*=\s*/=/' )"
		[[ -n "${details:-}" ]] || die "Database '${db}' lacks a configuration block in '${filename}'"
		debug "${db}:\n${details}\n"

		local host="$( ${mgrep} 'host=' <<<"${details}" | cut -d'=' -f 2 )"
		if [[ -n "${host:-}" ]]; then
			output "Database '${db}' has write master '${host}'"
		else
			local cluster="$( ${mgrep} 'cluster=' <<<"${details}" | cut -d'=' -f 2 )"
			[[ -n "${cluster:-}" ]] || die "Database '${db}' has no defined cluster membership in '${filename}'"

			local master="$( ${mgrep} "^${cluster}=" <<<"${hosts}" | cut -d'=' -f 2 )"
			[[ -n "${master:-}" ]] || die "Cluster '${cluster}' (of which '${db}' is a stated member) is not defined in '${filename}'"

			output "Database '${db}' is a member of cluster '${cluster}' with write master '${master}'"
		fi

		exit 0
	fi

	unset mgrep

	local -i result rc=0 founddb=0

	debug 'Establishing lock ...'

	if [[ -s "${lockfile}" ]]; then
		local -i blockingpid
		blockingpid="$( <"${lockfile}" )"
		if (( blockingpid > 1 )); then
			# shellcheck disable=SC2086
			if kill -0 ${blockingpid} >/dev/null 2>&1; then
				#local processname="$( ps -e | grep "^${blockingpid}" | rev | cut -d' ' -f 1 | rev )"
				local processname="$( pgrep -lF "${lockfile}" | cut -d' ' -f 2- )"
				die "Lock file '${lockfile}' (belonging to process '${processname:-}', PID '${blockingpid}') exists - aborting"
			else
				warn "Lock file '${lockfile}' (belonging to obsolete PID '${blockingpid}') exists - removing stale lock"
				rm -f "${lockfile}" || die "Lock file removal failed: ${?}"
			fi
		else
			warn "Lock file '${lockfile}' exists with invalid content '$( head -n 1 "${lockfile}" )' - removing broken lock"
			rm -f "${lockfile}" || die "Lock file removal failed: ${?}"
		fi
	fi

	if [[ -e "${lockfile}" ]]; then
		warn "Lock file '${lockfile}' exists, but is empty - removing broken lock"
		rm -f "${lockfile}" || die "Lock file removal failed: ${?}"
	fi

	lock "${lockfile}" || die "Creating lock file '${lockfile}' failed - aborting"
	sleep 0.1

	local lockpid="$( <"${lockfile}" )"
	if [[ -e "${lockfile}" && -n "${lockpid:-}" && "${lockpid}" == "${$}" ]]; then
		:
	elif [[ -e "${lockfile}" && -n "${lockpid:-}" ]]; then
		die "Lock file '${lockfile}' is for process ${lockpid}, not our PID ${$} - aborting"
	elif [[ -e "${lockfile}" ]]; then
		die "Lock file '${lockfile}' exists but is empty - aborting"
	else
		die "Lock file '${lockfile}' does not exist - aborting"
	fi
	unset lockpid

	# We have a lock...

	(( std_TRACE )) && set -o xtrace

	# Variables we expect to source when we 'eval' the configuration block,
	# below...
	#
	# shellcheck disable=SC2034
	local options_debug options_force options_notice options_quiet \
	      options_silent options_warn
	# shellcheck disable=SC2034
	local mysql_compat mysql_relaxed
	local backups backups_compress backups_extended backups_keep \
	      backups_keeplock backups_lock backups_separate \
	      backups_skipmeta backups_transactional
	local cluster dbadmin environment managed passwd path procedures \
	      procedures_marker version_max
	local preprocessor_validate parser_allowdrop

	# Vertica-specific options
	#
	local dsn database schema
	local -l syntax

	# Additionally, we want to ensure that ${defaults} and ${databases}
	# *only* set the variables defined above, and that ${hosts} (checked
	# further below) /never/ clashes with a variable defined above (or any
	# other)...
	#
	local var val
	while read -r var; do
		[[ "${var}" =~ [a-zA-Z_][a-zA-Z_0-9]* ]] || die "Keyword '${var}' from section '[DEFAULT]' is invalid"

		debug "Checking '[DEFAULT]' keyword '${var}'"
		if ! val="$( typeset -p 2>/dev/null | grep -E "^declare -. ${var}(=.*$|$)" 2>/dev/null )"; then

			# For no reason I can discern, and only with DEBUG and
			# TRACE disabled, the test above can incorrectly fail,
			# which makes me suspect a timing issue.
			#
			# The (unfortunately intentional) repetition of the
			# same test below isn't a nice solution, but is
			# hopefully at least a sensible work-around...
			#
			val="$( typeset -p 2>/dev/null | grep -E "^declare -. ${var}(=.*$|$)" 2>/dev/null )"
		fi
		if [[ -n "${val:-}" ]]; then
			[[ "${val}" =~ = ]] && debug "Replacing current value '$( cut -d'=' -f 2- | sed 's/^"// ; s/"$//' )' for variable '${var}' in [DEFAULT] section"
		else
			die "Unrecognised keyword '${var}' in [DEFAULT] section"
		fi
	done < <( grep -o '^[^=]\+=' <<<"${defaults}" | sed 's/=$//' | sort | uniq )
	while read -r var; do
		[[ "${var}" =~ [a-zA-Z_][a-zA-Z_0-9]* ]] || die "Keyword '${var}' from section '[CLUSTERHOSTS]' is invalid"

		debug "Checking '[CLUSTERHOSTS]' keyword '${var}'"
		if ! val="$( typeset -p 2>/dev/null | grep -E "^declare -. ${var}(=.*$|$)" 2>/dev/null )"; then

			# As above, this test appears to be non-deterministic :(
			#
			val="$( typeset -p 2>/dev/null | grep -E "^declare -. ${var}(=.*$|$)" 2>/dev/null )"
		fi
		if [[ -n "${val:-}" ]]; then
			# shellcheck disable=SC2016
			die "Reserved keyword '${var}' ${!var:+with value '${!var}' }in [CLUSTERHOSTS] section"
		fi
	done < <( grep -o '^[^=]\+=' <<<"${hosts}" | sed 's/=$//' | sort | uniq )

	# We're going to eval our config file sections - hold onto your hats!
	eval "${defaults}"
	eval "${hosts}"

	# We only have configuration options past this point...

	# Command-line options override configuration-file settings, so we only
	# check the value of 'options.quiet' when ${quiet} hasn't already been
	# set to one...
	if ! (( quiet )); then
		if [[ -n "${options_quiet:-}" ]]; then
			if grep -Eiq "${truthy}" <<<"${options_quiet}"; then
				quiet=1
			fi
		fi
	fi
	if ! (( silent )); then
		if [[ -n "${options_silent:-}" ]]; then
			if grep -Eiq "${truthy}" <<<"${options_silent}"; then
				silent=1
			fi
		fi
	fi

	# Ditto for 'preprocessor.validate' when ${validate} hasn't already
	# been set to zero...
	if (( validate )); then
		if [[ -n "${preprocessor_validate:-}" ]]; then
			if grep -Eiq "${falsy}" <<<"${preprocessor_validate}"; then
				validate=0
			fi
		fi
	fi

	if ! (( validate )); then
		warn 'Validation disabled - applied schema may not be standards-compliant'
	else
		# stdlib.sh prior to v2.0.0 incorrectly didn't accept
		# multi-argument calls to std::requires
		#
		#if validator="$( std::requires --no-exit --path "${VALIDATOR}" )" && [[ -x "${validator:-}" ]]; then
		if validator="$( type -pf "${VALIDATOR}" 2>/dev/null )" && [[ -x "${validator:-}" ]]; then
			debug "Using '${validator}' to validate data"
		else
			if (( force || dryrun )); then
				warn "Cannot locate script '${VALIDATOR}' to perform data validation"
				validate=0
				unset validator
			else
				error "Cannot locate script '${VALIDATOR}' to perform data validation"
				die "Invoke with '--force' parameter to proceed without validation - aborting"
			fi
		fi
	fi

	for db in ${databases}; do
		# Run the block below in a sub-shell so that we don't have to
		# manually sanitise the environment on each iteration.
		#
		( # {{{
		if [[ -n "${dblist:-}" ]] && ! grep -q ",${db}," <<<",${dblist},"; then
			debug "Skipping deselected database '${db}' ..."
			exit 0 # continue
		fi

		local details="$( std::getfilesection "${filename}" "${db}" | sed -r 's/#.*$// ; /^[^[:space:]]+\.[^[:space:]]+\s*=/s/\./_/' | grep -Ev '^\s*$' | sed -r 's/\s*=\s*/=/' )"
		[[ -n "${details:-}" ]] || die "Database '${db}' lacks a configuration block in '${filename}'"
		debug "${db}:\n${details}\n"

		# Validate [DEFAULT] overrides for the database in question...
		# (Unfortunately, we must do this here, before we know whether
		# this database is unmanaged or to be skipped - so we /can/
		# fail due to configuration errors in databases we were never
		# going to deploy)
		#
		while read -r var; do
			[[ "${var}" =~ [a-zA-Z_][a-zA-Z_0-9]* ]] || die "Keyword '${var}' from section '[${db}]' is invalid"

			debug "Checking database [${db}] keyword '${var}'"
			if ! val="$( typeset -p 2>/dev/null | grep -E "^declare -. ${var}(=.*$|$)" 2>/dev/null )"; then

				# As above, this test appears to be
				# non-deterministic :(
				#
				val="$( typeset -p 2>/dev/null | grep -E "^declare -. ${var}(=.*$|$)" 2>/dev/null )"
			fi
			if [[ -n "${val:-}" ]]; then
				[[ "${val}" =~ = ]] && debug "Replacing current value '$( cut -d'=' -f 2- | sed 's/^"// ; s/"$//' )' for variable '${var}' in [${db}] section"
			else
				die "Unrecognised keyword '${var}' in [${db}] section"
			fi
		done < <( grep -o '^[^=]\+=' <<<"${details}" | sed 's/=$//' | sort | uniq )
		eval "${details}"

		if [[ -n "${clist:-}" ]] && ! grep -q ",${cluster:-.*}," <<<",${clist},"; then
			(( silent )) || info "Skipping database '${db}' from deselected cluster '${cluster:-all}' ..."
			exit 0 # continue
		fi

		if grep -Eiq "${falsy}" <<<"${managed:-}"; then
			(( silent )) || info "Skipping unmanaged database '${db}' ..."
			founddb=1
			exit 3 # See below...
		fi

		if ! (( silent )); then
			(( founddb )) && output

			info "Processing configuration for database '${db}' ..."
			if ! (( std_DEBUG )); then
				[[ -n "${dblist:-}" && "${dblist}" == "${db}" ]] && output
			fi
		fi

		local -a messages=()

		[[ -n "${dbadmin:-}" ]] || messages+=( "No database user ('dbadmin') specified for database '${db}'" )
		[[ -n "${passwd:-}" ]] || messages+=( "No database user password ('passwd') specified for database '${db}'" )

		if [[ -z "${actualpath:-}" ]]; then
			# Allow command-line parameter to override config file
			actualpath="${path:-}"
		fi
		path="$( readlink -e "${actualpath:-.}" )" || die "Failed to canonicalise path '${actualpath}': ${?}"
		actualpath=""

		if [[ -z "${path:-}" ]]; then
			messages+=( "Path to schema files and stored procedures not defined for database '${db}'" )
		else
			if [[ ! -d "${path}" ]]; then
				messages+=( "Schema-file directory '${path}' does not exist for database '${db}'" )
			else
				if [[ "$( basename "${path}" )" == "${db}" ]]; then
					if grep -Eiq "${truthy}" <<<"${procedures:-}"; then
						messages+=( "Cannot load Stored Procedures for database '${db}' since a database-specific schema-file location is specified" )
					else
						debug "Using schema-file '${path}' for database '${db}'"
						actualpath="${path%/}"
					fi
				else
					if [[ "$( basename "${path}" )" == "schema" ]]; then
						local text="Correcting path '${path}'"
						path="$( dirname "${path}" )"
						text+=" to '${path}' for database '${db}' ..."
						debug "${text}"
						unset text
					fi
					if [[ ! -d "${path}"/schema/"${db}" ]]; then
						messages+=( "Cannot determine schema-files path for database '${db}'" )
					else
						if [[ -d "${path}"/schema/"${db}"/"${db}" ]]; then
							actualpath="${path}"/schema/"${db}"/"${db}"
							debug "Using schema-files path '${actualpath}' for database '${db}'"

						else
							debug "Using schema-files path '${path}/schema/${db}' for database '${db}'"
						fi
						if grep -Eiq "${truthy}" <<<"${procedures:-}"; then
							#if [[ -d "${path}"/procedures/"${db}" ]]; then
							#	debug "Using '${path}/procedures/${db}' for '${db}' Stored Procedures"
							if [[ -n "${actualpath:-}" && -d "${actualpath}"/../procedures ]]; then
								debug "Using Stored Procedures path '$( readlink -e "${actualpath}/../procedures" )' for database '${db}'"
							elif (( $( find "${path}"/procedures/ -mindepth 1 -maxdepth 1 -type d -name "${db}*" 2>/dev/null | wc -l ) )); then
								debug "Using Stored Procedures path '${path}/procedures' for database '${db}'"
							else
								messages+=( "Cannot determine Stored Procedures path for database '${db}'" )
							fi
						fi
					fi
				fi
			fi
		fi
		(( ${#messages[@]} )) && die "${messages[@]}"

		if [[ -n "${host:-}" ]]; then
			# ${host} is verified below...
			:
		elif [[ -n "${cluster:-}" ]]; then
			if [[ -n "${!cluster:-}" ]]; then
				host="${!cluster}"
			else
				die "Database '${db}' has cluster '${cluster}', for which no write master is defined"
			fi
		else
			die "Neither 'host' nor 'cluster' membership is defined for database '${db}' in '${filename}'"
		fi
		if [[ -n "${syntax:-}" && "${syntax}" == 'vertica' && -z "${dsn:-}" ]]; then
			die "'dsn' is a mandatory parameter when 'syntax' is set to '${syntax}'"
		fi

		if [[ 'vertica' == "${syntax:-}" ]]; then
			if ! std::requires --no-exit --no-quiet 'vsql'; then
				warn "Vertica 'vsql' binary cannot be found - some integrity checks will be skipped, and errors may occur if databases or schema aren't in the anticipated state"
			fi
		else
			# It's rare for a system to have libmysqlclient and not
			# the 'mysql' client binary, but we shouldn't fail if
			# we lack the latter...
			if ! std::requires --no-exit --no-quiet 'mysql'; then
				warn "MySQL 'mysql' binary cannot be found - some integrity checks will be skipped, and errors may occur if databases or schema aren't in the anticipated state"
			fi
		fi

		debug "Attempting to resolve host '${host}' ..."
		if (( std_DEBUG )); then
			debug 'Not performing host resolution in DEBUG mode - skipping'
		else
			std::ensure "Failed to resolve host '${host}'" getent hosts "${host}"
		fi

		local -a params=( -u "${dbadmin}" -p "${passwd}" -h "${host}" -d "${db}" )
		local -a extraparams
		local option

		if (( validate )); then
			(( quiet | silent )) || info "Validating database '${db}' ..."
			# shellcheck disable=SC2046
			if ! ${validator} ${filename:+--config "${filename}"} -d "${db}" -s "${actualpath:-${path}/schema}" $( (( cache )) && echo '--cache-results' ) $( (( memcache )) || echo '--no-memory-cache' ) $( (( dryrun )) && echo '--dry-run' ) $( (( quiet )) && echo '--quiet' ) $( (( silent )) && echo '--silent' ) --from-applyschema; then
				die "Validation of database '${db}' failed - aborting"
			fi
		fi

		if [[ -n "${dsn:-}" ]]; then
			case "${syntax:-}" in
				vertica)
					params=( --syntax ${syntax} --dsn "${dsn}" )
					if ! grep -Eiq '(^|;)username=([^;]+)(;|$)' <<<"${dsn}"; then
						params+=( -u "${dbadmin}" )
					fi
					if ! grep -Eiq '(^|;)password=([^;]+)(;|$)' <<<"${dsn}"; then
						params+=( -p "${passwd}" )
					fi
					if ! grep -Eiq '(^|;)servername=([^;]+)(;|$)' <<<"${dsn}"; then
						params+=( -h "${host}" )
					fi
					if ! grep -Eiq '(^|;)database=([^;]+)(;|$)' <<<"${dsn}"; then
						if [[ -n "${database:-}" ]]; then
							params+=( -d "${database}" )
							vdb="${database}"
						else
							params+=( -d "${db}" )
							vdb="${db}"
						fi
					else
						vdb="$( grep -Eio '(^|;)database=([^;]+)(;|$)' <<<"${dsn}" | cut -d'=' -f 2 | cut -d';' -f 1 )"
						debug "Updated database from '${database:-${db}}' to '${vdb}' from DSN '${dsn}'"
					fi
					if [[ -n "${schema:-}" ]]; then
						params+=( --vertica-schema "${schema}" )
					else
						# Default to the name of the database being migrated?
						#warn "No 'schema' value specified for Vertica database - unless 'SEARCH_PATH' is set appropriately, statements may fail"
						schema="${db}"
						params+=( --vertica-schema "${schema}" )
						warn "No 'schema' value specified for Vertica database - defaulting to '${schema}'"
					fi
					;;
				'')
					die "'syntax' is a mandatory parameter when a DSN is used"
					;;
				*)
					die "Unknown database type '${syntax:-}'"
					;;
			esac
		fi

		[[ -n "${environment:-}" ]] && params+=( -e "${environment}" )

		for option in force warn notice debug silent quiet; do
			eval echo "\${options_${option}:-}" | grep -Eiq "${truthy}" && params+=( --${option} )
		done
		if [[ "${syntax:-}" != 'vertica' ]]; then
			for option in compat relaxed; do
				eval echo "\${mysql_${option}:-}" | grep -Eiq "${truthy}" && params+=( --mysql-${option} )
			done
			if grep -Eiq "${falsy}" <<<"${backups:-}"; then
				params+=( --no-backup )
			else
				[[ -n "${backups_compress:-}" ]] && params+=( --compress "${backups_compress}" )
				grep -Eiq "${truthy}" <<<"${backups_transactional:-}" && params+=( --transactional )
				grep -Eiq "${truthy}" <<<"${backups_lock:-}" && params+=( --lock )
				grep -Eiq "${truthy}" <<<"${backups_keeplock:-}" && params+=( --keep-lock )
				grep -Eiq "${truthy}" <<<"${backups_separate:-}" && params+=( --separate-files )
				grep -Eiq "${truthy}" <<<"${backups_skipmeta:-}" && params+=( --skip-metadata )
				grep -Eiq "${truthy}" <<<"${backups_extended:-}" && params+=( --extended-insert )

				grep -Eiq "${truthy}" <<<"${backups_keep:-}" && params+=( --keep-backup )
			fi
		fi
		grep -Eiq "${truthy}" <<<"${parser_allowdrop:-}" && params+=( --allow-unsafe )

		[[ -n "${progress:-}" ]] && params+=( --progress=${progress} )

		founddb=1

		# Initialise databases first, as they must be present before
		# Stored Procedures are loaded.
		#
		(( silent )) || info "Launching '${SCRIPT}' to perform database initialisation for database '${db}' ..."
		extraparams=()
		if [[ -n "${actualpath:-}" ]]; then
			extraparams=( --scripts "${actualpath}/"*.sql )
		else
			extraparams=( --scripts "${path}/schema/${db}/"*.sql )
		fi
		#if (( ${#extra[@]} )); then
		if [[ -n "${extra[*]:-}" ]]; then
			extraparams+=( "${extra[@]}" )
		fi
		if (( dryrun )); then
			extraparams+=( '--dry-run' )
		fi

		if [[ 'vertica' != "${syntax:-}" ]] && type -pf mysql >/dev/null 2>&1 && ! mysql -u "${dbadmin}" -p"${passwd}" -h "${host}" <<<'QUIT' >/dev/null 2>&1; then
			die "Cannot connect to MySQL instance on host '${host}' as user '${dbadmin}' - is database running?"
		elif [[ 'vertica' == "${syntax:-}" ]] && type -pf vsql >/dev/null 2>&1 && ! vsql -U "${dbadmin}" -w "${passwd}" -h "${host}" <<<'\q' >/dev/null 2>&1; then
			die "Cannot connect to Vertica instance on host '${host}' as user '${dbadmin}' - is database running?"
		fi

		debug "About to initialise database: '${myway} ${params[*]} ${extraparams[*]}'"
		debug "N.B. Parameters not required by the --init stage are not passed until later..."

		local response=''
		if (( silent )); then
			${myway} "${params[@]}" "${extraparams[@]}" --init >/dev/null 2>&1
		elif (( quiet )); then
			# Loses return code to grep :(
			#${myway} "${params[@]}" "${extraparams[@]}" --init 2>&1 >/dev/null | grep -Ev --line-buffered "${silentfilter}"

			# Throw away stdout but redirect stderr to stdout...
			# shellcheck disable=SC2069
			response="$( ${myway} "${params[@]}" "${extraparams[@]}" --init 2>&1 >/dev/null )"
		else
			${myway} "${params[@]}" "${extraparams[@]}" --init
		fi
		result=${?}

		local connectoutput=""
		local -i allowfail=0 canconnect=1

		if (( dryrun )); then
			allowfail=1
			keepgoing=1
		else
			# We may still have an empty database (or schema, in
			# Vertica terms) with no metadata tracking tables...
			#
			if [[ 'vertica' == "${syntax:-}" ]]; then
				if type -pf vsql >/dev/null 2>&1; then
					if connectoutput="$( vsql -U "${dbadmin}" -w "${passwd}" -h "${host}" -d "${vdb}" <<<'\q' 2>&1 )"; then
						allowfail=1
					else
						canconnect=0
					fi
				else
					# We don't know, we'll just have to
					# assume that we're good to continue,
					# and hope that Vertica can manage
					# things otherwise...
					allowfail=1
				fi
			else
				if type -pf mysql >/dev/null 2>&1; then
					if connectoutput="$( mysql -u "${dbadmin}" -p"${passwd}" -h "${host}" "${db}" <<<'QUIT' 2>&1 )"; then
						allowfail=1
					else
						canconnect=0
					fi
				else
					# As above...
					allowfail=1
				fi
			fi
		fi

		if (( result )); then
			if (( allowfail )); then
				(( quiet || silent )) || info "Initialisation of database '${db}' (${myway} ${params[*]} ${extraparams[*]} --init) expected failure: ${result}"
			else
				if (( keepgoing )); then
					warn "Initialisation of database '${db}' (${myway} ${params[*]} ${extraparams[*]} --init) failed: ${result}"
					rc=1

					# Output if we've no further errors...
					if (( canconnect )); then
						output $'\n\nContinuing to next database, if any ...\n'
					fi
				else
					output >&2 "${response}"
					die "Initialisation of database '${db}' (${myway} ${params[*]} ${extraparams[*]} --init) failed: ${result}"
				fi
			fi
		fi

		if ! (( canconnect )); then
			if [[ 'vertica' != "${syntax:-}" ]]; then
				(( silent )) || warn "Skipping migration for non-existent database '${db}'${connectoutput:+:}"
				[[ -n "${connectoutput:-}" ]] && output >&2 "${connectoutput}"
				rc=1
			elif [[ 'vertica' == "${syntax:-}" ]]; then
				(( silent )) || warn "Skipping migration for non-existent Vertica database '${db}'${connectoutput:+:}"
				[[ -n "${connectoutput:-}" ]] && output >&2 "${connectoutput}"
				rc=1
			fi

			if (( keepgoing )); then
				output $'\n\nContinuing to next database, if any ...\n'
			fi
		elif (( result )) && ! (( allowfail )); then
			# Don't perform schema migration if we failed to
			# initialise and we're not allowing failures...
			:
		else
			# At this point, we've successfully initialised (or
			# we've failed at this but ${allowfail} is set), and we
			# have been able to connect to the database we're
			# attempting to migrate.
			#

			# Load stored-procedures next, as references to tables aren't
			# checked until the SP is actually executed, but SPs may be
			# invoked as part of schema deployment.
			#
			# N.B. Vertica 8.x does support Stored Procedures.
			#
			if grep -Eiq "${truthy}" <<<"${procedures:-}"; then
				local -a procparams=( --mode procedure )
				if [[ -n "${procedures_marker:-}" ]]; then
					procparams+=( --substitute --marker "${procedures_marker}" )
				else
					procparams+=( --substitute )
				fi

				local -a reorder=( sort -V )
				if (( novsort )); then
					reorder=( tac )
				fi

				local procedurepath="${path}/procedures"
				if [[ -d "${path}"/schema/"${db}"/procedures ]]; then
					procedurepath="${path}"/schema/"${db}"/procedures
				fi

				local ppath
				local -i sploaded=0
				while read -r ppath; do
					[[ -f "${ppath}/${db}.metadata" ]] || continue

					extraparams=( --scripts "${ppath}" )
					#if (( ${#extra[@]} )); then
					if [[ -n "${extra[*]:-}" ]]; then
						extraparams+=( "${extra[@]}" )
					fi
					if (( dryrun )); then
						extraparams+=( '--dry-run' )
					fi

					(( silent )) || info "Launching '${SCRIPT}' with path '${ppath}' to update Stored Procedures for database '${db}' ..."

					debug "About to apply Stored Procedures: ${myway} ${params[*]} ${procparams[*]} ${extraparams[*]} ${extra[*]:-}"
					debug "N.B. Parameters not required by the --mode=procedure stage are not passed until later..."
					if (( silent )); then
						${myway} "${params[@]}" "${procparams[@]}" "${extraparams[@]}" >/dev/null 2>&1
					elif (( quiet )); then
						# Loses return code to grep :(
						#${myway} "${params[@]}" "${procparams[@]}" "${extraparams[@]}" 2>&1 >/dev/null | grep -Ev --line-buffered "${silentfilter}"

						# Throw away stdout but redirect stderr to stdout...
						# shellcheck disable=SC2069
						${myway} "${params[@]}" "${procparams[@]}" "${extraparams[@]}" 2>&1 >/dev/null
					else
						${myway} "${params[@]}" "${procparams[@]}" "${extraparams[@]}"
					fi
					result=${?}
					if (( result )); then
						if (( keepgoing )); then
							warn "Loading of stored procedures into database '${db}' (${myway} ${params[*]} ${procparams[*]} ${extraparams[*]}${extra[*]:+ ${extra[*]}}) failed: ${result}"
							output $'\n\nContinuing to next database, if any ...\n'
							rc=1
						else
							die "Loading of stored procedures into database '${db}' (${myway} ${params[*]} ${extraparams[*]}${extra[*]:+ ${extra[*]}}) failed: ${result}"
						fi
					else
						sploaded=1
					fi
				done < <( find "${procedurepath}"/ -mindepth 1 -maxdepth 2 -type d 2>/dev/null | grep "${db}" | "${reorder[@]}" )

				if ! (( sploaded )); then
					if (( keepgoing )); then
						warn "Stored procedure load requested for database '${db}', but no valid Stored Procedures were processed"
						output $'\n\nContinuing to next database, if any ...\n'
						rc=1
					else
						die "Stored procedure load requested for database '${db}', but no valid Stored Procedures were processed"
					fi
				fi

				debug "Stored Procedures loaded for database '${db}'\n"

				unset sploaded ppath procedurepath reorder procparams
			fi

			# ... and finally, perform schema deployment.
			#
			# shellcheck disable=SC2016,SC2154
			(( silent )) || info "Launching '${SCRIPT}' to perform database migration for database '${db}' ${version_max:+with target version '${version_max}' }..."
			[[ -n "${version_max:-}" ]] && params+=( --target-limit "${version_max}" )
			extraparams=()
			if [[ -n "${actualpath:-}" ]]; then
				extraparams=( --scripts "${actualpath}/"*.sql )
			else
				extraparams=( --scripts "${path}/schema/${db}/"*.sql )
			fi
			#if (( ${#extra[@]} )); then
			if [[ -n "${extra[*]:-}" ]]; then
				extraparams+=( "${extra[@]}" )
			fi
			if (( dryrun )); then
				extraparams+=( '--dry-run' )
			fi

			debug "About to apply schema: '${myway} ${params[*]} ${extraparams[*]}'"
			debug "N.B. All outstanding parameters are passed at this stage..."
			if (( silent )); then
				${myway} "${params[@]}" "${extraparams[@]}" >/dev/null 2>&1
			elif (( quiet )); then
				# Loses return code to grep :(
				#${myway} "${params[@]}" "${extraparams[@]}" 2>&1 >/dev/null | grep -Ev --line-buffered "${silentfilter}"

				# Throw away stdout but redirect stderr to stdout...
				# shellcheck disable=SC2069
				${myway} "${params[@]}" "${extraparams[@]}" 2>&1 >/dev/null
			else
				${myway} "${params[@]}" "${extraparams[@]}"
			fi
			result=${?}
			if (( result )); then
				if (( keepgoing )); then
					warn "Migration of database '${db}' (${myway} ${params[*]} ${extraparams[*]}) failed: ${result}"
					output $'\n\nContinuing to next database, if any ...\n'
					rc=1
				else
					die "Migration of database '${db}' (${myway} ${params[*]} ${extraparams[*]}) failed: ${result}"
				fi
			fi

			debug "Load completed for database '${db}'\n"
			if ! (( silent )); then
				[[ -n "${dblist:-}" && "${dblist}" == "${db}" ]] && [[ -n "${std_LASTOUTPUT:-}" ]] && output
			fi
		fi

		(( founddb && rc )) && exit 4
		(( founddb && !( rc ) )) && exit 3

		# shellcheck disable=SC2015
		(( rc )) && false || true

		# Run in sub-shell so that the following is not necessary...
		unset response allowfail option extraparams params messages details

		) # }}}
		result=${?}

		debug "Sub-shell exit-code was '${result}'"
		case ${result} in
			4)
				# Slightly non-obviously, we've found a
				# database but hit an error...
				founddb=1
				rc=1
				;;
			3)
				# ... or we've found a database without hitting
				# an error...
				founddb=1
				;;
			2)
				# Sub-shell called die
				debug "die() invoked by sub-shell"
				rc=1
				break
				;;
			*)
				rc+=${result}
				;;
		esac
	done # db in ${databases}

	(( std_TRACE )) && set +o xtrace

	debug 'Releasing lock ...'
	[[ -e "${lockfile}" && "$( <"${lockfile}" )" == "${$}" ]] && rm "${lockfile}"

	# ${rc} should have recovered from the sub-shell, above...
	if (( rc )) && (( dryrun )); then
		(( silent )) || warn "Load completed with errors, or database doesn't exist"
	elif (( rc )); then
		(( silent )) || error 'Load completed with errors'
	elif (( !( founddb ) )); then
		(( silent )) || error 'Specified database(s) not present in configuration file'
		rc=1
	else
		(( silent )) || info 'Load completed'
	fi

	return ${rc}
} # main # }}}

export LC_ALL='C'
set -o pipefail

std::requires --no-quiet 'pgrep'
std::requires --no-quiet 'perl'
std::requires --no-quiet "${SCRIPT}"
#std::requires --no-exit --no-quiet "${VALIDATOR}" # Checked above

main "${@:-}"

exit ${?}

# vi: set filetype=sh syntax=sh commentstring=#%s foldmarker=\ {{{,\ }}} foldmethod=marker colorcolumn=80 nowrap:
