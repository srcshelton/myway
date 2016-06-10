#!/bin/bash

# stdlib.sh should be in /usr/local/lib/stdlib.sh, which can be found as
# follows by scripts located in /usr/local/{,s}bin/...
declare std_LIB="stdlib.sh"
for std_LIBPATH in							\
	"$( dirname -- "${BASH_SOURCE:-${0:-.}}" )"			\
	"."								\
	"$( dirname -- "$( type -pf "${std_LIB}" 2>/dev/null )" )"	\
	"$( dirname -- "${BASH_SOURCE:-${0:-.}}" )/../lib"		\
	"/usr/local/lib"						\
	 ${FPATH:+${FPATH//:/ }}					\
	 ${PATH:+${PATH//:/ }}
do
	if [[ -r "${std_LIBPATH}/${std_LIB}" ]]; then
		break
	fi
done

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
# shellcheck disable=SC2015
# shellcheck source=/usr/local/lib/stdlib.sh
[[ -r "${std_LIBPATH}/${std_LIB}" ]] && source "${std_LIBPATH}/${std_LIB}" || {
	echo >&2 "FATAL:  Unable to source ${std_LIB} functions"
	exit 1
}

std_DEBUG="${DEBUG:-0}"
std_TRACE="${TRACE:-0}"

SCRIPT="myway.pl"
COMPATIBLE="1.2.0"

# We want to be able to debug applyschema.sh without debugging myway.pl...
if [[ -n "${MYDEBUG:-}" ]]; then
	DEBUG="${MYDEBUG:-0}"
else
	unset DEBUG
fi

# Override `die` to return '2' on fatal error...
function die() {
	[[ -n "${*:-}" ]] && std_DEBUG=1 std::log >&2 "FATAL: " "${*}"
	std::cleanup 2

	# Unreachable
	return 1
}

function lock() {
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
} # lock

# shellcheck disable=SC2155
function main() {
	local myway="$( std::requires --path "${SCRIPT}" )"

	local truthy="^(on|y(es)?|true|1)$"
	local falsy="^(off|n(o)?|false|0)$"
	#local silentfilter='^((Useless|Use of|Cannot parse|!>) |\s*$)'

	local actualpath filename
	local lockfile="/var/lock/${NAME}.lock"

	# Ensure that 'fuser' will work...
	#(( EUID )) && die "This script must be run with super-user privileges"

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
	if ! sort -V <<<"" >/dev/null 2>&1; then
		warn "Version sort unavailable - Stored Procedure load-order" \
		     "cannot be guaranteed."
		warn "Press ctrl+c now to abort ..."
		sleep 5
		warn "Proceeding ..."
		novsort=1
	fi

	local arg schema db dblist clist
	local -i dryrun=0 quiet=0 silent=0 keepgoing=0
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
					dblist="${1}"
				fi
				;;
			-h|--help)
				export std_USAGE="[--config <file>] [--schema <path>] [ [--databases <database>[,...]] | [--clusters <cluster>[,...]] ] [--keep-going] [--dry-run] [--silent] | [--locate <database>]"
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
			-q|--quiet)
				quiet=1
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
					clist="${1}"
				fi
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
		die "Options --databases and --clusters are mutually exclusive"
	fi

	#for filename in "${filename:-}" /etc/iod/schema.conf /etc/schema.conf ~/schema.conf "$( dirname "$( readlink -e "${0}" )" )"/schema.conf; do
	#	[[ -r "${filename:-}" ]] && break
	#done
	filename="$( std::findfile -app dbtools -name schema.conf -dir /etc ${filename:+-default "${filename}"} )"
	if [[ ! -r "${filename}" ]]; then
		die "Cannot read configuration file"
	else
		(( silent )) || info "Using configuration file '${filename}' ..."
	fi

	local defaults="$( std::getfilesection "${filename}" "DEFAULT" | sed -r 's/#.*$// ; /^[^[:space:]]+\.[^[:space:]]+\s*=/s/\./_/' | grep -Ev '^\s*$' | sed -r 's/\s*=\s*/=/' )"
	local hosts="$( std::getfilesection "${filename}" "CLUSTERHOSTS" | sed -r 's/#.*$// ; /^[^[:space:]]+\.[^[:space:]]+\s*=/s/\./_/' | grep -Ev '^\s*$' | sed -r 's/\s*=\s*/=/' )"
	local databases="$( std::getfilesection "${filename}" "DATABASES" | sed -r 's/#.*$// ; /^[^[:space:]]+\.[^[:space:]]+\s*=/s/\./_/' | grep -Ev '^\s*$' | sed -r 's/\s*=\s*/=/' )"

	[[ -n "${databases:-}" ]] || die "No databases defined in '${filename}'"

	debug "DEFAULTs:\n${defaults}\n"
	debug "CLUSTERHOSTS:\n${hosts}\n"
	debug "DATABASES:\n${databases}\n"

	if [[ -n "${db:-}" ]]; then
		local name="$( grep -om 1 "^${db}$" <<<"${databases}" )"
		[[ -n "${name:-}" ]] || die "Database '${db}' not defined in '${filename}'"

		local details="$( std::getfilesection "${filename}" "${name}" | sed -r 's/#.*$// ; /^[^[:space:]]+\.[^[:space:]]+\s*=/s/\./_/' | grep -Ev '^\s*$' | sed -r 's/\s*=\s*/=/' )"
		[[ -n "${details:-}" ]] || die "Database '${db}' lacks a configuration block in '${filename}'"
		debug "${db}:\n${details}\n"

		local host="$( grep -m 1 "host=" <<<"${details}" | cut -d'=' -f 2 )"
		if [[ -n "${host:-}" ]]; then
			output "Database '${db}' has write master '${host}'"
		else
			local cluster="$( grep -m 1 "cluster=" <<<"${details}" | cut -d'=' -f 2 )"
			[[ -n "${cluster:-}" ]] || die "Database '${db}' has no defined cluster membership in '${filename}'"

			local master="$( grep -m 1 "^${cluster}=" <<<"${hosts}" | cut -d'=' -f 2 )"
			[[ -n "${master:-}" ]] || die "Cluster '${cluster}' (of which '${db}' is a stated member) is not defined in '${filename}'"

			output "Database '${db}' is a member of cluster '${cluster}' with write master '${master}'"
		fi

		exit 0
	fi

	local -i result rc=0 founddb=0

	debug "Establishing lock ..."

	[[ -e "${lockfile}" ]] && die "Lock '${lockfile}' exists - aborting"
	lock "${lockfile}" || die "Creating lock '${lockfile}' failed - aborting"
	sleep 0.1
	[[ -e "${lockfile}" && "$( <"${lockfile}" )" == "${$}" ]] || die "Lock '${lockfile}' is for process $( <"${lockfile}" ), not our PID ${$} - aborting"

	# We have a lock...

	(( std_TRACE )) && set -o xtrace

	local -l syntax

	# We're going to eval our config file sections - hold onto your hats!
	eval "${defaults}"
	eval "${hosts}"

	for db in ${databases}; do
		# Run the block below in a sub-shell so that we don't have to
		# manually sanitise the environment on each iteration.
		#
		(

		if [[ -n "${dblist:-}" ]] && ! grep -q ",${db}," <<<",${dblist},"; then
			(( silent )) || info "Skipping deselected database '${db}' ..."
			exit 0 # continue
		fi

		local details="$( std::getfilesection "${filename}" "${db}" | sed -r 's/#.*$// ; /^[^[:space:]]+\.[^[:space:]]+\s*=/s/\./_/' | grep -Ev '^\s*$' | sed -r 's/\s*=\s*/=/' )"
		[[ -n "${details:-}" ]] || die "Database '${db}' lacks a configuration block in '${filename}'"
		debug "${db}:\n${details}\n"

		eval "${details}"

		if [[ -n "${clist:-}" ]] && ! grep -q ",${cluster:-.*}," <<<",${clist},"; then
			(( silent )) || info "Skipping database '${db}' from deselected cluster '${cluster:-all}' ..."
			exit 0 # continue
		fi

		if grep -Eiq "${falsy}" <<<"${managed:-}"; then
			(( silent )) || info "Skipping unmanaged database '${db}' ..."
			founddb=1
			exit 3 # See below...
		else
			(( silent )) || info "Processing configuration for database '${db}' ..."
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
			host="$( eval echo "\$${cluster}" )"
		else
			die "Neither 'host' nor 'cluster' membership is defined for database '${db}' in '${filename}'"
		fi
		if [[ -n "${syntax:-}" && "${syntax}" == "vertica" && -z "${dsn:-}" ]]; then
			die "'dsn' is a mandatory parameter when 'syntax' is set to '${syntax}'"
		fi

		debug "Attempting to resolve host '${host}' ..."
		if (( std_DEBUG )); then
			debug "Not performing host resolution in DEBUG mode - skipping"
		else
			std::ensure "Failed to resolve host '${host}'" getent hosts "${host}"
		fi

		local -a params=( -u "${dbadmin}" -p "${passwd}" -h "${host}" -d "${db}" )
		local -a extraparams
		local option

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
						params+=( -d "${db}" )
					fi
					if [[ -n "${schema:-}" ]]; then
						params+=( --vertica-schema "${schema}" )
					else
						warn "No 'schema' value specified for Vertica database - unless 'SEARCH_PATH' is set appropraitely, statements may fail"
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

		for option in force verbose warn debug quiet silent; do
			eval echo "\${options_${option}:-}" | grep -Eiq "${truthy}" && params+=( --${option} )
		done
		if [[ "${syntax:-}" != "vertica" ]]; then
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
		grep -Eiq "${truthy}" <<<"${parser_trustname:-}" && params+=( --trust-filename )
		grep -Eiq "${truthy}" <<<"${parser_allowdrop:-}" && params+=( --allow-unsafe )
		[[ -n "${version_max:-}" ]] && params+=( --target-limit "${version_max}" )

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
			extraparams+=( "--dry-run" )
		fi

		debug "About to prepare schema: '${myway} ${params[*]} ${extraparams[*]}'"
		local -i allowfail=0
		if (( dryrun )); then
			allowfail=1
			keepgoing=1
		else
			if mysql -u "${dbadmin}" -p"${passwd}" -h "${host}" "${db}" <<<'QUIT' >/dev/null 2>&1; then
				# We may still have an empty database with no
				# metadata tracking tables...
				allowfail=1
			fi
		fi
		if (( silent )); then
			${myway} "${params[@]}" "${extraparams[@]}" --init >/dev/null 2>&1
		elif (( quiet )); then
			# Loses return code to grep :(
			#${myway} "${params[@]}" "${extraparams[@]}" --init 2>&1 >/dev/null | grep -Ev --line-buffered "${silentfilter}"

			# Throw away stdout but redirect stderr to stdout...
			# shellcheck disable=SC2069
			${myway} "${params[@]}" "${extraparams[@]}" --init 2>&1 >/dev/null
		else
			${myway} "${params[@]}" "${extraparams[@]}" --init
		fi
		result=${?}
		if (( result )); then
			if (( allowfail )); then
				info "Initialisation of database '${db}' (${myway} ${params[*]} ${extraparams[*]} --init) expected failure: ${result}"
			else
				if (( keepgoing )); then
					warn "Initialisation of database '${db}' (${myway} ${params[*]} ${extraparams[*]} --init) failed: ${result}"
					output "\n\nContinuing to next database, if any ...\n"
					rc=1
				else
					die "Initialisation of database '${db}' (${myway} ${params[*]} ${extraparams[*]} --init) failed: ${result}"
				fi
			fi
		fi

		# Load stored-procedures next, as references to tables aren't
		# checked until the SP is actually executed, but SPs may be
		# invoked as part of schema deployment.
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
			find "${procedurepath}"/ -mindepth 1 -maxdepth 1 -type d -name "${db}" 2>/dev/null | "${reorder[@]}" | while read -r ppath; do
				extraparams=( --scripts "${ppath}" )
				#if (( ${#extra[@]} )); then
				if [[ -n "${extra[*]:-}" ]]; then
					extraparams+=( "${extra[@]}" )
				fi
				if (( dryrun )); then
					extraparams+=( "--dry-run" )
				fi

				(( silent )) || info "Launching '${SCRIPT}' with path '${ppath}' to update Stored Procedures for database '${db}' ..."

				debug "About to apply Stored Procedures: ${myway} ${params[*]} ${procparams[*]} ${extraparams[*]} ${extra[*]:-}"
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
						warn "Loading of stored procedures to database '${db}' (${myway} ${params[*]} ${procparams[*]} ${extraparams[*]}${extra[*]:+ ${extra[*]}}) failed: ${result}"
						output "\n\nContinuing to next database, if any ...\n"
						rc=1
					else
						die "Loading of stored procedures to database '${db}' (${myway} ${params[*]} ${extraparams[*]}${extra[*]:+ ${extra[*]}}) failed: ${result}"
					fi
				fi
			done

			debug "Stored Procedures loaded for database '${db}'\n"
		fi

		# ... and finally, perform schema deployment.
		#
		(( silent )) || info "Launching '${SCRIPT}' to perform database migration for database '${db}' ..."
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
			extraparams+=( "--dry-run" )
		fi

		debug "About to apply schema: '${myway} ${params[*]} ${extraparams[*]}'"
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
				output "\n\nContinuing to next database, if any ...\n"
				rc=1
			else
				die "Migration of database '${db}' (${myway} ${params[*]} ${extraparams[*]}) failed: ${result}"
			fi
		fi

		debug "Load completed for database '${db}'\n"

		(( founddb && rc )) && exit 4
		(( founddb && !( rc ) )) && exit 3

		# shellcheck disable=SC2015
		(( rc )) && false || true

		)

		result=${?}
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
				rc=1
				break
				;;
			*)
				rc+=${result}
				;;
		esac
	done

	(( std_TRACE )) && set +o xtrace


	debug "Releasing lock ..."
	[[ -e "${lockfile}" && "$( <"${lockfile}" )" == "${$}" ]] && rm "${lockfile}"

	(( silent )) || {
		# ${rc} should have recovered from the sub-shell, above...
		if (( rc )) && (( dryrun )); then
			warn "Load completed with errors, or database doesn't exist"
		elif (( rc )); then
			warn "Load completed with errors"
		elif (( !( founddb ) )); then
			error "Specified database(s) not present in configuration file"
			rc=1
		else
			info "Load completed"
		fi
	}

	return ${rc}
} # main

export LC_ALL="C"
set -o pipefail

std::requires perl "${SCRIPT}"

main "${@:-}"

exit ${?}

# vi: set syntax=sh colorcolumn=80 foldmethod=marker:
