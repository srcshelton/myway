#!/bin/bash

# stdlib.sh should be in /usr/local/lib/stdlib.sh, which can be found as
# follows by scripts located in /usr/local/{,s}bin/...
std_LIB="stdlib.sh"
for std_LIBPATH in \
	"." \
	"$( dirname "$( type -pf "${std_LIB}" 2>/dev/null )" )" \
	"$( readlink -e "$( dirname -- "${BASH_SOURCE:-${0:-.}}" )/../lib" )" \
	"/usr/local/lib" \
	${FPATH:+${FPATH//:/ }} \
	${PATH:+${PATH//:/ }}
do
	if [[ -r "${std_LIBPATH}/${std_LIB}" ]]; then
		break
	fi
done
[[ -r "${std_LIBPATH}/${std_LIB}" ]] && source "${std_LIBPATH}/${std_LIB}" || {
	echo >&2 "FATAL:  Unable to source ${std_LIB} functions"
	exit 1
}

std_DEBUG="${DEBUG:-0}"
std_TRACE="${TRACE:-0}"

SCRIPT="myway.pl"
COMPATIBLE="1.1.2"

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

function main() {
	local myway="$( source "${std_LIBPATH}/${std_LIB}" 2>/dev/null ; std::requires --path "${SCRIPT}" )"

	local truthy="^(on|y(es)?|true|1)$"
	local falsy="^(off|n(o)?|false|0)$"
	local silentfilter='^((Useless|Use of|Cannot parse|!>) |\s*$)'

	local filename
	local lockfile="/var/lock/${NAME}.lock"

	# Ensure that 'fuser' will work...
	#(( EUID )) && die "This script must be run with super-user privileges"

	${myway} --help >/dev/null 2>&1 || die "${myway} is failing to" \
		"execute - please confirm that all required perl modules are" \
		"available"
	# Alternatively...
	# eval "$( source "${std_LIBPATH}/${std_LIB}" 2>/dev/null ; std::requires --path "perl" ) -c ${myway}" || die "${myway} is failing to compile - please confirm that all required perl modules are available"

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

	local arg schema db dblist
	local -i dryrun=0 quiet=0 silent=0 keepgoing=0
	local -a extra
	while [[ -n "${1:-}" ]]; do
		arg="${1}"
		case "${arg}" in
			--config|-c)
				shift
				if [[ -z "${1:-}" ]]; then
					die "Option ${arg} requires an argument"
				elif [[ ! -r "${1}" ]]; then
					die "Path ${1} cannot be read"
				else
					filename="${1}"
				fi
				;;
			--schema|--schemata|-s)
				shift
				if [[ -z "${1:-}" ]]; then
					die "Option ${arg} requires an argument"
				elif [[ ! -d "${1}" ]]; then
					die "Directory ${1} does not exist"
				else
					path="${1}"
				fi
				;;
			--locate|--whereis|--server|--host|-l)
				shift
				if [[ -z "${1:-}" ]]; then
					die "Option ${arg} requires an argument"
				else
					db="${1}"
				fi
				;;
			--only|-o)
				shift
				if [[ -z "${1:-}" ]]; then
					die "Option ${arg} requires an argument"
				else
					dblist="${1}"
				fi
				;;
			--keep-going|--keepgoing|-k)
				keepgoing=1
				;;
			--dry-run|--verify|-v)
				dryrun=1
				;;
			--quiet|-q)
				quiet=1
				;;
			--silent|-s)
				silent=1
				;;
			--help|-h)
				export std_USAGE="[--config <file>] [--schema <path>] [--only <database>[,...]] [--keep-going] [--dry-run] [--silent] | [--locate <database>]"
				std::usage
				;;
			--)
				shift
				while [[ -n "${1:-}" ]]; do
					extra=( "${1}" )
					shift
				done
				;;
			*)
				die "Unknown argument '${arg}'"
				;;
		esac
		shift
	done

	for filename in "${filename:-}" /etc/iod/schema.conf /etc/schema.conf ~/schema.conf "$( dirname "$( readlink -e "${0}" )" )"/schema.conf; do
		[[ -r "${filename:-}" ]] && break
	done
	if [[ ! -r "${filename}" ]]; then
		die "Cannot read configuration file"
	else
		(( silent )) || info "Using configuration file '${filename}' ..."
	fi

	local defaults="$( source "${std_LIBPATH}/${std_LIB}" 2>/dev/null ; std::getfilesection "${filename}" "DEFAULT" | sed -r 's/#.*$// ; /^[^[:space:]]+\.[^[:space:]]+\s*=/s/\./_/' | grep -Ev '^\s*$' | sed -r 's/\s*=\s*/=/' )"
	local hosts="$( source "${std_LIBPATH}/${std_LIB}" 2>/dev/null ; std::getfilesection "${filename}" "CLUSTERHOSTS" | sed -r 's/#.*$// ; /^[^[:space:]]+\.[^[:space:]]+\s*=/s/\./_/' | grep -Ev '^\s*$' | sed -r 's/\s*=\s*/=/' )"
	local databases="$( source "${std_LIBPATH}/${std_LIB}" 2>/dev/null ; std::getfilesection "${filename}" "DATABASES" | sed -r 's/#.*$// ; /^[^[:space:]]+\.[^[:space:]]+\s*=/s/\./_/' | grep -Ev '^\s*$' | sed -r 's/\s*=\s*/=/' )"

	[[ -n "${databases:-}" ]] || die "No databases defined in '${filename}'"

	debug "DEFAULTs:\n${defaults}\n"
	debug "CLUSTERHOSTS:\n${hosts}\n"
	debug "DATABASES:\n${databases}\n"

	if [[ -n "${db:-}" ]]; then
		local name="$( grep -om 1 "^${db}$" <<<"${databases}" )"
		[[ -n "${name:-}" ]] || die "Database '${db}' not defined in '${filename}'"

		local details="$( source "${std_LIBPATH}/${std_LIB}" 2>/dev/null ; std::getfilesection "${filename}" "${name}" | sed -r 's/#.*$// ; /^[^[:space:]]+\.[^[:space:]]+\s*=/s/\./_/' | grep -Ev '^\s*$' | sed -r 's/\s*=\s*/=/' )"
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

	local -i result rc=0

	debug "Establishing lock ..."

	[[ -e "${lockfile}" ]] && return 1
	lock "${lockfile}" || return 1
	sleep 0.1
	[[ -e "${lockfile}" && "$( <"${lockfile}" )" == "${$}" ]] || return 1

	# We have a lock...

	(( std_TRACE )) && set -o xtrace

	local -l syntax

	# We're going to eval our config file sections - hold onto your hats!
	eval "${defaults}"
	eval "${hosts}"

	local database
	for db in ${databases}; do
		# Run the block below in a sub-shell so that we don't have to
		# manually sanitise the environment on each iteration.
		#
		(

		if [[ -n "${dblist:-}" ]] && ! grep -q ",${db}," <<<",${dblist},"; then
			(( silent )) || info "Skipping deselected database '${db}' ..."
			continue
		fi

		local details="$( source "${std_LIBPATH}/${std_LIB}" 2>/dev/null ; std::getfilesection "${filename}" "${db}" | sed -r 's/#.*$// ; /^[^[:space:]]+\.[^[:space:]]+\s*=/s/\./_/' | grep -Ev '^\s*$' | sed -r 's/\s*=\s*/=/' )"
		[[ -n "${details:-}" ]] || die "Database '${db}' lacks a configuration block in '${filename}'"
		debug "${db}:\n${details}\n"

		eval "${details}"

		if grep -Eq "${falsy}" <<<"${managed:-}"; then
			(( silent )) || info "Skipping unmanaged database '${db}' ..."
			continue
		else
			(( silent )) || info "Processing configuration for database '${db}' ..."
		fi

		local -a messages=()

		[[ -n "${dbadmin:-}" ]] || messages+=( "No database user ('dbadmin') specified for database '${db}'" )
		[[ -n "${passwd:-}" ]] || messages+=( "No database user password ('passwd') specified for database '${db}'" )

		local actualpath="${path:-}"
		path="$( readlink -e "${actualpath:-.}" )" || die "Failed to canonicalise path '${actualpath}': ${?}"
		actualpath=""

		if [[ -z "${path:-}" ]]; then
			messages+=( "Path to schema files and stored procedures not defined for database '${db}'" )
		else
			if [[ ! -d "${path}" ]]; then
				messages+=( "Schema-file directory '${path}' does not exist for database '${db}'" )
			else
				if [[ "$( basename "${path}" )" == "${db}" ]]; then
					if grep -Eq "${truthy}" <<<"${procedures:-}"; then
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
						if grep -Eq "${truthy}" <<<"${procedures:-}"; then
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
						params+=( -h "${passwd}" )
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

		for option in force verbose warn debug quiet silent; do
			eval echo "\${options_${option}:-}" | grep -Eq "${truthy}" && params+=( --${option} )
		done
		if [[ "${syntax:-}" != "vertica" ]]; then
			for option in compat relaxed; do
				eval echo "\${mysql_${option}:-}" | grep -Eq "${truthy}" && params+=( --mysql-${option} )
			done
			if grep -Eq "${falsy}" <<<"${backups:-}"; then
				params+=( --no-backup )
			else
				[[ -n "${backups_compress:-}" ]] && params+=( --compress "${backups_compress}" )
				grep -Eq "${truthy}" <<<"${backups_transactional:-}" && params+=( --transactional )
				grep -Eq "${truthy}" <<<"${backups_lock:-}" && params+=( --lock )
				grep -Eq "${truthy}" <<<"${backups_keeplock:-}" && params+=( --keep-lock )
				grep -Eq "${truthy}" <<<"${backups_separate:-}" && params+=( --separate-files )
				grep -Eq "${truthy}" <<<"${backups_skipmeta:-}" && params+=( --skip-metadata )
				grep -Eq "${truthy}" <<<"${backups_extended:-}" && params+=( --extended-insert )

				grep -Eq "${truthy}" <<<"${backups_keep:-}" && params+=( --keep-backup )
			fi
		fi
		grep -Eq "${truthy}" <<<"${parser_trustname:-}" && params+=( --trust-filename )
		grep -Eq "${truthy}" <<<"${parser_allowdrop:-}" && params+=( --allow-unsafe )
		[[ -n "${version_max:-}" ]] && params+=( --target-limit "${version_max}" )

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
			${myway} "${params[@]}" "${extraparams[@]}" --init 2>&1 >/dev/null
		else
			${myway} "${params[@]}" "${extraparams[@]}" --init
		fi
		result=${?}
		if (( ${result} )); then
			if (( allowfail )); then
				info "Initialisation of database '${db}' (${myway} ${params[*]} ${extraparams[*]} --init) expected failure: ${result}"
			else
				if (( keepgoing )); then
					warn "Initialisation of database '${db}' (${myway} ${params[*]} ${extraparams[*]} --init) failed: ${result}"
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
		if grep -Eq "${truthy}" <<<"${procedures:-}"; then
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

			find "${procedurepath}" -mindepth 1 -maxdepth 1 -type d -name "${db}" 2>/dev/null | "${reorder[@]}" | while read -r path; do
				extraparams=()
				extraparams+=( --scripts "${path}" )
				#if (( ${#extra[@]} )); then
				if [[ -n "${extra[*]:-}" ]]; then
					extraparams+=( "${extra[@]}" )
				fi
				if (( dryrun )); then
					extraparams+=( "--dry-run" )
				fi

				debug "About to apply Stored Procedures: ${myway} ${params[*]} ${procparams[*]} ${extraparams[*]} ${extra[*]:-}"
				if (( silent )); then
					${myway} "${params[@]}" "${procparams[@]}" "${extraparams[@]}" >/dev/null 2>&1
				elif (( quiet )); then
					# Loses return code to grep :(
					#${myway} "${params[@]}" "${procparams[@]}" "${extraparams[@]}" 2>&1 >/dev/null | grep -Ev --line-buffered "${silentfilter}"
					${myway} "${params[@]}" "${procparams[@]}" "${extraparams[@]}" 2>&1 >/dev/null
				else
					${myway} "${params[@]}" "${procparams[@]}" "${extraparams[@]}"
				fi
				result=${?}
				if (( ${result} )); then
					if (( keepgoing )); then
						warn "Loading of stored procedures to database '${db}' (${myway} ${params[*]} ${procparams[*]} ${extraparams[*]}${extra[*]:+ ${extra[*]}}) failed: ${result}"
						rc=1
					else
						die "Loading of stored procedures to database '${db}' (${myway} ${params[*]} ${extraparams[*]}${extra[*]:+ ${extra[*]}}) failed: ${result}"
					fi
				fi
			done
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
			${myway} "${params[@]}" "${extraparams[@]}" 2>&1 >/dev/null
		else
			${myway} "${params[@]}" "${extraparams[@]}"
		fi
		result=${?}
		if (( ${result} )); then
			if (( keepgoing )); then
				warn "Migration of database '${db}' (${myway} ${params[*]} ${extraparams[*]}) failed: ${result}"
				rc=1
			else
				die "Migration of database '${db}' (${myway} ${params[*]} ${extraparams[*]}) failed: ${result}"
			fi
		fi

		debug "Load completed for database '${db}'\n"

		(( rc )) && false || true

		) # <- Syntax highlight fail

		result=${?}
		if (( 2 == result )); then
			# Sub-shell called die
			rc=1
			break
		else
			rc+=${result}
		fi
	done

	(( std_TRACE )) && set +o xtrace


	debug "Releasing lock ..."
	[[ -e "${lockfile}" && "$( <"${lockfile}" )" == "${$}" ]] && rm "${lockfile}"

	(( silent )) || {
		# ${rc} should have recovered from the sub-shell, above...
		if (( rc )); then
			warn "Load completed with errors"
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
