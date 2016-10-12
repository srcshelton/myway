#!/bin/bash

# READ THIS!!!!!!
#
# This script wipes all databases that are managed by myway!
# Under no circumstances should this script be deployed to any environment
# other than integration

# stdlib.sh should be in /usr/local/lib/stdlib.sh, which can be found as
# follows by scripts located in /usr/local/{,s}bin/...
declare std_LIB='stdlib.sh'
# shellcheck disable=SC2153
type -pf 'dirname' >/dev/null 2>&1 || function dirname() { : ; }
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
	echo >&2 "FATAL:  Unable to source ${std_LIB} functions: ${?}"
	exit 1
}

std_DEBUG="${DEBUG:-0}"
std_TRACE="${TRACE:-0}"

# We want to be able to debug applyschema.sh without debugging myway.pl...
if [[ -n "${MYDEBUG:-}" ]]; then
	DEBUG="${MYDEBUG:-0}"
else
	unset DEBUG
fi

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
	local filename
	local lockfile="/var/lock/${NAME}.lock"
	[[ -w /var/lock ]] || lockfile="${TMPDIR:-/tmp}/${NAME}.lock"


	local falsy="^(off|n(o)?|false|0)$"

	# Ensure that 'fuser' will work...
	#(( EUID )) && die "This script must be run with super-user privileges"

	local arg db
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
			-h|--help)
				export std_USAGE="[--config <file>]"
				std::usage
				;;
		esac
		shift
	done

	#for filename in "${filename:-}" /etc/iod/schema.conf /etc/schema.conf ~/schema.conf "$( dirname "$( readlink -e "${0}" )" )"/schema.conf; do
	#	[[ -r "${filename:-}" ]] && break
	#done
	filename="$( std::findfile -app dbtools -name schema.conf -dir /etc ${filename:+-default "${filename}"} )"
	if [[ ! -r "${filename}" ]]; then
		die "Cannot read configuration file"
	fi

	local defaults="$( std::getfilesection "${filename}" "DEFAULT" | sed -r 's/#.*$// ; /^[^[:space:]]+\.[^[:space:]]+\s*=/s/\./_/' | grep -Ev '^\s*$' | sed -r 's/\s*=\s*/=/' )"
	local hosts="$( std::getfilesection "${filename}" "CLUSTERHOSTS" | sed -r 's/#.*$// ; /^[^[:space:]]+\.[^[:space:]]+\s*=/s/\./_/' | grep -Ev '^\s*$' | sed -r 's/\s*=\s*/=/' )"
	local databases="$( std::getfilesection "${filename}" "DATABASES" | sed -r 's/#.*$// ; /^[^[:space:]]+\.[^[:space:]]+\s*=/s/\./_/' | grep -Ev '^\s*$' | sed -r 's/\s*=\s*/=/' )"

	[[ -n "${databases:-}" ]] || die "No databases defined in '${filename}'"

	debug "DEFAULTs:\n${defaults}\n"
	debug "CLUSTERHOSTS:\n${hosts}\n"
	debug "DATABASES:\n${databases}\n"

	local -i rc=0

	(( std_TRACE )) && set -o xtrace

	debug "Establishing lock ..."

	[[ -e "${lockfile}" ]] && return 1
	lock "${lockfile}" || return 1
	sleep 0.1
	[[ -e "${lockfile}" && "$( <"${lockfile}" )" == "${$}" ]] || return 1

	# We're going to eval our config file sections - hold onto your hats!
	eval "${defaults}"
	eval "${hosts}"

	for db in ${databases}; do
		# Run the block below in a sub-shell so that we don't have to
		# manually sanitise the environment on each iteration.
		#

		[[ "${databases}" =~ ^${db} ]] || echo

		( # ) # <- Syntax highlight fail

		local details="$( std::getfilesection "${filename}" "${db}" | sed -r 's/#.*$// ; /^[^[:space:]]+\.[^[:space:]]+\s*=/s/\./_/' | grep -Ev '^\s*$' | sed -r 's/\s*=\s*/=/' )"
		[[ -n "${details:-}" ]] || die "Unconfigured database '${db}'"
		debug "${db}:\n${details}\n"

		eval "${details}"

		if grep -Eq "${falsy}" <<<"${managed:-}"; then
			info "Skipping unmanaged database '${db}' ..."
			exit 0 # continue
		else
			info "Processing configuration for database '${db}' ..."
		fi

		local -a messages=()

		[[ -n "${dbadmin:-}" ]] || messages+=( "No database user ('dbadmin') specified for database '${db}'" )
		[[ -n "${passwd:-}" ]] || messages+=( "No database user password ('passwd') specified for database '${db}'" )


		(( ${#messages[@]} )) && die "${messages[@]}"

		if [[ -n "${host:-}" ]]; then
			# ${host} is verified below...
			:
		elif [[ -n "${cluster:-}" ]]; then
			host="$( eval echo "\$${cluster}" )"
		else
			die "Neither 'host' nor 'cluster' membership is defined for database '${db}'"
		fi
		debug "Attempting to resolve host '${host}' ..."
		if (( std_DEBUG )); then
			debug "Not performing host resolution in DEBUG mode - skipping"
		else
			std::ensure "Failed to resolve host '${host}'" getent hosts "${host}"
		fi

		# Let's drop the schema.....
		warn "Dropping database '${db}' ..."
		mysql -u "${dbadmin}" -p"${passwd}" -h "${host}" <<<"DROP SCHEMA \`${db}\`"
		)
	done

	(( std_TRACE )) && set +o xtrace

	debug "Releasing lock ..."
	[[ -e "${lockfile}" && "$( <"${lockfile}" )" == "${$}" ]] && rm "${lockfile}"

	return ${rc}
} # main # }}}

export LC_ALL="C"

main "${@:-}"

exit ${?}

# vi: set syntax=sh colorcolumn=80 foldmethod=marker:
