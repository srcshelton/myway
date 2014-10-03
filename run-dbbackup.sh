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

(( std_TRACE )) && set -o xtrace

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
	local dirname="${1:-/backup}"
	local db="${2:-}"

	local lockfile="/var/lock/${NAME}.lock"

	local user=root
	local pass=SleepingCats
	local host=localhost

	local -i rc

	[[ -d "${dirname}" ]] || die "Backup directory '${dirname}' does not exist"

	[[ -e "${lockfile}" ]] && return 1
	lock "${lockfile}" || return 1
	sleep 0.1
	[[ -e "${lockfile}" && "$( <"${lockfile}" )" == "${$}" ]] || return 1

	# We have a lock!
	/usr/local/sbin/dbbackup.sh -u "${user}" -p "${pass}" -o "${host}" -l "${dirname}" ${db:+-d "${db}"}
	rc=${?}

	[[ -e "${lockfile}" && "$( <"${lockfile}" )" == "${$}" ]] && rm "${lockfile}"

	return ${rc}
} # main

main "${@:-}"

exit ${?}

# vi: set syntax=sh colorcolumn=80 foldmethod=marker:
