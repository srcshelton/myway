#! /bin/bash

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

function getmysqlport() {
	local block="${@:-}"

	local mysqlconfroot="${SYSCONFDIR}"/mysql
	local mysqlconffile='my.cnf'

	[[ -r "${mysqlconfroot}"/"${mysqlconffile}" ]] || die "Cannot read MySQL configuration file '${mysqlconfroot}/${mysqlconffile}'"

	local mysqldsection="$( std::getfilesection "${mysqlconfroot}"/"${mysqlconffile}" 'mysqld' )"
	local -i mysqlport="$( grep -Ei -- '^\s*port\s*=\s*[0-9]{1,5}\s*$' <<<"${mysqldsection}" | cut -d'=' -f 2- )"
	debug "Found MySQL port ${mysqlport:-<unknown>} from '${mysqlconfroot}/${mysqlconffile}'"

	local -i mysqlpid="$( fuser ${mysqlport}/tcp 2>&1 | cut -d':' -f 2 )"
	debug "Found MySQL PID ${mysqlpid:-<unknown>} using fuser"

	[[ -d /proc/${mysqlpid} ]] || die "MySQL process '${mysqlpid:-<not set>}' cannot be located"

	if [[ -n "${block:-}" ]]; then
		eval ${block} || exit 1
	fi

	debug "MySQL process '${mysqlpid:-<not found>}' listening on port '${mysqlport:-<unknown>}'"

	respond ${mysqlport:-0}

	return 0
} # getmysqlport

function processarg() {
	local opt="${1:-}"; shift
	local arg="${1:-}"; shift

	[[ -n "${opt:-}" ]] || return 0

	if [[ -n "${arg:-}" ]]; then
		respond "${arg}"
		return 1
	else
		return 0
	fi
} # processarg

function main() {
	local args user pass host db location
	local -i rc

	# Ensure that 'fuser' will work...
	(( EUID )) && die "This script must be run with super-user privileges"

	std::define std_USAGE <<EOF
	 --username <name>
			 --password <password>
			[--hostname <host>]
			[--database <database>]
			 --location <directory>
EOF

	std::configure \
		-prefix		/		\
		-eprefix	/usr		\
		-datarootdir	/usr/share	\
		-includedir	/usr/include	\
		-sharedstatedir	/var/lib	\
		-localstatedir	/etc 		\
	|| die "Cannot locate standard system directories"

	local mysql="$( std::requires --path mysql )"
	[[ -x "${mysql}" ]] || die "Cannot execute required binary '${mysql}'"

	args="$( getopt -o 'u:p:d:l:h' --longoptions 'user:,username:,pass:,password:,database:,location:,help' -n "${NAME}" -- "${@:-}" )"
	if (( ${?} )); then
		die "Cannot parse command-line options '${@:-}'"
	fi
	eval set -- "${args:-}"
	while true; do
		case "${1:-}" in
			-h|--help)
				std::usage
				exit 0
				;;
			-u|--user|--username)
				user="$( processarg "${1}" "${2:-}" )" ; rc=${?}
				(( rc )) || die "Parameter '${1}' requires an argument"
				shift ${rc}
				;;
			-p|--pass|--password)
				pass="$( processarg "${1}" "${2:-}" )" ; rc=${?}
				(( rc )) || die "Parameter '${1}' requires an argument"
				shift ${rc}
				;;
			-d|--database)
				db="$( processarg "${1}" "${2:-}" )" ; rc=${?}
				(( rc )) || die "Parameter '${1}' requires an argument"
				shift ${rc}
				;;
			-l|--location)
				location="$( processarg "${1}" "${2:-}" )" ; rc=${?}
				(( rc )) || die "Parameter '${1}' requires an argument"
				shift ${rc}
				;;
			--)
				shift
				break
				;;
			*)
				die "Unknown option or parameter '${1:-}' while parsing command-line arguments: '${@:-}' remaining"
				;;
		esac
		shift
	done
	if ! [[ -n "${user:-}" && -n "${pass:-}" && -n "${location:-}" ]]; then
		std::usage
		exit 1
	fi
	if [[ -e "${location}" && ! -d "${location}" ]]; then
		die "File system $( stat -c '%F' "${location}" ) '${location}' is not a directory"
	fi
	(( std_DEBUG )) || { mkdir -p "${location}" || die "Cannot create directory '${location}': ${?}" ; }

	local mysqlslaveidcodeblock
	std::define mysqlslaveidcodeblock <<-'EOF'
		local -i mysqlid="$( grep -Ei -- '^\s*server-id\s*=\s*[0-9]{1,5}\s*$' <<<"${mysqldsection}" | cut -d'=' -f 2- )" ;
		debug "Found MySQL server ID ${mysqlid:-<unknown>} from '${mysqlconfroot}/${mysqlconffile}'" ;

		(( mysqlid )) || { die "MySQL server ID is not set or cannot be determined from '${mysqlconfroot}/${mysqlconffile}' - is instance a replication slave?" ; exit 1 ; } ;
	EOF
	local -i port="$( getmysqlport "${mysqlslaveidcodeblock}" )"

	args="-u ${user} -p${pass} -h localhost"
	[[ -n "${db:-}" ]] && args="${args} ${db}"
	$mysql ${args} <<<'SHOW STATUS'
} # main

main "${@:-}"

exit 0

# vi: set filetype=sh syntax=sh commentstring=#%s foldmarker=\ {{{,\ }}} foldmethod=marker colorcolumn=80:
