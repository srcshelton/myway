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
	local args user pass host="localhost" db="" location
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

	args="$( getopt -o 'u:p:o:d:l:h' --longoptions 'user:,username:,pass:,password:,database:,location:,help' -n "${NAME}" -- "${@:-}" )"
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
			-o|--host|--hostname)
				host="$( processarg "${1}" "${2:-}" )" ; rc=${?}
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

	args="-u ${user} -p${pass} -h ${host:-localhost}"
	[[ -n "${db:-}" ]] && args="${args} ${db}"

	local slavestatus
	std::emktemp slavestatus "${$}" || die "std::emktemp failed in ${FUNCNAME}: ${?}"
	[[ -n "${slavestatus:-}" ]] || die "std::emktemp failed to create a file"
	[[ -r "${slavestatus}" ]] || die "std::emktemp failed to create file '${slavestatus}'"

	$mysql ${args} <<<'SHOW SLAVE STATUS \G' > "${slavestatus}"
	grep -q 'Slave_IO_Running: Yes$' "${slavestatus}" || warn "MySQL/MariaDB Slave I/O thread not running - updates are not being received"
	grep -q 'Slave_SQL_Running: Yes$' "${slavestatus}" || warn "MySQL/MariaDB Slave SQL thread not running - updates are not being applied"
	grep -q 'Slave_IO_State: Waiting for master to send event$' "${slavestatus}" || die "Slave is not in a quiescent state - aborting"
	local errtype
	for errtype in "" _IO _SQL; do
		if ! grep -q "Last${errtype:-}_Errno: 0$" "${slavestatus}"; then
			local -i lasterrno
			local lasterrmsg
			lasterrno=$(( $( grep "Last${errtype:-}_Errno: " "${slavestatus}" | cut -d':' -f 2- ) ))
			if (( lasterrno != 0 )); then
				lasterrmsg="$( grep "Last${errtype:-}_Error: " "${slavestatus}" | cut -d':' -f 2- | cut -d' ' -f 2- )"
				die "Slave has ${errtype:+${errtype#_} }error ${lasterrno}${lasterrmsg:+: ${lasterrmsg}} - aborting"
			fi
		fi
	done
	grep -q 'Seconds_Behind_Master: 0$' "${slavestatus}" || die "MySQL/MariaDB Slave is running behind master - aborting"

	# Slave is good to go!
	$mysql ${args} <<<'STOP SLAVE SQL_THREAD'
	$mysql ${args} <<<'SHOW SLAVE STATUS \G' > "${slavestatus}"
	grep -q 'Slave_SQL_Running: No$' "${slavestatus}" || die "Failed to stop Slave SQL thread"

	info "Executing 'myway.pl -u ${user} -p ${pass} -h ${host:-localhost} --backup ${location}/$( date +%Y%m%d ).${host:-localhost}.backup.sql --compress xz --lock'"
	time myway.pl -u "${user}" -p "${pass}" -h "${host:-localhost}" --backup "${location}"/"$( date +%Y%m%d )"."${host:-localhost}".backup.sql --compress xz --lock && {
		info "Backup '${location}/$( date +%Y%m%d ).${host:-localhost}.backup.sql' successfully created"
	} || {
		error "Backup to '${location}/$( date +%Y%m%d ).${host:-localhost}.backup.sql' failed: ${?}"
	}

	$mysql ${args} <<<'START SLAVE SQL_THREAD'
	$mysql ${args} <<<'SHOW SLAVE STATUS \G' > "${slavestatus}"
	grep -q 'Slave_SQL_Running: Yes$' "${slavestatus}" || die "Failed to start Slave SQL thread"

	return 0
} # main

std::requires myway.pl
myway.pl --help >/dev/null 2>/dev/null || die "Cannot execute dbtools/myway.pl - are appropriate Perl modules installed?"

main "${@:-}"

exit 0

# vi: set filetype=sh syntax=sh commentstring=#%s foldmarker=\ {{{,\ }}} foldmethod=marker colorcolumn=80:
