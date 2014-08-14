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

function setconfigurevars() {
	local prefix eprefix bindir sbindir libexecdir sysconfdir
	local sharedstatedir localstatedir libdir includedir oldincludedir
	local datarootdir datadir infodir localedir mandir docdir htmldir
	eval $( std::parseargs "${@:-}" ) || {
		set -- $( std::parseargs --strip -- "${@:-}" )
		prefix="${1:-}"
		eprefix="${2:-}"
		bindir="${3:-}"
		sbindir="${4:-}"
		libexecdir="${5:-}"
		sysconfdir="${6:-}"
		sharedstatedir="${7:-}"
		localstatedir="${8:-}"
		libdir="${9:-}"
		includedir="${10:-}"
		oldincludedir="${11:-}"
		datarootdir="${12:-}"
		datadir="${13:-}"
		infodir="${14:-}"
		localedir="${15:-}"
		mandir="${16:-}"
		docdir="${17:-}"
		htmldir="${18:-}"
	}

	if [[ -n "${prefix:-}" ]]; then
		export PREFIX="${prefix%/}"
	else
		export PREFIX="/usr/local"
	fi
	if [[ -n "${eprefix:-}" ]]; then
		export EPREFIX="${eprefix%/}"
	else
		export EPREFIX="${PREFIX}"
	fi

	export BINDIR="${bindir:-${EPREFIX}/bin}"
	export SBINDIR="${sbindir:-${EPREFIX}/sbin}"
	export LIBEXECDIR="${libexecdir:-${EPREFIX}/libexec}"
	export SYSCONFDIR="${sysconfdir:-${PREFIX}/etc}"
	export SHAREDSTATEDIR="${sharedstatedir:-${PREFIX}/com}"
	export LOCALSTATEDIR="${localstatedir:-${PREFIX}/var}"
	export LIBDIR="${libdir:-${EPREFIX}/lib}"
	export INCLUDEDIR="${includedir:-${PREFIX}/include}"
	export OLDINCLUDEDIR="${oldincludedir:-/usr/include}"
	export DATAROOTDIR="${datarootdir:-${PREFIX}/share}"
	export DATADIR="${datadir:-${DATAROOTDIR}}"
	export INFODIR="${infodir:-${DATAROOTDIR}/info}"
	export LOCALEDIR="${localedir:-${DATAROOTDIR}/locale}"
	export MANDIR="${mandir:-${DATAROOTDIR}/man}"
	export DOCDIR="${docdir:-${DATAROOTDIR}/doc}"
	export HTMLDIR="${htmldir:-${DOCDIR}}"

	if [[ -d "${PREFIX:-}/" && -d "${EPREFIX:-}/" ]]; then
		return 0
	fi

	return 1
} # setconfigurevars

function getinifilesection() {
	local file="${1:-}" ; shift
	local section="${1:-}" ; shift

	[[ -n "${file:-}" && -s "${file}" ]] || return 1
	[[ -n "${section:-}" ]] || return 1

	local script
	# By printing output before setting output to 1, we prevent the section
	# header itself from being returned.
	std::define script <<-EOF
		BEGIN				{ output = 0 }
		/^\s*\[.*\]\s*$/		{ output = 0 }
		( 1 == output )			{ print \$0 }
		/^\s*\[${section}\]\s*$/	{ output = 1 }
	EOF

	respond "$( awk -- "${script:-}" "${file}" )"

	return ${?}
} # getinifilesection

function getmysqlport() {
	local block="${@:-}"

	local mysqlconfroot="${SYSCONFDIR}"/mysql
	local mysqlconffile='my.cnf'

	[[ -r "${mysqlconfroot}"/"${mysqlconffile}" ]] || die "Cannot read MySQL configuration file '${mysqlconfroot}/${mysqlconffile}'"

	local mysqldsection="$( getinifilesection "${mysqlconfroot}"/"${mysqlconffile}" 'mysqld' )"
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

	setconfigurevars \
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
	[[ -n "${db}" ]] && args="${args} ${db}"
	$mysql ${args} <<<'SHOW STATUS'
} # main

main "${@:-}"

exit 0

# vi: set filetype=sh syntax=sh commentstring=#%s foldmarker=\ {{{,\ }}} foldmethod=marker colorcolumn=80:
