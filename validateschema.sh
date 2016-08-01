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

export STDLIB_WANT_API=2

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
	echo >&2 "FATAL:  Unable to source ${std_LIB} functions: ${?}${std_ERRNO:+ (ERRNO ${std_ERRNO})}"
	exit 1
}

std_DEBUG="${DEBUG:-0}"
std_TRACE="${TRACE:-0}"

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

function validate() {
	local type max name="" file=""
	local version="" migrationversion=""
	local newversion="" newmigrationversion="" metadescription=""
	local description="" environment="" filetype="" fullname="" engine=""
	local schema="" environmentdirective=""
	local -l desc=""
	local -i std_PARSEARGS_parsed=0 rc=0 warnings=0 notices=0 styles=0
	local -a files=()
	local -A seen=()
	
	eval std::inherit -- versions descriptions metadescriptions foundinit

	eval "$( std::parseargs --var files -- "${@:-}" )"
	(( std_PARSEARGS_parsed )) || {
		eval "set -- '$( std::parseargs --strip -- "${@:-}" )'"
		type="${1:-}" ; shift
		max="${1:-}" ; shift
		files=( "${@:-}" )
	}

	(( ${#files[@]} )) || return 1

	case "${type:-}" in
		metadata|procedure|schema|vertica-schema)
			:
			;;
		*)
			warn "Unknown type '${type:-}'"
			return 1
			;;
	esac

	debug "validate() called on ${type:-unknown} file(s) '${files[*]}'"

	local script=""
	std::define script <<-EOF
		BEGIN		{ output = 0 }
		/\/\*/		{ output = 1 }
		( 1 == output )	{ print \$0 }
		/\*\//		{ exit }
	EOF

	# Basic tests of metadata headers -
	#  Description:
	#  Engine:
	#  Database:
	#  Schema:
	#  Previous version:
	#  Target version:
	#  Environment:
	#  Restore:

	if [[ 'procedure' == "${type}" ]]; then
		for file in "${files[@]:-}"; do
			name="$( basename "${file}" )"
			if ! [[ "${name}" =~ \.sql$ ]]; then
				error "File '${name}' MUST end with '.sql'"
				(( warnings++ ))
			fi
		done

		if (( warnings )); then
			warn "Completed checking file '${name}': ${warnings} Fatal Errors or Warnings, ${notices} Notices, ${styles} Comments"
			rc=1
		elif (( notices )); then
			note "Completed checking file '${name}': ${warnings} Fatal Errors or Warnings, ${notices} Notices, ${styles} Comments"
		else
			debug "Completed checking file '${name}': ${warnings} Fatal Errors or Warnings, ${notices} Notices, ${styles} Comments"
		fi

		warnings=0 notices=0 styles=0

	else
		for file in "${files[@]:-}"; do
			name="$( basename "${file}" )"
			if [[ "${type}" == "metadata" ]]; then
				if ! [[ "${file}" =~ /${name%.metadata}/ ]]; then
					warn "Metadata file '${name}' SHOULD reside in a directory named '${name%.metadata}'"
					(( warnings++ ))
				fi
			else
				if ! [[ "${name}" =~ ^V ]]; then
					error "File '${name}' MUST start with a version-string with first character 'V'"
					(( warnings++ ))
				fi
				if ! [[ "${name}" =~ __ ]]; then
					error "File '${name}' MUST contain a '__' character sequence to separate version and description components"
					(( warnings++ ))
				fi
				if ! [[ "${name}" =~ \.sql$ ]]; then
					error "File '${name}' MUST end with '.sql'"
					(( warnings++ ))
				else
					if [[ ! "${name}" =~ ^V.*__V.*__ ]] && [[ ! "${name}" =~ \.d[dmc]l\.sql$ ]]; then
						note "File '${name}' SHOULD contain 'ddl', 'dml', or 'dcl' to indicate its change-type"
						(( notices++ ))
					fi
				fi
				if grep -Eq 'V.*[A-Z]' <<<"${name}"; then
					note "File '${name}' SHOULD be entirely lower-case after the initial capital 'V'"
					(( notices++ ))
				fi
				version="$( grep -Po '^V.*?__' <<<"${name}" | sed 's/^V// ; s/__$//' )"
				migrationversion="$( grep -Po '__V.*?__' <<<"${name}" | sed 's/^__V// ; s/__$//' )"
				description="$( grep -Po '__.*?\.' <<<"${name}" | sed 's/^__// ; s/\.$//' )"
				desc="$( sed 's/[^A-Za-z]/_/g' <<<"${description:-}" )"
				environment="$( grep -Po "\\Q${description:-}\\E\.(not-)?.*?\." <<<"${name}" | cut -d'.' -f 2 )"
				filetype="$( grep -o '\.d[dmc]l\.sql$' <<<"${name}" | cut -d'.' -f 2 )"
				[[ "${environment}" == "${filetype}" ]] && unset environment
				fullname="V${version:-<version>}${migrationversion:+__V${migrationversion}}__${description:-<description>}${environment:+.${environment}}.${filetype:-dml}.sql"
				debug "version is '${version:-}'"
				debug "migrationversion is '${migrationversion:-}'"
				debug "description is '${description:-}'"
				debug "environment is '${environment:-}'"
				debug "filetype is '${filetype:-}'"
				debug "fullname is '${fullname:-}'"
				if ! [[ "${fullname}" == "${name}" ]]; then
					warn "Filename '${name}' appears to be non-standard: expected '${fullname}'"
					(( warnings++ ))
				else
					if ! [[ "${desc}" == "${description}" ]]; then
						note "Description of file '${name}' (\"${description}\") would be better expressed as \"${desc}\""
						(( styles++ ))
					fi
				fi
				if grep -Pq '^0*\d+\.0*\d+(\.0*\d+)(\.0*\d+)$' <<<"${version}"; then
					if grep -Eq '^0|\.0' <<<"${version}"; then
						newversion="$( sed -r 's/^0+// ; s/^\.// ; s/^0+// ; s/\.0+$// ; s/\.0+$//' <<<"${version}" )"
						local -i digit=0
						if grep -Pq '^\d+\.\d+\.\d+$' <<<"${newversion}"; then
							digit=$( cut -d'.' -f 3 <<<"${newversion}" )
							(( digit++ ))
							newversion="$( cut -d'.' -f 1,2 <<<"${newversion}" ).${digit}"
						fi
						if grep -Pq '^\d+\.\d+$' <<<"${newversion}"; then
							digit=$( cut -d'.' -f 2 <<<"${newversion}" )
							(( digit++ ))
							newversion="$( cut -d'.' -f 1 <<<"${newversion}" ).${digit}"
						fi
						if grep -Pq '^\d+$' <<<"${newversion}"; then
							newversion+=".1"
						fi
						if [[ -n "${migrationversion:-}" ]] && grep -Pq '^0*\d+\.0*\d+(\.0*\d+)(\.0*\d+)$' <<<"${migrationversion}"; then
							newmigrationversion="$( sed -r 's/^0+// ; s/^\.// ; s/^0+// ; s/\.0+$// ; s/\.0+$//' <<<"${migrationversion}" )"
							if grep -Pq '^\d+\.\d+\.\d+$' <<<"${newmigrationversion}"; then
								digit=$( cut -d'.' -f 3 <<<"${newmigrationversion}" )
								(( digit++ ))
								newmigrationversion="$( cut -d'.' -f 1,2 <<<"${newmigrationversion}" ).${digit}"
							fi
							if grep -Pq '^\d+\.\d+$' <<<"${newmigrationversion}"; then
								digit=$( cut -d'.' -f 2 <<<"${newmigrationversion}" )
								(( digit++ ))
								newmigrationversion="$( cut -d'.' -f 1 <<<"${newmigrationversion}" ).${digit}"
							fi
							if grep -Pq '^\d+$' <<<"${newmigrationversion}"; then
								newmigrationversion+=".1"
							fi
						fi
						unset digit
					fi
				fi
			fi

			debug "Examining '${file}' for metadata ..."
			local line="" fragment="" directive="" value=""
			local -l frag=""

#echo >&2 "file is '${file}'"
#echo >&2 "foundinit is '${foundinit}'"
#echo >&2 "versions contains '${!versions[@]}'"
#echo >&2 "descriptions contains '${!descriptions[@]}'"
#echo >&2 "metadescriptions contains '${!metadescriptions[@]}'"

			while read -r line; do
				if ! grep -q '[a-zA-Z]' <<<"${line}"; then
					debug "Ignoring line '${line}'"
				else
					debug "Read line '${line}'"
					fragment="$( sed 's/^[^a-zA-Z!]*//' <<<"${line}" )"
					debug "Fragment is '${fragment}'"

					# FIXME: Forces capitalisation of 'Version'...
					directive="$( sed -r 's/\s+Version/_Version/i' <<<"${fragment}" | sed -r 's/\s+/ /g' | cut -d' ' -f 1 )"

					frag="${directive}"
					directive="$( sed 's/_/ /' <<<"${directive}" )"
					debug "Directive is '${directive}'"
					value="$( cut -d':' -f 2- <<<"${fragment}" | sed -r 's/^\s+//' )"
					debug "Value is '${value}'"
					debug "Read metadata directive '${directive} ${value}'"

					case "${frag}" in
						"description:")
							if (( "${seen[${frag%:}]:-}" )); then
								error "Metadata from file '${name}' specifies '${frag%:}' directive multiple times"
								(( warnings++ ))
							else
								seen[${frag%:}]=1
							fi

							local -l metadesc="$( sed 's/[^A-Za-z]/_/g ; s/_\+/_/g' <<<"${value:-}" )"
							# ${desc} may be shorter or longer, dependent on wrapping...
							if ! [[ "${type}" == 'metadata' ]]; then
								if ! [[ "${desc}" =~ ${metadesc%_} ]]; then
									info "Description embedded in filename '${name}' differs from metadata description, rendered as '${metadesc%_}'"
									(( styles++ ))
									metadescription="${metadesc%_}"
								fi

								if (( ${descriptions[${desc}]:-0} )); then
									note "Filename '${name}' contains duplicate description '${desc}'"
									(( notices++ ))
								else
									descriptions[${desc}]=1
								fi

								if (( ${metadescriptions[${metadesc%_}]:-0} )); then
									note "Metadata from file '${name}' contains duplicate description '${metadesc%_}'"
									(( notices++ ))
								else
									metadescriptions[${metadesc%_}]=1
								fi
							fi

							unset metadesc
							;;
						"engine:")
							if (( "${seen[${frag%:}]:-}" )); then
								error "Metadata from file '${name}' specifies '${frag%:}' directive multiple times"
								(( warnings++ ))
							else
								seen[${frag%:}]=1
							fi

							local -l metavalue="${value:-}"
							case "${metavalue:-}" in
								"mysql")
									info "Engine specified in file '${name}' does not have to be specified if it is MySQL, which is default"
									(( styles++ ))
									;;
								"vertica")
									engine="vertica"
									if (( $( sed 's/"[^"]*"//' "${filename}" | sed "s/'[^']*'//" | grep -o '`' | wc -l ) )); then
										warn "Vertica schema '${name}' appears to contain MySQL-style quoting"
										(( warnings++ ))
									fi
									;;
								*)
									error "Engine '${value:-}' specified in file '${name}' is not recognised"
									(( warnings++ ))
									;;
							esac
							unset metavalue
							;;
						"database:")
							if (( "${seen[${frag%:}]:-}" )); then
								error "Metadata from file '${name}' specifies '${frag%:}' directive multiple times"
								(( warnings++ ))
							else
								seen[${frag%:}]=1
							fi

							if ! [[ "${file}" =~ /${value}/ ]]; then
								note "Path for file '${name}' does include database '${value}' specified in metadata"
								(( notices++ ))
							fi
							;;
						"schema:")
							if (( "${seen[${frag%:}]:-}" )); then
								error "Metadata from file '${name}' specifies '${frag%:}' directive multiple times"
								(( warnings++ ))
							else
								seen[${frag%:}]=1
							fi

							schema="${value:-}"
							;;
						"previous_version:")
							if (( "${seen[${frag%:}]:-}" )); then
								error "Metadata from file '${name}' specifies '${frag%:}' directive multiple times"
								(( warnings++ ))
							else
								seen[${frag%:}]=1
							fi

							if ! grep -Eiq '^n\/?a' <<<"${value:-}" && ! [[ "${value:-}" =~ ^[0-9.]+$ ]]; then
								error "Metadata from file '${name}' specifies invalid 'Previous Version: ${value:-}'"
								(( warnings++ ))
							elif ! grep -Eiq '^n\/?a' <<<"${value:-}" && [[ "${value:-}" =~ ^[0-9.]+$ ]]; then
								local pver="${value}"
								local -i found=0
								if (( ${versions[${value}]:-0} )); then
									found=1
								fi
								if ! (( found )); then
									if [[ "${value}" =~ \.0+$ ]]; then
										pver="${value%%0}"
										pver="${pver%.}"
									else # ! [[ "${value}" =~ \.0+$ ]]; then
										pver="${value}.0"
									fi
									if (( ${versions[${pver}]:-0} )); then
										found=1
									fi
								fi
								if (( found )); then
									debug "Metadata from file '${name}' specifies valid 'Previous Version: ${pver}'"
								else
									error "Metadata from file '${name}' specifies 'Previous Version: ${value}' for which no schema-file is present"
									debug "Recorded versions are: '${!versions[@]}'"
									(( warnings++ ))
								fi
								unset found pver
							elif ! (( foundinit )); then # grep -Eiq '^n\/?a' <<<"${value:-}"
								debug "Metadata from file '${name}' specifies no 'Previous Version' - must be initialiser"
								foundinit=1
							else # (( foundinit ))
								error "Multiple schema files found with no 'Previous Version', which is only valid for initialisers (of which there may be only one)"
								(( warnings++ ))
							fi

							if [[ -n "${value:-}" && -n "${migrationversion:-}" ]]; then
								if [[ "${value}" == "${version:-}" ]]; then
									error "Migration schema '${name}' MUST NOT have an initial version ('${version}') matching its own metadata 'Previous Version(: ${value})'"
									info "The initial version of a migration schema file should be the first version which it causes to be skips - so to proceed from versions 5, 6, and 7 to version 10 the migration schema file should be named 'V8__V10__migration_schema.sql'"
									(( warnings++ ))
								fi
							fi
							;;
						"target_version:")
							if (( "${seen[${frag%:}]:-}" )); then
								error "Metadata from file '${name}' specifies '${frag%:}' directive multiple times"
								(( warnings++ ))
							else
								seen[${frag%:}]=1
							fi

							if [[ -n "${value:-}" ]]; then
								debug "Recording 'Target Version: ${value}'"
								versions[${value}]=1
								debug "Recorded versions are now: '${!versions[@]}'"
							else
								error "Metadata from file '${name}' has empty 'Target Version' directive"
								(( warnings++ ))
							fi

							if ! [[ "${type}" == "metadata" || "${name}" =~ ^V${value}__ ]]; then
								warn "Metadata from file '${name}' specifies 'Target Version: ${value}', which does not match filename"
								(( warnings++ ))
							fi
							;;
						"environment:")
							if (( "${seen[${frag%:}]:-}" )); then
								error "Metadata from file '${name}' specifies '${frag%:}' directive multiple times"
								(( warnings++ ))
							else
								seen[${frag%:}]=1
							fi

							environmentdirective="${value}"
							;;
						"restore:")
							if (( "${seen[${frag%:}]:-}" )); then
								error "Metadata from file '${name}' specifies '${frag%:}' directive multiple times"
								(( warnings++ ))
							else
								seen[${frag%:}]=1
							fi

							local ppath
							if [[ "${value}" =~ ^/ ]]; then
								ppath="${value}"
							else # if [[ "${value}" =~ ^.{0,1}?/ ]]; then
								ppath="$( dirname "${file}" )/${value}"
							fi
							if ! [[ -r "${ppath}" ]]; then
								error "Metadata from file '${name}' requires restore from missing file '${ppath}'"
								(( warnings++ ))
							fi
							unset ppath
							;;
						*:)
							if (( "${seen[${frag%:}]:-}" )); then
								error "Metadata from file '${name}' specifies '${frag%:}' directive multiple times"
								(( warnings++ ))
							else
								seen[${frag%:}]=1
							fi

							error "Metadata from file '${name}' contains unknown directive '${directive}'"
							(( warnings++ ))
							;;
						*)
							note "Metadata from file '${name}' appears to contain multi-line entries - please reduce these to a single line, adding further detail in a separate comment if necessary"
							(( notices++ ))
							;;
					esac
				fi
				debug "Finished checking line"
			done < <( awk -- "${script:-}" "${file}" ) # while read -r line

			if [[ -n "${newversion:-}" ]]; then
				note "File '${name}' contains legacy version string '${version}' - suggest migration to new naming scheme 'V${newversion}${newmigrationversion:+__V${newmigrationversion}}__${metadescription:-${description:-<description>}}${environment:+.${environment}}.${filetype:-dml}.sql'"
				(( notices++ ))
			fi

			if [[ -n "${environmentdirective:-}" ]]; then
				if [[ "${environmentdirective}" =~ ^! ]]; then
					info "File '${name}' is not valid in environment '${environmentdirective%!}'"
					if ! [[ "${name}" =~ \.not-${environmentdirective%!}\. ]]; then
						warn "Filename '${name}' does not include environment 'not-${environmentdirective%!}'"
						(( warnings++ ))
					fi
				else
					info "File '${name}' is only valid in environment '${environmentdirective}'"
					warn "There should be corresponding environment-locked schema files present in order to provide a comprehensive collection whereby every possible environment has a schema file present which applies to it"
					local suggestion=""
					if [[ -n "${newversion:-}" ]]; then

						suggestion="V${newversion}__V${newversion}__${metadescription:-${description:-<description>}}.not-${environmentdirective}.sql"
					else
						#suggestion="$( sed -r "s/^(V.*__)(.*)$/\\1\\1\\2/ ; s/\.d[mdc]l\./.not-${environmentdirective}./" <<<"${name}" )"
						suggestion="V${version}__V${version}__${metadescription:-${description:-<description>}}.not-${environmentdirective}.sql"
					fi
					info "If nothing needs to be done for other environments, please provide a migration schema to skip this step (possibly '${suggestion}' if only this version is to be stepped-over)"
					unset suggestion
					if ! [[ "${name}" =~ \.${environmentdirective%!}\. ]]; then
						warn "Filename '${name}' does not include environment '${environmentdirective%!}'"
						(( warnings++ ))
					fi
				fi
			fi
			environmentdirective=""
			metadescription=""

			line="" fragment="" directive="" value="" frag=""

			if [[ -n "${schema:-}" ]]; then
				if ! [[ "${engine:-}" == "vertica" ]]; then
					error "File '${name}' includes a Vertica schema directive ('${schema}') without specifying 'Engine: Vertica'"
					(( warnings++ ))
				fi
			fi
			if [[ "${type}" == "vertica-schema" && ! "${engine:-}" == "vertica" ]]; then
				error "File '${name}' is defiend to be a Vertica schema-file but does not specify 'Engine: Vertica'"
				(( warnings++ ))
			fi

			if ! (( ${seen["description"]:-} )); then
				note "Metadata from file '${name}' lacks a 'Description' directive"
				(( notices++ ))
			fi
			if ! [[ "${type}" == "metadata" ]]; then
				if ! (( ${seen["previous_version"]:-} )); then
					error "Metadata from file '${name}' lacks a 'Previous Version' directive"
					(( warnings++ ))
				fi
			fi
			if ! (( ${seen["target_version"]:-} )); then
				error "Metadata from file '${name}' lacks a 'Target Version' directive"
				(( warnings++ ))
			fi

			debug "Finished checking metadata"

			case "${filetype:-dml}" in
				dml)
					if sed -r 's|/\*.*\*/|| ; s/(CREATE|DROP)\s+TEMPORARY\s+TABLE//' "${file}" | grep -Eiq '\s+(CREATE|ALTER|DROP)\s+'; then # DDL
						warn "Detected DDL in DML-only file '${name}':"
						warn "$( grep -Ei '\s+(CREATE|ALTER|DROP)\s+' "${file}" | grep -Ev "(CREATE|DROP)\s+TEMPORARY\s+TABLE)" )"
						(( warnings++ ))
					fi
					if sed 's|/\*.*\*/||' "${file}" | grep -Eiq '\s+(GRANT|REVOKE)\s+'; then # DCL
						warn "Detected DCL in DML-only file '${name}':"
						warn "$( grep -Ei '\s+(GRANT|REVOKE)\s+' )"
						(( warnings++ ))
					fi
					;;
				ddl)
					if sed 's|/\*.*\*/||' "${file}" | grep -Eiq '\s+(UPDATE|INSERT|DELETE\s+FROM)\s+'; then # DML
						warn "Detected DML in DDL-only file '${name}':"
						warn "$( grep -Ei '\s+(UPDATE|INSERT|DELETE\s+FROM)\s+' )"
						(( warnings++ ))
					fi
					if sed 's|/\*.*\*/||' "${file}" | grep -Eiq '\s+(GRANT|REVOKE)\s+'; then # DCL
						warn "Detected DCL in DDL-only file '${name}':"
						warn "$( grep -Ei '\s+(GRANT|REVOKE)\s+' )"
						(( warnings++ ))
					fi
					;;
				dcl)
					if sed 's|/\*.*\*/||' "${file}" | grep -Eiq '\s+(UPDATE|INSERT|DELETE\s+FROM)\s+'; then # DML
						warn "Detected DML in DCL-only file '${name}':"
						warn "$( grep -Ei '\s+(UPDATE|INSERT|DELETE\s+FROM)\s+' )"
						(( warnings++ ))
					fi
					if sed -r 's|/\*.*\*/|| ; s/(CREATE|DROP)\s+TEMPORARY\s+TABLE//' "${file}" | grep -Eiq '\s+(CREATE|ALTER|DROP)\s+'; then # DDL
						warn "Detected DDL in DCL-only file '${name}':"
						warn "$( grep -Ei '\s+(CREATE|ALTER|DROP)\s+' "${file}" | grep -Ev "(CREATE|DROP)\s+TEMPORARY\s+TABLE)" )"
						(( warnings++ ))
					fi
					;;
				*)
					warn "Unknown SQL type '${filetype:-}'"
					;;
			esac

			if (( warnings )); then
				warn "Completed checking schema file '${name}': ${warnings} Fatal Errors or Warnings, ${notices} Notices, ${styles} Comments"
				rc=1
			elif (( notices )); then
				note "Completed checking schema file '${name}': ${warnings} Fatal Errors or Warnings, ${notices} Notices, ${styles} Comments"
			else
				info "Completed checking schema file '${name}': ${warnings} Fatal Errors or Warnings, ${notices} Notices, ${styles} Comments"
			fi

			warnings=0 notices=0 styles=0

		done # for file in "${files[@]:-}"
	fi

	(( rc )) && error "Warnings or Fatal Errors found - Validation failed"

	return ${rc:-1}
} # validate

# shellcheck disable=SC2155
function main() {
	local truthy="^(on|y(es)?|true|1)$"
	local falsy="^(off|n(o)?|false|0)$"

	local actualpath filename
	local lockfile="/var/lock/${NAME}.lock"

	local -i child=0
	local -i warnings=0 notices=0 styles=0

	local -i novsort=0
	if ! sort -V <<<"" >/dev/null 2>&1; then
		warn "Version sort unavailable - Stored Procedure load-order" \
		     "cannot be guaranteed."
		warn "Press ctrl+c now to abort ..."
		sleep 5
		warn "Proceeding ..."
		novsort=1
		(( warnings++ ))
	fi

	local arg schema db dblist clist
	local -i dryrun=0 quiet=0 silent=0 keepgoing=0

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
			   --from-applyschema)
			   	child=1
				;;
			   --)
				shift
				while [[ -n "${1:-}" ]]; do
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

	filename="$( std::findfile -app dbtools -name schema.conf -dir /etc ${filename:+-default "${filename}"} )"
	if [[ ! -r "${filename}" ]]; then
		die "Cannot read configuration file: please use --config to specify location"
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

	[[ -e "${lockfile}" ]] && die "Lock file '${lockfile}' (belonging to PID '$( <"${lockfile}" 2>/dev/null )') exists - aborting"
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

	local -l syntax

	# We're going to eval our config file sections - hold onto your hats!
	eval "${defaults}"
	eval "${hosts}"

	for db in ${databases}; do
		# Run the block below in a sub-shell so that we don't have to
		# manually sanitise the environment on each iteration.
		#
		(
		declare -A versions=()
		declare -A descriptions=()
		declare -A metadescriptions=()
		declare -i foundinit=0

		if [[ -n "${dblist:-}" ]] && ! grep -q ",${db}," <<<",${dblist},"; then
			if ! (( child )); then
				(( silent )) || echo
				(( silent )) || info "Skipping deselected database '${db}' ..."
			fi
			exit 0 # continue
		fi

		local details="$( std::getfilesection "${filename}" "${db}" | sed -r 's/#.*$// ; /^[^[:space:]]+\.[^[:space:]]+\s*=/s/\./_/' | grep -Ev '^\s*$' | sed -r 's/\s*=\s*/=/' )"
		[[ -n "${details:-}" ]] || die "Database '${db}' lacks a configuration block in '${filename}'"
		debug "${db}:\n${details}\n"

		eval "${details}"

		if [[ -n "${clist:-}" ]] && ! grep -q ",${cluster:-.*}," <<<",${clist},"; then
			if ! (( child )); then
				(( silent )) || echo
				(( silent )) || info "Skipping database '${db}' from deselected cluster '${cluster:-all}' ..."
			fi
			exit 0 # continue
		fi

		if grep -Eiq "${falsy}" <<<"${managed:-}"; then
			if ! (( child )); then
				(( silent )) || echo
				(( silent )) || info "Skipping unmanaged database '${db}' ..."
			fi
			founddb=1
			exit 3 # See below...
		else
			(( silent )) || { echo ; info "Processing configuration for database '${db}' ..." ; }
		fi

		local -a messages=()

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
			:
		elif [[ -n "${cluster:-}" ]]; then
			host="$( eval echo "\$${cluster}" )"
		else
			die "Neither 'host' nor 'cluster' membership is defined for database '${db}' in '${filename}'"
		fi

		if grep -Eiq "${truthy}" <<<"${procedures:-}"; then
			local -a reorder=( sort -V )
			if (( novsort )); then
				reorder=( tac )
			fi

			local procedurepath="${path}/procedures"
			if [[ -d "${path}"/schema/"${db}"/procedures ]]; then
				procedurepath="${path}"/schema/"${db}"/procedures
			fi

			local ppath
			while read -r ppath; do
				for filename in "${ppath}"/*; do
					if [[ -f "${filename}" && "${filename}" =~ \.sql$ ]]; then
						validate -type 'procedure' -files "${filename}"
						(( rc += ${?} ))
						founddb=1
					elif [[ -d "${filename}" ]]; then
						note "Directory '${filename}' should not be present in directory '${ppath}' and will be ignored"
						(( notices++ ))
					elif [[ -f "${filename}" ]]; then
						if [[ "${filename}" =~ \.metadata$ ]]; then
							debug "File '${filename}' is Stored Procedure metadata"
							validate -type 'metadata' -files "${filename}"
						else
							warn "File '${filename}' does not end in '.sql' and so should not be present in directory '${ppath}'"
							(( warnings++ ))
						fi
					else
						warn "Object '${filename}' should not be present in directory '${ppath}' and will be ignored"
						(( warnings++ ))
					fi
				done
			done < <( find "${procedurepath}"/ -mindepth 1 -maxdepth 1 -type d -name "${db}" 2>/dev/null | "${reorder[@]}" )

			debug "Stored Procedures processed for database '${db}'\n"

			# Clear any versions assocaited with Stored Procedures,
			# as these are independent of schema-file versions...
			versions=()
		fi


		local ppath
		if [[ -n "${actualpath:-}" ]]; then
			ppath="${actualpath}"
		else
			ppath="${path}/schema/${db}"
		fi
		for filename in $( find "${ppath}" -mindepth 1 -maxdepth 1 -print0 | sort -Vz | xargs -r0 echo ); do
			if [[ -f "${filename}" && "${filename}" =~ \.sql$ ]]; then
				if [[ -n "${syntax:-}" && "${syntax}" == "vertica" ]]; then
					validate -type 'vertica-schema' ${version_max:+-max "${version_max}" }-files "${filename}"
				else
					validate -type 'schema' ${version_max:+-max "${version_max}" }-files "${filename}"
				fi
				(( rc += ${?} ))
				founddb=1
			elif [[ -d "${filename}" ]]; then
				note "Directory '${filename}' should not be present in directory '${ppath}' and will be ignored"
				(( notices++ ))
			elif [[ -f "${filename}" ]]; then

				# TODO: Alternatively, any referenced files
				#       could be returned by validate()...

				if grep -Eiq "${truthy}" <<<"${procedures:-}" && [[ "${filename}" =~ \.metadata$ ]]; then
					debug "File '${filename}' is Stored Procedure metadata"
				else
					local script restorefiles files
					std::define script <<-EOF
						BEGIN		{ output = 0 }
						/\/\*/		{ output = 1 }
						( 1 == output )	{ print \$0 }
						/\*\//		{ exit }
					EOF
					if (( std_DEBUG )); then
						for files in "${ppath}"/*; do
							debug "Checking file '${files}' for 'Restore:' directive ..."
							awk -- "${script:-}" "${files}"
							awk -- "${script:-}" "${files}" | grep --colour -o 'Restore:\s*[^[:space:]]\+\s*'
						done
					fi
					restorefiles="$( for files in "${ppath}"/*; do
						awk -- "${script:-}" "${files}" | grep -o 'Restore:\s*[^[:space:]]\+\s*'
					done | cut -d':' -f 2- | xargs echo )"
					if ! grep -qw "$( basename "${filename}" )" <<<"${restorefiles}"; then
						warn "File '${filename}' does not end in '.sql' and so should not be present in directory '${ppath}'"
						(( warnings++ ))
					else
						debug "File '${filename}' is referenced in a metadata 'Restore:' directive"
					fi
				fi
			else
				warn "Object '${filename}' should not be present in directory '${ppath}' and will be ignored"
				(( warnings++ ))
			fi
		done
		debug "Schema processed for database '${db}'\n"

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
			warn "Validation completed with errors, or database doesn't exist"
		elif (( rc )); then
			warn "Validation completed with errors"
		elif (( !( founddb ) )); then
			error "Specified database(s) not present in configuration file"
			rc=1
		else
			info "Validation completed"
		fi
	}

	return ${rc}
} # main

export LC_ALL="C"
set -o pipefail

main "${@:-}"

exit ${?}

# vi: set syntax=sh colorcolumn=80 foldmethod=marker:
