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

# For std::inherit...
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
# shellcheck disable=SC1091,SC2015
# shellcheck source=/usr/local/lib/stdlib.sh
[[ -r "${std_LIBPATH}/${std_LIB}" ]] && source "${std_LIBPATH}/${std_LIB}" || {
	# shellcheck disable=SC2154
	echo >&2 "FATAL:  Unable to source ${std_LIB} functions: ${?}${std_ERRNO:+ (ERRNO ${std_ERRNO})}"
	exit 1
}

std_DEBUG="${DEBUG:-0}"
std_TRACE="${TRACE:-0}"

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

function validate() { # {{{
	local type max name='' file=''
	local version='' migrationversion=''
	local newversion='' newmigrationversion='' metadescription=''
	local description='' environment='' filetype='' fullname='' engine=''
	local schema='' environmentdirective='' defaulttype='ddl'
	local -l desc=''
	local -i std_PARSEARGS_parsed=0 rc=0 warnings=0 notices=0 styles=0
	local -a files=() versionsegment=()
	local -A seen=()

	eval std::inherit -- versions descriptions metadescriptions foundinit

	eval "$( std::parseargs --var files -- "${@:-}" )"
	(( std_PARSEARGS_parsed )) || {
		eval "set -- '$( std::parseargs --strip -- "${@:-}" )'"
		type="${1:-}" ; shift
		max="${1:-}" ; shift
		files=( "${@:-}" )
	}

	if ! (( ${#files[@]} )); then
		error "Function validate() requires at least one file to operate upon"
		return 1
	fi

	grep -P 'x' <<<'x' >/dev/null 2>&1 || {
		error "Your 'grep' binary ($( type -pf grep 2>/dev/null )) does not support '-P'erl-mode - aborting"
		return 1
	}

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

	local script=''
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
			(( silent )) || warn "Completed checking file '${name}': ${warnings} Fatal Errors or Warnings, ${notices} Notices, ${styles} Comments"
			rc=1
		elif (( notices )); then
			(( silent )) || note "Completed checking file '${name}': ${warnings} Fatal Errors or Warnings, ${notices} Notices, ${styles} Comments"
		else
			(( silent )) || debug "Completed checking file '${name}': ${warnings} Fatal Errors or Warnings, ${notices} Notices, ${styles} Comments"
		fi

		warnings=0 notices=0 styles=0

	else
		for file in "${files[@]:-}"; do
			if ! [[ -r "${file}" ]]; then
				error "Cannot read file '${file}' - skipping verification"
				(( warnings++ ))
				continue
			fi

			if ! (( $( tail -n 1 "${file}" | wc -l ) )); then
				error "File '${file}' is lacking a trailing newline at the end of the file - the last statement may be lost"
				(( warnings++ ))
			fi

			name="$( basename "${file}" )"
			if [[ "${type}" == 'metadata' ]]; then
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
				else
					# shellcheck disable=SC2126
					case $(( $( grep -o '__' <<<"${name}" | wc -l ) )) in
						0)
							# Dealt with above...
							:
							;;
						1)
							# Normal schema file...
							:
							;;
						2)
							# Migration schema?
							if ! [[ "${name}" =~ ^V[0-9.]+__V[0-9.]+__ ]]; then
								error "File '${name}' is not a migration schema, and contains too many '__' sequences"
								(( warnings++ ))
							fi
							;;
						*)
							error "File '${name}' contains too many '__' sequences"
							(( warnings++ ))
							;;
					esac
				fi
				if ! [[ "${name}" =~ \.sql$ ]]; then
					error "File '${name}' MUST end with '.sql'"
					(( warnings++ ))
				else
					if [[ "${name}" =~ ^V[0-9.]+__V[0-9.]+__ ]]; then
						unset defaulttype
					elif [[ ! "${name}" =~ \.d[dmc]l\.sql$ ]]; then
						note "File '${name}' SHOULD contain 'ddl', 'dml', or 'dcl' to indicate its change-type"
						(( notices++ ))
						warn "Assuming '${name}' is of type '${defaulttype}'"
					fi
				fi
				if [[ "${name}" =~ ^V[0-9.]+__V[0-9.]+__ ]]; then
					if [[ "${name}" =~ ^V[0-9.]+__V[0-9.]+__.*[A-Z] ]]; then
						note "File '${name}' SHOULD be entirely lower-case after the initial capital 'V'"
						(( notices++ ))
					fi
				elif [[ "${name}" =~ ^V[0-9.]+__.*[A-Z] ]]; then
					note "File '${name}' SHOULD be entirely lower-case after the initial capital 'V'"
					(( notices++ ))
				fi
				version="$( grep -oP '^V.*?__' <<<"${name}" | sed 's/^V// ; s/__$//' )"
				migrationversion="$( grep -oP '__V.*?__' <<<"${name}" | sed 's/^__V// ; s/__$//' )"
				description="$( grep -oP '__.*?\.' <<<"${name}" | tail -n 1 | sed 's/^.*__// ; s/\.$//' )"
				desc="$( sed 's/[^A-Za-z]/_/g' <<<"${description:-}" )"
				environment="$( grep -oP "\\Q${description:-}\\E\.(not-)?.*?\." <<<"${name}" | cut -d'.' -f 2 )"
				filetype="$( grep -o '\.d[dmc]l\.sql$' <<<"${name}" | cut -d'.' -f 2 )"
				[[ "${environment}" == "${filetype}" ]] && unset environment
				fullname="V${version:-<version>}${migrationversion:+__V${migrationversion}}__${description:-<description>}${environment:+.${environment}}.${filetype:-${defaulttype:-}}.sql"
				[[ -z "${defaulttype:-}" ]] && fullname="${fullname/../.}" # Fix migration schema <sigh>
				debug "version is '${version:-}'"
				debug "migrationversion is '${migrationversion:-}'"
				debug "description is '${description:-}'"
				debug "environment is '${environment:-}'"
				debug "filetype is '${filetype:-}'"
				debug "fullname is '${fullname:-}'"

				if ! [[ -n "${version:-}" && -n "${desc:-}" ]]; then
					error "File name '${name}' cannot be parsed into valid version components - skipping further validation of this file"
					(( warnings++ ))
					continue
				fi

				if ! [[ "${fullname}" == "${name}" ]]; then
					warn "Filename '${name}' appears to be non-standard: expected '${fullname}'"
					(( warnings++ ))
				else
					if ! [[ "${desc}" == "${description}" ]]; then
						note "Description of file '${name}' (\"${description}\") would be better expressed as \"${desc}\""
						(( styles++ ))
					fi
				fi
				if ! grep -Pq '^0*\d+(\.0*\d+)?(\.0*\d+)?(\.0*\d+)?$' <<<"${version:-}"; then
					if grep -Pq '^0*\d+(\.0*\d+)?(\.0*\d+)?(\.0*\d+)?' <<<"${version:-}"; then
						error "Filename '${name}' does not include a recognised version number - too many sets of digits?"
						version="$( grep -oP '^0*\d+(\.0*\d+)?(\.0*\d+)?(\.0*\d+)?' <<<"${version:-}" )"
					else
						error "Filename '${name}' does not include a recognised version number"
					fi
					(( warnings++ ))
				else # if grep -Pq '^0*\d+(\.0*\d+)?(\.0*\d+)?(\.0*\d+)?$' <<<"${version:-}"; then

					if grep -Pq '^\d+\.\d+\.\d+\.\d+$' <<<"${version}"; then
						versionsegment[ 3 ]="$( cut -d'.' -f 4 <<<"${version}" | sed 's/^0\+//' )"
					fi
					if grep -Pq '^\d+\.\d+\.\d+' <<<"${version}"; then
						versionsegment[ 2 ]="$( cut -d'.' -f 3 <<<"${version}" | sed 's/^0\+//' )"
					fi
					if grep -Pq '^\d+\.\d+' <<<"${version}"; then
						versionsegment[ 1 ]="$( cut -d'.' -f 2 <<<"${version}" | sed 's/^0\+//' )"
					fi
					if grep -Pq '^\d+' <<<"${version}"; then
						versionsegment[ 0 ]="$( cut -d'.' -f 1 <<<"${version}" | sed 's/^0\+//' )"
					fi
					if ! (( ${versionsegment[ 0 ]:-0} || ${versionsegment[ 1 ]:-0} || ${versionsegment[ 2 ]:-0} || ${versionsegment[ 3 ]:-0} )); then
						if grep -q '[^0.]' <<<"${version}"; then
							warn "Cannot determine valid version from file '${name}' version '${version}'"
							(( warnings++ ))
						else
							note "File '${name}' has version '${version}' which is only valid for a baseline/initialiser schema"
						fi
					else
						# shellcheck disable=SC2016
						(( quiet )) || info "$( sed 's/, $//' <<<"File '${name}' version '${version}' indicates ${versionsegment[ 0 ]:+Release '${versionsegment[ 0 ]}', }${versionsegment[ 1 ]:+Change '${versionsegment[ 1 ]}', }${versionsegment[ 2 ]:+Step '${versionsegment[ 2 ]}', }${versionsegment[ 3 ]:+Hotfix '${versionsegment[ 3 ]}', }" )"
						if (( ${versionsegment[ 3 ]:-0} )); then
							warn "This schema file is hot-fix '${versionsegment[ 3 ]}' to version '${versionsegment[ 0 ]:-0}.${versionsegment[ 1 ]:-0}.${versionsegment[ 2 ]:-0}', and will be applied in-sequence even if more highly versioned schema have already applied, unless already present"
						fi
					fi
					if grep -q '[^0.]' <<<"${version}"; then
						if grep -Eq '^0+[^0]' <<<"${versionsegment[ 2 ]:-0}"; then
							warn "Schema file version '${version}' appears to include a zero-prefixed step version '${versionsegment[ 2 ]:-0}"
							(( notices++ ))
						fi
						if grep -Eq '^0+[^0]' <<<"${versionsegment[ 3 ]:-0}"; then
							warn "Schema file version '${version}' appears to include a zero-prefixed hot-fix version '${versionsegment[ 3 ]:-0}"
							(( notices++ ))
						fi
						if grep -Eq '(\.0+(\.|$))+' <<<"${version}"; then
							warn "Schema file version '${version}' appears to include unnecessary version-zeroes: release versions may be zero; change, step, and hotfix versions should count up from one"
							(( notices++ ))
						fi
					fi

					if grep -Eq '^0|\.0' <<<"${version}"; then
						newversion="$( sed -r 's/^0+// ; s/^\.// ; s/^0+// ; s/\.0+$// ; s/\.0+$//' <<<"${version}" )"
						local -i digit=0
						if grep -Pq '^\d+\.\d+\.\d+$' <<<"${newversion}"; then
							digit=$( cut -d'.' -f 3 <<<"${newversion}" | sed 's/^0\+//' )
							(( digit++ ))
							newversion="$( cut -d'.' -f 1,2 <<<"${newversion}" ).${digit}"
						fi
						if grep -Pq '^\d+\.\d+$' <<<"${newversion}"; then
							digit=$( cut -d'.' -f 2 <<<"${newversion}" | sed 's/^0\+//' )
							(( digit++ ))
							newversion="$( cut -d'.' -f 1 <<<"${newversion}" ).${digit}"
						fi
						if grep -Pq '^\d+$' <<<"${newversion}"; then
							newversion+='.1'
						else
							# We should only get here if the original version was 0 or 00.0000.0,
							# indicating a baseline file - which /should/ be V0.
							newversion='0'
						fi

						[[ "${newversion}" != "${version}" ]] &&
							info "File '${filename}' version '${version}' appears to be a legacy version - assuming modern version of '${newversion}'"

						if [[ -n "${migrationversion:-}" ]] && grep -Pq '^0*\d+(\.0*\d+)?(\.0*\d+)?(\.0*\d+)?$' <<<"${migrationversion}"; then
							newmigrationversion="$( sed -r 's/^0+// ; s/^\.// ; s/^0+// ; s/\.0+$// ; s/\.0+$//' <<<"${migrationversion}" )"
							if grep -Pq '^\d+\.\d+\.\d+$' <<<"${newmigrationversion}"; then
								digit=$( cut -d'.' -f 3 <<<"${newmigrationversion}" | sed 's/^0\+//' )
								(( digit++ ))
								newmigrationversion="$( cut -d'.' -f 1,2 <<<"${newmigrationversion}" ).${digit}"
							fi
							if grep -Pq '^\d+\.\d+$' <<<"${newmigrationversion}"; then
								digit=$( cut -d'.' -f 2 <<<"${newmigrationversion}" | sed 's/^0\+//' )
								(( digit++ ))
								newmigrationversion="$( cut -d'.' -f 1 <<<"${newmigrationversion}" ).${digit}"
							fi
							if grep -Pq '^\d+$' <<<"${newmigrationversion}"; then
								newmigrationversion+='.1'
							fi
						fi
						unset digit
					fi # grep -Eq '^0|\.0' <<<"${version}"

					if [[ -n "${max:-}" && "${max}" != "${version}" ]]; then
						local vsort='sort -Vr'
						if (( novsort )); then
							vsort='sort -gr'
						fi

						if [[ "${version}" == "$( echo -e "${version}\n${max}" | $vsort | head -n 1 )" ]]; then
							error "Schema file version '${version}' is higher than the configured maximum version to apply, '${max}'"
							(( warnings++ ))
						fi
					fi # [[ -n "${max:-}" ]]
				fi # grep -Pq '^0*\d+(\.0*\d+)?(\.0*\d+)?(\.0*\d+)?$' <<<"${version}"
			fi # ! [[ "${type}" == "metadata" ]]

			debug "Examining '${file}' for metadata ..."
			local line='' fragment='' directive='' value=''
			local -l frag=''

			#output >&2 "file is '${file}'"
			#output >&2 "foundinit is '${foundinit}'"
			#output >&2 "versions contains '${!versions[@]}'"
			#output >&2 "descriptions contains '${!descriptions[@]}'"
			#output >&2 "metadescriptions contains '${!metadescriptions[@]}'"

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
						'description:')
							if (( "${seen[${frag%:}]:-}" )); then
								error "Metadata from file '${name}' specifies '${frag%:}' directive multiple times"
								(( warnings++ ))
							else
								seen[${frag%:}]=1
							fi

							local -l metadesc
							metadesc="$( sed 's/[^A-Za-z]/_/g ; s/_\+/_/g' <<<"${value:-}" )"
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
						'engine:')
							if (( "${seen[${frag%:}]:-}" )); then
								error "Metadata from file '${name}' specifies '${frag%:}' directive multiple times"
								(( warnings++ ))
							else
								seen[${frag%:}]=1
							fi

							local -l metavalue="${value:-}"
							case "${metavalue:-}" in
								'mysql')
									info "Engine specified in file '${name}' does not have to be specified if it is MySQL, which is default"
									(( styles++ ))
									;;
								'vertica')
									engine="${metavalue}"
									# shellcheck disable=SC2126
									if (( $( sed 's/"[^"]*"//' "${file}" | sed "s/'[^']*'//" | grep -o '`' | wc -l ) )); then
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
						'database:')
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
						'schema:')
							if (( "${seen[${frag%:}]:-}" )); then
								error "Metadata from file '${name}' specifies '${frag%:}' directive multiple times"
								(( warnings++ ))
							else
								seen[${frag%:}]=1
							fi

							schema="${value:-}"
							;;
						'previous_version:')
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
									debug "Recorded versions are: '${!versions[*]}'"
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
						'target_version:')
							if (( "${seen[${frag%:}]:-}" )); then
								error "Metadata from file '${name}' specifies '${frag%:}' directive multiple times"
								(( warnings++ ))
							else
								seen[${frag%:}]=1
							fi

							if [[ -n "${value:-}" ]]; then
								debug "Recording 'Target Version: ${value}'"
								versions[${value}]=1
								debug "Recorded versions are now: '${!versions[*]}'"
							else
								error "Metadata from file '${name}' has empty 'Target Version' directive"
								(( warnings++ ))
							fi

							# Lack of caret before V allows matching migration schema files...
							if ! [[ "${type}" == "metadata" || "${name}" =~ V${value}__ ]]; then
								warn "Metadata from file '${name}' specifies 'Target Version: ${value}', which does not match filename"
								(( warnings++ ))
							fi
							;;
						'environment:')
							if (( "${seen[${frag%:}]:-}" )); then
								error "Metadata from file '${name}' specifies '${frag%:}' directive multiple times"
								(( warnings++ ))
							else
								seen[${frag%:}]=1
							fi

							environmentdirective="${value}"
							;;
						'restore:')
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
						*':')
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
				debug 'Finished checking line'
			done < <( awk -- "${script:-}" "${file}" ) # while read -r line

			if [[ -n "${newversion:-}" && "${version}" =~ ^[0-9]+(\.[0-9]+){2,}$ ]]; then
				fullname="V${newversion}${newmigrationversion:+__V${newmigrationversion}}__${metadescription:-${description:-<description>}}${environment:+.${environment}}.${filetype:-${defaulttype:-}}.sql"
				[[ -z "${defaulttype:-}" ]] && fullname="${fullname/../.}" # Fix migration schema <sigh>

				note "File '${name}' appears to contain legacy version string '${version}' - suggest migration to new naming scheme '${fullname}'"
				(( notices++ ))
			elif [[ "${version}" =~ (^|\.)0\\d+(\.|$) ]]; then
				note "File '${name}' appears to contain unnecessary zeroes in version string '${version}'"
				(( notices++ ))
			fi

			if [[ -n "${environmentdirective:-}" ]]; then
				if [[ "${environmentdirective}" =~ ^! ]]; then
					info "File '${name}' is not valid in environment '${environmentdirective#!}'"
					if ! [[ "${name}" =~ \.not-${environmentdirective#!}\. ]]; then
						warn "Filename '${name}' does not include environment 'not-${environmentdirective#!}'"
						(( warnings++ ))
					fi
				else
					info "File '${name}' is only valid in environment '${environmentdirective}'"
					note 'There should be corresponding environment-locked schema files present in order to provide a comprehensive set whereby every possible environment has a schema file present which applies to it'
					local suggestion=''
					if [[ -n "${newversion:-}" ]]; then

						suggestion="V${newversion}__V${newversion}__${metadescription:-${description:-<description>}}.not-${environmentdirective}.sql"
					else
						#suggestion="$( sed -r "s/^(V.*__)(.*)$/\\1\\1\\2/ ; s/\.d[mdc]l\./.not-${environmentdirective}./" <<<"${name}" )"
						suggestion="V${version}__V${version}__${metadescription:-${description:-<description>}}.not-${environmentdirective}.sql"
					fi
					info "If nothing needs to be done for other environments, please provide a migration schema to skip this step (possibly '${suggestion}' if only this version is to be stepped-over)"
					unset suggestion
					if ! [[ "${name}" =~ \.${environmentdirective#!}\. ]]; then
						warn "Filename '${name}' does not include environment '${environmentdirective#!}'"
						(( warnings++ ))
					fi
				fi
			fi
			environmentdirective=''
			metadescription=''

			line='' fragment='' directive='' value='' frag=''

			if [[ -n "${schema:-}" ]]; then
				if ! [[ "${engine:-}" == 'vertica' ]]; then
					error "File '${name}' includes a Vertica schema directive ('${schema}') without specifying 'Engine: Vertica'"
					(( warnings++ ))
				fi
			fi
			if [[ "${type}" == 'vertica-schema' && ! -n "${migrationversion:-}" && ! "${engine:-}" == 'vertica' ]]; then
				error "File '${name}' is defined to be a Vertica schema-file but does not specify 'Engine: Vertica'"
				(( warnings++ ))
			fi

			if ! (( ${seen['description']:-} )); then
				note "Metadata from file '${name}' lacks a 'Description' directive"
				(( notices++ ))
			fi
			if ! [[ "${type}" == 'metadata' ]]; then
				if ! (( ${seen['previous_version']:-} )); then
					error "Metadata from file '${name}' lacks a 'Previous Version' directive"
					(( warnings++ ))
				fi
			fi
			if ! (( ${seen['target_version']:-} )); then
				error "Metadata from file '${name}' lacks a 'Target Version' directive"
				(( warnings++ ))
			fi

			debug 'Finished checking metadata'

			# Does gitbash support coloured output?
			# Best not assume...
			local cgrep='grep --colour=always'
			${cgrep} 'x' <<<'x' >/dev/null 2>&1 || cgrep='grep'

			case "${filetype:-${defaulttype:-}}" in
				'dml')
					if sed -r 's|/\*.*\*/|| ; s/--.*$// ; s/#.*$// ; s/(CREATE|DROP)\s+TEMPORARY\s+TABLE//' "${file}" | grep -Eiq '\s+(CREATE|ALTER|DROP)\s+'; then # DDL
						warn "Detected DDL in DML-only file '${name}':"
						warn "$( ${cgrep} -Ei '\s+(CREATE|ALTER|DROP)\s+' "${file}" | grep -Ev "(CREATE|DROP)\s+TEMPORARY\s+TABLE)" )"
						(( warnings++ ))
					fi
					if sed 's|/\*.*\*/|| ; s/--.*$// ; s/#.*$//' "${file}" | grep -Eiq '\s+(GRANT|REVOKE)\s+'; then # DCL
						warn "Detected DCL in DML-only file '${name}':"
						warn "$( ${cgrep} -Ei '\s+(GRANT|REVOKE)\s+' "${file}" )"
						(( warnings++ ))
					fi
					;;
				'ddl')
					if sed -r 's|/\*.*\*/|| ; s/--.*$// ; s/#.*$// ; s/before\s+insert\s+on//i' "${file}" | grep -Eiq '\s+(UPDATE\s+.*SET\s+|INSERT|DELETE\s+FROM)\s+'; then # DML
						warn "Detected DML in DDL-only file '${name}':"
						warn "$( ${cgrep} -Ei '\s+(UPDATE\s+.*SET|INSERT|DELETE\s+FROM)\s+' "${file}" )"
						(( warnings++ ))
					fi
					if sed 's|/\*.*\*/|| ; s/--.*$// ; s/#.*$//' "${file}" | grep -Eiq '\s+(GRANT|REVOKE)\s+'; then # DCL
						warn "Detected DCL in DDL-only file '${name}':"
						warn "$( ${cgrep} -Ei '\s+(GRANT|REVOKE)\s+' "${file}" )"
						(( warnings++ ))
					fi
					;;
				'dcl')
					if sed -r 's|/\*.*\*/|| ; s/--.*$// ; s/#.*$// ; s/before\s+insert\s+on//i' "${file}" | grep -Eiq '\s+(UPDATE\s+.*SET\s+|INSERT|DELETE\s+FROM)\s+'; then # DML
						warn "Detected DML in DCL-only file '${name}':"
						warn "$( ${cgrep} -Ei '\s+(UPDATE\s+.*SET|INSERT|DELETE\s+FROM)\s+' "${file}" )"
						(( warnings++ ))
					fi
					if sed -r 's|/\*.*\*/|| ; s/--.*$// ; s/#.*$// ; s/(CREATE|DROP)\s+TEMPORARY\s+TABLE//' "${file}" | grep -Eiq '\s+(CREATE|ALTER|DROP)\s+'; then # DDL
						warn "Detected DDL in DCL-only file '${name}':"
						warn "$( ${cgrep} -Ei '\s+(CREATE|ALTER|DROP)\s+' "${file}" | grep -Ev "(CREATE|DROP)\s+TEMPORARY\s+TABLE)" )"
						(( warnings++ ))
					fi
					;;
				'')
					if sed 's|/\*.*\*/|| ; s/--.*// ; s/#.*$//' "${file}" | grep -Eiq '[a-z]'; then # Anything!
						warn "Detected non-commented statements in migration file '${name}':"
						sed 's|/\*.*\*/|| ; s/--.*// ; s/#.*$//' "${file}" | grep -v '^\s*$'
						(( warnings++ ))
					fi
					;;
				*)
					warn "Unknown SQL type '${filetype:-}'"
					;;
			esac

			unset cgrep

			if (( warnings )); then
				(( silent )) || warn "Completed checking schema file '${name}': ${warnings} Fatal Errors or Warnings, ${notices} Notices, ${styles} Comments"
				rc=1
			elif (( notices )); then
				(( silent )) || note "Completed checking schema file '${name}': ${warnings} Fatal Errors or Warnings, ${notices} Notices, ${styles} Comments"
			else
				(( silent )) || info "Completed checking schema file '${name}': ${warnings} Fatal Errors or Warnings, ${notices} Notices, ${styles} Comments"
			fi

			warnings=0 notices=0 styles=0

		done # for file in "${files[@]:-}"
	fi

	if (( rc )); then
		error "Warnings or Fatal Errors found - Validation of '$( basename "$( dirname "${file}" )" )/$( basename "${file}" )' failed"
	else
		(( silent )) || info "Validation of '$( basename "$( dirname "${file}" )" )/$( basename "${file}" )' succeeded"
	fi

	return ${rc:-1}
} # validate # }}}

# shellcheck disable=SC2155
function main() { # {{{
	local truthy="^(on|y(es)?|true|1)$"
	local falsy="^(off|n(o)?|false|0)$"

	local actualpath filename
	local lockfile="/var/lock/${NAME}.lock"
	[[ -w /var/lock ]] || lockfile="${TMPDIR:-/tmp}/${NAME}.lock"

	local -i child=0
	local -i warnings=0 notices=0 styles=0

	local -i novsort=0
	if ! sort -V <<<'' >/dev/null 2>&1; then
		warn 'Version sort unavailable - Stored Procedure and Schema' \
		     'file load-order cannot be guaranteed.'
		error 'You may see spurious warnings about missing prior' \
		      'versions if you continue.'
		info 'To resolve this issue, please upgrade to the latest' \
		     'git-for-windows release...'
		output
		warn 'Press ctrl+c now to abort ...'
		sleep 10
		warn 'Proceeding ...'
		novsort=1
		(( warnings++ ))
	fi

	local arg schema db dblist clist
	local -i dryrun=0 quiet=0 silent=0

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
			-h|--help)
				export std_USAGE='[--config <file>] [--schema <path>] [ [--databases <database>[,...]] | [--clusters <cluster>[,...]] ] [--dry-run] [--quiet|--silent] [--no-wrap] | [--locate <database>]'
				std::usage
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
			-n|--nowrap|--no-wrap)
				export STDLIB_WANT_WORDWRAP=0
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
					clist="${clist:+,}${1}"
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
	# shellcheck disable=SC2126
	(( 2 == $( grep -o 'x' <<<'xx' | wc -l ) )) || die "grep does not support '-o' option"

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

	local -l syntax

	# We're going to eval our config file sections - hold onto your hats!
	eval "${defaults}"
	eval "${hosts}"

	for db in ${databases}; do
		# Run the block below in a sub-shell so that we don't have to
		# manually sanitise the environment on each iteration.
		#
		( # {{{
		local -A versions=()
		local -A descriptions=()
		local -A metadescriptions=()
		local -i foundinit=0

		if [[ -n "${dblist:-}" ]] && ! grep -q ",${db}," <<<",${dblist},"; then
			if (( std_DEBUG )) && ! (( child )); then
				output
				debug "Skipping deselected database '${db}' ..."
			fi
			exit 0 # continue
		fi

		if ! (( quiet | silent )); then
			# If founddb == 1 then we've already seen prior data-
			# base entries, and produced output for them.  We
			# then want a blank line here to separate the output.
			if ! (( std_DEBUG )); then
				(( founddb )) && [[ -n "${std_LASTOUTPUT:-}" ]] && output
			fi
		fi

		local details="$( std::getfilesection "${filename}" "${db}" | sed -r 's/#.*$// ; /^[^[:space:]]+\.[^[:space:]]+\s*=/s/\./_/' | grep -Ev '^\s*$' | sed -r 's/\s*=\s*/=/' )"
		[[ -n "${details:-}" ]] || die "Database '${db}' lacks a configuration block in '${filename}'"
		debug "${db}:\n${details}\n"

		eval "${details}"

		if [[ -n "${clist:-}" ]] && ! grep -q ",${cluster:-.*}," <<<",${clist},"; then
			if ! (( child )) && ! (( quiet | silent )); then
				output
				info "Skipping database '${db}' from deselected cluster '${cluster:-all}' ..."
			fi
			exit 0 # continue
		fi

		if grep -Eiq "${falsy}" <<<"${managed:-}"; then
			if (( child )); then
				if ! (( quiet | silent )); then
					output
					info "Skipping unmanaged database '${db}' ..."
				fi
				founddb=1
				exit 3 # See below...
			fi
		else
			if ! (( silent )); then
				info "Processing configuration to validate database '${db}' ..."
				if ! (( std_DEBUG )); then
					# If we're not producing debug output,
					# which wants to be as succinct
					# (e.g. compressed) as possible, and
					# if we're processing only a single
					# database, then output a blank line
					# here...
					[[ -n "${dblist:-}" && "${dblist}" == "${db}" ]] && [[ -n "${std_LASTOUTPUT:-}" ]] && output
				fi
			fi
		fi

		local -a messages=()

		if [[ -z "${actualpath:-}" ]]; then
			# Allow command-line parameter to override config file
			actualpath="${path:-}"
		fi
		path="$( readlink -e "${actualpath:-.}" )" || die "Failed to canonicalise path '${actualpath}': ${?}"
		actualpath=''

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
					if [[ "$( basename "${path}" )" == 'schema' ]]; then
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
			if [[ -n "${!cluster:-}" ]]; then
				host="${!cluster}"
			else
				error "Database '${db}' has cluster '${cluster}', for which no write master is defined"
			fi
		else
			die "Neither 'host' nor 'cluster' membership is defined for database '${db}' in '${filename}'"
		fi

		if grep -Eiq "${truthy}" <<<"${procedures:-}"; then
			local -a reorder=( sort -V )
			if (( novsort )); then
				reorder=( cat )
			fi

			local procedurepath="${path}/procedures"
			if [[ -d "${path}"/schema/"${db}"/procedures ]]; then
				procedurepath="${path}"/schema/"${db}"/procedures
			fi

			local ppath
			local hasfile=0 hasdir=0
			while read -r ppath; do
				while read -r filename; do
					if [[ -f "${filename}" ]]; then
						hasfile=1
						(( hasdir )) && break
					elif [[ -d "${filename}" ]]; then
						hasdir=1
						(( hasfile )) && break
					elif ! [[ -e "${filename}" ]]; then
						die "LOGIC ERROR: (1): Filesystem object '${filename}' does not exist"
					fi
				done < <( find "${ppath}"/ -mindepth 1 -maxdepth 1 2>/dev/null | "${reorder[@]}" )
				while read -r filename; do
					if [[ -f "${filename}" && "$( basename "${filename}" )" =~ \.sql$ ]]; then
						if (( hasdir )); then
							warn 'Invalid mix of files and directories at same level'
							(( warnings++ ))
						fi
						if (( silent )); then
							validate -type 'procedure' -files "${filename}" >/dev/null 2>&1
						elif (( quiet )); then
							validate -type 'procedure' -files "${filename}" >/dev/null
						else
							validate -type 'procedure' -files "${filename}"
						fi
						(( rc += ${?} ))
						founddb=1
					elif [[ -d "${filename}" ]]; then
						if (( hasfile )); then
							note "Directory '$( basename "${filename}" )' should not be present in directory '${ppath}' and will be ignored during Stored Procedure processing"
							(( notices++ ))
						fi
					elif [[ -f "${filename}" ]]; then
						if (( hasdir )); then
							warn 'Invalid mix of files and directories at same level'
							(( warnings++ ))
						fi
						if [[ "$( basename "${filename}" )" =~ \.metadata$ ]]; then
							debug "File '$( basename "${filename}" )' is Stored Procedure metadata"
							if (( silent )); then
								validate -type 'metadata' -files "${filename}" >/dev/null 2>&1
							elif (( quiet )); then
								validate -type 'metadata' -files "${filename}" >/dev/null
							else
								validate -type 'metadata' -files "${filename}"
							fi
						else
							warn "File '$( basename "${filename}" )' does not end in '.sql' and so should not be present in directory '${ppath}'"
							(( warnings++ ))
						fi
					elif [[ -e "${filename}" ]]; then
						warn "Object '$( basename "${filename}" )' should not be present in directory '${ppath}' and will be ignored during Stored Procedure processing"
						(( warnings++ ))
					else
						die "LOGIC ERROR: (2): Filesystem object '${filename}' does not exist"
					fi
				done < <( find "${ppath}"/ -mindepth 1 -maxdepth 1 2>/dev/null | "${reorder[@]}" )
			done < <( find "${procedurepath}"/ -mindepth 1 -maxdepth 2 -type d 2>/dev/null | grep "${db}" | "${reorder[@]}" )
			unset hasdir hasfile ppath

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
		for filename in $(
			local k o v
			local -A prefices=()

			while read -r o; do
				v="$( cut -d'_' -f 1 <<<"$( basename "${o}" )" )"
				prefices["${v}"]="${o}"
			done < <( find "${ppath}"/ -mindepth 1 -maxdepth 1 -print 2>/dev/null )

			while read -r v; do
				echo "${prefices["${v}"]}"
			done < <( for k in "${!prefices[@]}"; do echo "${k}"; done | sort -V )

			unset prefices v o k
		); do
			# This is not always guaranteed to be the case, so
			# let's ensure that we always have the same data...
			filename="$( basename "${filename}" )"

			if [[ -f "${ppath}/${filename}" && "${filename}" =~ \.sql$ ]]; then
				debug "Validating file '${ppath}/${filename}' ..."
				if [[ -n "${syntax:-}" && "${syntax}" == 'vertica' ]]; then
					if (( silent )); then
						# shellcheck disable=SC2154
						validate -type 'vertica-schema' ${version_max:+-max "${version_max}" }-files "${ppath}/${filename}" >/dev/null 2>&1
					elif (( quiet )); then
						# shellcheck disable=SC2154
						validate -type 'vertica-schema' ${version_max:+-max "${version_max}" }-files "${ppath}/${filename}" >/dev/null && std_LASTOUTPUT=""
					else
						# shellcheck disable=SC2154
						validate -type 'vertica-schema' ${version_max:+-max "${version_max}" }-files "${ppath}/${filename}"
					fi
				else
					if (( silent )); then
						# shellcheck disable=SC2154
						validate -type 'schema' ${version_max:+-max "${version_max}" }-files "${ppath}/${filename}" >/dev/null 2>&1
					elif (( quiet )); then
						# shellcheck disable=SC2154
						validate -type 'schema' ${version_max:+-max "${version_max}" }-files "${ppath}/${filename}" >/dev/null && std_LASTOUTPUT=""
					else
						# shellcheck disable=SC2154
						validate -type 'schema' ${version_max:+-max "${version_max}" }-files "${ppath}/${filename}"
					fi
				fi
				(( rc += ${?} ))
				founddb=1

			elif [[ -d "${ppath}/${filename}" ]]; then
				note "Directory '${filename}' should not be present in directory '${ppath}' and will be ignored during schema file processing"
				(( notices++ ))

			elif [[ -f "${ppath}/${filename}" ]]; then

				# TODO: Alternatively, any referenced files
				#       could be returned by validate()...

				if grep -Eiq "${truthy}" <<<"${procedures:-}" && [[ "${filename}" =~ \.metadata$ ]]; then
					debug "File '${filename}' in directory '${ppath}' is Stored Procedure metadata"
				else
					local script restorefiles files
					std::define script <<-EOF
						BEGIN		{ output = 0 }
						/\/\*/		{ output = 1 }
						( 1 == output )	{ print \$0 }
						/\*\//		{ exit }
					EOF
					if (( std_DEBUG )); then
						find "${ppath}" -mindepth 1 -maxdepth 1 | while read -r files; do
							debug "Checking file '${files}' for 'Restore:' directive ..."
							awk -- "${script:-}" "${files}"
							awk -- "${script:-}" "${files}" | grep --colour -o 'Restore:\s*[^[:space:]]\+\s*'
						done
					fi
					restorefiles="$( find "${ppath}" -mindepth 1 -maxdepth 1 | while read -r files; do
						awk -- "${script:-}" "${files}" | grep -o 'Restore:\s*[^[:space:]]\+\s*'
					done | cut -d':' -f 2- | xargs echo )"
					if ! grep -qw "${filename}" <<<"${restorefiles}"; then
						warn "File '${filename}' does not end in '.sql' and so should not be present in directory '${ppath}'"
						(( warnings++ ))
					else
						debug "File '${filename}' is referenced in a metadata 'Restore:' directive"
					fi
					unset files restorefiles script
				fi
			else
				warn "Object '${filename}' should not be present in directory '${ppath}' and will be ignored during schema file processing"
				(( warnings++ ))
			fi

			if ! (( silent )); then
				# If we think we've produced output and we're
				# processing only a single database, then
				# output a blank line here...
				[[ -n "${dblist:-}" && "${dblist}" == "${db}" ]] && [[ -n "${std_LASTOUTPUT:-}" ]] && output
			fi
		done # filename
		debug "Schema processed for database '${db}'\n"

		(( founddb && rc )) && exit 4
		(( founddb && !( rc ) )) && exit 3

		# shellcheck disable=SC2015
		(( rc )) && false || true

		# Run in sub-shell so that the following is not necessary...
		unset versions descriptions metadescriptions foundinit details messages reorder procedurepath ppath

		) # }}}

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
	done # db in ${databases}

	(( std_TRACE )) && set +o xtrace

	debug 'Releasing lock ...'
	[[ -e "${lockfile}" && "$( <"${lockfile}" )" == "${$}" ]] && rm "${lockfile}"

	# ${rc} should have recovered from the sub-shell, above...
	if (( rc )) && (( dryrun )); then
		(( silent )) || warn "Validation completed with errors, or database doesn't exist"
	elif (( rc )); then
		(( silent )) || warn 'Validation completed with errors'
	elif (( !( founddb ) )); then
		(( silent )) || error 'Specified database(s) not present in configuration file'
		rc=1
	else
		(( silent )) || info 'Validation completed'
	fi

	return ${rc}
} # main # }}}

export LC_ALL='C'
set -o pipefail

std::requires --no-quiet 'pgrep'

main "${@:-}"

exit ${?}

# vi: set filetype=sh syntax=sh commentstring=#%s foldmarker=\ {{{,\ }}} foldmethod=marker colorcolumn=80 nowrap:
