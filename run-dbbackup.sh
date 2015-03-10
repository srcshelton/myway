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

	[[ -x /usr/local/sbin/dbbackup.sh ]] || die "Backup script 'dbbackup.sh' does not exist"
	[[ -d "${dirname}" ]] || die "Backup directory '${dirname}' does not exist"

	# Ensure that 'fuser' will work...
	(( EUID )) && die "This script must be run with super-user privileges"

	local -i ts=$(( $( date +"%s" ) ))

	[[ -e "${lockfile}" ]] && return 1
	lock "${lockfile}" || return 1
	sleep 0.1
	[[ -e "${lockfile}" && "$( <"${lockfile}" )" == "${$}" ]] || return 1

	# We have a lock!

	# Backup policy (hard-coded for now):
	# * Keep all backups for up to a week;
	# * Keep one weekly backup for up to a month;
	# * Keep one monthly backup for up to a year.
	#
	info "Preparing to process existing backups ..."

	# We'll handle backup-pruning in two stages - the first will
	# enumerate all backups (newest to oldest, so that files have a chance
	# to be aged-out) and determine which should be erased, then the second
	# will ensure that we're not removing *all* backups (which could happen
	# if no backup has been created for some period) and will actually
	# action any required deletions.
	#
	info "Archiving and removing old backup files from '${dirname}' ..."

	# N.B. Whilst providing a rich selection of date-related figures,
	#      'date' lacks a direct way to determine the week-number in a
	#      month within which a date falls. Instead, we can record the
	#      starting week number (in the year) for a given month, and then
	#      subtract this from the week number for each date.

	# bash's $(( ... )) syntax doesn't like values beginning with '0' - I
	# guess it assumes they're octal...
	local -i thisweek=$(( $( date +"%Y%m%d" -d "@$(( ts - ( 7 * 24 * 60 * 60 ) ))" | sed 's/^0\+//' ) ))
	local -i thismonth=$(( $( date +"%Y%m%d" -d "@$(( ts - ( 30 * 24 * 60 * 60 ) ))" | sed 's/^0\+//' ) ))
	local -i thisyear=$(( $( date +"%Y%m%d" -d "@$(( ts - ( 365 * 24 * 60 * 60 ) ))" | sed 's/^0\+//' ) ))
	local -i startingweek=$(( $( date +"%W" -d "@$(( ts - ( 7 * 24 * 60 * 60 ) ))" | sed 's/^0\+//' ) ))
	local -ai week month
	local -a keep delete
	local -i stamp number
	local files

	# Process the newest files first, but override their entries with older
	# files as appropriate ...
	while read -r stamp; do
		if (( stamp < thisyear )); then
			while read -r files; do
				delete+=( "${files}" )
			done < <( ls -1d "${dirname}"/"${stamp}".* )
		elif (( stamp < thismonth )); then
			number=$(( $( sed -r 's/^[0-9]{4}([0-9]{2})[0-9]{2}$/\1/' <<<"${stamp}" ) ))
			month[number]=${stamp}
		elif (( stamp < thisweek )); then
			number=$(( $( date +"%W" -d "${stamp}" ) - startingweek ))
			# This will potentially do odd things for the first week of a new month...
			(( number < 0 )) && number=0
			week[number]=${stamp}
		else
			while read -r files; do
				keep+=( "${files}" )
			done < <( ls -1d "${dirname}"/"${stamp}".* )
		fi
	done < <(
		find "${dirname}"/ -mindepth 1 -maxdepth 1 -type f -or -type d	\
			| sed 's|^.*/||'					\
			| grep -E '^[0-9]{8}\.'					\
			| cut -d'.' -f 1					\
			| sort -rn						\
			| uniq
	)

	# Process files in time-order ...
	while read -r stamp; do
		files="$( ls -1d "${dirname}"/"${stamp}".* )"
		if (( stamp < thisyear )); then
			warn "Backup files $( std::formatlist $( sed "s/^/'/ ; s/ /' '/g ; s/$/'/" <<<"${files}" ) ) are more than a year old and will be removed ..."
		elif (( stamp < thismonth )); then
			if grep -qw " ${stamp} " <<<" ${month[@]:-} "; then
				note "Backup files $( std::formatlist $( sed "s/^/'/ ; s/ /' '/g ; s/$/'/" <<<"${files}" ) ) are more than a month old, but will be preserved ..."
				while read -r files; do
					keep+=( "${files}" )
				done < <( ls -1d "${dirname}"/"${stamp}".* )
			else
				warn "Backup files $( std::formatlist $( sed "s/^/'/ ; s/ /' '/g ; s/$/'/" <<<"${files}" ) ) are more than a month old, and will be removed ..."
				while read -r files; do
					delete+=( "${files}" )
				done < <( ls -1d "${dirname}"/"${stamp}".* )
			fi
		elif (( stamp < thisweek )); then
			if grep -qw " ${stamp} " <<<" ${week[@]:-} "; then
				note "Backup files $( std::formatlist $( sed "s/^/'/ ; s/ /' '/g ; s/$/'/" <<<"${files}" ) ) are more than a week old, but will be preserved ..."
				while read -r files; do
					keep+=( "${files}" )
				done < <( ls -1d "${dirname}"/"${stamp}".* )
			else
				warn "Backup files $( std::formatlist $( sed "s/^/'/ ; s/ /' '/g ; s/$/'/" <<<"${files}" ) ) are more than a week old, and will be removed ..."
				while read -r files; do
					delete+=( "${files}" )
				done < <( ls -1d "${dirname}"/"${stamp}".* )
			fi
		else
			note "Backup files $( std::formatlist $( sed "s/^/'/ ; s/ /' '/g ; s/$/'/" <<<"${files}" ) ) will be preserved ..."
		fi
	done < <(
		find "${dirname}"/ -mindepth 1 -maxdepth 1 -type f -or -type d	\
			| sed 's|^.*/||'					\
			| grep -E '^[0-9]{8}\.'					\
			| cut -d'.' -f 1					\
			| sort -n						\
			| uniq
	)

	if [[ -z "${delete[@]:-}" ]]; then
		warn "No files scheduled to be removed - continuing"
	else
		if [[ -z "${keep[@]:-}" ]]; then
			error "Cowardly refusing to delete all backups (have no backups been written recently?) - aborting pruning process"
		else
			for files in "${delete[@]}"; do
				if (( std_DEBUG )); then
					debug "Would run 'rm -frv \"${files}\"' ..."
				else
					rm -frv "${files}" ; (( rc += ${?} ))
				fi
			done
			if (( rc )); then
				error "Backup removal failed"
			fi
		fi
	fi

	if [[ -v db && -n "${db:-}" ]]; then
		info "Backing up database '${db}' ..."
	else
		info "Backing up instance from '${host}' ..."
	fi
	/usr/local/sbin/dbbackup.sh -u "${user}" -p "${pass}" -o "${host}" -l "${dirname}" ${db:+-d "${db}"}
	rc=${?}

	[[ -e "${lockfile}" && "$( <"${lockfile}" )" == "${$}" ]] && rm "${lockfile}"

	return ${rc}
} # main

main "${@:-}"

exit ${?}

# vi: set syntax=sh colorcolumn=80 foldmethod=marker:
