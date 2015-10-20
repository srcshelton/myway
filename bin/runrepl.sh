#! /bin/bash

std_DEBUG="${DEBUG:-0}"
std_TRACE="${TRACE:-0}"

dbuser="root"
dbpass="SleepingCats"
declare -i timeout="${TIMEOUT:-5}"

(( timeout < 5 )) && timeout=5

(( std_TRACE )) && set -o xtrace

while true; do
	if
		  mysql -u "${dbuser}" -p${dbpass} <<<'SHOW SLAVE STATUS \G' \
		| grep -q 'Slave_SQL_Running: Yes'
	then
		  (( std_DEBUG )) && \
			echo "$( date ) Replication SQL thread running -$(
				  mysql -u "${dbuser}" -p${dbpass} <<<'SHOW SLAVE STATUS \G' \
				| grep 'Seconds_Behind_Master:' \
				| cut -d':' -f 2-
			) seconds behind master, refresh in ${TIMEOUT:-5} seconds ..."
	else
		  mysql -u "${dbuser}" -p${dbpass} <<<'SHOW SLAVE STATUS \G' \
		| grep -E '^\s*Last_(IO_)?Error:' | sed 's/^\s*//'
		  mysql -u "${dbuser}" -p${dbpass} <<<'STOP SLAVE; SET GLOBAL SQL_SLAVE_SKIP_COUNTER = 1; START SLAVE;'
	fi
	sleep ${timeout}
done
