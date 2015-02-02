#!/usr/bin/env bash

for HOST in $( grep -Ei '(galera|mariadb)' /etc/iod/hosts | grep db1 ); do
	echo "$HOST"
	mysql -u root -pSleepingCats -h "${HOST}" mysql <<<'SHOW DATABASES' | grep -Ev '^(Database|mysql)$|_schema$' | sed 's/^/\t/' # Highlight fail
done
