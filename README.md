
myway - Flyway-compatible MySQL & Vertica schema-management tools
=================================================================

### Requirements
Please note that `applyschema.sh` relies upon [stdlib.sh for bash](https://github.com/srcshelton/stdlib.sh) being installed.

`myway.pl` requires the following non-core Perl modules, which must be separately installed:

* `Data::GUID` (requires `Sub::Exporter`, `Params::Util`, `Data::OptList`, `Sub::Install`)
* `DBI::Format` (requires `DBI::Shell`, `Text::Reform`, `IO::Tee` )
* `File::Touch`
* `File::Which`
* `match::smart` (actually provided by the `match::simple` package)
* `Regexp::Common`
* `Sort::Versions`

... noting that `Data:GUID` is conditionally `use`d only if operating on a [Vertica](https://my.vertica.com/) database

### Using dbtools

```
Usage: applyschema.sh [--config <file>] --locate <database>

Usage: applyschema.sh [--config <file>] [--schema <path>] [--quiet | --silent]
                      [--databases <database>[,...] | --clusters <cluster>[,...]]
                      [--keep-going] [--dry-run] [--force] [--no-validate]
                      [--progress=<always|auto|never>] [--no-wrap]
```

See [schema.example.conf](../../blob/master/conf/schema.example.conf) for configuration `<file>` format.  The contents of this file provide the default settings passed to `myway.pl`, which accepts the options shown below.

* The `path` specified is the location of the schema root-directory which contains per-database subdirectories containing schema-files and Stored Procedure definitions.
* The data to be deployed may be filtered by using `--databases` (to deploy only specified databases) or `--clusters` (to deploy only those databases which exist on a given host or cluster).
* Finally, `--locate` will take no action but will output the cluster with which a specified database is associated.

***

```
Usage: myway.pl <--username <user> --password <passwd> --host <node> ...
                 [--port <port>] --database <db>> ...
                | <--dsn <dsn>> [[:syntax:]] ...
                <--backup [directory] [:backup options:]|...
                 --restore <file> [:restore options:]|--init [version]>|...
                [--migrate|--check] <--scripts <directory>|--file <schema>> ...
                [--target-limit <version>] [[:mode:]] [[:syntax:]] ...
                [--mysql-compat] [--no-backup|--keep-backup] ...
                [--clear-metadata] [--force] [--dry-run] [--silent] [--quiet]
                [--notice] [--warn] [--debug]

                backup options:   [--compress [:scheme:]] [--transactional]
                                  [--lock [--keep-lock]] [--separate-files]
                                  [--skip-metadata] [--no-skip-definer]
                                  [--no-skip-drop] [--no-skip-procedures]
                                  [--skip-reorder] [--ddl|--dml]
                                  [--extended-insert]
                scheme:           <gzip|bzip2|xz|lzma>

                restore options:  [--progress[=<always|auto|never>]]

                mode:              --mode <schema|procedure>
                                  [--substitute [--marker <marker>]

                syntax:            --syntax <mysql|vertica>

                  * MySQL compatibility:

                --mysql-compat   - Required for MySQL prior to v5.6.4
                --mysql-relaxed  - Do not operate in STRICT mode

                  * Backup options:

                --no-backup      - Do not take backups before making changes
                --keep-backup    - Copy backups to a local directory on exit

                --compress       - Compress backups [using <scheme> algorithm]
                --transactional  - Don't lock transactional tables
                --lock           - Lock instance for duration of backup
                --keep-lock      - Keep lock for up to 24 hours after backup

                  * Stored Procedure options:

                --substitute     - Replace the string '`<<VERSION>>' with version
                                   number from stored procedure directory name
                --marker         - Use string in place of '`<<VERSION>>'

                  * Schema options:

                --allow-unsafe   - Allow DROP TABLE & DROP DATABASE statements

                  * Metadata options:

                --description    - Override description for a single schema file

                --environment    - Specify environment for metadata filtering
                --target-limit   - Specify required final schema version

                --clear-metadata - Remove all {my,fly}way metadata tables

                --force          - Allow a database to be re-initialised or
                                   ignore previous and target versions when
                                   applying schema files

                  * Output control:

                --warn           - Output additional warning messages only
                --notice         - Output standard progress messages
                --debug          - Output copious debugging statements

                --silent         - Output only fatal errors
                --quiet          - Output only essential messages

                --dry-run        - Validate but do not execute schema SQL

(N.B. '--dry-run' requires an initialised database to prepare statements against)
```

For full documentation of database standards, please refer to [the myway wiki](../../wiki/Schema-Standards).
