
MySQL- & Vertica- oriented, Flyway-compatible database deployment tools
=======================================================================

### Requirements
Please note that `applyschema.sh` relies upon [stdlib.sh for bash](https://github.com/srcshelton/stdlib.sh) being installed.

`myway.pl` requires the following non-core Perl modules, which may need to be separately installed:

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
Usage: applyschema.sh [--config <file>] [--schema <path>]
                      [ [--databases <database>[,...]] | [--clusters <cluster>[,...]] ]
                      [--keep-going] [--dry-run] [--silent] | [--locate <database>]
```

See [schema.conf](../../blob/master/conf/schema.conf) and [example.conf](../../blob/master/conf/schema.example.conf) for configuration `file` details.  The contents of this file provide the default settings to invoke `myway.pl`, which has the options shown below.
* The `path` specified is the location of the root directory which contains per-database directories containing schema-files and Stored Procedure definitions.
* The data to be deployed may be filtered by using `--databases` (to deploy only specified databases) or `--clusters` (to deploy only those databases which exist on a given host or cluster).
* Finally, `--locate` will take no action but to output the cluster which which a specified database is associated.

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
                [--notice] [--warn] [--debug] [--verbose]

                backup options:   [--compress [:scheme:]] [--transactional]
                                  [--lock [--keep-lock]] [--separate-files]
                                  [--skip-metadata] [--extended-insert]
                scheme:           <gzip|bzip2|xz|lzma>

                restore options:  [--progress[=<always|auto|never>]]

                mode:              --mode <schema|procedure>
                                  [--substitute [--marker <marker>]

                syntax:            --syntax <mysql|vertica>

                --trust-filename - Rely on filename version rather than metadata

                --mysql-compat   - Required for MySQL prior to v5.6.4
                --mysql-relaxed  - Do not operate in STRICT mode

                --compress       - Compress backups [using <scheme> algorithm]
                --transactional  - Don't lock transactional tables
                --lock           - Lock instance for duration of backup
                --keep-lock      - Keep lock for up to 24 hours after backup

                --substitute     - Replace the string '`<<VERSION>>' with version
                                   number from stored procedure directory name
                --marker         - Use string in place of '`<<VERSION>>'

                --no-backup      - Do not take backups before making changes
                --keep-backup    - Copy backups to a local directory on exit

                --description    - Override description for single schema files
                --allow-unsafe   - Allow DROP TABLE & DROP DATABASE statements

                --environment    - Specify environment for metadata filtering
                --target-limit   - Specify required final schema version

                --clear-metadata - Remove all {my,fly}way metadata tables
                --force          - Allow a database to be re-initialised or
                                   ignore previous and target versions when
                                   applying schema files

                --dry-run        - Validate but do not execute schema SQL
                --silent         - Output only fatal errors
                --quiet          - Output only essential messages
                --notice         - Output standard progress messages
                --warn           - Output additional warning messages
                --debug          - Output copious debugging statements
                --verbose        - Provide more detailed status information
```

For full documentation of database standards, please refer to [the tools-db wiki](../../wiki/Schema-Standards).
