# gretel - command line tool

`gretel` is a command line interface for gretel services. It is installed by default from the `gretel-transformer` package.


## How to use the CLI tool

To display the default help screen run

```
$ gretel --help
```

The following commands are valid for all sub-commands

* `--project` - The name of the project to operate on.
* `--api-key` - A valid Gretel api key. If no api key is passed, the environment variable `GRETEL_API_KEY` will be used as a fallback.

## Commands

### write

The `write` sub-command can be used to write records from various input sources to Gretel record APIs.

Example usage, write a CSV file:
```
$ gretel --project safecast write --file path/to/file.csv --reader csv
```

The following url schemes are valid file identifiers

```
s3://my_bucket/my_key
s3://my_key:my_secret@my_bucket/my_key
s3://my_key:my_secret@my_server:my_port@my_bucket/my_key
gs://my_bucket/my_blob
hdfs:///path/file
hdfs://path/file
webhdfs://host:port/path/file
./local/path/file
~/local/path/file
local/path/file
./local/path/file.gz
file:///home/user/file
file:///home/user/file.bz2
[ssh|scp|sftp]://username@host//path/file
[ssh|scp|sftp]://username@host/path/file
[ssh|scp|sftp]://username:password@host/path/file
```

Uploading a JSON file via stdin

```
$ cat input.json | gretel --project safecast write --stdin --reader json
```

For a list of additional valid options run `gretel write --help`.

### tail

The `tail` command is used to follow a record stream of data. This command behaves similarly to `tail(1)` and will continuously follow the stream until the program is manually terminated.

Example usage, tail a record stream:
```
$ gretel --project safecast tail
```

For a list of valid options run `gretel tail --help`.
