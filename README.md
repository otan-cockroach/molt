# molt

[![Go Tests](https://github.com/cockroachdb/molt/actions/workflows/go.yaml/badge.svg)](https://github.com/cockroachdb/molt/actions/workflows/go.yaml)

Migrate Off Legacy Things is CockroachDB's suite of tools to assist migrations.
This repo contains any open-source MOLT tooling.

Certain packages can be re-used by external tools and are subject to the
[Apache License](LICENSE).

## Build

All commands require `molt` to be built. Example:

```shell
# Build molt for the local machine (goes to artifacts/molt)
go build -o artifacts/molt .

# Cross compiling.
GOOS=linux GOARCH=amd64 go build -v -o artifacts/molt .
```

## Verification

`molt verify` does the following:
* Verifies that tables between the two data sources are the same.
* Verifies that table column definitions between the two data sources are the same.
* Verifies that tables contain the row values between data sources.

It currently supports PostgreSQL and MySQL comparisons with CockroachDB.

It takes in two connection strings as arguments.
For names that are easy to read, append `<name>===` in front of the string.
The first argument is considered as the "source of truth".

```shell
# Compare postgres and CRDB instance.
molt verify \
  'pg_truth===postgres://user:pass@url:5432/db' \
  'crdb_compare===postgres://root@localhost:26257?sslmode=disable'

# Compare mysql and CRDB instance.
molt verify \
  'mysql===jdbc:mysql://root@tcp(localhost:3306)/defaultdb' \
  'postgresql://root@127.0.0.1:26257/defaultdb?sslmode=disable'
```

See `molt verify --help` for all available parameters.

### Continuous verification
If you want all tables to be verified in a loop, you can use `--continuous`.

### Live verification
If you expect data to change as you do data verification, you can use `--live`.
This makes verifier re-check rows before marking them as problematic.

### Limitations
* MySQL enums and set types are not supported.
* Supports only comparing one MySQL database vs a whole CRDB schema (which is assumed to be "public").
* Geospatial types cannot yet be compared.
* We do not handle schema changes between commands well.

## Data move

`molt datamove` is able to migrate your PG tables to CockroachDB.

It currently supports the following:
* Pulling a table, uploading CSVs to S3 and running IMPORT on Cockroach for you.
* Pulling a table, uploading CSVs to S3 and running COPY TO on Cockroach from that CSV.
* Pulling a table and running COPY TO directly onto the CRDB table without an intermediate store.

This works for both MySQL and PostgreSQL URLs.

For now, schemas must be identical on both sides. 

```sh
# For S3
export AWS_REGION='us-east-1'
export AWS_SECRET_ACCESS_KEY='key'
export AWS_ACCESS_KEY_ID='id'
go run . datamove \
  --source 'postgres://postgres@pgurl:5432/replicationload' \
  --target 'postgres://root@35.229.89.45:26257/defaultdb?sslmode=disable' \
  --table 'good_table' \
  --s3-bucket 'otan-test-bucket'
```
```sh
# For gcp
# Ensure credentials are loaded using `gcloud init`
go run . datamove \
  --source 'postgres://postgres@pgurl:5432/replicationload' \
  --target 'postgres://root@35.229.89.45:26257/defaultdb?sslmode=disable' \
  --table 'good_table' \
  --gcp-bucket 'otan-test-bucket'
```
```sh
# For COPY
go run . datamove \
  --source 'postgres://postgres@pgurl:5432/replicationload' \
  --target 'postgres://root@35.229.89.45:26257/defaultdb?sslmode=disable' \
  --table 'good_table' \
  --direct-copy
```
```sh
# Look at files locally
go run . datamove \
  --source 'postgres://postgres@pgurl:5432/replicationload' \
  --target 'postgres://root@35.229.89.45:26257/defaultdb?sslmode=disable' \
  --table 'good_table' \
  --local-path /tmp/basic
```

You can use `--live` if you need target data to be queriable during
loading.

## Local Setup

### Running Tests
* Ensure a local postgres instance is setup and can be logged in using
  `postgres://postgres:postgres@localhost:5432/testdb` (this can be overriden with the
  `POSTGRES_URL` env var):
```sql
CREATE USER 'postgres' PASSWORD 'postgres' ADMIN;
CREATE DATABASE testdb;
```
* Ensure a local, insecure CockroachDB instance is setup
  (this can be overriden with the `COCKROACH_URL` env var):
  `cockroach demo --insecure --empty`.
* Ensure a local MySQL is setup with username `root` and an empty password,
  with a `defaultdb` database setup 
  (this can be overriden with the `MYSQL_URL` env var):
```sql
CREATE DATABASE defaultdb;
```
* Run the tests: `go test ./...`.
  * Data-driven tests can be run with `-rewrite`, e.g. `go test ./verification -rewrite`.
