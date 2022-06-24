# zio-postgres 

A Scala/ZIO native implementation of PostgreSQL protocol.

[![Stand With Ukraine](https://raw.githubusercontent.com/vshymanskyy/StandWithUkraine/main/banner-direct-single.svg)](https://stand-with-ukraine.pp.ua)

## Motivation

The implementation is intended to be an async ZIO based implementation of postgres client.
The main intention - is to use it as a logical streaming client for CDC purposes.
So that it serves as a "subscription" side of the logical replication.

The library is not intended (at least now) to be used for running arbitrary sql queries, there is no sql dsl, only simple queries mode is supported. But running queries is still supported just to the extent that is needed to run the streaming replication.

The library as at early stage of development, so some bits and pieces can be missing yet. Maybe even essential ones ;).

## Usage

To stream the changes from the database table:

1. Create a publication in the postgres database (e.g. `create publication testpub for table test`)
2. Create a logical replication slot (e.g. `pg_create_logical_replication_slot('testsub', 'pgoutput')`)
3. Get the ZStream of changes in the scala application via `Protocol#simpleQuery`.
4. "Commit" the wal offset to the database while consuming the messages via `Protocol#standbyStatusUpdate`. If `Wal.Message.PrimaryKeepAlive` would not be replied by Standby Status Update messages - the server would terminate the connection.

A complete example can be found in [Main.scala](src/main/scala/zio/postgres/example/Main.scala).

A prerequisite to run the example Main file above are:
- `test` database
- owned by user `test` with password `test`
- `sbt "runMain zio.postgres.example.Main --init"` - this creates a table, publication and replication slot

After this you can run `sbt "runMain zio.postgres.example.Main"` - to see that the in-application state (a Map) is updated from the `test` table data changes, that are performed in parallel.

## Related PostgreSQL documentation

- https://www.postgresql.org/docs/current/protocol.html
- https://www.postgresql.org/docs/current/protocol-message-formats.html
- https://www.postgresql.org/docs/current/logical-replication.html
- https://www.postgresql.org/docs/14/protocol-replication.html
- https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html

## Supported PostgreSQL auth methods

- trust
- plain
- md5 (via sasl)
