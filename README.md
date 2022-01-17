# persistent-clickhouse

ClickHouse backend for Persistent.
Implemented features are: 
 - DML
 - Partially DDL - only insert subset of PersistStoreWrite. ClickHouse DELETE\SET\UPDATE statements are different from ANSI SQL ones, and should be used with caution. Please use rawSql if you feel like you need them.
 - Migrations

Note - ClickHouse internals are different from regular RDBMS id column autogeneration is disabled and you must provide primary key manually.