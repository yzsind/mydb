# SHOW Result Columns

Column order and wire types are part of the client interface. The tables list metadata types; a cell can be SQL NULL where its owning page describes an unavailable value. Types shown as VARCHAR are string protocol metadata, not maximum SQL storage widths. A zero-row result keeps the same columns. Dynamic `<db>` labels use the selected database.

## SHOW COLUMNS

| Column | Wire type |
| --- | --- |
| `Field` | VARCHAR |
| `Type` | VARCHAR |
| `Null` | VARCHAR |
| `Key` | VARCHAR |
| `Default` | VARCHAR |
| `Extra` | VARCHAR |

## SHOW FULL COLUMNS

| Column | Wire type |
| --- | --- |
| `Field` | VARCHAR |
| `Type` | VARCHAR |
| `Collation` | VARCHAR |
| `Null` | VARCHAR |
| `Key` | VARCHAR |
| `Default` | VARCHAR |
| `Extra` | VARCHAR |
| `Privileges` | VARCHAR |
| `Comment` | VARCHAR |

## SHOW TABLES

| Column | Wire type |
| --- | --- |
| `Tables_in_<db>` | VARCHAR |

## SHOW FULL TABLES

| Column | Wire type |
| --- | --- |
| `Tables_in_<db>` | VARCHAR |
| `Table_type` | VARCHAR |
| `TABLE_ROWS` | BIGINT |
| `DATA_LENGTH` | BIGINT |
| `INDEX_LENGTH` | BIGINT |
| `CREATE_TIME` | DATETIME |
| `TABLE_COMMENT` | VARCHAR |

## SHOW DATABASES

| Column | Wire type |
| --- | --- |
| `Database` | VARCHAR |

## SHOW SEQUENCES

| Column | Wire type |
| --- | --- |
| `Sequences_in_<db>` | VARCHAR |

## SHOW VARIABLES

| Column | Wire type |
| --- | --- |
| `Variable_name` | VARCHAR |
| `Value` | VARCHAR |

## SHOW STATUS

| Column | Wire type |
| --- | --- |
| `Variable_name` | VARCHAR |
| `Value` | VARCHAR |

## SHOW WARNINGS AND ERRORS

| Column | Wire type |
| --- | --- |
| `Level` | VARCHAR |
| `Code` | INT |
| `Message` | VARCHAR |

## SHOW ENGINE

| Column | Wire type |
| --- | --- |
| `Type` | VARCHAR |
| `Name` | VARCHAR |
| `Status` | VARCHAR |

## SHOW TABLE STATUS

| Column | Wire type |
| --- | --- |
| `Name` | VARCHAR |
| `Engine` | VARCHAR |
| `Version` | BIGINT |
| `Row_format` | VARCHAR |
| `Rows` | BIGINT |
| `Avg_row_length` | BIGINT |
| `Data_length` | BIGINT |
| `Max_data_length` | BIGINT |
| `Index_length` | BIGINT |
| `Data_free` | BIGINT |
| `Auto_increment` | BIGINT |
| `Create_time` | DATETIME |
| `Update_time` | DATETIME |
| `Check_time` | DATETIME |
| `Collation` | VARCHAR |
| `Checksum` | BIGINT |
| `Create_options` | VARCHAR |
| `Comment` | VARCHAR |

## SHOW INDEX

| Column | Wire type |
| --- | --- |
| `Table` | VARCHAR |
| `Non_unique` | INT |
| `Key_name` | VARCHAR |
| `Seq_in_index` | INT |
| `Column_name` | VARCHAR |
| `Collation` | VARCHAR |
| `Cardinality` | BIGINT |
| `Sub_part` | BIGINT |
| `Packed` | VARCHAR |
| `Null` | VARCHAR |
| `Index_type` | VARCHAR |
| `Comment` | VARCHAR |
| `Index_comment` | VARCHAR |
| `Visible` | VARCHAR |
| `Expression` | VARCHAR |

## SHOW CREATE DATABASE

| Column | Wire type |
| --- | --- |
| `Database` | VARCHAR |
| `Create Database` | VARCHAR |

## SHOW CREATE TABLE

| Column | Wire type |
| --- | --- |
| `Table` | VARCHAR |
| `Create Table` | VARCHAR |

## SHOW CREATE VIEW

| Column | Wire type |
| --- | --- |
| `View` | VARCHAR |
| `Create View` | VARCHAR |
| `character_set_client` | VARCHAR |
| `collation_connection` | VARCHAR |

## SHOW CREATE SEQUENCE

| Column | Wire type |
| --- | --- |
| `Sequence` | VARCHAR |
| `Create Sequence` | VARCHAR |

## SHOW CREATE PROCEDURE

| Column | Wire type |
| --- | --- |
| `Procedure` | VARCHAR |
| `sql_mode` | VARCHAR |
| `Create Procedure` | VARCHAR |
| `character_set_client` | VARCHAR |
| `collation_connection` | VARCHAR |
| `Database Collation` | VARCHAR |

## SHOW CREATE FUNCTION

| Column | Wire type |
| --- | --- |
| `Function` | VARCHAR |
| `sql_mode` | VARCHAR |
| `Create Function` | VARCHAR |
| `character_set_client` | VARCHAR |
| `collation_connection` | VARCHAR |
| `Database Collation` | VARCHAR |

## SHOW ROUTINE STATUS

| Column | Wire type |
| --- | --- |
| `Db` | VARCHAR |
| `Name` | VARCHAR |
| `Type` | VARCHAR |
| `Definer` | VARCHAR |
| `Modified` | DATETIME |
| `Created` | DATETIME |
| `Security_type` | VARCHAR |
| `Comment` | VARCHAR |
| `character_set_client` | VARCHAR |
| `collation_connection` | VARCHAR |
| `Database Collation` | VARCHAR |

## SHOW CREATE TRIGGER

| Column | Wire type |
| --- | --- |
| `Trigger` | VARCHAR |
| `sql_mode` | VARCHAR |
| `SQL Original Statement` | VARCHAR |
| `character_set_client` | VARCHAR |
| `collation_connection` | VARCHAR |
| `Database Collation` | VARCHAR |
| `Created` | DATETIME |

## SHOW TRIGGERS

| Column | Wire type |
| --- | --- |
| `Trigger` | VARCHAR |
| `Event` | VARCHAR |
| `Table` | VARCHAR |
| `Statement` | VARCHAR |
| `Timing` | VARCHAR |
| `Created` | DATETIME |
| `sql_mode` | VARCHAR |
| `Definer` | VARCHAR |
| `character_set_client` | VARCHAR |
| `collation_connection` | VARCHAR |
| `Database Collation` | VARCHAR |

## SHOW CREATE EVENT

| Column | Wire type |
| --- | --- |
| `Event` | VARCHAR |
| `sql_mode` | VARCHAR |
| `time zone` | VARCHAR |
| `Create Event` | VARCHAR |
| `character_set_client` | VARCHAR |
| `collation_connection` | VARCHAR |
| `Database Collation` | VARCHAR |

## SHOW EVENTS

| Column | Wire type |
| --- | --- |
| `Db` | VARCHAR |
| `Name` | VARCHAR |
| `Definer` | VARCHAR |
| `Time zone` | VARCHAR |
| `Type` | VARCHAR |
| `Execute at` | VARCHAR |
| `Interval value` | VARCHAR |
| `Interval field` | VARCHAR |
| `Starts` | VARCHAR |
| `Ends` | VARCHAR |
| `Status` | VARCHAR |
| `Originator` | BIGINT |
| `character_set_client` | VARCHAR |
| `collation_connection` | VARCHAR |
| `Database Collation` | VARCHAR |

## SHOW CHARACTER SET

| Column | Wire type |
| --- | --- |
| `Charset` | VARCHAR |
| `Description` | VARCHAR |
| `Default collation` | VARCHAR |
| `Maxlen` | INT |

## SHOW COLLATION

| Column | Wire type |
| --- | --- |
| `Collation` | VARCHAR |
| `Charset` | VARCHAR |
| `Id` | INT |
| `Default` | VARCHAR |
| `Compiled` | VARCHAR |
| `Sortlen` | INT |
| `Pad_attribute` | VARCHAR |

## SHOW ENGINES

| Column | Wire type |
| --- | --- |
| `Engine` | VARCHAR |
| `Support` | VARCHAR |
| `Comment` | VARCHAR |
| `Transactions` | VARCHAR |
| `XA` | VARCHAR |
| `Savepoints` | VARCHAR |

## SHOW PLUGINS

| Column | Wire type |
| --- | --- |
| `Name` | VARCHAR |
| `Status` | VARCHAR |
| `Type` | VARCHAR |
| `Library` | VARCHAR |
| `License` | VARCHAR |

## SHOW OPEN TABLES

| Column | Wire type |
| --- | --- |
| `Database` | VARCHAR |
| `Table` | VARCHAR |
| `In_use` | BIGINT |
| `Name_locked` | BIGINT |

## SHOW PROCESSLIST

| Column | Wire type |
| --- | --- |
| `Id` | BIGINT |
| `User` | VARCHAR |
| `Host` | VARCHAR |
| `db` | VARCHAR |
| `Command` | VARCHAR |
| `Time` | INT |
| `State` | VARCHAR |
| `Info` | VARCHAR |

## SHOW MYDB MEMORY SESSIONS

| Column | Wire type |
| --- | --- |
| `CONNECTION_ID` | BIGINT |
| `USER` | VARCHAR |
| `HOST` | VARCHAR |
| `DB` | VARCHAR |
| `COMMAND` | VARCHAR |
| `QUERY_ID` | BIGINT |
| `EXEC_CURRENT_BYTES` | BIGINT |
| `QUERY_PEAK_BYTES` | BIGINT |
| `SESSION_PEAK_BYTES` | BIGINT |
| `QUERY_LIMIT_BYTES` | BIGINT |
| `SPILL_BYTES` | BIGINT |
| `RETAINED_CACHE_BYTES` | BIGINT |
| `TOTAL_CURRENT_BYTES` | BIGINT |
| `STATE` | VARCHAR |

## Units and Interpretation

Rows, cardinalities and cache entries are counts; DATA_LENGTH/INDEX_LENGTH and *_BYTES are bytes. PROCESSLIST Time is seconds. Estimates are not guaranteed to match a concurrent COUNT(*). Stored definition timestamps and CREATE output can carry captured SQL mode/character-set metadata; SHOW does not re-evaluate the body. Engine STATUS values are string diagnostics and each metric carries its own units in its name.

SHOW COUNT(*) WARNINGS/ERRORS returns one numeric column named `@@session.warning_count` or `@@session.error_count`, respectively. It counts conditions, not only rows retained under max_error_count. USE returns OK rather than a result set. The global memory summary has its own [accounting reference](show-mydb-memory.md).
