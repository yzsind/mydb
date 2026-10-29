use crate::parser::{Expr, RoutineKind, ShowEngineKind, ShowFilter};
use crate::storage::sequence::{SeqType, SequenceSchema};
use crate::storage::{
    value_to_string, DataTypeKind, Database, EventScheduleSchema, EventSchema, EventStatusSchema,
    IndexColumnDirection, RoutineKindSchema, RoutineSchema, TableSchema, TriggerSchema, ViewSchema,
};
use crate::types::{Column, ColumnType, QueryError, Value};

use super::api::{QueryResponse, ResultSet, SessionContext};
use super::auth::{
    scope_name, AccountKey, Privilege, PrivilegeScope, RequiredPrivilege, SslRequirement,
};
use super::executor::qualification::qualify_table_name;
use super::session_registry::SessionRegistry;
/// Render a raw string as a SQL string literal safe to embed in DDL
/// (SHOW CREATE TABLE, information_schema.COLUMN_TYPE …). MySQL 8.0's
/// mysqldump escapes `'` → `''` and passes backslash through, but a
/// backslash in a member name would be re-consumed as an escape char
/// on parse, so we double both. Keep other control characters literal:
/// MySQL does not recognize the C-style \xNN escape.
pub(crate) fn escape_sql_string_literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for ch in s.chars() {
        match ch {
            '\'' => out.push_str("''"),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\0' => out.push_str("\\0"),
            c => out.push(c),
        }
    }
    out.push('\'');
    out
}

pub(crate) fn describe_table(
    db: &Database,
    session: &SessionContext,
    table_name: &str,
) -> Result<QueryResponse, QueryError> {
    show_columns(db, session, table_name, false, None)
}

pub(crate) fn apply_show_filter(
    rows: &mut Vec<Vec<Value>>,
    columns: &[Column],
    filter: Option<&ShowFilter>,
    like_column: &str,
) -> Result<(), QueryError> {
    match filter {
        None => Ok(()),
        Some(ShowFilter::Like(pattern)) => {
            if let Some(idx) = columns
                .iter()
                .position(|column| column.name.eq_ignore_ascii_case(like_column))
            {
                rows.retain(|row| {
                    row.get(idx)
                        .map(|value| {
                            show_name_matches_pattern(
                                &value.to_display_string(),
                                Some(pattern.as_str()),
                            )
                        })
                        .unwrap_or(false)
                });
            }
            Ok(())
        }
        Some(ShowFilter::Where(expr)) => {
            let mut filtered = Vec::with_capacity(rows.len());
            for row in rows.drain(..) {
                if show_where_matches(expr, columns, &row)? {
                    filtered.push(row);
                }
            }
            *rows = filtered;
            Ok(())
        }
    }
}

pub(crate) fn describe_show_columns(
    db: &Database,
    session: &SessionContext,
    table_name: &str,
    full: bool,
) -> Result<Vec<Column>, QueryError> {
    let qualified = qualify_table_name(session, table_name);
    if let Some(err) = crate::engine::information_schema::hidden_system_table_error(&qualified) {
        return Err(err);
    }
    if crate::engine::information_schema::find_system_table_name(&qualified).is_some() {
        return Ok(show_columns_columns(full));
    }
    if db.table(&qualified).is_ok() {
        return Ok(show_columns_columns(full));
    }
    if db
        .view_definition(&session.current_database, table_name)
        .is_some()
    {
        return Ok(show_columns_columns(full));
    }
    db.table(&qualified)?;
    Ok(show_columns_columns(full))
}

pub(crate) fn show_columns(
    db: &Database,
    session: &SessionContext,
    table_name: &str,
    full: bool,
    filter: Option<&ShowFilter>,
) -> Result<QueryResponse, QueryError> {
    let qualified = qualify_table_name(session, table_name);
    if let Some(err) = crate::engine::information_schema::hidden_system_table_error(&qualified) {
        return Err(err);
    }
    if let Some(def) = crate::engine::information_schema::find_system_table_name(&qualified) {
        let mut rows = def
            .columns
            .iter()
            .filter(|column| {
                !column_privileges_for_show(db, session, def.schema, def.name, column.name)
                    .is_empty()
            })
            .map(|column| {
                let mysql80 = crate::engine::information_schema::generated_system_column_metadata(
                    def,
                    column.name,
                );
                let default_value = match mysql80.and_then(|metadata| metadata.describe_default) {
                    Some(value) => Value::String(value.to_string()),
                    None => Value::Null,
                };
                let mut row = vec![
                    Value::String(column.name.to_string()),
                    Value::String(column.column_type_sql.to_string()),
                    Value::String(if column.nullable { "YES" } else { "NO" }.to_string()),
                    mysql80
                        .map(|metadata| Value::String(metadata.column_key.to_string()))
                        .unwrap_or_else(|| {
                            Value::String(
                                crate::engine::mysql_schema::column_key(def, column.name)
                                    .to_string(),
                            )
                        }),
                    default_value,
                    Value::String(
                        mysql80
                            .map(|metadata| metadata.extra)
                            .unwrap_or("")
                            .to_string(),
                    ),
                ];
                if full {
                    let collation = mysql80
                        .and_then(|metadata| metadata.collation)
                        .map(|collation| Value::String(collation.to_string()))
                        .unwrap_or_else(|| {
                            if column.char_len.is_some() || column.data_type == "text" {
                                Value::String(crate::charset::DEFAULT_COLLATION.to_string())
                            } else {
                                Value::Null
                            }
                        });
                    row.insert(2, collation);
                    row.push(Value::String(column_privileges_for_show(
                        db,
                        session,
                        def.schema,
                        def.name,
                        column.name,
                    )));
                    row.push(Value::String(
                        mysql80
                            .map(|metadata| metadata.comment)
                            .unwrap_or("")
                            .to_string(),
                    ));
                }
                row
            })
            .collect();
        let columns = show_columns_columns(full);
        apply_show_filter(&mut rows, &columns, filter, "Field")?;
        return Ok(QueryResponse::ResultSet(ResultSet { columns, rows }));
    }
    if let Ok(table) = db.table(&qualified) {
        // SHOW COLUMNS / SHOW FULL COLUMNS must hide tombstones left
        // behind by INSTANT DROP COLUMN — those columns are gone as
        // far as SQL is concerned.
        let mut rows = table
            .iter_visible_columns()
            .filter(|(_, column)| {
                !column_privileges_for_show(db, session, &table.database, &table.name, &column.name)
                    .is_empty()
            })
            .map(|(_, column)| {
                let key = crate::engine::information_schema::column_key(&table, &column.name);
                let mut row = vec![
                    Value::String(column.name.clone()),
                    Value::String(mysql_type_name_with_attrs(
                        &column.data_type,
                        column.unsigned,
                        column.zerofill,
                    )),
                    Value::String(if column.nullable { "YES" } else { "NO" }.to_string()),
                    Value::String(key.to_string()),
                    column_default_value(column),
                    Value::String(column_extra(column)),
                ];
                if full {
                    row.insert(2, column_collation_for_show(column));
                    row.push(Value::String(column_privileges_for_show(
                        db,
                        session,
                        &table.database,
                        &table.name,
                        &column.name,
                    )));
                    row.push(Value::String(column.comment.clone()));
                }
                row
            })
            .collect();
        let columns = show_columns_columns(full);
        apply_show_filter(&mut rows, &columns, filter, "Field")?;
        return Ok(QueryResponse::ResultSet(ResultSet { columns, rows }));
    }
    if let Some(view) = db.view_definition(&session.current_database, table_name) {
        let mut rows = view
            .columns
            .iter()
            .filter(|column| {
                !column_privileges_for_show(db, session, &view.database, &view.name, &column.name)
                    .is_empty()
            })
            .map(|column| {
                let type_str = column
                    .data_type
                    .as_ref()
                    .map(mysql_type_name)
                    .unwrap_or_else(|| "varchar(255)".to_string());
                let mut row = vec![
                    Value::String(column.name.clone()),
                    Value::String(type_str),
                    Value::String(if column.nullable { "YES" } else { "NO" }.to_string()),
                    Value::String(String::new()),
                    Value::Null,
                    Value::String(String::new()),
                ];
                if full {
                    let collation = column
                        .data_type
                        .as_ref()
                        .map(|data_type| match data_type {
                            DataTypeKind::Char { .. }
                            | DataTypeKind::VarChar { .. }
                            | DataTypeKind::Text { .. }
                            | DataTypeKind::Enum { .. }
                            | DataTypeKind::Set { .. } => {
                                Value::String(column.collation.clone().unwrap_or_else(|| {
                                    column
                                        .charset
                                        .as_deref()
                                        .and_then(|charset| {
                                            crate::charset::default_collation_for_charset(charset)
                                                .ok()
                                        })
                                        .unwrap_or(crate::charset::DEFAULT_COLLATION)
                                        .to_string()
                                }))
                            }
                            _ => Value::Null,
                        })
                        .unwrap_or(Value::Null);
                    row.insert(2, collation);
                    row.push(Value::String(column_privileges_for_show(
                        db,
                        session,
                        &view.database,
                        &view.name,
                        &column.name,
                    )));
                    row.push(Value::String(String::new()));
                }
                row
            })
            .collect();
        let columns = show_columns_columns(full);
        apply_show_filter(&mut rows, &columns, filter, "Field")?;
        return Ok(QueryResponse::ResultSet(ResultSet { columns, rows }));
    }
    // Falls through to surface the standard 1146 error from `db.table`.
    db.table(&qualified)?;
    unreachable!()
}

pub(crate) fn show_columns_columns(full: bool) -> Vec<Column> {
    let mut columns = vec![
        col("Field", ColumnType::VarString),
        col("Type", ColumnType::VarString),
        col("Null", ColumnType::VarString),
        col("Key", ColumnType::VarString),
        col("Default", ColumnType::VarString),
        col("Extra", ColumnType::VarString),
    ];
    if full {
        columns.insert(2, col("Collation", ColumnType::VarString));
        columns.push(col("Privileges", ColumnType::VarString));
        columns.push(col("Comment", ColumnType::VarString));
    }
    columns
}

fn column_privileges_for_show(
    db: &Database,
    session: &SessionContext,
    database: &str,
    table: &str,
    column: &str,
) -> String {
    if session.internal_system_session {
        return "select,insert,update,references".into();
    }
    let auth = db.auth_catalog();
    let ctx = session.authorization_context();
    let privileges = [
        (Privilege::Select, "select"),
        (Privilege::Insert, "insert"),
        (Privilege::Update, "update"),
        (Privilege::References, "references"),
    ]
    .into_iter()
    .filter_map(|(privilege, label)| {
        let required = RequiredPrivilege {
            privilege,
            scope: PrivilegeScope::Column {
                database: database.to_string(),
                table: table.to_string(),
                column: column.to_string(),
            },
        };
        auth.account_has_privilege(&ctx, &required, false)
            .then_some(label)
    })
    .collect::<Vec<_>>()
    .join(",");
    if privileges.is_empty()
        && crate::engine::information_schema::implicitly_readable_metadata_table(database, table)
    {
        "select".into()
    } else {
        privileges
    }
}

fn column_collation_for_show(column: &crate::storage::ColumnSchema) -> Value {
    match column.data_type {
        DataTypeKind::Char { .. }
        | DataTypeKind::VarChar { .. }
        | DataTypeKind::Text { .. }
        | DataTypeKind::Enum { .. }
        | DataTypeKind::Set { .. } => column
            .collation
            .clone()
            .map(Value::String)
            .unwrap_or_else(|| Value::String(crate::charset::DEFAULT_COLLATION.to_string())),
        _ => Value::Null,
    }
}

pub(crate) fn show_databases(
    db: &Database,
    session: &SessionContext,
    filter: Option<&ShowFilter>,
) -> Result<QueryResponse, QueryError> {
    let columns = vec![col("Database", ColumnType::VarString)];
    let mut rows = db
        .catalog()
        .databases
        .keys()
        // MySQL only lists schemas the account holds some privilege on.
        .filter(|name| {
            crate::engine::information_schema::object_visible_to_session(db, session, name, None)
        })
        .map(|name| vec![Value::String(name.clone())])
        .collect();
    apply_show_filter(&mut rows, &columns, filter, "Database")?;
    Ok(QueryResponse::ResultSet(ResultSet { columns, rows }))
}

pub(crate) fn show_tables(
    db: &Database,
    session: &SessionContext,
    database: &str,
    full: bool,
    filter: Option<&ShowFilter>,
) -> Result<QueryResponse, QueryError> {
    let columns = show_tables_columns(database, full);
    if let Some(rows) = crate::engine::information_schema::show_tables_rows(db, database, full) {
        let mut rows = rows;
        rows.retain(|row| {
            row.first().is_some_and(|name| {
                crate::engine::information_schema::object_visible_to_session(
                    db,
                    session,
                    database,
                    Some(&name.to_display_string()),
                )
            })
        });
        apply_show_filter(
            &mut rows,
            &columns,
            filter,
            &format!("Tables_in_{database}"),
        )?;
        return Ok(QueryResponse::ResultSet(ResultSet { columns, rows }));
    }
    // Tables and views appear in a single sorted listing — MySQL clients
    // and tooling (`mysqldump`, `mycli`) expect a unified result. The
    // extended metadata on FULL reuses the same statistics view as SHOW
    // TABLE STATUS and information_schema.TABLES so all three surfaces agree.
    let mut rows = Vec::new();
    let catalog = db.catalog();
    // MySQL lists only the tables/views the account has a privilege on.
    let visible = |name: &str| {
        crate::engine::information_schema::object_visible_to_session(
            db,
            session,
            database,
            Some(name),
        )
    };
    for table in catalog.tables.values() {
        if !table.database.eq_ignore_ascii_case(database) || !visible(&table.name) {
            continue;
        }
        let mut row = vec![Value::String(table.name.clone())];
        if full {
            let stats = crate::engine::table_stats_view::TableStatsView::for_table(db, table);
            row.extend([
                Value::String("BASE TABLE".to_string()),
                Value::Int(saturating_i64(stats.rows)),
                Value::Int(saturating_i64(stats.data_length)),
                Value::Int(saturating_i64(stats.index_length)),
                mysql_time_value(table.created_ms),
                Value::String(table.comment.clone()),
            ]);
        }
        rows.push(row);
    }
    for view in catalog.views.values() {
        if !view.database.eq_ignore_ascii_case(database) || !visible(&view.name) {
            continue;
        }
        let mut row = vec![Value::String(view.name.clone())];
        if full {
            row.extend([
                Value::String("VIEW".to_string()),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::String("VIEW".to_string()),
            ]);
        }
        rows.push(row);
    }
    rows.sort_by(|a, b| a[0].to_display_string().cmp(&b[0].to_display_string()));
    apply_show_filter(
        &mut rows,
        &columns,
        filter,
        &format!("Tables_in_{database}"),
    )?;
    Ok(QueryResponse::ResultSet(ResultSet { columns, rows }))
}

// The extra metadata columns of SHOW FULL TABLES are an intentional mydb enhancement:
// after MySQL's Tables_in_<db> and Table_type columns we append TABLE_ROWS, DATA_LENGTH,
// INDEX_LENGTH, CREATE_TIME and TABLE_COMMENT. The design covers user and system
// databases alike and must not be dropped as a MySQL compatibility gap; plain SHOW TABLES still returns only the table-name column.
pub(crate) fn show_tables_columns(database: &str, full: bool) -> Vec<Column> {
    let mut columns = vec![Column {
        source: Default::default(),
        name: format!("Tables_in_{database}"),
        column_type: ColumnType::VarString,
    }];
    if full {
        columns.push(col("Table_type", ColumnType::VarString));
        columns.extend([
            col("TABLE_ROWS", ColumnType::LongLong),
            col("DATA_LENGTH", ColumnType::LongLong),
            col("INDEX_LENGTH", ColumnType::LongLong),
            col("CREATE_TIME", ColumnType::DateTime),
            col("TABLE_COMMENT", ColumnType::VarString),
        ]);
    }
    columns
}

pub(crate) fn show_table_status(
    db: &Database,
    session: &SessionContext,
    database: Option<&str>,
    filter: Option<&ShowFilter>,
) -> Result<QueryResponse, QueryError> {
    let database = database.unwrap_or(&session.current_database);
    let columns = show_table_status_columns();
    let mut rows = Vec::new();

    if is_system_schema(database) {
        for def in crate::engine::information_schema::system_table_definitions()
            .filter(|def| def.schema.eq_ignore_ascii_case(database))
            .filter(|def| !def.hidden)
            .filter(|def| {
                crate::engine::information_schema::object_visible_to_session(
                    db,
                    session,
                    def.schema,
                    Some(def.name),
                )
            })
        {
            rows.push(system_table_status_row(def));
        }
    } else {
        let catalog = db.catalog();
        // MySQL lists only the tables/views the account has a privilege on.
        let visible = |name: &str| {
            crate::engine::information_schema::object_visible_to_session(
                db,
                session,
                database,
                Some(name),
            )
        };
        for table in catalog
            .tables
            .values()
            .filter(|table| table.database.eq_ignore_ascii_case(database) && visible(&table.name))
        {
            rows.push(base_table_status_row(db, table));
        }
        for view in catalog
            .views
            .values()
            .filter(|view| view.database.eq_ignore_ascii_case(database) && visible(&view.name))
        {
            rows.push(view_table_status_row(&view.name));
        }
    }

    rows.sort_by(|a, b| a[0].to_display_string().cmp(&b[0].to_display_string()));
    apply_show_filter(&mut rows, &columns, filter, "Name")?;
    Ok(QueryResponse::ResultSet(ResultSet { columns, rows }))
}

pub(crate) fn show_table_status_columns() -> Vec<Column> {
    vec![
        col("Name", ColumnType::VarString),
        col("Engine", ColumnType::VarString),
        col("Version", ColumnType::LongLong),
        col("Row_format", ColumnType::VarString),
        col("Rows", ColumnType::LongLong),
        col("Avg_row_length", ColumnType::LongLong),
        col("Data_length", ColumnType::LongLong),
        col("Max_data_length", ColumnType::LongLong),
        col("Index_length", ColumnType::LongLong),
        col("Data_free", ColumnType::LongLong),
        col("Auto_increment", ColumnType::LongLong),
        col("Create_time", ColumnType::DateTime),
        col("Update_time", ColumnType::DateTime),
        col("Check_time", ColumnType::DateTime),
        col("Collation", ColumnType::VarString),
        col("Checksum", ColumnType::LongLong),
        col("Create_options", ColumnType::VarString),
        col("Comment", ColumnType::VarString),
    ]
}

fn is_system_schema(database: &str) -> bool {
    matches!(
        database.to_ascii_lowercase().as_str(),
        "information_schema" | "mysql" | "performance_schema" | "sys"
    )
}

fn show_name_matches_pattern(name: &str, pattern: Option<&str>) -> bool {
    // B13: SHOW … LIKE semantics go through the metadata collation as CI (aligned with MySQL 8.0).
    // The old implementation manually to_ascii_lowercase'd both sides then used the CS
    // `match_like_pattern` — exactly equivalent for ASCII table names but silently
    // drifting on CI semantics for multi-byte bytes (e.g. `Ⅷ` LIKE `ⅷ`). Unifying on
    // `match_like_pattern_with_info(utf8mb4_general_ci)` removes the CI fork, consistent with IS/`_metadata`.
    pattern
        .map(|pattern| {
            let info = crate::charset::resolve_collation("utf8mb4_general_ci")
                .expect("utf8mb4_general_ci is a builtin collation");
            crate::engine::expr::match_like_pattern_with_info(name, pattern, Some('\\'), info)
        })
        .unwrap_or(true)
}

fn base_table_status_row(db: &Database, table: &TableSchema) -> Vec<Value> {
    // SHOW TABLE STATUS and `information_schema.tables` go through the
    // same view so the two queries can never disagree about a table's
    // approximate size — see `engine::table_stats_view::TableStatsView`.
    let stats = crate::engine::table_stats_view::TableStatsView::for_table(db, table);
    let create_options = crate::engine::table_stats_view::table_create_options(table);
    let auto_increment = match stats.auto_increment {
        Some(value) => i64::try_from(value)
            .map(Value::Int)
            .unwrap_or(Value::UInt(value)),
        None => Value::Null,
    };
    vec![
        Value::String(table.name.clone()),
        Value::String(table.engine.clone()),
        Value::Int(10),
        Value::String(crate::engine::table_stats_view::table_row_format(table).to_string()),
        Value::Int(saturating_i64(stats.rows)),
        Value::Int(saturating_i64(stats.avg_row_length)),
        Value::Int(saturating_i64(stats.data_length)),
        Value::Int(saturating_i64(stats.max_data_length)),
        Value::Int(saturating_i64(stats.index_length)),
        Value::Int(saturating_i64(stats.data_free)),
        auto_increment,
        mysql_time_value(table.created_ms),
        mysql_time_value(table.updated_ms),
        Value::Null,
        Value::String(table.collation.clone()),
        Value::Null,
        Value::String(create_options),
        Value::String(table.comment.clone()),
    ]
}

fn view_table_status_row(name: &str) -> Vec<Value> {
    vec![
        Value::String(name.to_string()),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::String("VIEW".to_string()),
    ]
}

fn system_table_status_row(def: &crate::engine::information_schema::SystemTableDef) -> Vec<Value> {
    let is_view = def.table_type.to_ascii_uppercase().contains("VIEW");
    vec![
        Value::String(def.name.to_string()),
        if def.engine.is_empty() {
            Value::Null
        } else {
            Value::String(def.engine.to_string())
        },
        if is_view { Value::Null } else { Value::Int(10) },
        if is_view {
            Value::Null
        } else {
            Value::String("Dynamic".to_string())
        },
        Value::Int(0),
        Value::Int(0),
        Value::Int(0),
        Value::Int(0),
        Value::Int(0),
        Value::Int(0),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::String(crate::charset::DEFAULT_COLLATION.to_string()),
        Value::Null,
        Value::String(String::new()),
        Value::String(if is_view { "VIEW" } else { "" }.to_string()),
    ]
}

fn saturating_i64(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

pub(crate) fn show_index(
    db: &Database,
    session: &SessionContext,
    table_name: &str,
    filter: Option<&ShowFilter>,
) -> Result<QueryResponse, QueryError> {
    let table_name = qualify_table_name(session, table_name);
    if let Some(err) = crate::engine::information_schema::hidden_system_table_error(&table_name) {
        return Err(err);
    }
    if let Some(def) = crate::engine::information_schema::find_system_table_name(&table_name) {
        let columns = show_index_columns();
        let mut rows = system_show_index_rows(def);
        apply_show_filter(&mut rows, &columns, filter, "Key_name")?;
        return Ok(QueryResponse::ResultSet(ResultSet { columns, rows }));
    }
    let table = db.table(&table_name)?;
    let estimated_rows =
        crate::engine::table_stats_view::TableStatsView::for_table(db, &table).rows;
    let mut rows = Vec::new();
    // SHOW INDEX hides Building entries — they aren't queryable yet,
    // so surfacing them would mislead clients into believing the
    // index is available.
    for index in table.iter_public_indexes() {
        for (seq, column) in index.columns.iter().enumerate() {
            let key_part = index.key_part(seq);
            let is_expression =
                matches!(key_part, Some(crate::storage::IndexKeyPart::Expression(_)));
            let (column_value, expression_value, nullable) =
                if let Some(expr) = index.key_expression(seq) {
                    // MySQL convention for functional indexes: Column_name
                    // is NULL and Expression carries the SQL text. Functional
                    // expressions may produce NULL on any NULL input, so
                    // mirror that on the Null column.
                    (
                        Value::Null,
                        Value::String(expr.expr_sql.clone()),
                        expr.nullable,
                    )
                } else {
                    let nullable = table
                        .columns
                        .iter()
                        .find(|col| col.name.eq_ignore_ascii_case(column))
                        .is_some_and(|col| col.nullable);
                    (Value::String(column.clone()), Value::Null, nullable)
                };
            rows.push(vec![
                Value::String(table.name.clone()),
                Value::Int((!index.unique) as i64),
                Value::String(index.name.clone()),
                Value::Int((seq + 1) as i64),
                column_value,
                if index.is_fulltext() || index.index_type.eq_ignore_ascii_case("SPATIAL") {
                    Value::Null
                } else {
                    Value::String(
                        match index.column_direction(seq) {
                            IndexColumnDirection::Asc => "A",
                            IndexColumnDirection::Desc => "D",
                        }
                        .to_string(),
                    )
                },
                Value::Int(saturating_i64(
                    table
                        .index_prefix_cardinality(&index.name, seq + 1)
                        .filter(|count| *count > 0)
                        .unwrap_or_else(|| {
                            crate::engine::table_stats_view::fallback_index_cardinality(
                                estimated_rows,
                                index.unique,
                                seq + 1,
                                index.columns.len(),
                            )
                        }),
                )),
                if is_expression {
                    Value::Null
                } else {
                    index
                        .column_prefix_len(seq)
                        .map(|len| Value::Int(len as i64))
                        .unwrap_or(Value::Null)
                },
                Value::Null,
                Value::String(if nullable { "YES" } else { "" }.to_string()),
                Value::String(index.index_type.clone()),
                Value::String(String::new()),
                Value::String(index.comment.clone()),
                Value::String(if index.visible { "YES" } else { "NO" }.to_string()),
                expression_value,
            ]);
        }
    }
    let columns = show_index_columns();
    apply_show_filter(&mut rows, &columns, filter, "Key_name")?;
    Ok(QueryResponse::ResultSet(ResultSet { columns, rows }))
}

pub(crate) fn describe_show_index(
    db: &Database,
    session: &SessionContext,
    table_name: &str,
) -> Result<Vec<Column>, QueryError> {
    let table_name = qualify_table_name(session, table_name);
    if let Some(err) = crate::engine::information_schema::hidden_system_table_error(&table_name) {
        return Err(err);
    }
    if crate::engine::information_schema::find_system_table_name(&table_name).is_some() {
        return Ok(show_index_columns());
    }
    db.table(&table_name)?;
    Ok(show_index_columns())
}

fn system_show_index_rows(
    def: &crate::engine::information_schema::SystemTableDef,
) -> Vec<Vec<Value>> {
    let mut rows = Vec::new();
    for index in def.indexes {
        for (seq, column) in index.columns.iter().enumerate() {
            let nullable = def
                .columns
                .iter()
                .find(|col| col.name.eq_ignore_ascii_case(column))
                .is_some_and(|col| col.nullable);
            rows.push(vec![
                Value::String(def.name.to_string()),
                Value::Int((!index.unique) as i64),
                Value::String(index.name.to_string()),
                Value::Int((seq + 1) as i64),
                Value::String((*column).to_string()),
                Value::String("A".to_string()),
                Value::Int(0),
                Value::Null,
                Value::Null,
                Value::String(if nullable { "YES" } else { "" }.to_string()),
                Value::String(index.index_type.to_string()),
                Value::String(String::new()),
                Value::String(String::new()),
                Value::String(if index.visible { "YES" } else { "NO" }.to_string()),
                Value::Null,
            ]);
        }
    }
    rows
}

pub(crate) fn show_index_columns() -> Vec<Column> {
    vec![
        col("Table", ColumnType::VarString),
        col("Non_unique", ColumnType::Long),
        col("Key_name", ColumnType::VarString),
        col("Seq_in_index", ColumnType::Long),
        col("Column_name", ColumnType::VarString),
        col("Collation", ColumnType::VarString),
        col("Cardinality", ColumnType::LongLong),
        col("Sub_part", ColumnType::LongLong),
        col("Packed", ColumnType::VarString),
        col("Null", ColumnType::VarString),
        col("Index_type", ColumnType::VarString),
        col("Comment", ColumnType::VarString),
        col("Index_comment", ColumnType::VarString),
        col("Visible", ColumnType::VarString),
        col("Expression", ColumnType::VarString),
    ]
}

pub(crate) fn show_create_database(
    db: &Database,
    name: &str,
    if_not_exists: bool,
) -> Result<QueryResponse, QueryError> {
    let key = crate::storage::normalize_name(name);
    let catalog = db.catalog();
    let schema = catalog.databases.get(&key).ok_or_else(|| {
        crate::storage::error(
            crate::error::catalog::ER_BAD_DB_ERROR,
            format!("Unknown database '{name}'"),
        )
    })?;
    let mut create_sql = String::from("CREATE DATABASE ");
    if if_not_exists {
        create_sql.push_str("/*!32312 IF NOT EXISTS*/ ");
    }
    create_sql.push_str(&quoted_identifier(&schema.name));
    create_sql.push_str(" /*!40100 DEFAULT CHARACTER SET ");
    create_sql.push_str(&schema.charset);
    create_sql.push_str(" COLLATE ");
    create_sql.push_str(&schema.collation);
    create_sql.push_str(" */");
    Ok(QueryResponse::ResultSet(ResultSet {
        columns: vec![
            col("Database", ColumnType::VarString),
            col("Create Database", ColumnType::VarString),
        ],
        rows: vec![vec![
            Value::String(schema.name.clone()),
            Value::String(create_sql),
        ]],
    }))
}

fn quoted_identifier(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

pub(crate) fn show_create_table(
    db: &Database,
    session: &SessionContext,
    table_name: &str,
) -> Result<QueryResponse, QueryError> {
    // System tables (information_schema / performance_schema / mysql) have no
    // on-disk definition; synthesize CREATE TABLE from the registered column
    // metadata so clients like Workbench / DBeaver can introspect them and so
    // performance_schema tables report ENGINE=PERFORMANCE_SCHEMA.
    let (sys_schema, sys_table) =
        crate::storage::split_table_name(&session.current_database, table_name);
    if crate::engine::mysql_schema::is_hidden_dd_table(&sys_schema, &sys_table) {
        return Err(crate::engine::mysql_schema::inaccessible_table_error(
            &sys_table,
        ));
    }
    if let Some(def) = crate::engine::information_schema::find_system_table(&sys_schema, &sys_table)
    {
        if sys_schema.eq_ignore_ascii_case("sys") && def.table_type.eq_ignore_ascii_case("VIEW") {
            return Ok(QueryResponse::ResultSet(ResultSet {
                columns: show_create_view_columns(),
                rows: vec![vec![
                    Value::String(def.name.to_string()),
                    Value::String(crate::engine::sys_schema::create_view_sql(
                        def.name,
                        def.columns,
                    )),
                    Value::String(session.character_set_client.clone()),
                    Value::String(session.collation_connection.clone()),
                ]],
            }));
        }
        return Ok(QueryResponse::ResultSet(ResultSet {
            columns: show_create_table_columns(),
            rows: vec![vec![
                Value::String(def.name.to_string()),
                Value::String(
                    def.create_sql
                        .map(str::to_string)
                        .unwrap_or_else(|| synth_system_create_table_sql(def)),
                ),
            ]],
        }));
    }
    let is_temporary = session.resolve_temp_table_name(table_name).is_some();
    let qualified = qualify_table_name(session, table_name);
    if let Ok(table) = db.table(&qualified) {
        let mut create_sql = create_table_sql_with_auto_increment(
            &table,
            db.auto_increment_next_value(&table).ok().flatten(),
        )?;
        if is_temporary {
            create_sql = create_sql.replacen("CREATE TABLE", "CREATE TEMPORARY TABLE", 1);
        }
        return Ok(QueryResponse::ResultSet(ResultSet {
            columns: show_create_table_columns(),
            rows: vec![vec![
                Value::String(table.name.clone()),
                Value::String(create_sql),
            ]],
        }));
    }
    if let Some(view) = db.view_definition(&session.current_database, table_name) {
        let required = RequiredPrivilege {
            privilege: Privilege::ShowView,
            scope: PrivilegeScope::Table {
                database: view.database.clone(),
                table: view.name.clone(),
            },
        };
        if !session.internal_system_session
            && !db.auth_catalog().account_has_privilege(
                &session.authorization_context(),
                &required,
                false,
            )
        {
            return Err(crate::engine::auth::privilege_denied(
                &required.privilege,
                &required.scope,
            ));
        }
        return Ok(QueryResponse::ResultSet(ResultSet {
            columns: show_create_view_columns(),
            rows: vec![vec![
                Value::String(view.name.clone()),
                Value::String(create_view_sql(&view)),
                Value::String(view.character_set_client.clone()),
                Value::String(view.collation_connection.clone()),
            ]],
        }));
    }
    db.table(&qualified)?;
    unreachable!()
}

/// Build `CREATE TABLE` text for a system table (information_schema /
/// performance_schema / mysql) from its registered column metadata. These
/// tables have no on-disk schema; the `SystemColumn` list is the source of
/// truth. performance_schema tables report `ENGINE=PERFORMANCE_SCHEMA`.
fn synth_system_create_table_sql(
    def: &crate::engine::information_schema::SystemTableDef,
) -> String {
    use std::fmt::Write as _;
    let mut sql = format!("CREATE TABLE `{}` (\n", def.name);
    for (i, col) in def.columns.iter().enumerate() {
        let sep = if i + 1 < def.columns.len() || !def.indexes.is_empty() {
            ","
        } else {
            ""
        };
        let null = if col.nullable { "" } else { " NOT NULL" };
        let _ = writeln!(
            sql,
            "  `{}` {}{}{}",
            col.name, col.column_type_sql, null, sep
        );
    }
    for (i, index) in def.indexes.iter().enumerate() {
        let sep = if i + 1 < def.indexes.len() { "," } else { "" };
        let columns = index
            .columns
            .iter()
            .map(|column| format!("`{column}`"))
            .collect::<Vec<_>>()
            .join(",");
        if index.name == "PRIMARY" {
            let _ = writeln!(sql, "  PRIMARY KEY ({columns}){sep}");
        } else if index.unique {
            let _ = writeln!(sql, "  UNIQUE KEY `{}` ({columns}){sep}", index.name);
        } else {
            let _ = writeln!(sql, "  KEY `{}` ({columns}){sep}", index.name);
        }
    }
    sql.push(')');
    if !def.engine.is_empty() {
        let _ = write!(sql, " ENGINE={}", def.engine);
    }
    sql.push_str(" DEFAULT CHARSET=utf8mb4");
    sql
}

pub(crate) fn show_create_routine(
    db: &Database,
    session: &SessionContext,
    kind: RoutineKind,
    name: &str,
) -> Result<QueryResponse, QueryError> {
    let schema_kind = routine_kind_schema(kind);
    let routine = db
        .routine_definition(&session.current_database, schema_kind.clone(), name)
        .or_else(|| {
            crate::engine::sys_schema::routine_definition(
                &session.current_database,
                schema_kind,
                name,
            )
            .map(std::sync::Arc::new)
        })
        .ok_or_else(|| {
            crate::storage::error(
                crate::error::catalog::ER_SP_DOES_NOT_EXIST,
                format!("{} {} does not exist", kind.as_str(), name),
            )
        })?;
    let columns = if kind == RoutineKind::Procedure {
        show_create_procedure_columns()
    } else {
        show_create_function_columns()
    };
    if !crate::engine::information_schema::routine_visible_to_session(db, session, &routine) {
        return Err(crate::storage::error(
            crate::error::catalog::ER_SP_DOES_NOT_EXIST,
            format!("{} {} does not exist", kind.as_str(), name),
        ));
    }
    let create_sql = if crate::engine::information_schema::routine_definition_visible_to_session(
        db, session, &routine,
    ) {
        Value::String(create_routine_sql(&routine))
    } else {
        Value::Null
    };
    Ok(QueryResponse::ResultSet(ResultSet {
        columns,
        rows: vec![vec![
            Value::String(routine.name.clone()),
            Value::String(routine.sql_mode.clone()),
            create_sql,
            Value::String(routine.character_set_client.clone()),
            Value::String(routine.collation_connection.clone()),
            Value::String(routine.database_collation.clone()),
        ]],
    }))
}

pub(crate) fn show_routine_status(
    db: &Database,
    session: &SessionContext,
    kind: RoutineKind,
    filter: Option<&ShowFilter>,
) -> Result<QueryResponse, QueryError> {
    let schema_kind = routine_kind_schema(kind);
    let columns = show_routine_status_columns();
    let mut rows = db
        .routines_in_database(&session.current_database)
        .into_iter()
        .chain(if session.current_database.eq_ignore_ascii_case("sys") {
            crate::engine::sys_schema::routine_schemas()
        } else {
            Vec::new()
        })
        .filter(|routine| routine.kind == schema_kind)
        // B3: MySQL only lists routines the caller has some privilege on
        // (its schema, the routine, or globally). Reuse the IS visibility
        // predicate (system sessions / filter-off see all).
        .filter(|routine| {
            crate::engine::information_schema::routine_visible_to_session(db, session, routine)
        })
        .map(|routine| {
            let definer = routine.definer();
            vec![
                Value::String(routine.database),
                Value::String(routine.name),
                Value::String(routine.kind.as_str().to_string()),
                Value::String(definer),
                mysql_time_value(routine.updated_ms),
                mysql_time_value(routine.created_ms),
                Value::String(routine.security_type.as_str().to_string()),
                Value::String(routine.comment),
                Value::String(routine.character_set_client),
                Value::String(routine.collation_connection),
                Value::String(routine.database_collation),
            ]
        })
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| a[1].to_display_string().cmp(&b[1].to_display_string()));
    apply_show_filter(&mut rows, &columns, filter, "Name")?;
    Ok(QueryResponse::ResultSet(ResultSet { columns, rows }))
}

pub(crate) fn show_create_trigger(
    db: &Database,
    session: &SessionContext,
    name: &str,
) -> Result<QueryResponse, QueryError> {
    let (database, trigger_name) =
        crate::storage::split_table_name(&session.current_database, name);
    let trigger = db
        .trigger_definition(&database, &trigger_name)
        .or_else(|| {
            if database.eq_ignore_ascii_case("sys") {
                crate::engine::sys_schema::trigger_schemas()
                    .into_iter()
                    .find(|trigger| trigger.name.eq_ignore_ascii_case(&trigger_name))
            } else {
                None
            }
        })
        .ok_or_else(|| {
            crate::storage::error(
                crate::error::catalog::ER_TRG_DOES_NOT_EXIST,
                format!("Trigger '{name}' does not exist"),
            )
        })?;
    // TRIGGER-privilege gate for SHOW CREATE TRIGGER lives in the central
    // `authorize_statement` path (`trigger_name_for_table_privilege`).
    Ok(QueryResponse::ResultSet(ResultSet {
        columns: show_create_trigger_columns(),
        rows: vec![vec![
            Value::String(trigger.name.clone()),
            Value::String(trigger.sql_mode.clone()),
            Value::String(create_trigger_sql(&trigger)),
            Value::String(trigger.character_set_client.clone()),
            Value::String(trigger.collation_connection.clone()),
            Value::String(trigger.database_collation.clone()),
            mysql_time_value(trigger.created_ms),
        ]],
    }))
}

pub(crate) fn show_triggers(
    db: &Database,
    session: &SessionContext,
    database: Option<&str>,
    filter: Option<&ShowFilter>,
) -> Result<QueryResponse, QueryError> {
    let database = database.unwrap_or(&session.current_database);
    let columns = show_triggers_columns();
    let mut trigger_list = db.triggers_in_database(database);
    if database.eq_ignore_ascii_case("sys") {
        trigger_list.extend(crate::engine::sys_schema::trigger_schemas());
    }
    let mut rows = trigger_list
        .into_iter()
        // B3: only list triggers on tables the caller holds TRIGGER on.
        .filter(|trigger| {
            has_trigger_privilege(db, session, &trigger.table_database, &trigger.table_name)
        })
        .map(|trigger| {
            vec![
                Value::String(trigger.name.clone()),
                Value::String(trigger.event.as_str().to_string()),
                Value::String(trigger.table_name.clone()),
                Value::String(trigger.body_sql.clone()),
                Value::String(trigger.timing.as_str().to_string()),
                mysql_time_value(trigger.created_ms),
                Value::String(trigger.sql_mode.clone()),
                Value::String(trigger.definer()),
                Value::String(trigger.character_set_client.clone()),
                Value::String(trigger.collation_connection.clone()),
                Value::String(trigger.database_collation.clone()),
            ]
        })
        .collect::<Vec<_>>();
    apply_show_filter(&mut rows, &columns, filter, "Table")?;
    Ok(QueryResponse::ResultSet(ResultSet { columns, rows }))
}

fn show_create_trigger_columns() -> Vec<Column> {
    vec![
        col("Trigger", ColumnType::VarString),
        col("sql_mode", ColumnType::VarString),
        col("SQL Original Statement", ColumnType::VarString),
        col("character_set_client", ColumnType::VarString),
        col("collation_connection", ColumnType::VarString),
        col("Database Collation", ColumnType::VarString),
        col("Created", ColumnType::DateTime),
    ]
}

fn show_triggers_columns() -> Vec<Column> {
    vec![
        col("Trigger", ColumnType::VarString),
        col("Event", ColumnType::VarString),
        col("Table", ColumnType::VarString),
        col("Statement", ColumnType::VarString),
        col("Timing", ColumnType::VarString),
        col("Created", ColumnType::DateTime),
        col("sql_mode", ColumnType::VarString),
        col("Definer", ColumnType::VarString),
        col("character_set_client", ColumnType::VarString),
        col("collation_connection", ColumnType::VarString),
        col("Database Collation", ColumnType::VarString),
    ]
}

/// B3: MySQL gates trigger metadata (SHOW TRIGGERS / SHOW CREATE TRIGGER)
/// and DROP TRIGGER behind the `TRIGGER` privilege on the table the trigger
/// is defined on. A global or schema-level `TRIGGER` grant covers the table
/// via `scope_implies`. Returns true when the caller may see/drop triggers
/// on `database`.`table`.
pub(crate) fn has_trigger_privilege(
    db: &Database,
    session: &SessionContext,
    database: &str,
    table: &str,
) -> bool {
    let auth = db.auth_catalog();
    let ctx = session.authorization_context();
    let required = RequiredPrivilege {
        privilege: Privilege::Trigger,
        scope: PrivilegeScope::Table {
            database: database.to_string(),
            table: table.to_string(),
        },
    };
    auth.account_has_privilege(&ctx, &required, false)
}

/// G11: MySQL gates event metadata behind the `EVENT` privilege on the
/// event's schema (a global `EVENT` grant covers every schema). Returns
/// true when the caller may see events in `database`.
fn has_event_privilege(db: &Database, session: &SessionContext, database: &str) -> bool {
    let auth = db.auth_catalog();
    let ctx = session.authorization_context();
    let required = RequiredPrivilege {
        privilege: Privilege::Event,
        scope: PrivilegeScope::Database {
            database: database.to_string(),
        },
    };
    auth.account_has_privilege(&ctx, &required, false)
}

pub(crate) fn show_create_event(
    db: &Database,
    session: &SessionContext,
    name: &str,
) -> Result<QueryResponse, QueryError> {
    let (event_db, _) = crate::storage::split_table_name(&session.current_database, name);
    if event_db.is_empty() {
        return Err(crate::storage::error(
            crate::error::catalog::ER_NO_DB_ERROR,
            "No database selected",
        ));
    }
    // G11: without the EVENT privilege the event is hidden — report it as
    // unknown (1539) rather than leaking its existence.
    if !has_event_privilege(db, session, &event_db) {
        return Err(crate::storage::error(
            crate::error::catalog::ER_EVENT_DOES_NOT_EXIST,
            format!("Unknown event '{name}'"),
        ));
    }
    let event = db
        .event_definition(&session.current_database, name)
        .ok_or_else(|| {
            crate::storage::error(
                crate::error::catalog::ER_EVENT_DOES_NOT_EXIST,
                format!("Unknown event '{name}'"),
            )
        })?;
    Ok(QueryResponse::ResultSet(ResultSet {
        columns: show_create_event_columns(),
        rows: vec![vec![
            Value::String(event.name.clone()),
            Value::String(event.sql_mode.clone()),
            Value::String(event.time_zone.clone()),
            Value::String(create_event_sql(&event)),
            Value::String(event.character_set_client.clone()),
            Value::String(event.collation_connection.clone()),
            Value::String(event.database_collation.clone()),
        ]],
    }))
}

pub(crate) fn show_events(
    db: &Database,
    session: &SessionContext,
    database: Option<&str>,
    filter: Option<&ShowFilter>,
) -> Result<QueryResponse, QueryError> {
    let database = database.unwrap_or(&session.current_database);
    if database.is_empty() {
        return Err(crate::storage::error(
            crate::error::catalog::ER_NO_DB_ERROR,
            "No database selected",
        ));
    }
    if !db
        .catalog()
        .databases
        .contains_key(&crate::storage::normalize_name(database))
    {
        return Err(crate::storage::error(
            crate::error::catalog::ER_BAD_DB_ERROR,
            format!("Unknown database '{database}'"),
        ));
    }
    let columns = show_events_columns();
    // G11: MySQL only lists events in schemas where the caller holds the
    // EVENT privilege; otherwise the result is empty (not an error).
    let mut rows = if has_event_privilege(db, session, database) {
        db.events_in_database(database)
            .into_iter()
            .map(event_show_row)
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    apply_show_filter(&mut rows, &columns, filter, "Name")?;
    Ok(QueryResponse::ResultSet(ResultSet { columns, rows }))
}

fn event_show_row(event: EventSchema) -> Vec<Value> {
    let (event_type, execute_at, interval_value, interval_field, starts, ends) =
        event_schedule_show_fields(&event);
    vec![
        Value::String(event.database.clone()),
        Value::String(event.name.clone()),
        Value::String(event.definer()),
        Value::String(event.time_zone.clone()),
        Value::String(event_type),
        execute_at,
        interval_value,
        interval_field,
        starts,
        ends,
        Value::String(event.status.as_str().to_string()),
        Value::Int(crate::engine::executor::events::current_server_id() as i64),
        Value::String(event.character_set_client.clone()),
        Value::String(event.collation_connection.clone()),
        Value::String(event.database_collation.clone()),
    ]
}

fn event_schedule_show_fields(event: &EventSchema) -> (String, Value, Value, Value, Value, Value) {
    let tz = &event.time_zone;
    match &event.schedule {
        EventScheduleSchema::OneShot { execute_at_ms } => (
            "ONE TIME".to_string(),
            Value::String(crate::engine::executor::events::format_ms_in_tz(
                *execute_at_ms,
                tz,
            )),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ),
        EventScheduleSchema::Recurring {
            interval,
            starts_ms,
            ends_ms,
        } => (
            "RECURRING".to_string(),
            Value::Null,
            Value::String(interval.value.clone()),
            Value::String(interval.unit.clone()),
            Value::String(crate::engine::executor::events::format_ms_in_tz(
                *starts_ms, tz,
            )),
            ends_ms
                .map(|ms| Value::String(crate::engine::executor::events::format_ms_in_tz(ms, tz)))
                .unwrap_or(Value::Null),
        ),
    }
}

pub(crate) fn create_event_sql(event: &EventSchema) -> String {
    let mut sql = String::new();
    sql.push_str("CREATE DEFINER=`");
    sql.push_str(&event.definer_user.replace('`', "``"));
    sql.push_str("`@`");
    sql.push_str(&event.definer_host.replace('`', "``"));
    sql.push_str("` EVENT `");
    sql.push_str(&event.name.replace('`', "``"));
    sql.push_str("` ON SCHEDULE ");
    let tz = &event.time_zone;
    match &event.schedule {
        EventScheduleSchema::OneShot { execute_at_ms } => {
            sql.push_str("AT '");
            sql.push_str(&crate::engine::executor::events::format_ms_in_tz(
                *execute_at_ms,
                tz,
            ));
            sql.push('\'');
        }
        EventScheduleSchema::Recurring {
            interval,
            starts_ms,
            ends_ms,
        } => {
            sql.push_str("EVERY ");
            sql.push_str(&interval.value);
            sql.push(' ');
            sql.push_str(&interval.unit);
            sql.push_str(" STARTS '");
            sql.push_str(&crate::engine::executor::events::format_ms_in_tz(
                *starts_ms, tz,
            ));
            sql.push('\'');
            if let Some(ends) = ends_ms {
                sql.push_str(" ENDS '");
                sql.push_str(&crate::engine::executor::events::format_ms_in_tz(*ends, tz));
                sql.push('\'');
            }
        }
    }
    sql.push_str(" ON COMPLETION ");
    sql.push_str(event.on_completion.as_str());
    sql.push(' ');
    sql.push_str(match event.status {
        EventStatusSchema::Enabled => "ENABLE",
        EventStatusSchema::Disabled => "DISABLE",
        EventStatusSchema::DisabledOnSlave => "DISABLE ON SLAVE",
    });
    if !event.comment.is_empty() {
        sql.push_str(" COMMENT '");
        sql.push_str(&event.comment.replace('\\', "\\\\").replace('\'', "''"));
        sql.push('\'');
    }
    sql.push_str(" DO ");
    sql.push_str(&event.body_sql);
    sql
}

fn show_create_event_columns() -> Vec<Column> {
    vec![
        col("Event", ColumnType::VarString),
        col("sql_mode", ColumnType::VarString),
        col("time zone", ColumnType::VarString),
        col("Create Event", ColumnType::VarString),
        col("character_set_client", ColumnType::VarString),
        col("collation_connection", ColumnType::VarString),
        col("Database Collation", ColumnType::VarString),
    ]
}

fn show_events_columns() -> Vec<Column> {
    vec![
        col("Db", ColumnType::VarString),
        col("Name", ColumnType::VarString),
        col("Definer", ColumnType::VarString),
        col("Time zone", ColumnType::VarString),
        col("Type", ColumnType::VarString),
        col("Execute at", ColumnType::VarString),
        col("Interval value", ColumnType::VarString),
        col("Interval field", ColumnType::VarString),
        col("Starts", ColumnType::VarString),
        col("Ends", ColumnType::VarString),
        col("Status", ColumnType::VarString),
        col("Originator", ColumnType::LongLong),
        col("character_set_client", ColumnType::VarString),
        col("collation_connection", ColumnType::VarString),
        col("Database Collation", ColumnType::VarString),
    ]
}

pub(crate) fn create_trigger_sql(trigger: &TriggerSchema) -> String {
    let mut sql = String::new();
    sql.push_str("CREATE DEFINER=`");
    sql.push_str(&trigger.definer_user.replace('`', "``"));
    sql.push_str("`@`");
    sql.push_str(&trigger.definer_host.replace('`', "``"));
    sql.push_str("` TRIGGER `");
    sql.push_str(&trigger.name.replace('`', "``"));
    sql.push_str("` ");
    sql.push_str(trigger.timing.as_str());
    sql.push(' ');
    sql.push_str(trigger.event.as_str());
    sql.push_str(" ON `");
    sql.push_str(&trigger.table_database.replace('`', "``"));
    sql.push_str("`.`");
    sql.push_str(&trigger.table_name.replace('`', "``"));
    sql.push_str("` FOR EACH ROW ");
    sql.push_str(&trigger.body_sql);
    sql
}

fn show_where_matches(expr: &Expr, columns: &[Column], row: &[Value]) -> Result<bool, QueryError> {
    let mut bound = expr.clone();
    bind_show_row_expr(&mut bound, columns, row);
    Ok(crate::engine::expr::truthy(
        &crate::engine::expr::eval_const_runtime(&bound)?,
    ))
}

fn bind_show_row_expr(expr: &mut Expr, columns: &[Column], row: &[Value]) {
    match expr {
        Expr::Identifier(name) => {
            if let Some(value) = show_row_value(columns, row, name) {
                *expr = Expr::BoundValue(value);
            }
        }
        Expr::CompoundIdentifier(parts) => {
            if let Some(name) = parts.last() {
                if let Some(value) = show_row_value(columns, row, name) {
                    *expr = Expr::BoundValue(value);
                }
            }
        }
        _ if crate::parser::visit::scalar_expr_children(expr) => {
            crate::parser::visit::for_each_expr_child_mut(expr, |child| {
                bind_show_row_expr(child, columns, row);
            });
        }
        _ => {}
    }
}

fn show_row_value(columns: &[Column], row: &[Value], name: &str) -> Option<Value> {
    columns
        .iter()
        .position(|column| column.name.eq_ignore_ascii_case(name))
        .and_then(|idx| row.get(idx).cloned())
}

fn show_create_procedure_columns() -> Vec<Column> {
    vec![
        col("Procedure", ColumnType::VarString),
        col("sql_mode", ColumnType::VarString),
        col("Create Procedure", ColumnType::VarString),
        col("character_set_client", ColumnType::VarString),
        col("collation_connection", ColumnType::VarString),
        col("Database Collation", ColumnType::VarString),
    ]
}

fn show_create_function_columns() -> Vec<Column> {
    vec![
        col("Function", ColumnType::VarString),
        col("sql_mode", ColumnType::VarString),
        col("Create Function", ColumnType::VarString),
        col("character_set_client", ColumnType::VarString),
        col("collation_connection", ColumnType::VarString),
        col("Database Collation", ColumnType::VarString),
    ]
}

fn show_routine_status_columns() -> Vec<Column> {
    vec![
        col("Db", ColumnType::VarString),
        col("Name", ColumnType::VarString),
        col("Type", ColumnType::VarString),
        col("Definer", ColumnType::VarString),
        col("Modified", ColumnType::DateTime),
        col("Created", ColumnType::DateTime),
        col("Security_type", ColumnType::VarString),
        col("Comment", ColumnType::VarString),
        col("character_set_client", ColumnType::VarString),
        col("collation_connection", ColumnType::VarString),
        col("Database Collation", ColumnType::VarString),
    ]
}

pub(crate) fn create_routine_sql(routine: &RoutineSchema) -> String {
    let mut sql = String::new();
    sql.push_str("CREATE DEFINER=");
    sql.push_str(&quoted_identifier(&routine.definer_user));
    sql.push('@');
    sql.push_str(&quoted_identifier(&routine.definer_host));
    sql.push(' ');
    sql.push_str(routine.kind.as_str());
    sql.push(' ');
    sql.push_str(&quoted_identifier(&routine.name));
    sql.push_str(" (");
    let params = routine
        .params
        .iter()
        .map(|param| {
            let mut p = String::new();
            if routine.kind == RoutineKindSchema::Procedure {
                p.push_str(param.mode.as_str());
                p.push(' ');
            }
            p.push_str(&quoted_identifier(&param.name));
            p.push(' ');
            p.push_str(&param.data_type_sql);
            p
        })
        .collect::<Vec<_>>();
    sql.push_str(&params.join(", "));
    sql.push(')');
    if routine.kind == RoutineKindSchema::Function {
        sql.push_str(" RETURNS ");
        sql.push_str(routine.return_type_sql.as_deref().unwrap_or("varchar(255)"));
    }
    if routine.deterministic {
        sql.push_str(" DETERMINISTIC");
    } else {
        sql.push_str(" NOT DETERMINISTIC");
    }
    sql.push(' ');
    sql.push_str(routine.sql_data_access.as_str());
    sql.push_str(" SQL SECURITY ");
    sql.push_str(routine.security_type.as_str());
    if !routine.comment.is_empty() {
        sql.push_str(" COMMENT '");
        sql.push_str(&routine.comment.replace('\\', "\\\\").replace('\'', "''"));
        sql.push('\'');
    }
    sql.push(' ');
    sql.push_str(&routine.body_sql);
    sql
}

fn routine_kind_schema(kind: RoutineKind) -> RoutineKindSchema {
    match kind {
        RoutineKind::Procedure => RoutineKindSchema::Procedure,
        RoutineKind::Function => RoutineKindSchema::Function,
    }
}

fn mysql_time_value(unix_ms: u64) -> Value {
    crate::engine::information_schema::mysql_time_value(unix_ms)
}

pub(crate) fn describe_show_create_table(
    db: &Database,
    session: &SessionContext,
    table_name: &str,
) -> Result<Vec<Column>, QueryError> {
    let qualified = qualify_table_name(session, table_name);
    if let Some(err) = crate::engine::information_schema::hidden_system_table_error(&qualified) {
        return Err(err);
    }
    if let Some(def) = crate::engine::information_schema::find_system_table_name(&qualified) {
        if def.schema.eq_ignore_ascii_case("sys") && def.table_type.eq_ignore_ascii_case("VIEW") {
            return Ok(show_create_view_columns());
        }
        return Ok(show_create_table_columns());
    }
    if db.table(&qualified).is_ok() {
        return Ok(show_create_table_columns());
    }
    if db
        .view_definition(&session.current_database, table_name)
        .is_some()
    {
        return Ok(show_create_view_columns());
    }
    db.table(&qualified)?;
    unreachable!()
}

pub(crate) fn show_create_table_columns() -> Vec<Column> {
    vec![
        col("Table", ColumnType::VarString),
        col("Create Table", ColumnType::VarString),
    ]
}

/// Reconstruct a canonical `CREATE SEQUENCE` statement from a stored
/// definition (used by `SHOW CREATE SEQUENCE`).
pub(crate) fn create_sequence_sql(seq: &SequenceSchema) -> String {
    let mut s = format!("CREATE SEQUENCE `{}`", seq.name.replace('`', "``"));
    if seq.as_type != SeqType::BigInt {
        s.push_str(&format!(" AS {}", seq.as_type.as_str()));
    }
    s.push_str(&format!(" START WITH {}", seq.start_value));
    s.push_str(&format!(" INCREMENT BY {}", seq.increment));
    s.push_str(&format!(" MINVALUE {}", seq.min_value));
    s.push_str(&format!(" MAXVALUE {}", seq.max_value));
    s.push_str(&format!(" CACHE {}", seq.cache));
    s.push_str(if seq.cycle { " CYCLE" } else { " NO CYCLE" });
    s
}

fn show_create_sequence_columns() -> Vec<Column> {
    vec![
        col("Sequence", ColumnType::VarString),
        col("Create Sequence", ColumnType::VarString),
    ]
}

pub(crate) fn show_create_sequence(
    db: &Database,
    session: &SessionContext,
    name: &str,
) -> Result<QueryResponse, QueryError> {
    let def = db.resolve_sequence(&session.current_database, name)?;
    Ok(QueryResponse::ResultSet(ResultSet {
        columns: show_create_sequence_columns(),
        rows: vec![vec![
            Value::String(def.name.clone()),
            Value::String(create_sequence_sql(&def)),
        ]],
    }))
}

pub(crate) fn show_sequences(
    db: &Database,
    session: &SessionContext,
    database: Option<&str>,
    filter: Option<&ShowFilter>,
) -> Result<QueryResponse, QueryError> {
    let target_db = crate::storage::normalize_name(database.unwrap_or(&session.current_database));
    let label = format!("Sequences_in_{target_db}");
    let columns = vec![col(&label, ColumnType::VarString)];
    let mut rows: Vec<Vec<Value>> = db
        .list_sequences()?
        .into_iter()
        .filter(|s| crate::storage::normalize_name(&s.database) == target_db)
        .filter(|s| {
            session.internal_system_session
                || db.auth_catalog().account_can_access_schema(
                    &session.authorization_context(),
                    &s.database,
                    Some(&s.name),
                )
        })
        .map(|s| vec![Value::String(s.name.clone())])
        .collect();
    rows.sort_by(|a, b| a[0].to_display_string().cmp(&b[0].to_display_string()));
    apply_show_filter(&mut rows, &columns, filter, &label)?;
    Ok(QueryResponse::ResultSet(ResultSet { columns, rows }))
}

pub(crate) fn show_create_view_columns() -> Vec<Column> {
    vec![
        col("View", ColumnType::VarString),
        col("Create View", ColumnType::VarString),
        col("character_set_client", ColumnType::VarString),
        col("collation_connection", ColumnType::VarString),
    ]
}

pub(crate) fn create_view_sql(view: &ViewSchema) -> String {
    let mut header = String::from("CREATE ");
    header.push_str("ALGORITHM=");
    header.push_str(&view.algorithm);
    header.push(' ');
    header.push_str("DEFINER=`");
    header.push_str(&view.definer_user.replace('`', "``"));
    header.push_str("`@`");
    header.push_str(&view.definer_host.replace('`', "``"));
    header.push_str("` ");
    header.push_str("SQL SECURITY ");
    header.push_str(view.security_type.as_str());
    header.push(' ');
    header.push_str("VIEW `");
    header.push_str(&view.name.replace('`', "``"));
    header.push('`');
    if !view.columns.is_empty() {
        header.push_str(" (");
        let col_list: Vec<String> = view
            .columns
            .iter()
            .map(|c| quoted_identifier(&c.name))
            .collect();
        header.push_str(&col_list.join(", "));
        header.push(')');
    }
    header.push_str(" AS ");
    // Echo the MySQL-style canonical body; fall back to the user's raw text
    // for views written before normalization, or bodies the normalizer could
    // not faithfully render (it stores an empty string in that case).
    if view.definition_normalized.is_empty() {
        header.push_str(&view.sql);
    } else {
        header.push_str(&view.definition_normalized);
    }
    if let Some(opt) = &view.check_option {
        header.push_str(" WITH ");
        header.push_str(opt);
        header.push_str(" CHECK OPTION");
    }
    header
}

pub(crate) fn col(name: &str, column_type: ColumnType) -> Column {
    Column {
        source: Default::default(),
        name: name.to_string(),
        column_type,
    }
}

pub(crate) fn mysql_type_name(data_type: &DataTypeKind) -> String {
    mysql_type_name_with_unsigned(data_type, false)
}

/// Render a temporal type with its declared fsp suffix when non-zero.
/// MySQL's `SHOW COLUMNS` omits the `(0)` for fsp=0 so callers see the
/// same shape they would against upstream (`datetime` not `datetime(0)`).
fn format_temporal_with_fsp(name: &str, fsp: u8) -> String {
    if fsp == 0 {
        name.to_string()
    } else {
        format!("{name}({fsp})")
    }
}

/// Render `FLOAT`/`DOUBLE` with the optional `(M,D)` display modifier.
/// MySQL 8.0 keeps the deprecated `(M, D)` echo in SHOW COLUMNS, e.g.
/// `float(7,2)`. Bare `FLOAT` / `DOUBLE` with neither M nor D render
/// without parentheses to match upstream.
fn format_float_double(name: &str, precision: Option<u8>, scale: Option<u8>) -> String {
    match (precision, scale) {
        (Some(p), Some(s)) => format!("{name}({p},{s})"),
        (Some(p), None) => format!("{name}({p})"),
        _ => name.to_string(),
    }
}

pub(crate) fn mysql_type_name_with_unsigned(data_type: &DataTypeKind, unsigned: bool) -> String {
    mysql_type_name_with_attrs(data_type, unsigned, false)
}

pub(crate) fn mysql_type_name_with_attrs(
    data_type: &DataTypeKind,
    unsigned: bool,
    zerofill: bool,
) -> String {
    let base = match data_type {
        DataTypeKind::TinyInt => "tinyint".to_string(),
        DataTypeKind::SmallInt => "smallint".to_string(),
        DataTypeKind::MediumInt => "mediumint".to_string(),
        DataTypeKind::Int => "int".to_string(),
        DataTypeKind::BigInt => "bigint".to_string(),
        DataTypeKind::Bool => "tinyint(1)".to_string(),
        DataTypeKind::Float { precision, scale } => {
            format_float_double("float", *precision, *scale)
        }
        DataTypeKind::Double { precision, scale } => {
            format_float_double("double", *precision, *scale)
        }
        DataTypeKind::Decimal { precision, scale } => match (precision, scale) {
            (Some(p), Some(s)) => format!("decimal({p},{s})"),
            (Some(p), None) => format!("decimal({p})"),
            _ => "decimal".to_string(),
        },
        DataTypeKind::Char { len } => format!("char({})", len.unwrap_or(1)),
        DataTypeKind::VarChar { len } => format!("varchar({})", len.unwrap_or(255)),
        DataTypeKind::Binary { len } => format!("binary({})", len.unwrap_or(1)),
        DataTypeKind::VarBinary { len } => format!("varbinary({})", len.unwrap_or(255)),
        DataTypeKind::Text { max_len } => match *max_len {
            crate::storage::schema::TINY_LOB_MAX => "tinytext".to_string(),
            crate::storage::schema::DEFAULT_LOB_MAX => "text".to_string(),
            crate::storage::schema::MEDIUM_LOB_MAX => "mediumtext".to_string(),
            crate::storage::schema::LONG_LOB_MAX => "longtext".to_string(),
            _ => "text".to_string(),
        },
        DataTypeKind::Blob { max_len } => match *max_len {
            crate::storage::schema::TINY_LOB_MAX => "tinyblob".to_string(),
            crate::storage::schema::DEFAULT_LOB_MAX => "blob".to_string(),
            crate::storage::schema::MEDIUM_LOB_MAX => "mediumblob".to_string(),
            crate::storage::schema::LONG_LOB_MAX => "longblob".to_string(),
            _ => "blob".to_string(),
        },
        DataTypeKind::Date => "date".to_string(),
        DataTypeKind::Time { fsp } => format_temporal_with_fsp("time", *fsp),
        DataTypeKind::DateTime { fsp } => format_temporal_with_fsp("datetime", *fsp),
        DataTypeKind::Timestamp { fsp } => format_temporal_with_fsp("timestamp", *fsp),
        DataTypeKind::Year => "year".to_string(),
        DataTypeKind::Json => "json".to_string(),
        DataTypeKind::Geometry { subtype, .. } => subtype.type_name().to_string(),
        DataTypeKind::Vector { dim, datatype } => match dim {
            Some(d) if *datatype == crate::vector::VectorDataType::Float32 => {
                format!("vector({d})")
            }
            Some(d) => format!("vector({d},{})", datatype.name().to_ascii_lowercase()),
            None => "vector".to_string(),
        },
        DataTypeKind::Enum { values } => {
            format!(
                "enum({})",
                values
                    .iter()
                    .map(|v| escape_sql_string_literal(v))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        }
        DataTypeKind::Set { values } => {
            format!(
                "set({})",
                values
                    .iter()
                    .map(|v| escape_sql_string_literal(v))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        }
        DataTypeKind::Bit { len } => format!("bit({})", len.unwrap_or(1)),
    };
    // ZEROFILL implies UNSIGNED and is printed after it (MySQL renders
    // `int unsigned zerofill`, including for DECIMAL/FLOAT/DOUBLE).
    if zerofill {
        format!("{base} unsigned zerofill")
    } else if unsigned
        && (data_type.is_integer()
            || matches!(
                data_type,
                DataTypeKind::Decimal { .. }
                    | DataTypeKind::Float { .. }
                    | DataTypeKind::Double { .. }
            ))
    {
        format!("{base} unsigned")
    } else {
        base
    }
}

pub(crate) fn column_default_value(column: &crate::storage::ColumnSchema) -> Value {
    if column.generated_expr.is_some() {
        return Value::Null;
    }
    if let Some(expr) = &column.default_expr {
        return Value::String(expr.clone());
    }
    // BIT columns store the default as the raw bit magnitude; MySQL's
    // information_schema.COLUMNS.COLUMN_DEFAULT echoes it as a `b'…'`
    // literal rather than the raw (possibly non-printable) bytes.
    if matches!(column.data_type, DataTypeKind::Bit { .. }) {
        if let Some(value) = &column.default {
            return Value::String(bit_default_sql(value));
        }
    }
    column.default.clone().unwrap_or(Value::Null)
}

pub(crate) fn column_extra(column: &crate::storage::ColumnSchema) -> String {
    let mut parts = Vec::new();
    if column.auto_increment {
        parts.push("auto_increment".to_string());
    }
    if column.default_expr.is_some() {
        parts.push("DEFAULT_GENERATED".to_string());
    }
    if let Some(expr) = &column.on_update_expr {
        parts.push(format!("on update {}", expr.to_ascii_lowercase()));
    }
    if column.generated_expr.is_some() {
        parts.push(if column.generated_stored {
            "STORED GENERATED".to_string()
        } else {
            "VIRTUAL GENERATED".to_string()
        });
    }
    if column.invisible {
        parts.push("INVISIBLE".to_string());
    }
    parts.join(" ")
}

/// Render a `BIT(M)` column default as MySQL's `b'<binary digits>'`
/// literal. The value is stored as the raw bit magnitude (`Value::Bytes`
/// big-endian, or `Value::UInt`); we recover the integer and print its
/// minimal binary form (`0` → `b'0'`), matching `SHOW CREATE TABLE`.
pub(crate) fn bit_default_sql(value: &Value) -> String {
    let n: u64 = match value {
        Value::UInt(v) => *v,
        Value::Int(v) => *v as u64,
        Value::Bytes(b) => b.iter().fold(0u64, |acc, &byte| (acc << 8) | byte as u64),
        _ => 0,
    };
    format!("b'{n:b}'")
}

pub(crate) fn default_value_sql(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::String(value) | Value::Decimal(value) => {
            format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
        }
        Value::Bytes(value) => {
            let mut literal = String::with_capacity(value.len() * 2 + 3);
            literal.push_str("X'");
            crate::util::hex::append_lower(&mut literal, value);
            literal.push('\'');
            literal
        }
        other => value_to_string(other),
    }
}

/// Column-level `CHARACTER SET` / `COLLATE` echo, following MySQL's
/// SHOW CREATE TABLE elision rules: nothing when the column inherits
/// the table default; `CHARACTER SET` only when the charset differs
/// from the table's; `COLLATE` only when the collation is not the
/// charset's default one.
fn column_charset_collate_sql(
    table: &TableSchema,
    column: &crate::storage::ColumnSchema,
) -> String {
    if !matches!(
        column.data_type,
        crate::storage::DataTypeKind::Char { .. }
            | crate::storage::DataTypeKind::VarChar { .. }
            | crate::storage::DataTypeKind::Text { .. }
            | crate::storage::DataTypeKind::Enum { .. }
            | crate::storage::DataTypeKind::Set { .. }
    ) {
        return String::new();
    }
    let table_charset = crate::charset::normalize_charset_name(&table.charset);
    let table_collation = crate::charset::normalize_collation_name(&table.collation);
    let charset = column
        .charset
        .as_deref()
        .map(crate::charset::normalize_charset_name);
    let collation = column
        .collation
        .as_deref()
        .map(crate::charset::normalize_collation_name);
    // Resolve the effective pair: an explicit collation implies its
    // charset; an explicit charset implies its default collation.
    let (effective_charset, effective_collation) = match (charset, collation) {
        (None, None) => return String::new(),
        (cs, coll) => {
            let coll = coll.or_else(|| {
                cs.as_deref()
                    .and_then(|c| crate::charset::default_collation_for_charset(c).ok())
                    .map(str::to_string)
            });
            let Some(coll) = coll else {
                return String::new();
            };
            let cs = cs.or_else(|| {
                crate::charset::resolve_collation(&coll)
                    .ok()
                    .map(|info| info.charset.to_string())
            });
            let Some(cs) = cs else {
                return String::new();
            };
            (cs, coll)
        }
    };
    if effective_charset == table_charset && effective_collation == table_collation {
        return String::new();
    }
    let mut out = String::new();
    if effective_charset != table_charset {
        out.push_str(&format!(" CHARACTER SET {effective_charset}"));
    }
    let charset_default = crate::charset::default_collation_for_charset(&effective_charset)
        .map(crate::charset::normalize_collation_name)
        .unwrap_or_default();
    if effective_collation != charset_default {
        out.push_str(&format!(" COLLATE {effective_collation}"));
    }
    out
}

/// Canonical `CREATE TABLE` text for a resolved schema, with no runtime
/// AUTO_INCREMENT counter. Used to synthesize the DDL event that a
/// row-based CTAS must write to the binlog *before* its row events, so a
/// replica creates the table before the WRITE_ROWS stream arrives.
pub(crate) fn create_table_sql(table: &TableSchema) -> Result<String, QueryError> {
    create_table_sql_with_auto_increment(table, None)
}

fn create_table_sql_with_auto_increment(
    table: &TableSchema,
    next_value: Option<u64>,
) -> Result<String, QueryError> {
    // SHOW CREATE TABLE must omit tombstone (Dropped) columns —
    // they are no longer part of the table's logical schema.
    let mut defs = table
        .iter_visible_columns()
        .map(|(_, column)| {
            let type_name =
                mysql_type_name_with_attrs(&column.data_type, column.unsigned, column.zerofill);
            let generated_part = column.generated_expr.as_ref().map(|expr| {
                let expr = column
                    .generated_expr_ast
                    .as_ref()
                    .map(crate::parser::expr_to_sql)
                    .unwrap_or_else(|| expr.clone());
                format!(
                    " GENERATED ALWAYS AS ({expr}) {}",
                    if column.generated_stored {
                        "STORED"
                    } else {
                        "VIRTUAL"
                    }
                )
            });
            let default_part = if column.generated_expr.is_some() {
                String::new()
            } else if let Some(expr) = &column.default_expr {
                if expr.eq_ignore_ascii_case("CURRENT_TIMESTAMP")
                    || expr.to_ascii_uppercase().starts_with("CURRENT_TIMESTAMP(")
                {
                    format!(" DEFAULT {expr}")
                } else {
                    // MySQL wraps expression defaults in an additional pair
                    // of parentheses in SHOW CREATE TABLE. The expression
                    // itself often already retains its parser parentheses.
                    format!(" DEFAULT ({expr})")
                }
            } else if matches!(column.data_type, DataTypeKind::Bit { .. }) {
                // BIT defaults are stored as the raw bit value; MySQL echoes
                // them as a `b'…'` binary literal, not a quoted byte string
                // (which would emit control characters and break dump/restore).
                column
                    .default
                    .as_ref()
                    .map(|value| format!(" DEFAULT {}", bit_default_sql(value)))
                    .unwrap_or_else(|| {
                        if column.nullable {
                            " DEFAULT NULL".to_string()
                        } else {
                            String::new()
                        }
                    })
            } else {
                let explicit = column
                    .default
                    .as_ref()
                    .map(|value| format!(" DEFAULT {}", default_value_sql(value)))
                    .unwrap_or_default();
                if explicit.is_empty()
                    && column.nullable
                    && !matches!(
                        column.data_type,
                        DataTypeKind::Text { .. } | DataTypeKind::Blob { .. }
                    )
                {
                    " DEFAULT NULL".to_string()
                } else {
                    explicit
                }
            };
            let on_update_part = column
                .on_update_expr
                .as_ref()
                .map(|expr| format!(" ON UPDATE {expr}"))
                .unwrap_or_default();
            let comment_part = if column.comment.is_empty() {
                String::new()
            } else {
                format!(
                    " COMMENT '{}'",
                    column.comment.replace('\\', "\\\\").replace('\'', "''")
                )
            };
            let invisible_part = if column.invisible { " INVISIBLE" } else { "" };
            let charset_part = column_charset_collate_sql(table, column);
            let srid_part = match &column.data_type {
                DataTypeKind::Geometry {
                    srid: Some(srid), ..
                } => format!(" SRID {srid}"),
                _ => String::new(),
            };
            format!(
                "  `{}` {}{}{}{}{}{}{}{}{}{invisible_part}",
                column.name.replace('`', "``"),
                type_name,
                charset_part,
                generated_part.unwrap_or_default(),
                if !column.nullable {
                    " NOT NULL"
                } else if matches!(column.data_type, DataTypeKind::Timestamp { .. }) {
                    " NULL"
                } else {
                    ""
                },
                srid_part,
                default_part,
                on_update_part,
                if column.auto_increment {
                    " AUTO_INCREMENT"
                } else {
                    ""
                },
                comment_part
            )
        })
        .collect::<Vec<_>>();
    if !table.primary_key.is_empty() {
        defs.push(format!(
            "  PRIMARY KEY ({})",
            table
                .primary_key
                .iter()
                .map(|col| quoted_identifier(col))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    // Building indexes are omitted from SHOW CREATE TABLE: the index
    // is not yet queryable so surfacing it in DDL output would
    // mislead clients reverse-engineering the schema.
    for index in table.iter_public_indexes().filter(|index| !index.primary) {
        // FULLTEXT renders `FULLTEXT KEY ... [WITH PARSER ngram]`, SPATIAL
        // renders `SPATIAL KEY ...`; otherwise `[UNIQUE ]KEY ...`.
        let (key_kind, parser_suffix) = if index.is_fulltext() {
            let parser = index
                .fulltext
                .as_ref()
                .map(|m| m.parser.as_str())
                .unwrap_or("");
            let suffix = if parser.is_empty() {
                String::new()
            } else {
                format!(" WITH PARSER {}", quoted_identifier(parser))
            };
            ("FULLTEXT ", suffix)
        } else if index.index_type.eq_ignore_ascii_case("SPATIAL") {
            // S6 — a SPATIAL index must round-trip as `SPATIAL KEY`;
            // previously it rendered as a plain `KEY`, losing the type.
            ("SPATIAL ", String::new())
        } else if index.unique {
            ("UNIQUE ", String::new())
        } else {
            ("", String::new())
        };
        let columns = index
            .columns
            .iter()
            .enumerate()
            .map(|(pos, col)| {
                let direction = match index.column_direction(pos) {
                    IndexColumnDirection::Asc => "",
                    IndexColumnDirection::Desc => " DESC",
                };
                if let Some(expr) = index.key_expression(pos) {
                    // MySQL 8.0 round-trips functional key parts with
                    // doubled parens — outer is the column list, inner
                    // tells the parser the body is an expression.
                    let rebuilt;
                    let ast = match expr.ast.as_ref() {
                        Some(ast) => ast,
                        None => {
                            rebuilt = crate::parser::parse_catalog_expr(&expr.expr_sql)
                                .map_err(|err| err.spec.error(err.message))?;
                            &rebuilt
                        }
                    };
                    let sql =
                        crate::parser::index_expression_to_catalog_sql(ast, expr.multi_valued)
                            .map_err(|err| {
                                crate::error::catalog::ER_NOT_SUPPORTED_YET.error(err.to_string())
                            })?;
                    Ok(format!("({sql}){direction}"))
                } else {
                    let prefix = index
                        .column_prefix_len(pos)
                        .map(|len| format!("({len})"))
                        .unwrap_or_default();
                    Ok(format!("{}{prefix}{direction}", quoted_identifier(col)))
                }
            })
            .collect::<Result<Vec<_>, QueryError>>()?
            .join(", ");
        let comment = if index.comment.is_empty() {
            String::new()
        } else {
            format!(
                " COMMENT '{}'",
                index.comment.replace('\\', "\\\\").replace('\'', "''")
            )
        };
        // Vector indexes (HNSW / IVF) are a mydb extension created with
        // `USING <algo>`; echo that back so SHOW CREATE TABLE round-trips
        // through the parser instead of silently degrading to a plain B-tree
        // key on dump/restore. Non-vector B-tree keys omit USING (MySQL default).
        let using_suffix = if index.is_vector() {
            format!(
                " USING {}{}",
                index.index_type.to_ascii_uppercase(),
                index.vector.as_ref().map(|v| v.sql()).unwrap_or_default()
            )
        } else {
            String::new()
        };
        let visible = if index.visible { "" } else { " INVISIBLE" };
        // Non-key `INCLUDE (...)` columns (covering-only). Echoed so the
        // statement round-trips back through the inline-INCLUDE strip.
        let include_suffix = if index.included_columns.is_empty() {
            String::new()
        } else {
            let cols = index
                .included_columns
                .iter()
                .map(|c| format!("`{}`", c.replace('`', "``")))
                .collect::<Vec<_>>()
                .join(", ");
            format!(" INCLUDE ({cols})")
        };
        defs.push(format!(
            "  {key_kind}KEY `{}` ({columns}){include_suffix}{using_suffix}{parser_suffix}{comment}{visible}",
            index.name.replace('`', "``")
        ));
    }
    for check in &table.check_constraints {
        defs.push(format!(
            "  CONSTRAINT `{}` CHECK ({}){}",
            check.name.replace('`', "``"),
            check
                .expr_ast
                .as_ref()
                .map(crate::parser::expr_to_sql)
                .unwrap_or_else(|| check.expr.clone()),
            if check.enforced { "" } else { " NOT ENFORCED" }
        ));
    }
    for fk in &table.foreign_keys {
        let cols = fk
            .columns
            .iter()
            .map(|c| format!("`{}`", c.replace('`', "``")))
            .collect::<Vec<_>>()
            .join(", ");
        let ref_cols = fk
            .referenced_columns
            .iter()
            .map(|c| format!("`{}`", c.replace('`', "``")))
            .collect::<Vec<_>>()
            .join(", ");
        // Qualify the referenced table with its schema only when it differs
        // from this table's database — matching MySQL 8.0, which renders
        // `REFERENCES `otherdb`.`tbl`` for cross-schema FKs and a bare table
        // name otherwise. Without the qualifier a dumped cross-db FK would
        // resolve against the wrong schema on restore.
        let ref_table = if !fk.referenced_database.is_empty()
            && !fk.referenced_database.eq_ignore_ascii_case(&table.database)
        {
            format!(
                "`{}`.`{}`",
                fk.referenced_database.replace('`', "``"),
                fk.referenced_table.replace('`', "``"),
            )
        } else {
            format!("`{}`", fk.referenced_table.replace('`', "``"))
        };
        let mut clause = format!(
            "  CONSTRAINT `{}` FOREIGN KEY ({cols}) REFERENCES {ref_table} ({ref_cols})",
            fk.name.replace('`', "``"),
        );
        // MySQL omits the default NO ACTION; render only non-default rules.
        if fk.on_delete != crate::storage::ReferentialAction::NoAction {
            clause.push_str(&format!(" ON DELETE {}", fk.on_delete.as_sql()));
        }
        if fk.on_update != crate::storage::ReferentialAction::NoAction {
            clause.push_str(&format!(" ON UPDATE {}", fk.on_update.as_sql()));
        }
        defs.push(clause);
    }
    let auto_increment = next_value
        .filter(|value| *value > 1 && table.auto_increment_column_index().is_some())
        .map(|value| format!(" AUTO_INCREMENT={value}"))
        .unwrap_or_default();
    let partitioning = format_partition_clause(table);
    let charset = &table.charset;
    let collation = &table.collation;
    let table_comment = if table.comment.is_empty() {
        String::new()
    } else {
        format!(
            " COMMENT='{}'",
            table.comment.replace('\\', "\\\\").replace('\'', "''")
        )
    };
    // Per-table physical-storage knobs. Render only the non-zero side
    // to keep `SHOW CREATE TABLE` byte-identical for tables that never
    // touched these knobs (the common case).
    let pctfree = if table.pctfree > 0 {
        format!(" PCTFREE={}", table.pctfree)
    } else {
        String::new()
    };
    let pctused = if table.pctused > 0 {
        format!(" PCTUSED={}", table.pctused)
    } else {
        String::new()
    };
    // Accepted metadata-only MySQL options (ROW_FORMAT, MAX_ROWS, …) in
    // declaration order, before COMMENT — matching MySQL's SHOW CREATE
    // option placement.
    let generic_options: String = table
        .mysql_table_options
        .iter()
        .map(|(key, value)| format!(" {}", crate::parser::render_table_option(key, value)))
        .collect();
    // MySQL SHOW CREATE always renders the resolved engine, including when
    // the CREATE statement omitted ENGINE. The compatible default is InnoDB;
    // an explicit ENGINE=mydb remains visible as a mydb extension.
    let engine = format!(
        " ENGINE={}",
        if table.engine.is_empty() {
            "InnoDB"
        } else {
            &table.engine
        }
    );
    Ok(format!(
        "CREATE TABLE `{}` (\n{}\n){engine}{auto_increment} DEFAULT CHARSET={charset} COLLATE={collation}{pctfree}{pctused}{generic_options}{table_comment}{partitioning}",
        table.name.replace('`', "``"),
        defs.join(",\n")
    ))
}

/// Render the `PARTITION BY ...` suffix for SHOW CREATE TABLE. Empty
/// string when the table is not partitioned.
fn format_partition_clause(table: &TableSchema) -> String {
    use crate::storage::partitioning::{PartitionBounds, PartitionMethod};
    let Some(part) = table.partitioning.as_ref() else {
        return String::new();
    };
    let mut out = String::new();
    out.push_str("\n/*!50100 PARTITION BY ");
    out.push_str(part.method.keyword());
    out.push(' ');
    match part.method {
        PartitionMethod::Range
        | PartitionMethod::List
        | PartitionMethod::Hash
        | PartitionMethod::LinearHash => {
            out.push('(');
            out.push_str(&part.expr_sql);
            out.push(')');
        }
        PartitionMethod::RangeColumns
        | PartitionMethod::ListColumns
        | PartitionMethod::Key
        | PartitionMethod::LinearKey => {
            if let (PartitionMethod::Key | PartitionMethod::LinearKey, Some(algo)) =
                (part.method, part.key_algorithm)
            {
                out.push_str(&format!("ALGORITHM={} ", algo));
            }
            out.push('(');
            let cols: Vec<String> = part.columns.iter().map(|c| quoted_identifier(c)).collect();
            out.push_str(&cols.join(","));
            out.push(')');
        }
    }
    if let Some(sub) = part.subpartitioning.as_ref() {
        render_subpartition_clause(&mut out, sub);
    }
    // D4: only compress to `PARTITIONS n` when every partition uses
    // its default synthesized name `p{idx}`. The old `starts_with('p')`
    // was too loose and rewrote user-chosen names like `porange`/`pblue`
    // as anonymous defaults, breaking dump/restore round-trips and the
    // `PARTITION(<name>)` targeting language.
    if part.method.is_hash_like()
        && part.partitions.iter().enumerate().all(|(idx, p)| {
            p.bounds == PartitionBounds::Hash
                && p.comment.is_empty()
                && p.options.is_empty()
                && p.name == format!("p{}", idx)
        })
    {
        // Compact form for HASH/KEY with default-shape partitions.
        out.push_str(&format!("\nPARTITIONS {}", part.partitions.len()));
    } else {
        out.push_str("\n(");
        let mut entries: Vec<String> = Vec::with_capacity(part.partitions.len());
        for p in &part.partitions {
            let subpartition_list = render_explicit_subpartition_list(
                p,
                part.subpartitioning.as_ref().and_then(|sub| sub.count),
            );
            entries.push(format!(
                " PARTITION `{}` {}{}{}{}",
                p.name.replace('`', "``"),
                render_partition_bounds(&p.bounds),
                if p.comment.is_empty() {
                    String::new()
                } else {
                    format!(
                        " COMMENT = '{}'",
                        p.comment.replace('\\', "\\\\").replace('\'', "''")
                    )
                },
                render_partition_options(&p.options),
                subpartition_list
            ));
        }
        out.push_str(&entries.join(",\n"));
        out.push_str(")");
    }
    out.push_str(" */");
    out
}

fn render_explicit_subpartition_list(
    partition: &crate::storage::partitioning::Partition,
    declared_count: Option<u32>,
) -> String {
    if partition.subpartitions.is_empty() {
        return String::new();
    }
    let needs_explicit = declared_count.is_none()
        || partition
            .subpartitions
            .iter()
            .enumerate()
            .any(|(idx, sub)| {
                sub.name != format!("{}sp{}", partition.name, idx)
                    || !sub.comment.is_empty()
                    || !sub.options.is_empty()
            });
    if !needs_explicit {
        return String::new();
    }
    let entries: Vec<String> = partition
        .subpartitions
        .iter()
        .map(|sub| {
            let comment = if sub.comment.is_empty() {
                String::new()
            } else {
                format!(
                    " COMMENT = '{}'",
                    sub.comment.replace('\\', "\\\\").replace('\'', "''")
                )
            };
            format!(
                "SUBPARTITION `{}`{}{}",
                sub.name.replace('`', "``"),
                comment,
                render_partition_options(&sub.options)
            )
        })
        .collect();
    format!(" ({})", entries.join(", "))
}

/// Render declared per-(sub)partition physical options for SHOW CREATE
/// TABLE (leading space per emitted option; empty string when none). The
/// order is re-parseable in any arrangement; we follow MySQL's rough
/// ordering.
fn render_partition_options(
    options: &crate::storage::partitioning::PartitionPhysicalOptions,
) -> String {
    if options.is_empty() {
        return String::new();
    }
    let mut out = String::new();
    if let Some(engine) = &options.engine {
        out.push_str(&format!(" ENGINE = {}", engine));
    }
    if let Some(dir) = &options.data_directory {
        out.push_str(&format!(
            " DATA DIRECTORY = '{}'",
            dir.replace('\\', "\\\\").replace('\'', "''")
        ));
    }
    if let Some(dir) = &options.index_directory {
        out.push_str(&format!(
            " INDEX DIRECTORY = '{}'",
            dir.replace('\\', "\\\\").replace('\'', "''")
        ));
    }
    if let Some(v) = options.max_rows {
        out.push_str(&format!(" MAX_ROWS = {}", v));
    }
    if let Some(v) = options.min_rows {
        out.push_str(&format!(" MIN_ROWS = {}", v));
    }
    if let Some(v) = options.nodegroup {
        out.push_str(&format!(" NODEGROUP = {}", v));
    }
    if let Some(ts) = &options.tablespace {
        out.push_str(&format!(" TABLESPACE = {}", ts));
    }
    out
}

fn render_subpartition_clause(
    out: &mut String,
    sub: &crate::storage::partitioning::SubpartitioningInfo,
) {
    use crate::storage::partitioning::PartitionMethod;
    out.push_str("\nSUBPARTITION BY ");
    out.push_str(sub.method.keyword());
    out.push(' ');
    match sub.method {
        PartitionMethod::Hash | PartitionMethod::LinearHash => {
            out.push('(');
            out.push_str(&sub.expr_sql);
            out.push(')');
        }
        PartitionMethod::Key | PartitionMethod::LinearKey => {
            if let Some(algo) = sub.key_algorithm {
                out.push_str(&format!("ALGORITHM={} ", algo));
            }
            out.push('(');
            let cols: Vec<String> = sub.columns.iter().map(|c| quoted_identifier(c)).collect();
            out.push_str(&cols.join(","));
            out.push(')');
        }
        _ => {}
    }
    if let Some(count) = sub.count {
        out.push_str(&format!("\nSUBPARTITIONS {}", count));
    }
}

fn render_partition_bounds(b: &crate::storage::partitioning::PartitionBounds) -> String {
    use crate::storage::partitioning::{PartitionBounds, PartitionLiteral};
    match b {
        PartitionBounds::RangeLessThan(lits) => {
            if lits.len() == 1 && matches!(lits[0], PartitionLiteral::MaxValue) {
                "VALUES LESS THAN (MAXVALUE)".to_string()
            } else {
                let parts: Vec<String> = lits.iter().map(render_partition_literal).collect();
                format!("VALUES LESS THAN ({})", parts.join(", "))
            }
        }
        PartitionBounds::ListIn(groups) => {
            let parts: Vec<String> = groups
                .iter()
                .map(|t| {
                    if t.len() == 1 {
                        render_partition_literal(&t[0])
                    } else {
                        let inner: Vec<String> = t.iter().map(render_partition_literal).collect();
                        format!("({})", inner.join(", "))
                    }
                })
                .collect();
            format!("VALUES IN ({})", parts.join(", "))
        }
        PartitionBounds::Hash => String::new(),
    }
}

fn render_partition_literal(lit: &crate::storage::partitioning::PartitionLiteral) -> String {
    use crate::storage::partitioning::PartitionLiteral;
    match lit {
        PartitionLiteral::MaxValue => "MAXVALUE".to_string(),
        PartitionLiteral::Value(v) => {
            use crate::types::Value;
            match v {
                Value::Null => "NULL".to_string(),
                Value::Int(n) => n.to_string(),
                Value::UInt(n) => n.to_string(),
                Value::Double(n) => n.to_string(),
                Value::Float(n) => n.to_string(),
                Value::Decimal(s) => s.clone(),
                Value::String(_) | Value::SmallString(_) | Value::Bytes(_) => default_value_sql(v),
                Value::Date(_) | Value::DateTime(_) | Value::Timestamp(_) | Value::Time(_) => {
                    format!("'{}'", crate::storage::value_to_string(v))
                }
                _ => crate::storage::value_to_string(v),
            }
        }
    }
}

pub(crate) fn show_charset_result(
    filter: Option<&ShowFilter>,
) -> Result<QueryResponse, QueryError> {
    let columns = vec![
        col("Charset", ColumnType::VarString),
        col("Description", ColumnType::VarString),
        col("Default collation", ColumnType::VarString),
        col("Maxlen", ColumnType::Long),
    ];
    let mut rows = crate::charset::CHARSETS
        .iter()
        .map(|charset| {
            vec![
                Value::String(charset.name.to_string()),
                Value::String(charset.description.to_string()),
                Value::String(charset.default_collation.to_string()),
                Value::Int(charset.max_len as i64),
            ]
        })
        .collect();
    apply_show_filter(&mut rows, &columns, filter, "Charset")?;
    Ok(QueryResponse::ResultSet(ResultSet { columns, rows }))
}

pub(crate) fn show_engines_result() -> QueryResponse {
    QueryResponse::ResultSet(ResultSet {
        columns: vec![
            col("Engine", ColumnType::VarString),
            col("Support", ColumnType::VarString),
            col("Comment", ColumnType::VarString),
            col("Transactions", ColumnType::VarString),
            col("XA", ColumnType::VarString),
            col("Savepoints", ColumnType::VarString),
        ],
        rows: storage_engine_rows(),
    })
}

/// Storage engines advertised through both `SHOW ENGINES` and
/// `information_schema.ENGINES`. `InnoDB` is a compatibility alias: tables
/// declared with `ENGINE=InnoDB` are physically served by mydb's MVCC engine.
pub(crate) fn storage_engine_rows() -> Vec<Vec<Value>> {
    vec![
        vec![
            Value::String("mydb".to_string()),
            Value::String("DEFAULT".to_string()),
            Value::String("mydb heap storage engine".to_string()),
            Value::String("YES".to_string()),
            // XA two-phase commit is implemented for external coordinators;
            // an XA branch is durable across restart and joinable from any
            // session via XA RECOVER / XA COMMIT / XA ROLLBACK.
            Value::String("YES".to_string()),
            Value::String("YES".to_string()),
        ],
        vec![
            Value::String("InnoDB".to_string()),
            Value::String("YES".to_string()),
            Value::String("Supports transactions, row-level locking, and foreign keys".to_string()),
            Value::String("YES".to_string()),
            Value::String("YES".to_string()),
            Value::String("YES".to_string()),
        ],
    ]
}

struct BuiltinPlugin {
    name: &'static str,
    plugin_type: &'static str,
    description: &'static str,
}

const BUILTIN_PLUGINS: &[BuiltinPlugin] = &[
    BuiltinPlugin {
        name: "mydb",
        plugin_type: "STORAGE ENGINE",
        description: "mydb transactional heap storage engine",
    },
    BuiltinPlugin {
        name: "mysql_native_password",
        plugin_type: "AUTHENTICATION",
        description: "MySQL native password authentication",
    },
    BuiltinPlugin {
        name: "sha256_password",
        plugin_type: "AUTHENTICATION",
        description: "SHA-256 password authentication",
    },
    BuiltinPlugin {
        name: "caching_sha2_password",
        plugin_type: "AUTHENTICATION",
        description: "Caching SHA-2 password authentication",
    },
];

pub(crate) fn information_schema_plugin_rows() -> Vec<Vec<Value>> {
    BUILTIN_PLUGINS
        .iter()
        .map(|plugin| {
            vec![
                Value::String(plugin.name.to_string()),
                Value::String(env!("CARGO_PKG_VERSION").to_string()),
                Value::String("ACTIVE".to_string()),
                Value::String(plugin.plugin_type.to_string()),
                Value::String("1.0".to_string()),
                Value::Null,
                Value::Null,
                Value::String("mydb contributors".to_string()),
                Value::String(plugin.description.to_string()),
                Value::String("GPL".to_string()),
                Value::String("FORCE".to_string()),
            ]
        })
        .collect()
}

pub(crate) fn show_plugins_result() -> QueryResponse {
    let columns = vec![
        col("Name", ColumnType::VarString),
        col("Status", ColumnType::VarString),
        col("Type", ColumnType::VarString),
        col("Library", ColumnType::VarString),
        col("License", ColumnType::VarString),
    ];
    let rows = information_schema_plugin_rows()
        .into_iter()
        .map(|row| {
            vec![
                row[0].clone(),
                row[2].clone(),
                row[3].clone(),
                row[5].clone(),
                row[9].clone(),
            ]
        })
        .collect();
    QueryResponse::ResultSet(ResultSet { columns, rows })
}

pub(crate) fn show_open_tables(
    db: &Database,
    session: &SessionContext,
    database: Option<&str>,
    filter: Option<&ShowFilter>,
) -> Result<QueryResponse, QueryError> {
    let database = database.unwrap_or(&session.current_database);
    let columns = vec![
        col("Database", ColumnType::VarString),
        col("Table", ColumnType::VarString),
        col("In_use", ColumnType::LongLong),
        col("Name_locked", ColumnType::LongLong),
    ];
    // mydb has no MySQL-style table-definition cache; the honest analogue is
    // the set of base tables in the catalog for the target database. In_use
    // and Name_locked are 0 — mydb does not maintain the per-table
    // cache-use / name-lock counters this statement would otherwise expose.
    let catalog = db.catalog();
    let mut names: Vec<String> = catalog
        .tables
        .values()
        .filter(|table| table.database.eq_ignore_ascii_case(database))
        .filter(|table| {
            crate::engine::information_schema::object_visible_to_session(
                db,
                session,
                &table.database,
                Some(&table.name),
            )
        })
        .map(|table| table.name.clone())
        .collect();
    names.sort();
    let mut rows = names
        .into_iter()
        .map(|name| {
            vec![
                Value::String(database.to_string()),
                Value::String(name),
                Value::Int(0),
                Value::Int(0),
            ]
        })
        .collect();
    apply_show_filter(&mut rows, &columns, filter, "Table")?;
    Ok(QueryResponse::ResultSet(ResultSet { columns, rows }))
}

pub(crate) fn show_engine_result(
    db: &Database,
    engine: &str,
    kind: ShowEngineKind,
) -> Result<QueryResponse, QueryError> {
    if !engine.eq_ignore_ascii_case("mydb") {
        // Match MySQL's reaction to `SHOW ENGINE INNODB STATUS` against an
        // engine it doesn't have: a clean 1286 rather than a syntax error.
        return Err(crate::storage::error(
            crate::error::catalog::ER_UNKNOWN_STORAGE_ENGINE,
            format!("Unknown storage engine '{engine}'"),
        ));
    }
    match kind {
        ShowEngineKind::Status => Ok(show_engine_mydb_status(db)),
        // mydb does not surface per-latch spin/wait/call counters the way
        // InnoDB's MUTEX monitor does; emit the correct 3-column shape with
        // no rows rather than fabricate statistics.
        ShowEngineKind::Mutex => Ok(QueryResponse::ResultSet(ResultSet {
            columns: show_engine_mydb_status_columns(),
            rows: Vec::new(),
        })),
    }
}

fn statement_cache_field(
    project: impl FnOnce(&crate::engine::cache::CacheStats) -> String,
) -> String {
    crate::engine::cache::global_stats()
        .as_ref()
        .map(project)
        .unwrap_or_else(|| "0".to_string())
}

fn plan_cache_field(
    project: impl FnOnce(&crate::engine::cache::PlanCacheStats) -> String,
) -> String {
    crate::engine::cache::global_plan_stats()
        .as_ref()
        .map(project)
        .unwrap_or_else(|| "0".to_string())
}

fn prepared_dml_cache_field(
    project: impl FnOnce(&crate::engine::cache::PlanCacheStats) -> String,
) -> String {
    crate::engine::cache::global_prepared_dml_stats()
        .as_ref()
        .map(project)
        .unwrap_or_else(|| "0".to_string())
}

fn prepared_dml_batch_cache_field(
    project: impl FnOnce(&crate::engine::cache::PlanCacheStats) -> String,
) -> String {
    crate::engine::cache::global_prepared_dml_batch_stats()
        .as_ref()
        .map(project)
        .unwrap_or_else(|| "0".to_string())
}

fn plan_cache_hit_pct_string(stats: &crate::engine::cache::PlanCacheStats) -> String {
    let total = stats.hits.saturating_add(stats.misses);
    if total == 0 {
        "0".to_string()
    } else {
        (stats.hits.saturating_mul(100) / total).to_string()
    }
}

/// Process peak resident bytes (Windows peak working set). Best-effort —
/// returns 0 if the syscall fails. This is the OS ground truth that the
/// engine's `tracked_memory_bytes` roll-up is measured against; the gap
/// between them is untracked memory (allocator overhead, HNSW graphs,
/// per-thread caches, catalog).
fn peak_resident_bytes() -> u64 {
    crate::platform::memory::peak_resident_bytes().unwrap_or(0)
}

// Shared status-row layout. Metric expressions retain their native formatting
// and evaluate once, in declaration order; no dynamic dispatch is introduced.
macro_rules! status_rows {
    ($($name:literal => $value:expr),* $(,)?) => {
        vec![$(($name, $value)),*]
    };
}

pub(crate) fn show_engine_mydb_status(db: &Database) -> QueryResponse {
    let status = db.storage_status();
    let exec_permit = crate::util::exec_permit::status_snapshot();
    let async_row_lock = crate::mysql::session::async_row_lock_retry_stats();
    let ddl_copy = db.ddl_copy_perf_snapshot();
    let online_constraint = db.online_constraint_perf_snapshot();
    let load_shadow = db.load_shadow_perf_snapshot();
    let index_build_extended = crate::storage::index_build_extended_perf_snapshot();
    let connection_metrics = crate::mysql::server::connection_metrics();
    let (controlled_current, controlled_peak, controlled_categories, controlled_rejections) =
        crate::engine::executor::mem_budget::controlled_memory_snapshot();
    let known_cache_bytes = crate::engine::cache_registry::collect()
        .into_iter()
        .filter(|cache| {
            matches!(
                cache.name,
                "statement_ast"
                    | "physical_plan"
                    | "prepared_dml_plan"
                    | "prepared_dml_batch"
                    | "col_meta_intern"
            )
        })
        .fold(0u128, |sum, cache| {
            sum.saturating_add(cache.bytes.unwrap_or(0) as u128)
        });
    let admission_reclaim = crate::engine::executor::mem_budget::admission_reclaim_snapshot();
    let idle_reclaim = crate::engine::memory_reclaimer::snapshot();
    let controlled_rejections_by_category =
        crate::engine::executor::mem_budget::controlled_memory_rejections_by_category();
    let work_area = crate::engine::executor::work_area::snapshot();
    let rows = status_rows! {
        "durability" => status.durability.to_string(),
        "ddl_copy_total" => ddl_copy[0].to_string(),
        "ddl_copy_scan_us" => ddl_copy[1].to_string(),
        "ddl_copy_convert_us" => ddl_copy[2].to_string(),
        "ddl_copy_index_us" => ddl_copy[3].to_string(),
        "ddl_copy_replay_us" => ddl_copy[4].to_string(),
        "ddl_copy_swap_us" => ddl_copy[5].to_string(),
        "ddl_copy_source_bytes" => ddl_copy[6].to_string(),
        "ddl_copy_shadow_bytes" => ddl_copy[7].to_string(),
        "ddl_copy_spill_bytes" => ddl_copy[8].to_string(),
        "ddl_copy_direct_heap_builds" => ddl_copy[9].to_string(),
        "ddl_copy_peak_batch_bytes" => ddl_copy[10].to_string(),
        "ddl_copy_index_feed_us" => ddl_copy[11].to_string(),
        "ddl_copy_index_build_us" => ddl_copy[12].to_string(),
        "ddl_final_barrier_count" => ddl_copy[13].to_string(),
        "ddl_final_barrier_total_us" => ddl_copy[14].to_string(),
        "ddl_final_barrier_max_us" => ddl_copy[15].to_string(),
        "ddl_final_barrier_le_1ms" => ddl_copy[16].to_string(),
        "ddl_final_barrier_le_10ms" => ddl_copy[17].to_string(),
        "ddl_final_barrier_le_100ms" => ddl_copy[18].to_string(),
        "ddl_final_barrier_le_1s" => ddl_copy[19].to_string(),
        "ddl_final_barrier_gt_1s" => ddl_copy[20].to_string(),
        "ddl_copy_barrier_wait_us" => ddl_copy[21].to_string(),
        "ddl_copy_lock_hold_us" => ddl_copy[22].to_string(),
        "ddl_copy_lock_hold_max_us" => ddl_copy[23].to_string(),
        "ddl_copy_final_tail_rows" => ddl_copy[24].to_string(),
        "load_shadow_hits" => load_shadow[0].to_string(),
        "load_shadow_fallbacks" => load_shadow[1].to_string(),
        "load_shadow_source_bytes" => load_shadow[2].to_string(),
        "load_shadow_heap_bytes" => load_shadow[3].to_string(),
        "load_shadow_index_bytes" => load_shadow[4].to_string(),
        "load_shadow_heap_write_us" => load_shadow[5].to_string(),
        "load_shadow_index_feed_us" => load_shadow[6].to_string(),
        "load_shadow_index_finish_us" => load_shadow[7].to_string(),
        "load_shadow_sync_swap_us" => load_shadow[8].to_string(),
        "load_shadow_parse_encode_us" => load_shadow[9].to_string(),
        "load_direct_decode_rows" => load_shadow[10].to_string(),
        "load_direct_decode_fallback_rows" => load_shadow[11].to_string(),
        "load_auto_increment_fast_batches" => load_shadow[12].to_string(),
        "load_auto_increment_fast_rows" => load_shadow[13].to_string(),
        "load_auto_increment_assign_batches" => load_shadow[14].to_string(),
        "load_auto_increment_assign_rows" => load_shadow[15].to_string(),
        "current_lsn" => status.current_lsn.to_string(),
        "durable_lsn" => status.durable_lsn.to_string(),
        // WAL improvement plan Phase 4 observability:
        // - sync method actually in force (platform/durability-resolved + overrides)
        // - durable lag = appended-but-not-yet-fsynced LSN distance
        "wal_sync_method" => crate::wal::resolved_wal_sync_method().as_str().to_string(),
        "wal_durable_lag_lsn" => status .current_lsn .saturating_sub(status.durable_lsn) .to_string(),
        "wal_bytes" => status.wal_bytes.to_string(),
        "wal_logical_records" => status.wal_logical_records.to_string(),
        "wal_page_fallback_records" => status.wal_page_fallback_records.to_string(),
        "wal_index_batch_records" => status.wal_index_batch_records.to_string(),
        "wal_heap_payload_bytes" => status.wal_heap_payload_bytes.to_string(),
        "wal_index_payload_bytes" => status.wal_index_payload_bytes.to_string(),
        "wal_page_payload_bytes" => status.wal_page_payload_bytes.to_string(),
        "wal_heap_fpi_payload_bytes" => status.wal_heap_fpi_payload_bytes.to_string(),
        "reduced_redo_transactions" => status.reduced_redo_transactions.to_string(),
        "reduced_redo_skipped_records" => status.reduced_redo_skipped_records.to_string(),
        "reduced_redo_skipped_payload_bytes" => status.reduced_redo_skipped_payload_bytes.to_string(),
        "reduced_redo_fallback_records" => status.reduced_redo_fallback_records.to_string(),
        "reduced_redo_force_flushes" => status.reduced_redo_force_flushes.to_string(),
        "reduced_redo_force_flush_us" => status.reduced_redo_force_flush_us.to_string(),
        "wal_index_page_payload_bytes" => status.wal_index_page_payload_bytes.to_string(),
        "wal_other_page_payload_bytes" => status.wal_other_page_payload_bytes.to_string(),
        "wal_sync_total_us" => status.wal_sync_total_us.to_string(),
        "wal_sync_avg_us" => status.wal_sync_avg_us.to_string(),
        "wal_sync_max_us" => status.wal_sync_max_us.to_string(),
        "wal_rotation_count" => status.wal_rotation_count.to_string(),
        "wal_rotation_critical_total_us" => status.wal_rotation_critical_total_us.to_string(),
        "wal_rotation_critical_max_us" => status.wal_rotation_critical_max_us.to_string(),
        "wal_pending_sealed_sync" => status.wal_pending_sealed_sync.to_string(),
        "checkpoint_lsn" => status.checkpoint_lsn.to_string(),
        "checkpoint_age_lsn" => status.checkpoint_age_lsn.to_string(),
        "checkpoints_total" => status.checkpoints_total.to_string(),
        "checkpoint_failures_total" => status.checkpoint_failures_total.to_string(),
        "checkpoint_preflush_failures_total" => status.checkpoint_preflush_failures_total.to_string(),
        "checkpoint_redo_lsn" => status.checkpoint_redo_lsn.to_string(),
        "checkpoint_redo_age_lsn" => status.checkpoint_redo_age_lsn.to_string(),
        "last_checkpoint_dirty_pages" => status.last_checkpoint_dirty_pages.to_string(),
        "last_checkpoint_wal_reclaimed_bytes" => status.last_checkpoint_wal_reclaimed_bytes.to_string(),
        "checkpoint_wal_reclaimed_bytes_total" => status.checkpoint_wal_reclaimed_bytes_total.to_string(),
        "restartpoints_total" => status.restartpoints_total.to_string(),
        "replica_applied_lsn" => status.replica_applied_lsn.to_string(),
        "last_checkpoint_trigger" => status.last_checkpoint_trigger.to_string(),
        "last_checkpoint_kind" => status.last_checkpoint_kind.to_string(),
        "last_checkpoint_snapshot_us" => status.last_checkpoint_snapshot_us.to_string(),
        "last_checkpoint_flush_us" => status.last_checkpoint_flush_us.to_string(),
        "last_checkpoint_sync_us" => status.last_checkpoint_sync_us.to_string(),
        "last_checkpoint_compact_us" => status.last_checkpoint_compact_us.to_string(),
        "last_checkpoint_duration_us" => status.last_checkpoint_duration_us.to_string(),
        "checkpoint_duration_total_us" => status.checkpoint_duration_total_us.to_string(),
        "checkpoint_duration_max_us" => status.checkpoint_duration_max_us.to_string(),
        "buffer_hits" => status.buffer_hits.to_string(),
        "buffer_misses" => status.buffer_misses.to_string(),
        "buffer_hit_pct" => status.buffer_hit_pct.to_string(),
        "dirty_heap_pages" => status.dirty_heap_pages.to_string(),
        "dirty_index_pages" => status.dirty_index_pages.to_string(),
        "buffer_pool_used_pages" => status.buffer_pool_used_pages.to_string(),
        "buffer_pool_dirty_pages" => status.buffer_pool_dirty_pages.to_string(),
        "buffer_pool_freeable_pages" => status.buffer_pool_freeable_pages.to_string(),
        "buffer_pool_evictions" => status.buffer_pool_evictions.to_string(),
        "buffer_pool_flushes" => status.buffer_pool_flushes.to_string(),
        "buffer_pool_flush_errors" => status.buffer_pool_flush_errors.to_string(),
        "buffer_pool_background_io_errors" => status.buffer_pool_background_io_errors.to_string(),
        "buffer_pool_pinned_pages" => status.buffer_pool_pinned_pages.to_string(),
        "buffer_pool_waits" => status.buffer_pool_waits.to_string(),
        "buffer_pool_wait_ms" => status.buffer_pool_wait_ms.to_string(),
        "buffer_pool_background_flushes" => status.buffer_pool_background_flushes.to_string(),
        "buffer_pool_shards" => status.buffer_pool_shards.to_string(),
        "buffer_pool_read_ios" => status.buffer_pool_read_ios.to_string(),
        "buffer_pool_read_ios_heap" => status.buffer_pool_read_ios_by_kind[0].to_string(),
        "buffer_pool_read_ios_index" => status.buffer_pool_read_ios_by_kind[1].to_string(),
        "buffer_pool_read_ios_overflow" => status.buffer_pool_read_ios_by_kind[2].to_string(),
        "buffer_pool_read_ios_vector" => status.buffer_pool_read_ios_by_kind[3].to_string(),
        "buffer_pool_read_ios_zone_map" => status.buffer_pool_read_ios_by_kind[4].to_string(),
        "buffer_pool_read_syscalls" => status.buffer_pool_read_syscalls.to_string(),
        "buffer_pool_batch_read_ops" => status.buffer_pool_batch_read_ops.to_string(),
        "buffer_pool_batch_read_pages" => status.buffer_pool_batch_read_pages.to_string(),
        "buffer_pool_write_ios" => status.buffer_pool_write_ios.to_string(),
        "buffer_pool_write_syscalls" => status.buffer_pool_write_syscalls.to_string(),
        "buffer_pool_prefetches" => status.buffer_pool_prefetches.to_string(),
        "buffer_pool_prefetch_hits" => status.buffer_pool_prefetch_hits.to_string(),
        "buffer_pool_prefetch_useful" =>
            // A prefetch hit means the prefetched page was subsequently used;
            // both labels intentionally report that same physical event.
            status.buffer_pool_prefetch_hits.to_string(),
        "buffer_pool_prefetch_wasted" => status.buffer_pool_prefetch_wasted.to_string(),
        "buffer_pool_prefetch_demand_waits" => status.buffer_pool_prefetch_demand_waits.to_string(),
        "buffer_pool_eviction_scans" => status.buffer_pool_eviction_scans.to_string(),
        "buffer_pool_io_threads" => status.buffer_pool_io_threads.to_string(),
        "buffer_pool_prefetch_pages" => status.buffer_pool_prefetch_pages.to_string(),
        "buffer_pool_small_scan_pct" => status.buffer_pool_small_scan_pct.to_string(),
        "buffer_pool_small_scan_threshold_pages" => status.buffer_pool_small_scan_threshold_pages.to_string(),
        "buffer_pool_small_scan_accesses" => status.buffer_pool_small_scan_accesses.to_string(),
        "buffer_pool_small_scan_cold_accesses" => status.buffer_pool_small_scan_cold_accesses.to_string(),
        "buffer_pool_small_scan_promotions" => status.buffer_pool_small_scan_promotions.to_string(),
        "buffer_pool_small_scan_read_ios" => status.buffer_pool_small_scan_read_ios.to_string(),
        // P0-1 - extended buffer pool observability.
        "buffer_pool_read_ios_normal" => status.buffer_pool_read_ios_normal.to_string(),
        "buffer_pool_read_ios_scan" => status.buffer_pool_read_ios_scan.to_string(),
        "buffer_pool_read_ios_prefetch" => status.buffer_pool_read_ios_prefetch.to_string(),
        "buffer_pool_prefetch_queue_drops" => status.buffer_pool_prefetch_queue_drops.to_string(),
        "buffer_pool_prefetch_duplicate_drops" => status.buffer_pool_prefetch_duplicate_drops.to_string(),
        "buffer_pool_prefetch_pressure_drops" => status.buffer_pool_prefetch_pressure_drops.to_string(),
        "buffer_pool_prefetch_queue_full_drops" => status.buffer_pool_prefetch_queue_full_drops.to_string(),
        "buffer_pool_prefetch_stale_drops" => status.buffer_pool_prefetch_stale_drops.to_string(),
        "buffer_pool_prefetch_queue_wait_avg_us" => status.buffer_pool_prefetch_queue_wait_avg_us.to_string(),
        "buffer_pool_prefetch_lead_avg_us" => status.buffer_pool_prefetch_lead_avg_us.to_string(),
        "buffer_pool_ensure_capacity_busy_us" => status.buffer_pool_ensure_capacity_busy_us.to_string(),
        "buffer_pool_file_handle_wait_ns" => status.buffer_pool_file_handle_wait_ns.to_string(),
        "buffer_pool_dirty_ratio_samples" => status.buffer_pool_dirty_ratio_samples.to_string(),
        "buffer_pool_dirty_ratio_histogram" =>
            format!( "0-12:{} 12-25:{} 25-37:{} 37-50:{} 50-62:{} 62-75:{} 75-87:{} 87-100:{}", status.buffer_pool_dirty_ratio_histogram[0], status.buffer_pool_dirty_ratio_histogram[1], status.buffer_pool_dirty_ratio_histogram[2], status.buffer_pool_dirty_ratio_histogram[3], status.buffer_pool_dirty_ratio_histogram[4], status.buffer_pool_dirty_ratio_histogram[5], status.buffer_pool_dirty_ratio_histogram[6], status.buffer_pool_dirty_ratio_histogram[7], ),
        "buffer_pool_shard_imbalance_pct" => status.buffer_pool_shard_imbalance_pct.to_string(),
        "buffer_pool_shard_max_hits" => status.buffer_pool_shard_max_hits.to_string(),
        "buffer_pool_shard_min_hits" => status.buffer_pool_shard_min_hits.to_string(),
        "buffer_pool_scan_origin_share_pct" => status.buffer_pool_scan_origin_share_pct.to_string(),
        "buffer_pool_scan_origin_evictions" => status.buffer_pool_scan_origin_evictions.to_string(),
        "buffer_pool_cold_recycle_hits" => status.buffer_pool_cold_recycle_hits.to_string(),
        "buffer_pool_cold_recycle_fallbacks" => status.buffer_pool_cold_recycle_fallbacks.to_string(),
        "buffer_pool_raw_scan_pages" => status.buffer_pool_raw_scan_pages.to_string(),
        "buffer_pool_raw_page_allocations" => status.buffer_pool_raw_page_allocations.to_string(),
        "buffer_pool_raw_page_reuse_hits" => status.buffer_pool_raw_page_reuse_hits.to_string(),
        "buffer_pool_raw_page_reuse_misses" => status.buffer_pool_raw_page_reuse_misses.to_string(),
        "buffer_pool_recycled_raw_pages" => status.buffer_pool_recycled_raw_pages.to_string(),
        "buffer_pool_pages_heap" => status.buffer_pool_pages_heap.to_string(),
        "buffer_pool_pages_index" => status.buffer_pool_pages_index.to_string(),
        "buffer_pool_pages_lob" => status.buffer_pool_pages_lob.to_string(),
        "buffer_pool_pages_vector" => status.buffer_pool_pages_vector.to_string(),
        "buffer_pool_pages_zone_map" => status.buffer_pool_pages_zone_map.to_string(),
        "buffer_pool_quota_evictions" => status.buffer_pool_quota_evictions.to_string(),
        "buffer_pool_file_syncs" => status.buffer_pool_file_syncs.to_string(),
        "buffer_pool_file_sync_skipped" => status.buffer_pool_file_sync_skipped.to_string(),
        "buffer_pool_flush_data_cache_pages" => status.buffer_pool_flush_data_cache_pages.to_string(),
        "buffer_pool_flush_data_cache_us" => status.buffer_pool_flush_data_cache_us.to_string(),
        "buffer_pool_flush_data_cache_short_circuits" =>
            status .buffer_pool_flush_data_cache_short_circuits .to_string(),
        "buffer_pool_evict_sync_dirty_flushes" => status.buffer_pool_evict_sync_dirty_flushes.to_string(),
        "buffer_pool_evict_sync_dirty_flush_us" => status.buffer_pool_evict_sync_dirty_flush_us.to_string(),
        "buffer_pool_cooldown_deferred_flushes" => status.buffer_pool_cooldown_deferred_flushes.to_string(),
        "buffer_pool_headroom_flushes" => status.buffer_pool_headroom_flushes.to_string(),
        "buffer_pool_checkpoint_flush_pages" => status.buffer_pool_checkpoint_flush_pages.to_string(),
        "buffer_pool_checkpoint_flush_us" => status.buffer_pool_checkpoint_flush_us.to_string(),
        "buffer_pool_checkpoint_preflush_pages" => status.buffer_pool_checkpoint_preflush_pages.to_string(),
        "buffer_pool_checkpoint_preflush_us" => status.buffer_pool_checkpoint_preflush_us.to_string(),
        "buffer_pool_prefetch_queue_depth_peak" => status.buffer_pool_prefetch_queue_depth_peak.to_string(),
        "buffer_pool_oldest_dirty_lsn" => status.buffer_pool_oldest_dirty_lsn.to_string(),
        "bulk_index_direct_builds" => status.bulk_index_direct_builds.to_string(),
        "bulk_index_direct_pages" => status.bulk_index_direct_pages.to_string(),
        "bulk_index_direct_us" => status.bulk_index_direct_us.to_string(),
        "bulk_index_buffered_builds" => status.bulk_index_buffered_builds.to_string(),
        "bulk_index_buffered_pages" => status.bulk_index_buffered_pages.to_string(),
        "bulk_index_buffered_us" => status.bulk_index_buffered_us.to_string(),
        "index_build_requested_workers" => status.index_build_requested_workers.to_string(),
        "index_build_effective_workers" => status.index_build_effective_workers.to_string(),
        "index_build_parallel_builds" => status.index_build_parallel_builds.to_string(),
        "index_build_serial_builds" => status.index_build_serial_builds.to_string(),
        "index_build_degree_downgrades" => status.index_build_degree_downgrades.to_string(),
        "online_constraint_lock_hold_us" => online_constraint[0].to_string(),
        "online_constraint_lock_hold_max_us" => online_constraint[1].to_string(),
        "online_constraint_scanned_rows" => online_constraint[2].to_string(),
        "online_constraint_replayed_rows" => online_constraint[3].to_string(),
        "online_constraint_final_tail_max" => online_constraint[4].to_string(),
        "online_constraint_index_merge_accepted" => online_constraint[5].to_string(),
        "online_constraint_index_merge_fallbacks" => online_constraint[6].to_string(),
        "online_constraint_index_merge_entries" => online_constraint[7].to_string(),
        "online_constraint_baseline_builds" => online_constraint[8].to_string(),
        "online_constraint_index_merge_parallel_runs" => online_constraint[9].to_string(),
        "online_constraint_index_merge_workers_peak" => online_constraint[10].to_string(),
        "online_constraint_baseline_parallel_runs" => online_constraint[11].to_string(),
        "online_constraint_baseline_workers_peak" => online_constraint[12].to_string(),
        "online_index_prepare_us" => status.online_index_prepare_us.to_string(),
        "online_index_replay_us" => status.online_index_replay_us.to_string(),
        "online_index_validation_us" => status.online_index_validation_us.to_string(),
        "online_index_publish_us" => status.online_index_publish_us.to_string(),
        "online_index_barrier_wait_us" => status.online_index_barrier_wait_us.to_string(),
        "online_index_tail_entries_peak" => status.online_index_tail_entries_peak.to_string(),
        "online_index_tail_bytes_peak" => status.online_index_tail_bytes_peak.to_string(),
        "online_index_log_bytes_peak" => status.online_index_log_bytes_peak.to_string(),
        "index_build_scan_us" => status.index_build_scan_us.to_string(),
        "index_build_sort_us" => status.index_build_sort_us.to_string(),
        "index_build_leaf_us" => status.index_build_leaf_us.to_string(),
        "index_build_internal_us" => status.index_build_internal_us.to_string(),
        "index_build_fsync_us" => status.index_build_fsync_us.to_string(),
        "index_build_rows" => status.index_build_rows.to_string(),
        "index_build_heap_scans" => status.index_build_heap_scans.to_string(),
        "index_build_merge_us" => status.index_build_merge_us.to_string(),
        "index_build_spill_write_us" => status.index_build_spill_write_us.to_string(),
        "index_build_spill_read_us" => status.index_build_spill_read_us.to_string(),
        "index_build_peak_active_workers" => status.index_build_peak_active_workers.to_string(),
        "index_build_dictionary_builds" => index_build_extended[0].to_string(),
        "index_build_dictionary_rows" => index_build_extended[1].to_string(),
        "index_build_encoded_runs" => index_build_extended[2].to_string(),
        "index_build_scratch_granted_peak_bytes" => index_build_extended[3].to_string(),
        "buffer_pool_dirty_oldest_len" => status.buffer_pool_dirty_oldest_len.to_string(),
        "spill_bytes_written" => status.spill_bytes_written.to_string(),
        "spill_bytes_read" => status.spill_bytes_read.to_string(),
        "catalog_txn_commits" => status.catalog_txn_commits.to_string(),
        "catalog_txn_rollbacks" => status.catalog_txn_rollbacks.to_string(),
        "catalog_mutated_rows" => status.catalog_mutated_rows.to_string(),
        "catalog_epoch" => status.catalog_epoch.to_string(),
        "catalog_cache_limit_bytes" => status.catalog_cache_limit_bytes.to_string(),
        "catalog_cache_bytes" => status.catalog_cache_bytes.to_string(),
        "catalog_cache_entries" => status.catalog_cache_entries.to_string(),
        "dml_insert_rows" => status.dml_insert_rows.to_string(),
        "dml_update_rows" => status.dml_update_rows.to_string(),
        "dml_delete_rows" => status.dml_delete_rows.to_string(),
        "dml_ignore_warnings" => status.dml_ignore_warnings.to_string(),
        "dml_rollback_inserted_rows" => status.dml_rollback_inserted_rows.to_string(),
        "dml_lob_leaks_recovered" => status.dml_lob_leaks_recovered.to_string(),
        "dml_upsert_fast_path_hits" => status.dml_upsert_fast_path_hits.to_string(),
        "dml_upsert_batch_attempts" => status.dml_upsert_batch_attempts.to_string(),
        "dml_upsert_batch_rows" => status.dml_upsert_batch_rows.to_string(),
        "dml_upsert_batch_conflict_rows" => status.dml_upsert_batch_conflict_rows.to_string(),
        "dml_upsert_batch_fallback_rows" => status.dml_upsert_batch_fallback_rows.to_string(),
        "dml_upsert_batch_race_rollbacks" => status.dml_upsert_batch_race_rollbacks.to_string(),
        "dml_fk_cycle_short_circuits" => status.dml_fk_cycle_short_circuits.to_string(),
        "load_data_active" => status.load_data_active.to_string(),
        "load_data_rows_loaded" => status.load_data_rows_loaded.to_string(),
        // M1/M6 (LOB) — chain counts and bytes for large-field chain reuse / externalization.
        "dml_lob_chains_reused" => status.dml_lob_chains_reused.to_string(),
        "dml_lob_bytes_reused" => status.dml_lob_bytes_reused.to_string(),
        "dml_lob_chains_externalized" => status.dml_lob_chains_externalized.to_string(),
        "dml_lob_bytes_externalized" => status.dml_lob_bytes_externalized.to_string(),
        "dml_lob_pages_shrunk" => status.dml_lob_pages_shrunk.to_string(),
        "dml_lob_leaks_unrecovered" => status.dml_lob_leaks_unrecovered.to_string(),
        "active_transactions" => status.active_transactions.to_string(),
        "oldest_snapshot_scn" => status.oldest_snapshot_scn.to_string(),
        "oldest_active_txn_ms" => status.oldest_active_txn_ms.to_string(),
        "latch_waits" => status.latch_waits.to_string(),
        "lock_waits" => status.lock_waits.to_string(),
        "active_lock_waits" => status.active_lock_waits.to_string(),
        "oldest_lock_wait_ms" => status.oldest_lock_wait_ms.to_string(),
        "deadlocks" => status.deadlocks.to_string(),
        "lock_wait_ms" => status.lock_wait_ms.to_string(),
        "xa_prepared_active" => db.xa_status_counters().0.to_string(),
        "xa_commits_total" => db.xa_status_counters().1.to_string(),
        "xa_rollbacks_total" => db.xa_status_counters().2.to_string(),
        // rowid-6bytes M2 — slot reclaim. `slot_reclaim_total` counts
        // tombstones (`ROW_FLAG_DELETED`) flipped to `ROW_FLAG_FREE`
        // since startup; `slot_reclaim_runs` is the number of undo
        // purge passes that did so; `slot_reclaim_errors` is the
        // per-slot reclaim failures (heap open / WAL append). Failures
        // leave the slot in DELETED state — correctness is preserved,
        // only slot reuse stalls.
        "slot_reclaim_total" => db.slot_reclaim_status_counters().0.to_string(),
        "slot_reclaim_runs" => db.slot_reclaim_status_counters().1.to_string(),
        "slot_reclaim_errors" => db.slot_reclaim_status_counters().2.to_string(),
        // P0/4 — slot reclaim retry queue. Failed pairs sit in the
        // quarantine until they either succeed on retry or hit the
        // per-pair attempt cap, at which point they count as
        // permanent failures.
        "slot_reclaim_quarantine_size" => db.slot_reclaim_quarantine_counters().0.to_string(),
        "slot_reclaim_permanent_failures" => db.slot_reclaim_quarantine_counters().1.to_string(),
        // P2/14 — undo I/O latency observability. `_samples` count the
        // per-frame buffered or write-through path; `_ns_total` is the
        // cumulative wall-clock the undo path spent inside that I/O
        // (sum across all worker threads since process start); `_p50`
        // / `_p99` are log-bucket approximations (factor of 10).
        // `scan_total_ns` is the startup-recovery scan cost (once
        // per process unless re-opened in tests).
        "undo_io_write_frame_samples" => db.undo_io_latency_snapshot().0.to_string(),
        "undo_io_write_frame_ns_total" => db.undo_io_latency_snapshot().1.to_string(),
        "undo_io_write_frame_ns_p50" => db.undo_io_latency_snapshot().2.to_string(),
        "undo_io_write_frame_ns_p99" => db.undo_io_latency_snapshot().3.to_string(),
        "undo_io_flush_samples" => db.undo_io_latency_snapshot().4.to_string(),
        "undo_io_flush_ns_total" => db.undo_io_latency_snapshot().5.to_string(),
        "undo_io_flush_ns_p50" => db.undo_io_latency_snapshot().6.to_string(),
        "undo_io_flush_ns_p99" => db.undo_io_latency_snapshot().7.to_string(),
        "undo_io_scan_ns_total" => db.undo_io_latency_snapshot().8.to_string(),
        "undo_disk_cap_softfail_warns_emitted" => db.undo_disk_cap_softfail_warns_emitted().to_string(),
        // P1/6 — number of rollback-segment shards in use. Equal to
        // `MYDB_UNDO_WRITE_SHARDS` (default 8); operators read this
        // to confirm sharding is live before tuning the env.
        "undo_write_shards" => db.undo_shard_count().to_string(),
        // Server-side cursor counters (COM_STMT_EXECUTE with
        // CURSOR_TYPE_READ_ONLY). `_active` is the snapshot count from
        // the registry; the totals are monotonic counters since
        // process start. `_evicted_idle_total` and the two `rejected_*`
        // counters are the protective signals — non-zero values mean
        // either the idle-timeout fired or the per-session / global
        // cap was hit, both of which are worth investigating.
        "mydb_cursors_active" => crate::engine::cursor_registry::global() .active_count() .to_string(),
        "mydb_cursors_opened_total" =>
            crate::engine::cursor_registry::global() .stats() .opened_total .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "mydb_cursors_closed_total" =>
            crate::engine::cursor_registry::global() .stats() .closed_total .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "mydb_cursors_rows_fetched_total" =>
            crate::engine::cursor_registry::global() .stats() .rows_fetched_total .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "mydb_cursors_evicted_idle_total" =>
            crate::engine::cursor_registry::global() .stats() .evicted_idle_total .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "mydb_cursors_rejected_session_cap_total" =>
            crate::engine::cursor_registry::global() .stats() .rejected_session_cap .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "mydb_cursors_rejected_global_cap_total" =>
            crate::engine::cursor_registry::global() .stats() .rejected_global_cap .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        // SQL-level cursors opened by `DECLARE <name> CURSOR FOR ...`.
        // Same shape as the prepared-cursor counters above; presented
        // separately so an operator can tell which surface is generating
        // pressure.
        "mydb_named_cursors_active" => crate::engine::cursor_registry::named_active_count().to_string(),
        "mydb_named_cursors_opened_total" =>
            crate::engine::cursor_registry::named_stats_snapshot() .opened_total .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "mydb_named_cursors_closed_total" =>
            crate::engine::cursor_registry::named_stats_snapshot() .closed_total .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "mydb_named_cursors_rows_fetched_total" =>
            crate::engine::cursor_registry::named_stats_snapshot() .rows_fetched_total .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "mydb_named_cursors_evicted_idle_total" =>
            crate::engine::cursor_registry::named_stats_snapshot() .evicted_idle_total .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "mydb_named_cursors_rejected_session_cap_total" =>
            crate::engine::cursor_registry::named_stats_snapshot() .rejected_session_cap .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "mydb_named_cursors_rejected_global_cap_total" =>
            crate::engine::cursor_registry::named_stats_snapshot() .rejected_global_cap .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "group_commit_us" => status.group_commit_us.to_string(),
        "group_commit_adaptive" => u8::from(status.group_commit_adaptive).to_string(),
        "group_commit_batches" => status.group_commit_batches.to_string(),
        "group_commit_waiters" => status.group_commit_waiters.to_string(),
        "avg_group_commit_size" => status.avg_group_commit_size.to_string(),
        "durability_worker_enabled" => u8::from(status.durability_worker_enabled).to_string(),
        "durability_queue_depth" => status.durability_queue_depth.to_string(),
        "durability_queue_depth_peak" => status.durability_queue_depth_peak.to_string(),
        "durability_batches" => status.durability_batches.to_string(),
        "durability_requests" => status.durability_requests.to_string(),
        "durability_batch_max" => status.durability_batch_max.to_string(),
        "group_commit_leader_wait_total_us" => status.group_commit_leader_wait_total_us.to_string(),
        "group_commit_follower_wait_total_us" => status.group_commit_follower_wait_total_us.to_string(),
        "group_commit_sync_op_total_us" => status.group_commit_sync_op_total_us.to_string(),
        "group_commit_leader_wait_max_us" => status.group_commit_leader_wait_max_us.to_string(),
        "group_commit_follower_wait_max_us" => status.group_commit_follower_wait_max_us.to_string(),
        "group_commit_sync_op_max_us" => status.group_commit_sync_op_max_us.to_string(),
        "group_commit_high_water_released" => status.group_commit_high_water_released.to_string(),
        "group_commit_recent_avg_batch_size" =>
            format!( "{}.{:02}", status.group_commit_recent_avg_batch_size_x100 / 100, status.group_commit_recent_avg_batch_size_x100 % 100, ),
        "group_commit_batch_p50" => status.group_commit_batch_p50.to_string(),
        "group_commit_batch_p99" => status.group_commit_batch_p99.to_string(),
        "wal_syncs" => status.wal_syncs.to_string(),
        "wal_segment_bytes" => status.wal_segment_bytes.to_string(),
        "buffer_pool_pages" => status.buffer_pool_pages.to_string(),
        "row_migrations" => status.row_migrations.to_string(),
        // Phase 4c: process-wide ratios. `row_migrations_total` is the
        // aggregate across every heap (matches `row_migrations` for the
        // engine status's "current" heap; differs when the executor
        // touched several tables). `update_in_place` / `update_migrate`
        // tell us how often UPDATE landed in-place vs grew the row out
        // of its page. PCTFREE is good when migrate stays near zero.
        "row_migrations_total" => crate::storage::row_migrations_total().to_string(),
        "update_in_place_total" => crate::storage::update_in_place_total().to_string(),
        "update_migrate_total" => crate::storage::update_migrate_total().to_string(),
        "update_delta_hits" => crate::storage::update_delta_hits().to_string(),
        "update_delta_misses" => crate::storage::update_delta_misses().to_string(),
        "undo_bytes" => status.undo_bytes.to_string(),
        "undo_records" => status.undo_records.to_string(),
        "undo_retention_ms" => status.undo_retention_ms.to_string(),
        "temporal_earliest_available_unix_us" => status.temporal_earliest_available_unix_us.to_string(),
        "temporal_commit_ledger_bytes" => status.temporal_commit_ledger_bytes.to_string(),
        "temporal_history_index_bytes" => status.temporal_history_index_bytes.to_string(),
        "temporal_history_index_entries" => status.temporal_history_index_entries.to_string(),
        "temporal_history_compactions" => status.temporal_history_compactions.to_string(),
        "temporal_history_last_compaction_unix_us" =>
            status.temporal_history_last_compaction_unix_us.to_string(),
        "temporal_queries" => status.temporal_queries.to_string(),
        "temporal_history_index_hits" => status.temporal_history_index_hits.to_string(),
        "temporal_heap_fallbacks" => status.temporal_heap_fallbacks.to_string(),
        "temporal_hlc_rollbacks" => status.temporal_hlc_rollbacks.to_string(),
        "undo_purged_records" => status.undo_purged_records.to_string(),
        "plan_cache_hits" => status.plan_cache_hits.to_string(),
        "plan_cache_misses" => status.plan_cache_misses.to_string(),
        "plan_cache_hit_pct" => status.plan_cache_hit_pct.to_string(),
        "txn_admission_waits" => exec_permit.txn_admission_waits.to_string(),
        "txn_admission_wait_us" => exec_permit.txn_admission_wait_us.to_string(),
        "mixed_admission_waits" => exec_permit.mixed_admission_waits.to_string(),
        "mixed_admission_wait_us" => exec_permit.mixed_admission_wait_us.to_string(),
        "parallel_read_pressure_entries" => exec_permit.parallel_read_pressure_entries.to_string(),
        "parallel_read_pressure_suppressed_rw_txn" =>
            exec_permit .parallel_read_pressure_suppressed_rw_txn .to_string(),
        "parallel_read_pressure_active" => exec_permit.parallel_read_pressure_active.to_string(),
        "parallel_read_gate_waits" => exec_permit.parallel_read_gate_waits.to_string(),
        "parallel_read_gate_wait_us" => exec_permit.parallel_read_gate_wait_us.to_string(),
        "select_parse_cache_hits" => status.select_parse_cache_hits.to_string(),
        "select_compiled_plan_hits" => status.select_compiled_plan_hits.to_string(),
        "select_operator_compiles" => status.select_operator_compiles.to_string(),
        // Compatibility counters remapped to the prepared-handle L0 memo.
        "prepared_plan_reuses" => status.prepared_plan_reuses.to_string(),
        "prepared_plan_compiles" => status.prepared_plan_compiles.to_string(),
        // Same shape for the prepared DML (INSERT/UPDATE/DELETE) fast path —
        // skip the dispatch wrappers + per-execute statement/session clones.
        "prepared_dml_reuses" => status.prepared_dml_reuses.to_string(),
        "prepared_sparse_batch_hits" => crate::engine::executor::prepared_sparse_batch_hits().to_string(),
        "prepared_dml_compiles" => status.prepared_dml_compiles.to_string(),
        // Cross-session statement (parse + qualify) cache exposed by the
        // engine. Distinct from the prepared-statement counters above.
        "statement_cache_hits" => statement_cache_field(|s| s.hits.to_string()),
        "statement_cache_misses" => statement_cache_field(|s| s.misses.to_string()),
        "statement_cache_stale_evictions" => statement_cache_field(|s| s.stale_evictions.to_string()),
        "statement_cache_lru_evictions" => statement_cache_field(|s| s.lru_evictions.to_string()),
        "statement_cache_size" => statement_cache_field(|s| s.size.to_string()),
        "statement_cache_capacity" => statement_cache_field(|s| s.capacity.to_string()),
        "statement_cache_approx_bytes" => statement_cache_field(|s| s.approx_bytes.to_string()),
        "statement_cache_hit_pct" =>
            statement_cache_field(|s| { let total = s.hits.saturating_add(s.misses); if total == 0 { "0".to_string() } else { (s.hits.saturating_mul(100) / total).to_string() } }),
        // PhysicalPlan cache (COM_QUERY SELECT, no placeholders).
        "physical_plan_cache_hits" => plan_cache_field(|s| s.hits.to_string()),
        "physical_plan_cache_misses" => plan_cache_field(|s| s.misses.to_string()),
        "physical_plan_cache_stale_evictions" => plan_cache_field(|s| s.stale_evictions.to_string()),
        "physical_plan_cache_deps_invalidations" => plan_cache_field(|s| s.deps_invalidations.to_string()),
        "physical_plan_cache_lru_evictions" => plan_cache_field(|s| s.lru_evictions.to_string()),
        "physical_plan_cache_size" => plan_cache_field(|s| s.size.to_string()),
        "physical_plan_cache_capacity" => plan_cache_field(|s| s.capacity.to_string()),
        "physical_plan_cache_approx_bytes" => plan_cache_field(|s| s.approx_bytes.to_string()),
        "physical_plan_cache_hit_pct" => plan_cache_field(plan_cache_hit_pct_string),
        "prepared_dml_cache_hits" => prepared_dml_cache_field(|s| s.hits.to_string()),
        "prepared_dml_cache_misses" => prepared_dml_cache_field(|s| s.misses.to_string()),
        "prepared_dml_cache_stale_evictions" => prepared_dml_cache_field(|s| s.stale_evictions.to_string()),
        "prepared_dml_cache_deps_invalidations" =>
            prepared_dml_cache_field(|s| s.deps_invalidations.to_string()),
        "prepared_dml_cache_lru_evictions" => prepared_dml_cache_field(|s| s.lru_evictions.to_string()),
        "prepared_dml_cache_size" => prepared_dml_cache_field(|s| s.size.to_string()),
        "prepared_dml_cache_capacity" => prepared_dml_cache_field(|s| s.capacity.to_string()),
        "prepared_dml_cache_hit_pct" => prepared_dml_cache_field(plan_cache_hit_pct_string),
        "prepared_dml_batch_cache_hits" => prepared_dml_batch_cache_field(|s| s.hits.to_string()),
        "prepared_dml_batch_cache_misses" => prepared_dml_batch_cache_field(|s| s.misses.to_string()),
        "prepared_dml_batch_cache_stale_evictions" =>
            prepared_dml_batch_cache_field(|s| s.stale_evictions.to_string()),
        "prepared_dml_batch_cache_deps_invalidations" =>
            prepared_dml_batch_cache_field(|s| s.deps_invalidations.to_string()),
        "prepared_dml_batch_cache_lru_evictions" =>
            prepared_dml_batch_cache_field(|s| s.lru_evictions.to_string()),
        "prepared_dml_batch_cache_size" => prepared_dml_batch_cache_field(|s| s.size.to_string()),
        "prepared_dml_batch_cache_capacity" => prepared_dml_batch_cache_field(|s| s.capacity.to_string()),
        "prepared_dml_batch_cache_hit_pct" => prepared_dml_batch_cache_field(plan_cache_hit_pct_string),
        "prepared_dml_inline_would_block_retries" =>
            crate::storage::inline_lock_would_block_retry_count().to_string(),
        "prepared_dml_inline_retry_log_suppressed" =>
            crate::storage::inline_lock_would_block_log_suppressed_count().to_string(),
        "blocking_pool_base_workers" => crate::util::blocking_pool::stack_budget_status().0.to_string(),
        "blocking_pool_max_workers" => crate::util::blocking_pool::stack_budget_status().1.to_string(),
        "blocking_pool_managed_stack_bytes" =>
            crate::util::blocking_pool::stack_budget_status().2.to_string(),
        "async_row_lock_retries" => async_row_lock.0.to_string(),
        "async_row_lock_wakeups" => async_row_lock.1.to_string(),
        "async_row_lock_fallbacks" => async_row_lock.2.to_string(),
        // Compiled-expression-program cache and fallback counters.
        // Lets operators see how often the row-eval hot path stays on
        // the compiled tree versus falling back to the legacy interpreter,
        // and whether the thread-local program cache is too small.
        "expr_compile_cache_hits" =>
            crate::engine::expr_program::expr_program_stats() .compile_cache_hits .to_string(),
        "expr_compile_cache_misses" =>
            crate::engine::expr_program::expr_program_stats() .compile_cache_misses .to_string(),
        "expr_compile_unsupported" =>
            crate::engine::expr_program::expr_program_stats() .compile_unsupported .to_string(),
        "expr_program_eval_rows" =>
            crate::engine::expr_program::expr_program_stats() .program_eval_rows .to_string(),
        "expr_column_eval_rows" =>
            crate::engine::expr_program::expr_program_stats().column_eval_rows.to_string(),
        "expr_scalar_eval_rows" => {
            let stats = crate::engine::expr_program::expr_program_stats();
            stats.program_eval_rows.saturating_sub(stats.column_eval_rows).to_string()
        },
        "expr_legacy_fallback_rows" =>
            crate::engine::expr_program::expr_program_stats() .legacy_fallback_rows .to_string(),
        // Sub-expression escapes: rows where an unsupported sub-tree dropped
        // to legacy while the rest of the expression stayed compiled. Rising
        // here while `expr_legacy_fallback_rows` falls is the intended shift.
        "expr_legacy_escape_rows" =>
            crate::engine::expr_program::expr_program_stats() .legacy_escape_rows .to_string(),
        "expr_compile_source_sensitive_rejections" =>
            crate::engine::expr_program::expr_program_stats() .source_sensitive_rejections .to_string(),
        "expr_compile_local_escapes" =>
            crate::engine::expr_program::expr_program_stats() .local_escape_compiles .to_string(),
        "expr_compile_unescapable_rejections" =>
            crate::engine::expr_program::expr_program_stats() .unescapable_rejections .to_string(),
        "expr_legacy_fallback_pct" =>
            format!( "{:.2}", crate::engine::expr_program::expr_program_stats().legacy_fallback_pct() ),
        // Function-input compile caches (regex patterns / JSON paths) and
        // the column-metadata intern pool gauge — see
        // information_schema.mydb_caches for the unified view.
        "func_cache_regex_hits" => crate::engine::func_cache::stats().regex_hits.to_string(),
        "func_cache_regex_misses" => crate::engine::func_cache::stats().regex_misses.to_string(),
        "func_cache_json_path_hits" => crate::engine::func_cache::stats() .json_path_hits .to_string(),
        "func_cache_json_path_misses" => crate::engine::func_cache::stats() .json_path_misses .to_string(),
        "col_meta_intern_entries" => crate::engine::executor::col_meta_intern_stats().0.to_string(),
        "col_meta_intern_bytes" => crate::engine::executor::col_meta_intern_stats().1.to_string(),
        "auto_parameterize_mode" =>
            match crate::engine::auto_param::AutoParameterizeMode::from_env() { crate::engine::auto_param::AutoParameterizeMode::Off => "off".into(), crate::engine::auto_param::AutoParameterizeMode::Select => "select".into(), crate::engine::auto_param::AutoParameterizeMode::All => "all".into(), },
        "checkpoint_interval_ms" => status.checkpoint_interval_ms.to_string(),
        "checkpoint_wal_bytes" => status.checkpoint_wal_bytes.to_string(),
        "startup_validate_indexes" => status.startup_validate_indexes.to_string(),
        "last_checkpoint_unix_ms" =>
            status .last_checkpoint_unix_ms .map(|value| value.to_string()) .unwrap_or_default(),
        "last_recovery_summary" => status.last_recovery_summary,
        // M1.5 - Extended observability fields (Oracle-style)
        "snapshot_too_old_count" => status.snapshot_too_old_count.to_string(),
        "read_only_txn_count" => status.read_only_txn_count.to_string(),
        "min_retained_txn_id" => status.min_retained_txn_id.to_string(),
        "index_page_max_trx_shortcut_hits" => status.index_page_max_trx_shortcut_hits.to_string(),
        "index_page_max_trx_shortcut_misses" => status.index_page_max_trx_shortcut_misses.to_string(),
        "undo_peak_bytes" => status.undo_peak_bytes.to_string(),
        "undo_peak_records" => status.undo_peak_records.to_string(),
        "undo_longest_chain" => status.undo_longest_chain.to_string(),
        "undo_retention_pressure" => status.undo_retention_pressure.to_string(),
        "undo_segment_count" => status.undo_segment_count.to_string(),
        "undo_segment_bytes" => status.undo_segment_bytes.to_string(),
        "undo_disk_bytes" => status.undo_disk_bytes.to_string(),
        "undo_reclaimable_bytes" => status.undo_reclaimable_bytes.to_string(),
        "undo_reclaimed_bytes" => status.undo_reclaimed_bytes.to_string(),
        "undo_tablespace_max_bytes" => status.undo_tablespace_max_bytes.to_string(),
        "undo_mem_soft_limit_bytes" => status.undo_mem_soft_limit_bytes.to_string(),
        "undo_mem_cache_limit_bytes" => status.undo_mem_cache_limit_bytes.to_string(),
        "undo_resident_bytes" => status.undo_resident_bytes.to_string(),
        "undo_evicted_records" => status.undo_evicted_records.to_string(),
        "undo_txn_max_bytes" => status.undo_txn_max_bytes.to_string(),
        "undo_largest_txn_id" => status.undo_largest_txn_id.to_string(),
        "undo_largest_txn_bytes" => status.undo_largest_txn_bytes.to_string(),
        "undo_mem_pressure" => status.undo_mem_pressure.to_string(),
        "undo_retention_guarantee" => u8::from(status.undo_retention_guarantee).to_string(),
        "undo_purge_interval_ms" => status.undo_purge_interval_ms.to_string(),
        "undo_purge_batch_records" => status.undo_purge_batch_records.to_string(),
        "undo_purge_runs" => status.undo_purge_runs.to_string(),
        "undo_purge_errors" => status.undo_purge_errors.to_string(),
        "undo_purge_last_unix_ms" => status.undo_purge_last_unix_ms.to_string(),
        "undo_cache_hits" => status.undo_cache_hits.to_string(),
        "undo_cache_misses" => status.undo_cache_misses.to_string(),
        "row_x_lock_acquires" => status.row_x_lock_acquires.to_string(),
        "row_s_lock_acquires" => status.row_s_lock_acquires.to_string(),
        "lock_upgrades" => status.lock_upgrades.to_string(),
        "lock_upgrade_failures" => status.lock_upgrade_failures.to_string(),
        "table_lock_acquires" => status.table_lock_acquires.to_string(),
        "chunked_release_chunks" => status.chunked_release_chunks.to_string(),
        "lock_wait_histogram_ms" =>
            format!( "0:{} 1-9:{} 10-99:{} 100-999:{} 1000-4999:{} 5000-9999:{} 10000-29999:{} 30000+:{}", status.lock_wait_histogram[0], status.lock_wait_histogram[1], status.lock_wait_histogram[2], status.lock_wait_histogram[3], status.lock_wait_histogram[4], status.lock_wait_histogram[5], status.lock_wait_histogram[6], status.lock_wait_histogram[7], ),
        "wait_event_summary" =>
            if status.wait_event_summary.is_empty() { "none".to_string() } else { status .wait_event_summary .iter() .map(|(name, count, total_us, max_us)| { format!("{}={{count={},total_us={},max_us={}}}", name, count, total_us, max_us) }) .collect::<Vec<_>>() .join(", ") },
        "wal_segment_count" => status.wal_segment_count.to_string(),
        "wal_active_segment_id" => status.wal_active_segment_id.to_string(),
        "wal_archived_segment_count" => status.wal_archived_segment_count.to_string(),
        "wal_archive_enabled" => status.wal_archive_enabled.to_string(),
        "hash_join_count" =>
            crate::engine::executor::select::plan::HASH_JOIN_COUNT .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "bounded_inner_join_hits" =>
            crate::engine::executor::select::plan::BOUNDED_INNER_JOIN_HITS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_stream_chained" =>
            crate::engine::executor::select::join_stream::HASH_JOIN_STREAM_CHAINED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_stream_rows" =>
            crate::engine::executor::select::join_stream::HASH_JOIN_STREAM_ROWS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_stream_fallback" =>
            crate::engine::executor::select::join_stream::HASH_JOIN_STREAM_FALLBACK .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_stream_prebuilt" =>
            crate::engine::executor::select::join_stream::HASH_JOIN_STREAM_PREBUILT .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_stream_inlj_tail" =>
            crate::engine::executor::select::join_stream::HASH_JOIN_STREAM_INLJ_TAIL .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_materialized_out_rows" =>
            crate::engine::executor::select::join_stream::HASH_JOIN_MATERIALIZED_OUT_ROWS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_build_rows" =>
            crate::engine::executor::select::plan::HASH_JOIN_BUILD_ROWS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_probe_rows" =>
            crate::engine::executor::select::plan::HASH_JOIN_PROBE_ROWS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_null_skipped" =>
            crate::engine::executor::select::plan::HASH_JOIN_NULL_SKIPPED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_mem_exceeded" =>
            crate::engine::executor::select::plan::HASH_JOIN_MEM_EXCEEDED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_fallback_to_nlj" =>
            crate::engine::executor::select::plan::HASH_JOIN_FALLBACK_TO_NLJ .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_parallel_partitions" =>
            crate::engine::executor::select::plan::HASH_JOIN_PARALLEL_PARTITIONS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_bloom_built" =>
            crate::engine::executor::select::plan::HASH_JOIN_BLOOM_BUILT .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_bloom_filtered" =>
            crate::engine::executor::select::plan::HASH_JOIN_BLOOM_FILTERED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_probe_bloom_builds" =>
            crate::engine::executor::select::plan::INLJ_PROBE_BLOOM_BUILDS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_probe_bloom_keys_probed" =>
            crate::engine::executor::select::plan::INLJ_PROBE_BLOOM_KEYS_PROBED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_probe_bloom_keys_filtered" =>
            crate::engine::executor::select::plan::INLJ_PROBE_BLOOM_KEYS_FILTERED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_mrr_lru_hits" =>
            crate::engine::executor::select::plan::INLJ_MRR_LRU_HITS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_mrr_lru_misses" =>
            crate::engine::executor::select::plan::INLJ_MRR_LRU_MISSES .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_mrr_lru_evictions" =>
            crate::engine::executor::select::plan::INLJ_MRR_LRU_EVICTIONS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "mrr_windows" =>
            crate::engine::executor::select::plan::MRR_WINDOWS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "mrr_rids" =>
            crate::engine::executor::select::plan::MRR_RIDS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "mrr_unique_pages" =>
            crate::engine::executor::select::plan::MRR_UNIQUE_PAGES .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "mrr_page_reuses" =>
            crate::engine::executor::select::plan::MRR_PAGE_REUSES .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "mrr_workarea_flushes" =>
            crate::engine::executor::select::plan::MRR_WORKAREA_FLUSHES .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "mrr_peak_bytes" =>
            crate::engine::executor::select::plan::MRR_PEAK_BYTES .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_grace_partitions" =>
            crate::engine::executor::select::plan::HASH_JOIN_GRACE_PARTITIONS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_spill_bytes" =>
            crate::engine::executor::select::plan::HASH_JOIN_SPILL_BYTES .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_spill_read_bytes" =>
            crate::engine::executor::select::plan::HASH_JOIN_SPILL_READ_BYTES .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "setop_spill_count" =>
            crate::engine::executor::SETOP_SPILL_COUNT .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "setop_spill_bytes_written" =>
            crate::engine::executor::SETOP_SPILL_BYTES_WRITTEN .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "setop_spill_bytes_read" =>
            crate::engine::executor::SETOP_SPILL_BYTES_READ .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "setop_spill_partitions" =>
            crate::engine::executor::SETOP_SPILL_PARTITIONS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "setop_spill_repartitions" =>
            crate::engine::executor::SETOP_SPILL_REPARTITIONS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "setop_spill_max_depth" =>
            crate::engine::executor::SETOP_SPILL_MAX_DEPTH .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "setop_spill_file_opens" =>
            crate::engine::executor::SETOP_SPILL_FILE_OPENS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "setop_spill_merge_passes" =>
            crate::engine::executor::SETOP_SPILL_MERGE_PASSES .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_grace_max_depth" =>
            crate::engine::executor::select::plan::HASH_JOIN_GRACE_MAX_DEPTH .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_grace_repartitions" =>
            crate::engine::executor::select::plan::HASH_JOIN_GRACE_REPARTITIONS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_skew_chunks" =>
            crate::engine::executor::select::plan::HASH_JOIN_SKEW_CHUNKS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_peak_charged_bytes" =>
            crate::engine::executor::select::plan::HASH_JOIN_PEAK_CHARGED_BYTES .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_charged_bytes" =>
            crate::engine::executor::select::plan::HASH_JOIN_CHARGED_BYTES .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_mem_denied_operator" =>
            crate::engine::executor::select::plan::HASH_JOIN_MEM_DENIED_OPERATOR .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_mem_denied_query" =>
            crate::engine::executor::select::plan::HASH_JOIN_MEM_DENIED_QUERY .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_mem_denied_work_area" =>
            crate::engine::executor::select::plan::HASH_JOIN_MEM_DENIED_WORK_AREA .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "order_by_key_rows_prepared" =>
            crate::engine::executor::select::eval::ORDER_BY_KEY_ROWS_PREPARED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "join_aggregate_multiplicity_rows_saved" =>
            crate::engine::executor::select::vec_bridge::JOIN_AGG_MULTIPLICITY_ROWS_SAVED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hidden_first_value_fd_group_rows" =>
            crate::engine::executor::select::vec_bridge::HIDDEN_FIRST_VALUE_FD_GROUP_ROWS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "rand_order_stats_cache_hits" =>
            crate::storage::ORDER_STATS_CACHE_HITS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "rand_order_stats_cache_misses" =>
            crate::storage::ORDER_STATS_CACHE_MISSES .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "rand_order_stats_cache_invalidations" =>
            crate::storage::ORDER_STATS_CACHE_INVALIDATIONS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "active_stream_workers" =>
            crate::engine::executor::select::stream::active_stream_workers().to_string(),
        "row_hash_agg_adaptive_spill" =>
            crate::engine::executor::select::plan::row_hash_agg_adaptive_spill_count() .to_string(),
        "hash_join_typed_key_fast" =>
            crate::engine::executor::select::plan::HASH_JOIN_TYPED_KEY_FAST .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_typed_key_arity_1" =>
            crate::engine::executor::select::plan::HASH_JOIN_TYPED_KEY_ARITY_1 .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_typed_key_arity_2" =>
            crate::engine::executor::select::plan::HASH_JOIN_TYPED_KEY_ARITY_2 .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_typed_key_arity_3" =>
            crate::engine::executor::select::plan::HASH_JOIN_TYPED_KEY_ARITY_3 .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_typed_key_arity_4" =>
            crate::engine::executor::select::plan::HASH_JOIN_TYPED_KEY_ARITY_4 .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_typed_key_arity_5_plus" =>
            crate::engine::executor::select::plan::HASH_JOIN_TYPED_KEY_ARITY_5_PLUS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_typed_key_fallback_uint_range" =>
            crate::engine::executor::select::plan::HASH_JOIN_TYPED_KEY_FALLBACK_UINT_RANGE .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_typed_key_fallback_runtime_type" =>
            crate::engine::executor::select::plan::HASH_JOIN_TYPED_KEY_FALLBACK_RUNTIME_TYPE .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_parallel_build_preserved" =>
            crate::engine::executor::select::plan::HASH_JOIN_PARALLEL_BUILD_PRESERVED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_residual_precheck" =>
            crate::engine::executor::select::plan::HASH_JOIN_RESIDUAL_PRECHECK .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_projection_fused" =>
            crate::engine::executor::select::plan::HASH_JOIN_PROJECTION_FUSED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "vec_hash_join_hits" =>
            crate::engine::executor::select::vec_bridge::VEC_HASH_JOIN_HITS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "vec_hash_join_range_partition_count" =>
            crate::engine::executor::select::vec_bridge::VEC_HASH_JOIN_RANGE_PARTITION_COUNT .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "vec_hash_join_grace_parallel_workers" =>
            crate::engine::executor::select::vec_bridge::VEC_HASH_JOIN_GRACE_PARALLEL_WORKERS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "cte_batch_scan_hits" =>
            crate::engine::executor::select::vec_bridge::CTE_BATCH_SCAN_HITS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "vec_filter_input_hits" =>
            crate::engine::executor::select::vec_bridge::VEC_FILTER_INPUT_HITS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_parallel_scatter" =>
            crate::engine::executor::select::plan::HASH_JOIN_PARALLEL_SCATTER .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "hash_join_parallel_null_scattered" =>
            crate::engine::executor::select::plan::HASH_JOIN_PARALLEL_NULL_SCATTERED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "nlj_hash_fast_path" =>
            crate::engine::executor::select::plan::NLJ_HASH_FAST_PATH .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_covering_existence_fast" =>
            crate::engine::executor::select::plan::INLJ_COVERING_EXISTENCE_FAST .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "adaptive_join_count" =>
            crate::engine::executor::select::plan::ADAPTIVE_JOIN_COUNT .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "adaptive_join_chosen_hash" =>
            crate::engine::executor::select::plan::ADAPTIVE_JOIN_CHOSEN_HASH .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "adaptive_join_chosen_index_nlj" =>
            crate::engine::executor::select::plan::ADAPTIVE_JOIN_CHOSEN_INDEX_NLJ .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "adaptive_join_chosen_nlj" =>
            crate::engine::executor::select::plan::ADAPTIVE_JOIN_CHOSEN_NLJ .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "adaptive_join_switched_from_planner" =>
            crate::engine::executor::select::plan::ADAPTIVE_JOIN_SWITCHED_FROM_PLANNER .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        // WS-N8 NLJ/INLJ observability counters: bare NLJ main loop, Block-NL slots,
        // INLJ per-row fallback, outer/batch parallel chunks, fused agg hits,
        // N4 Range promote and N5 MRR cross-chunk sharing slots.
        "nlj_executions" =>
            crate::engine::executor::select::plan::NLJ_EXECUTIONS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "nlj_pairs_compared" =>
            crate::engine::executor::select::plan::NLJ_PAIRS_COMPARED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "nlj_pairs_kept" =>
            crate::engine::executor::select::plan::NLJ_PAIRS_KEPT .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "nlj_block_executions" =>
            crate::engine::executor::select::plan::NLJ_BLOCK_EXECUTIONS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "nlj_block_pairs_evaluated" =>
            crate::engine::executor::select::plan::NLJ_BLOCK_PAIRS_EVALUATED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "nlj_block_pairs_kept" =>
            crate::engine::executor::select::plan::NLJ_BLOCK_PAIRS_KEPT .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        // WS-N3: number of outer blocks that took the SIMD vectorized mask fill
        // (a numeric-compare residual hit the vectorized path instead of whole-block fallback to rows).
        "nlj_simd_fill_blocks" =>
            crate::engine::executor::select::plan::NLJ_SIMD_FILL_BLOCKS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "nlj_outer_parallel_chunks" =>
            crate::engine::executor::select::plan::NLJ_OUTER_PARALLEL_CHUNKS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_per_row_executions" =>
            crate::engine::executor::select::plan::INLJ_PER_ROW_EXECUTIONS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_outer_parallel_chunks" =>
            crate::engine::executor::select::plan::INLJ_OUTER_PARALLEL_CHUNKS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_batch_parallel_chunks" =>
            crate::engine::executor::select::plan::INLJ_BATCH_PARALLEL_CHUNKS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_probe_parallel_decisions" =>
            crate::engine::executor::select::plan::INLJ_PROBE_PARALLEL_DECISIONS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_probe_parallel_tasks" =>
            crate::engine::executor::select::plan::INLJ_PROBE_PARALLEL_TASKS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_probe_parallel_rows" =>
            crate::engine::executor::select::plan::INLJ_PROBE_PARALLEL_ROWS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_fused_semi_anti" =>
            crate::engine::executor::select::plan::INLJ_FUSED_SEMI_ANTI_COUNT .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_batch_range_promoted" =>
            crate::engine::executor::select::plan::INLJ_BATCH_RANGE_PROMOTED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_mrr_chunk_shared_hits" =>
            crate::engine::executor::select::plan::INLJ_MRR_CHUNK_SHARED_HITS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_probe_keys" =>
            crate::engine::executor::select::plan::INLJ_PROBE_KEYS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_index_entries" =>
            crate::engine::executor::select::plan::INLJ_INDEX_ENTRIES .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_candidate_row_ids" =>
            crate::engine::executor::select::plan::INLJ_CANDIDATE_ROW_IDS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_predicate_first_batches" =>
            crate::engine::executor::select::plan::INLJ_PREDICATE_FIRST_BATCHES .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_predicate_first_input_rows" =>
            crate::engine::executor::select::plan::INLJ_PREDICATE_FIRST_INPUT_ROWS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_predicate_first_output_rows" =>
            crate::engine::executor::select::plan::INLJ_PREDICATE_FIRST_OUTPUT_ROWS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_runtime_keyset_intersect_attempts" =>
            crate::engine::executor::select::plan::INLJ_RUNTIME_KEYSET_INTERSECT_ATTEMPTS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_runtime_keyset_intersect_chosen" =>
            crate::engine::executor::select::plan::INLJ_RUNTIME_KEYSET_INTERSECT_CHOSEN .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_runtime_keyset_intersect_cost_rejected" =>
            crate::engine::executor::select::plan::INLJ_RUNTIME_KEYSET_INTERSECT_COST_REJECTED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_runtime_keyset_intersect_budget_rejected" =>
            crate::engine::executor::select::plan::INLJ_RUNTIME_KEYSET_INTERSECT_BUDGET_REJECTED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_runtime_keyset_intersect_index_entries" =>
            crate::engine::executor::select::plan::INLJ_RUNTIME_KEYSET_INTERSECT_INDEX_ENTRIES .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "inlj_runtime_keyset_candidate_rejected" =>
            crate::engine::executor::select::plan::INLJ_RUNTIME_KEYSET_CANDIDATE_REJECTED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        // Phase 6: parallel-execution counters.
        "exchange_count" =>
            crate::engine::executor::select::parallel::EXCHANGE_COUNT .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "parallel_seq_scan_count" =>
            crate::engine::executor::select::parallel::PARALLEL_SEQ_SCAN_COUNT .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "parallel_zone_map_scan_count" =>
            crate::engine::executor::select::parallel::PARALLEL_ZONE_MAP_SCAN_COUNT .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "zone_map_candidate_build_ns" =>
            crate::engine::executor::select::parallel::ZONE_MAP_CANDIDATE_BUILD_NS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "zone_map_scan_tasks_scheduled" =>
            crate::engine::executor::select::parallel::ZONE_MAP_SCAN_TASKS_SCHEDULED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "zone_map_scan_pages_scheduled" =>
            crate::engine::executor::select::parallel::ZONE_MAP_SCAN_PAGES_SCHEDULED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "zone_map_scan_pages_skipped" =>
            crate::engine::executor::select::parallel::ZONE_MAP_SCAN_PAGES_SKIPPED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "zone_map_runtime_fallbacks" =>
            crate::engine::executor::select::parallel::ZONE_MAP_RUNTIME_FALLBACKS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "zone_map_batch_scan_hits" =>
            crate::engine::executor::select::vec_bridge::ZONE_MAP_BATCH_SCAN_HITS .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "parallel_index_fast_scan_count" =>
            crate::engine::executor::select::parallel::PARALLEL_INDEX_FAST_SCAN_COUNT .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "parallel_index_aggregate_count" =>
            crate::engine::executor::select::parallel::PARALLEL_INDEX_AGGREGATE_COUNT .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "parallel_hash_aggregate_count" =>
            crate::engine::executor::select::parallel::PARALLEL_HASH_AGGREGATE_COUNT .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "parallel_sort_count" =>
            crate::engine::executor::select::parallel::PARALLEL_SORT_COUNT .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "parallel_workers_spawned" =>
            crate::engine::executor::select::parallel::PARALLEL_WORKERS_SPAWNED .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "parallel_worker_panic_count" =>
            crate::engine::executor::select::parallel::PARALLEL_WORKER_PANIC_COUNT .load(std::sync::atomic::Ordering::Relaxed) .to_string(),
        "process_query_mem_used_bytes" =>
            crate::engine::executor::mem_budget::process_query_mem_used().to_string(),
        "process_query_mem_high_water_bytes" =>
            crate::engine::executor::mem_budget::process_query_mem_high_water().to_string(),
        "process_query_mem_limit_bytes" =>
            crate::engine::executor::mem_budget::process_mem_limit_bytes().to_string(),
        "query_mem_limit_bytes" => crate::engine::executor::mem_budget::query_mem_limit_bytes().to_string(),
        "query_memory_reservation_rejections" =>
            crate::engine::executor::mem_budget::query_reservation_rejections().to_string(),
        "process_memory_reservation_rejections" =>
            crate::engine::executor::mem_budget::process_reservation_rejections().to_string(),
        "executor_terminal_oom_cancellations" =>
            crate::engine::executor::mem_budget::terminal_oom_cancellations().to_string(),
        "physical_memory_pressure_rejections" =>
            crate::engine::memory_reclaimer::physical_pressure_rejections().to_string(),
        "memory_reclaim_attempts" => admission_reclaim.attempts.to_string(),
        "memory_reclaim_retry_successes" => admission_reclaim.retry_successes.to_string(),
        "memory_reclaim_cache_bytes" => admission_reclaim.cache_bytes.to_string(),
        "memory_reclaim_buffer_bytes" => admission_reclaim.buffer_bytes.to_string(),
        "memory_reclaim_allocator_bytes" => admission_reclaim.allocator_bytes.to_string(),
        "memory_oom_after_reclaim" => admission_reclaim.oom_after_retry.to_string(),
        // PGA-style work-area arbiter summary. The advisor view
        // `information_schema.mydb_memory_advisor` breaks each of
        // these out one-per-row with explanatory text.
        "memory_target_bytes" => crate::engine::executor::mem_budget::memory_target_bytes().to_string(),
        "memory_soft_target_configured_bytes" =>
            crate::engine::executor::mem_budget::configured_memory_target_bytes() .unwrap_or(0) .to_string(),
        "effective_memory_limit_bytes" =>
            crate::engine::executor::mem_budget::effective_memory_limit_bytes().to_string(),
        "sql_memory_hard_limit_bytes" =>
            crate::engine::executor::mem_budget::operational_memory_limit_bytes().to_string(),
        "process_memory_hard_limit_bytes" =>
            crate::engine::executor::mem_budget::managed_memory_limit_bytes().to_string(),
        "executor_memory_target_bytes" =>
            crate::engine::executor::mem_budget::executor_memory_target_bytes().to_string(),
        "idle_memory_reclaim_enabled" => u8::from(crate::engine::memory_reclaimer::enabled()).to_string(),
        "idle_memory_reclaim_active_statements" => idle_reclaim.active_statements.to_string(),
        "idle_memory_reclaim_attempts" => idle_reclaim.attempts.to_string(),
        "idle_memory_reclaim_successes" => idle_reclaim.successes.to_string(),
        "idle_memory_reclaim_skipped_active" => idle_reclaim.skipped_active.to_string(),
        "idle_memory_reclaim_unsupported" => idle_reclaim.unsupported.to_string(),
        "idle_memory_reclaim_last_before_bytes" => idle_reclaim.last_before_bytes.to_string(),
        "idle_memory_reclaim_last_after_bytes" => idle_reclaim.last_after_bytes.to_string(),
        "idle_memory_reclaim_last_released_bytes" => idle_reclaim.last_released_bytes.to_string(),
        "idle_memory_reclaim_last_duration_us" => idle_reclaim.last_duration_us.to_string(),
        "controlled_memory_current_bytes" => controlled_current.to_string(),
        "controlled_memory_managed_limit_bytes" =>
            crate::engine::executor::mem_budget::managed_memory_limit_bytes().to_string(),
        "controlled_memory_operational_limit_bytes" =>
            crate::engine::executor::mem_budget::operational_memory_limit_bytes().to_string(),
        "controlled_memory_performance_schema_limit_bytes" =>
            crate::engine::executor::mem_budget::performance_schema_memory_limit_bytes() .to_string(),
        "controlled_memory_peak_bytes" => controlled_peak.to_string(),
        "controlled_memory_buffer_raw_bytes" => controlled_categories[0].to_string(),
        "controlled_memory_storage_scratch_bytes" => controlled_categories[1].to_string(),
        "controlled_memory_executor_bytes" => controlled_categories[2].to_string(),
        "controlled_memory_derived_cache_bytes" => controlled_categories[3].to_string(),
        "controlled_memory_partition_dml_batch_bytes" => controlled_categories[4].to_string(),
        "controlled_memory_partition_ddl_bytes" => controlled_categories[5].to_string(),
        "controlled_memory_partition_stats_bytes" => controlled_categories[6].to_string(),
        "controlled_memory_ddl_copy_bytes" => controlled_categories[7].to_string(),
        "controlled_memory_system_metadata_bytes" => controlled_categories[8].to_string(),
        "controlled_memory_temporal_history_bytes" => controlled_categories[9].to_string(),
        "controlled_memory_protocol_transient_bytes" => controlled_categories[10].to_string(),
        "controlled_memory_binlog_transient_bytes" => controlled_categories[11].to_string(),
        "controlled_memory_parser_transient_bytes" => controlled_categories[12].to_string(),
        "controlled_memory_rejections" => controlled_rejections.to_string(),
        "controlled_memory_native_replication_transient_bytes" => controlled_categories[13].to_string(),
        "controlled_memory_performance_schema_bytes" => controlled_categories[14].to_string(),
        "controlled_memory_ddl_online_log_bytes" => controlled_categories[15].to_string(),
        "controlled_memory_rejections_buffer_raw" => controlled_rejections_by_category[0].to_string(),
        "controlled_memory_rejections_storage_scratch" => controlled_rejections_by_category[1].to_string(),
        "controlled_memory_rejections_executor" => controlled_rejections_by_category[2].to_string(),
        "controlled_memory_rejections_derived_cache" => controlled_rejections_by_category[3].to_string(),
        "controlled_memory_rejections_partition_dml_batch" => controlled_rejections_by_category[4].to_string(),
        "controlled_memory_rejections_partition_ddl" => controlled_rejections_by_category[5].to_string(),
        "controlled_memory_rejections_partition_stats" => controlled_rejections_by_category[6].to_string(),
        "controlled_memory_rejections_ddl_copy" => controlled_rejections_by_category[7].to_string(),
        "controlled_memory_rejections_system_metadata" => controlled_rejections_by_category[8].to_string(),
        "controlled_memory_rejections_temporal_history" => controlled_rejections_by_category[9].to_string(),
        "controlled_memory_rejections_protocol_transient" => controlled_rejections_by_category[10].to_string(),
        "controlled_memory_rejections_binlog_transient" => controlled_rejections_by_category[11].to_string(),
        "controlled_memory_rejections_parser_transient" => controlled_rejections_by_category[12].to_string(),
        "controlled_memory_rejections_native_replication_transient" => controlled_rejections_by_category[13].to_string(),
        "controlled_memory_rejections_performance_schema" => controlled_rejections_by_category[14].to_string(),
        "controlled_memory_rejections_ddl_online_log" => controlled_rejections_by_category[15].to_string(),
        "work_area_allocated_bytes" => work_area.allocated_bytes.to_string(),
        "work_area_high_water_bytes" => work_area.high_water_bytes.to_string(),
        "work_area_active_handles" => work_area.active_handles.to_string(),
        "work_area_optimal_grants" => work_area.optimal_grants.to_string(),
        "work_area_one_pass_grants" => work_area.one_pass_grants.to_string(),
        "work_area_min_grants" => work_area.min_grants.to_string(),
        "work_area_spill_requests" => work_area.spill_requests.to_string(),
        "work_area_spill_estimated_bytes" => work_area.spill_estimated_bytes.to_string(),
        "work_area_force_overflow_bytes" => work_area.force_overflow_bytes.to_string(),
        "work_area_force_overflow_events" => work_area.force_overflow_events.to_string(),
        // P0-3: temp-disk spill quota live state.
        "temp_disk_live_bytes" => crate::engine::executor::spill_quota::global_spilled_bytes().to_string(),
        "temp_disk_high_water_bytes" =>
            crate::engine::executor::spill_quota::global_spilled_high_water().to_string(),
        "temp_disk_quota_rejections" => crate::engine::executor::spill_quota::quota_rejections().to_string(),
        "temp_disk_global_quota_rejections" =>
            crate::engine::executor::spill_quota::global_quota_rejections().to_string(),
        "temp_disk_query_quota_rejections" =>
            crate::engine::executor::spill_quota::per_query_quota_rejections().to_string(),
        "active_spill_files" => crate::engine::executor::spill_quota::active_spill_files().to_string(),
        "spill_file_opens" => crate::engine::executor::spill_quota::spill_file_opens().to_string(),
        "orphan_spill_bytes" => crate::engine::executor::spill_quota::orphan_spill_bytes().to_string(),
        "spill_cleanup_failures" => crate::engine::executor::spill_quota::cleanup_failures().to_string(),
        "sort_spill_count" => crate::engine::executor::spill_quota::sort_spill_count().to_string(),
        "hash_join_spill_count" => crate::engine::executor::spill_quota::hash_join_spill_count().to_string(),
        "hash_agg_spill_count" => crate::engine::executor::spill_quota::hash_agg_spill_count().to_string(),
        "distinct_spill_count" => crate::engine::executor::spill_quota::distinct_spill_count().to_string(),
        "cte_spill_count" => crate::engine::executor::spill_quota::cte_spill_count().to_string(),
        // P0/D1: roll-up of the engine's independently-accounted memory
        // pools so operators can watch total tracked memory approach
        // physical RAM in one place. `tracked_memory_bytes` is a FLOOR —
        // buffer pool (resident frames) + executor operator state +
        // in-memory undo mirror — and intentionally excludes still-untracked
        // pools (HNSW graphs, per-thread visibility LRUs, catalog) and
        // allocator overhead. `resident_set_peak_bytes` (getrusage) is the
        // OS ground truth; a large gap above the tracked floor is untracked
        // memory. (Unifying these under one arbiter is P1/A.)
        "buffer_pool_resident_bytes" =>
            ((controlled_categories[0] as u128) .saturating_add(controlled_categories[1] as u128)) .to_string(),
        "tracked_memory_bytes" =>
            ((controlled_current as u128) .saturating_add(status.undo_resident_bytes as u128) .saturating_add(known_cache_bytes)) .to_string(),
        "resident_set_peak_bytes" => peak_resident_bytes().to_string(),
        "active_connections" => connection_metrics.active.to_string(),
        "max_connections" => connection_metrics.max_connections.to_string(),
        "effective_max_connections" => connection_metrics.effective_max_connections.to_string(),
        "admin_connections" => connection_metrics.admin_connections.to_string(),
        "admin_connection_reserve" => connection_metrics.admin_connection_reserve.to_string(),
        "accepted_connections" => connection_metrics.accepted.to_string(),
        "rejected_connections" => connection_metrics.rejected.to_string(),
        "connection_errors_max_connections" =>
            connection_metrics .connection_errors_max_connections .to_string(),
        "max_used_connections" => connection_metrics.max_used_connections.to_string(),
        "handshake_successes" => connection_metrics.handshake_successes.to_string(),
        "handshake_failures" => connection_metrics.handshake_failures.to_string(),
        "closed_connections" => connection_metrics.closed.to_string(),
        "listen_backlog" => connection_metrics.listen_backlog.to_string(),
        "handshake_timeout_secs" => connection_metrics.handshake_timeout_secs.to_string(),
        "open_files_soft_limit" => connection_metrics.open_files_soft_limit.to_string(),
        "open_files_hard_limit" => connection_metrics.open_files_hard_limit.to_string(),
        "open_files_requested_limit" => connection_metrics.open_files_requested_limit.to_string(),
        "open_files_reserve" => connection_metrics.open_files_reserve.to_string(),
        "max_allowed_packet_bytes" => crate::mysql::packet::max_allowed_packet_bytes().to_string(),
        "packet_write_buffer_max_bytes" => crate::mysql::packet::batch_buffer_max_bytes().to_string(),
        "net_write_timeout_secs" =>
            crate::mysql::packet::net_write_timeout() .map(|d| d.as_secs().to_string()) .unwrap_or_else(|| "0".to_string()),
        "killed_queries" => crate::engine::cancel::snapshot_kill_stats() .killed_queries .to_string(),
        "killed_connections" => crate::engine::cancel::snapshot_kill_stats() .killed_connections .to_string(),
        "kill_observed" => crate::engine::cancel::snapshot_kill_stats() .kill_observed .to_string(),
    };
    // Background Auto-ANALYZE observability.
    let aa = crate::engine::auto_analyze::stats_snapshot();
    let mut rows = rows;
    rows.extend([
        (
            "auto_analyze_enabled",
            if aa.enabled { "1" } else { "0" }.to_string(),
        ),
        ("auto_analyze_interval_s", aa.interval_s.to_string()),
        ("auto_analyze_ratio", format!("{:.4}", aa.ratio)),
        (
            "auto_analyze_last_run_unix_ms",
            aa.last_run_unix_ms.to_string(),
        ),
        ("auto_analyze_total_runs", aa.total_runs.to_string()),
        (
            "auto_analyze_total_tables_analyzed",
            aa.total_tables_analyzed.to_string(),
        ),
    ]);
    rows.extend(
        db.sequence_status_pairs()
            .map(|(name, value)| (name, value.to_string())),
    );
    rows.extend(db.zone_map_status_rows());
    rows.extend(
        crate::engine::status_counters::like_access_counters()
            .into_iter()
            .map(|(name, value)| (name, value.to_string())),
    );
    QueryResponse::ResultSet(ResultSet {
        columns: show_engine_mydb_status_columns(),
        rows: rows
            .into_iter()
            .map(|(name, value)| (name.to_string(), value))
            .chain(crate::binlog::semi_sync_status_pairs())
            .chain(hnsw_status_rows(db))
            .map(|(name, value)| {
                vec![
                    Value::String("mydb".to_string()),
                    Value::String(name),
                    Value::String(value),
                ]
            })
            .collect(),
    })
}

/// HNSW vector-index section for `SHOW ENGINE MYDB STATUS`: a `hnsw_indexes`
/// count header plus one packed-value row per index. Warm indexes report live
/// runtime stats (node counts, stale ratio, backend, persisted LSN); cold
/// (never-queried) indexes report their catalog build parameters with a
/// `(cold — not yet built)` marker. Shares its data source with
/// `information_schema.mydb_vector_indexes`.
fn hnsw_status_rows(db: &Database) -> Vec<(String, String)> {
    use crate::storage::naming::normalize_name;
    let runtime = db.vector_index_runtime_stats();
    let mut indexes: Vec<(String, String)> = Vec::new();
    for header in db.catalog().tables.values() {
        let table = if header.columns.is_empty() {
            match db.table(&header.qualified_name()) {
                Ok(table) => table,
                Err(_) => continue,
            }
        } else {
            header.clone()
        };
        for index in table.iter_public_indexes() {
            if !index.is_vector() {
                continue;
            }
            let meta = index.vector_meta();
            let label = format!("hnsw.{}.{}.{}", table.database, table.name, index.name);
            let quant = index
                .vector
                .as_ref()
                .map_or("none", |v| v.quantization.name());
            let value = match runtime.get(&(table.id, normalize_name(&index.name))) {
                Some(st) => {
                    let build = st.last_build_mode.map_or_else(String::new, |mode| {
                        format!(
                            " build_mode={} build_rows={} build_stats_ms={} \
                             build_encode_ms={} build_link_ms={} build_materialize_ms={} \
                             build_total_ms={} build_peak_scratch_bytes={} \
                             build_cache_hits={} build_cache_misses={} build_chunks={}",
                            mode,
                            st.last_build_rows,
                            st.last_build_stats_ms,
                            st.last_build_encode_ms,
                            st.last_build_link_ms,
                            st.last_build_materialize_ms,
                            st.last_build_total_ms,
                            st.last_build_peak_scratch_bytes,
                            st.last_build_cache_hits,
                            st.last_build_cache_misses,
                            st.last_build_chunks,
                        )
                    });
                    format!(
                        "backend={} metric={} dim={} total_nodes={} live_nodes={} \
                         stale_nodes={} stale_ratio={}% partitions={} generation={} \
                         last_persisted_lsn={} nav_pages={} sq8_exact_fallback_nodes={}{}",
                        st.backend,
                        meta.metric,
                        st.dim,
                        st.total_nodes,
                        st.live_nodes,
                        st.stale_nodes,
                        st.stale_ratio_pct(),
                        st.partitions_loaded,
                        st.generation,
                        st.last_persisted_lsn,
                        st.nav_pages,
                        st.sq8_exact_fallback_nodes,
                        build,
                    )
                }
                None => format!(
                    "metric={} m={} ef_construction={} (cold — not yet built)",
                    meta.metric, meta.m, meta.ef_construction,
                ),
            };
            let value=format!("{value} quantization={quant}{}",runtime.get(&(table.id,normalize_name(&index.name))).map(|st|format!(" quantizer_bytes={} compressed_bytes={} exceptions={} candidates={} heap_reranks={} exact_fallbacks={}",st.quantizer_bytes,st.compressed_bytes,st.quantization_exceptions,st.candidates_returned,st.heap_reranks,st.exact_fallbacks)).unwrap_or_default());
            indexes.push((label, value));
        }
    }
    let mut rows = vec![
        ("hnsw_indexes".to_string(), indexes.len().to_string()),
        (
            "hnsw_tombstones_rolled_back".to_string(),
            db.hnsw_tombstones_rolled_back().to_string(),
        ),
    ];
    rows.extend(indexes);
    rows
}

pub(crate) fn show_engine_mydb_status_columns() -> Vec<Column> {
    vec![
        col("Type", ColumnType::VarString),
        col("Name", ColumnType::VarString),
        col("Status", ColumnType::VarString),
    ]
}

/// MySQL 8.0: an account with the `PROCESS` privilege (or `SUPER`, which
/// implies it) sees every connection in `SHOW PROCESSLIST` /
/// `information_schema.PROCESSLIST`; without it a session only sees its own
/// threads. We match on the login user name, mirroring MySQL's per-account
/// scoping.
pub(crate) fn session_can_see_all_processes(db: &Database, session: &SessionContext) -> bool {
    let auth = db.auth_catalog();
    let ctx = session.authorization_context();
    let required = RequiredPrivilege {
        privilege: Privilege::Process,
        scope: PrivilegeScope::Global,
    };
    auth.account_has_privilege(&ctx, &required, false)
}

pub(crate) fn processlist_columns() -> Vec<Column> {
    let mut columns = vec![
        col("Id", ColumnType::LongLong),
        col("User", ColumnType::VarString),
        col("Host", ColumnType::VarString),
        col("db", ColumnType::VarString),
        col("Command", ColumnType::VarString),
        col("Time", ColumnType::Long),
        col("State", ColumnType::VarString),
        col("Info", ColumnType::VarString),
    ];
    columns[0].source.flags =
        crate::mysql::protocol::UNSIGNED_FLAG | crate::mysql::protocol::NOT_NULL_FLAG;
    columns
}

pub(crate) fn show_processlist_result(
    db: &Database,
    session: &SessionContext,
    registry: &SessionRegistry,
    full: bool,
) -> QueryResponse {
    let columns = processlist_columns();
    let mut processes = registry.snapshot();
    if !processes.iter().any(|p| p.id == session.connection_id) {
        processes.push(crate::engine::session_registry::ProcessInfo {
            id: session.connection_id,
            user: session.user.clone(),
            host: session.host.clone(),
            database: (!session.current_database.is_empty())
                .then(|| session.current_database.clone()),
            command: "Query".to_string(),
            time_secs: 0,
            state: Some("executing".to_string()),
            info: Some("SHOW PROCESSLIST".to_string()),
            connection_attrs: std::collections::BTreeMap::new(),
            session_variables: Vec::new(),
            user_variables: Vec::new(),
            autocommit: session.autocommit,
            isolation: session.isolation.as_mysql_name().replace('-', " "),
            prepared_statements: Vec::new(),
            current_txn: session.current_txn,
            lazy_trx_started_ms: 0,
            query_id: None,
            exec_current_bytes: 0,
            query_peak_bytes: None,
            session_peak_bytes: 0,
            query_limit_bytes: None,
            spill_bytes: None,
            prepared_cache_bytes: 0,
            cte_cache_bytes: 0,
            retained_cache_bytes: 0,
            kind: crate::engine::session_registry::ProcessKind::Foreground,
        });
    }
    // Without PROCESS, MySQL only surfaces the caller's own threads.
    if !session_can_see_all_processes(db, session) {
        processes.retain(|p| {
            p.id == session.connection_id
                || (p.kind == crate::engine::session_registry::ProcessKind::Foreground
                    && p.user == session.user)
        });
    }
    processes.sort_by_key(|p| p.id);
    let rows = processes
        .into_iter()
        .map(|p| {
            // MySQL truncates the non-FULL `Info` to 100 *characters*; take a
            // char-boundary prefix so multi-byte UTF-8 never panics.
            let info = match p.info {
                Some(sql) if full || sql.chars().count() <= 100 => Value::String(sql),
                Some(sql) => Value::String(sql.chars().take(100).collect::<String>()),
                None => Value::Null,
            };
            vec![
                Value::UInt(p.id as u64),
                Value::String(p.user),
                Value::String(p.host),
                p.database.map(Value::String).unwrap_or(Value::Null),
                Value::String(p.command),
                Value::Int(p.time_secs as i64),
                p.state.map(Value::String).unwrap_or(Value::Null),
                info,
            ]
        })
        .collect();
    QueryResponse::ResultSet(ResultSet { columns, rows })
}

pub(crate) fn show_grants_result(
    db: &Database,
    session: &SessionContext,
    account: Option<&AccountKey>,
    using_roles: &[AccountKey],
) -> Result<QueryResponse, QueryError> {
    let auth = db.auth_catalog();
    let include_mandatory = account.is_none();
    let account = account.cloned().unwrap_or_else(|| {
        if session.auth_epoch == 0 && session.user != session.authenticated_as.user {
            AccountKey::new(session.user.clone(), "%")
        } else {
            session.authenticated_as.clone()
        }
    });
    if !auth
        .accounts
        .contains_key(&crate::engine::auth::account_id(&account))
        && !(session.auth_epoch == 0 && (account == session.authenticated_as || include_mandatory))
    {
        return Err(crate::storage::error(
            crate::error::catalog::ER_NONEXISTING_GRANT,
            format!(
                "There is no such grant defined for user {}",
                account.display()
            ),
        ));
    }

    // Effective principals: the account itself + any role explicitly listed
    // via `USING role`. MySQL surfaces inherited grants as if owned by the
    // displayed account, so we collect all principals' grants and re-render
    // with `account.role_sql_name()` as the grantee.
    let mut principals: std::collections::BTreeSet<AccountKey> = std::collections::BTreeSet::new();
    principals.insert(account.clone());
    let applicable = auth.applicable_roles(&account);
    let using_seed: std::collections::BTreeSet<_> = using_roles.iter().cloned().collect();
    for role in &using_seed {
        if !applicable.contains(role) {
            return Err(crate::storage::error(
                crate::error::catalog::ER_ROLE_NOT_GRANTED,
                format!(
                    "Role {} has not been granted to {}",
                    role.display(),
                    account.display()
                ),
            ));
        }
    }
    principals.extend(auth.role_closure(&using_seed));
    if include_mandatory && account == session.authenticated_as {
        principals.extend(auth.effective_roles(&session.authorization_context()));
    }

    // Group grants by scope with a Vec-of-(priv, grant_option) so we can
    // (a) coalesce same-scope rows into a single `GRANT a, b ON s` line
    //     (M2), and
    // (b) reverse-fold the canonical static bundle into `ALL PRIVILEGES`
    //     (L11/L12).
    use std::collections::BTreeMap;
    let mut by_scope: BTreeMap<PrivilegeScope, ScopeBucket> = BTreeMap::new();
    for grant in principals
        .iter()
        .flat_map(|p| auth.grants_for(p))
        .filter(|g| {
            principals.contains(&g.grantee) && !matches!(g.scope, PrivilegeScope::Proxy { .. })
        })
    {
        if let PrivilegeScope::Column {
            database,
            table,
            column,
        } = &grant.scope
        {
            let entry = by_scope
                .entry(PrivilegeScope::Table {
                    database: database.clone(),
                    table: table.clone(),
                })
                .or_default();
            entry.any_grant_option |= grant.grant_option;
            entry
                .columns
                .entry(grant.privilege.sql_name())
                .or_default()
                .insert(column.clone());
        } else {
            by_scope
                .entry(grant.scope.clone())
                .or_default()
                .add(&grant.privilege, grant.grant_option);
        }
    }

    // MySQL row order:
    //   1) Global static GRANTs (folded ALL when bundle complete)
    //   2) Global dynamic GRANTs (one per dynamic priv, with ADMIN OPTION)
    //   3) Schema (db.*) ordered by db
    //   4) Table (db.t) ordered by db, t
    //   5) Routine
    //   6) Column
    //   7) Role GRANTs
    let mut rows: Vec<Vec<Value>> = Vec::new();
    let scope_order: Vec<PrivilegeScope> = by_scope.keys().cloned().collect();
    let mut emitted_dynamic = std::collections::BTreeSet::new();

    // Pass 1: Global static + Global "ALL PRIVILEGES" fold.
    if let Some(bucket) = by_scope.get(&PrivilegeScope::Global) {
        let lines = render_bucket(
            &PrivilegeScope::Global,
            bucket,
            &account,
            &mut emitted_dynamic,
        );
        for line in lines {
            rows.push(vec![Value::String(line)]);
        }
    }

    // Pass 2: All other scopes in deterministic order. The BTreeMap key
    // gives us (Global → Database → Routine → Table → Column) per
    // `PrivilegeScope`'s `Ord` derive; reorder explicitly to match MySQL.
    let mut by_kind: BTreeMap<u8, Vec<PrivilegeScope>> = BTreeMap::new();
    for scope in scope_order {
        if matches!(scope, PrivilegeScope::Global) {
            continue;
        }
        let kind = match scope {
            PrivilegeScope::Database { .. } => 1,
            PrivilegeScope::Table { .. } => 2,
            PrivilegeScope::Column { .. } => 3,
            PrivilegeScope::Routine { .. } => 4,
            PrivilegeScope::Proxy { .. } => continue,
            PrivilegeScope::Global => continue,
        };
        by_kind.entry(kind).or_default().push(scope);
    }

    for grant in auth.grants.iter().filter(|grant| {
        principals.contains(&grant.grantee)
            && matches!(&grant.privilege, Privilege::Dynamic(name) if name == "PROXY")
            && matches!(grant.scope, PrivilegeScope::Proxy { .. })
    }) {
        let suffix = if grant.grant_option {
            " WITH GRANT OPTION"
        } else {
            ""
        };
        rows.push(vec![Value::String(format!(
            "GRANT PROXY ON {} TO {}{}",
            scope_name(&grant.scope),
            account.role_sql_name(),
            suffix
        ))]);
    }
    for scopes in by_kind.into_values() {
        for scope in scopes {
            let bucket = &by_scope[&scope];
            let lines = render_bucket(&scope, bucket, &account, &mut emitted_dynamic);
            for line in lines {
                rows.push(vec![Value::String(line)]);
            }
        }
    }

    // L9: partial-revoke deny rows, grouped per (principal, db) so multiple
    // restrictions on the same db render as one `REVOKE a, b ON db.* FROM u`
    // line. MySQL emits these between the global GRANT and the role GRANTs.
    {
        use std::collections::BTreeMap;
        let mut by_db: BTreeMap<String, std::collections::BTreeSet<String>> = BTreeMap::new();
        let privileges: std::collections::BTreeSet<_> = principals
            .iter()
            .flat_map(|p| auth.grants_for(p))
            .filter(|g| matches!(g.scope, PrivilegeScope::Global))
            .map(|g| g.privilege.clone())
            .collect();
        for privilege in privileges {
            for database in auth.global_restrictions(&principals, &privilege) {
                by_db
                    .entry(database)
                    .or_default()
                    .insert(privilege.sql_name());
            }
        }
        for (db, privs) in by_db {
            let phrase: Vec<String> = privs.into_iter().collect();
            rows.push(vec![Value::String(format!(
                "REVOKE {} ON {}.* FROM {}",
                phrase.join(", "),
                db,
                account.role_sql_name()
            ))]);
        }
    }

    // Roles granted to the account (and to any of its USING-listed roles,
    // so `SHOW GRANTS FOR x USING r` surfaces r's role chain too).
    for edge in auth
        .role_edges
        .iter()
        .filter(|e| principals.contains(&e.to_account))
    {
        let admin = if edge.admin_option {
            " WITH ADMIN OPTION"
        } else {
            ""
        };
        rows.push(vec![Value::String(format!(
            "GRANT {} TO {}{admin}",
            edge.from_role.role_sql_name(),
            account.role_sql_name()
        ))]);
    }

    // Mandatory roles are implicit, so no role-edge row exists. MySQL shows
    // them only for SHOW GRANTS without FOR; SHOW GRANTS FOR intentionally
    // reports explicit assignments only.
    if include_mandatory {
        for role in auth.mandatory_role_keys() {
            rows.push(vec![Value::String(format!(
                "GRANT {} TO {}",
                role.role_sql_name(),
                account.role_sql_name()
            ))]);
        }
    }

    for row in &mut rows {
        if let Some(Value::String(sql)) = row.first_mut() {
            if let Some((privilege, tail)) = sql
                .strip_prefix("GRANT ")
                .and_then(|s| s.split_once(" ON *.* "))
            {
                if let Some(id) = crate::engine::auth::ai_model_privilege_id(privilege) {
                    if let Some((name, _)) =
                        db.ai_catalog()?.models.iter().find(|(_, m)| m.id == id)
                    {
                        if let Some((schema, model)) = name.rsplit_once('.') {
                            *sql = format!(
                                "GRANT AI_USE ON AI MODEL `{}`.`{}` {}",
                                schema.replace('`', "``"),
                                model.replace('`', "``"),
                                tail
                            );
                        }
                    }
                }
            }
        }
    }
    if rows.is_empty() {
        rows.push(vec![Value::String(format!(
            "GRANT USAGE ON *.* TO {}",
            account.role_sql_name()
        ))]);
    }
    Ok(QueryResponse::ResultSet(ResultSet {
        columns: vec![col(
            &format!("Grants for {}", account.current_user_value()),
            ColumnType::VarString,
        )],
        rows,
    }))
}

#[derive(Default)]
struct ScopeBucket {
    /// (privilege display name, grant_option) for static privileges.
    /// We keep the order MySQL uses by iterating
    /// `all_privileges_for_scope` at render time rather than insertion
    /// order.
    statics: std::collections::BTreeMap<String, bool>,
    dynamics: std::collections::BTreeMap<String, bool>,
    columns: std::collections::BTreeMap<String, std::collections::BTreeSet<String>>,
    /// True if any row in the bucket had grant_option set — used to emit
    /// `WITH GRANT OPTION` even when the privilege list folds into ALL.
    any_grant_option: bool,
}

impl ScopeBucket {
    fn add(&mut self, privilege: &Privilege, grant_option: bool) {
        if grant_option && !matches!(privilege, Privilege::Dynamic(_)) {
            self.any_grant_option = true;
        }
        match privilege {
            Privilege::GrantOption => {}
            Privilege::Dynamic(name) => {
                let entry = self.dynamics.entry(name.to_string()).or_insert(false);
                *entry |= grant_option;
            }
            other => {
                let name = other.sql_name();
                let entry = self.statics.entry(name).or_insert(false);
                *entry |= grant_option;
            }
        }
    }
}

fn sql_identifier(name: &str) -> String {
    quoted_identifier(name)
}
fn scope_sql_name(scope: &PrivilegeScope) -> String {
    match scope {
        PrivilegeScope::Database { database } => format!("{}.*", sql_identifier(database)),
        PrivilegeScope::Table { database, table }
        | PrivilegeScope::Column {
            database, table, ..
        } => format!("{}.{}", sql_identifier(database), sql_identifier(table)),
        PrivilegeScope::Routine {
            database,
            routine,
            kind,
        } => format!(
            "{} {}.{}",
            kind.to_ascii_uppercase(),
            sql_identifier(database),
            sql_identifier(routine)
        ),
        _ => scope_name(scope),
    }
}

fn render_bucket(
    scope: &PrivilegeScope,
    bucket: &ScopeBucket,
    account: &AccountKey,
    emitted_dynamic: &mut std::collections::BTreeSet<String>,
) -> Vec<String> {
    let mut lines = Vec::new();

    // Reverse-fold: if the bucket's statics equal `all_privileges_for_scope`,
    // emit "ALL PRIVILEGES" instead. Compare on sql_name strings for stable
    // matching against Privilege enum variants.
    let all_set: std::collections::BTreeSet<String> =
        crate::engine::auth::all_privileges_for_scope(scope)
            .iter()
            .map(|p| p.sql_name())
            .collect();
    let bucket_set: std::collections::BTreeSet<String> = bucket.statics.keys().cloned().collect();

    let (static_phrase, static_emitted_any) = if !matches!(scope, PrivilegeScope::Column { .. })
        && !bucket.statics.is_empty()
        && bucket_set == all_set
    {
        ("ALL PRIVILEGES".to_string(), true)
    } else if !bucket.statics.is_empty() {
        // Render statics in canonical scope order (= MySQL print order).
        let mut parts = Vec::new();
        for privilege in crate::engine::auth::all_privileges_for_scope(scope) {
            let name = privilege.sql_name();
            if bucket.statics.contains_key(&name) {
                parts.push(name);
            }
        }
        // Anything in bucket but not in all_set (shouldn't happen for
        // canonical statics; defensive) — append at end.
        for name in &bucket_set {
            if !parts.contains(name) {
                parts.push(name.clone());
            }
        }
        (parts.join(", "), true)
    } else {
        (
            "USAGE".to_string(),
            matches!(scope, PrivilegeScope::Global) || bucket.any_grant_option,
        )
    };
    let static_phrase = if let PrivilegeScope::Column { column, .. } = scope {
        static_phrase
            .split(", ")
            .map(|p| format!("{p} (`{}`)", column.replace('`', "``")))
            .collect::<Vec<_>>()
            .join(", ")
    } else {
        static_phrase
    };

    let mut parts = if static_emitted_any {
        vec![static_phrase]
    } else {
        Vec::new()
    };
    for (privilege, columns) in &bucket.columns {
        parts.push(format!(
            "{} ({})",
            privilege,
            columns
                .iter()
                .map(|column| quoted_identifier(column))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    let static_phrase = parts.join(", ");
    if !parts.is_empty() {
        let grant_option = if bucket.any_grant_option {
            " WITH GRANT OPTION"
        } else {
            ""
        };
        lines.push(format!(
            "GRANT {static_phrase} ON {} TO {}{grant_option}",
            scope_sql_name(scope),
            account.role_sql_name()
        ));
    }

    // Dynamic privileges only meaningful at Global scope in MySQL 8.0.
    if matches!(scope, PrivilegeScope::Global) {
        let mut with_grant_option = Vec::new();
        let mut without_grant_option = Vec::new();
        for (name, with_admin) in &bucket.dynamics {
            if emitted_dynamic.contains(name) {
                continue;
            }
            emitted_dynamic.insert(name.clone());
            if *with_admin {
                with_grant_option.push(name.clone());
            } else {
                without_grant_option.push(name.clone());
            }
        }
        if !without_grant_option.is_empty() {
            lines.push(format!(
                "GRANT {} ON *.* TO {}",
                without_grant_option.join(","),
                account.role_sql_name()
            ));
        }
        if !with_grant_option.is_empty() {
            lines.push(format!(
                "GRANT {} ON *.* TO {} WITH GRANT OPTION",
                with_grant_option.join(","),
                account.role_sql_name()
            ));
        }
    }

    lines
}

pub(crate) fn show_create_user_result(
    db: &Database,
    account: &AccountKey,
) -> Result<QueryResponse, QueryError> {
    let auth = db.auth_catalog();
    show_create_user_from_catalog(&auth, account)
}

pub(crate) fn show_create_user_from_catalog(
    auth: &crate::engine::auth::AuthCatalog,
    account: &AccountKey,
) -> Result<QueryResponse, QueryError> {
    let Some(user) = auth.accounts.get(&crate::engine::auth::account_id(account)) else {
        return Err(crate::storage::error(
            crate::error::catalog::ER_CANNOT_USER,
            format!(
                "Operation SHOW CREATE USER failed for {}",
                account.display()
            ),
        ));
    };
    let mut sql = format!(
        "CREATE USER {} IDENTIFIED WITH '{}' AS '{}'",
        account.display(),
        sql_quote(&user.plugin),
        sql_quote(&user.authentication_string)
    );
    match &user.ssl_type {
        SslRequirement::None => sql.push_str(" REQUIRE NONE"),
        SslRequirement::Ssl => sql.push_str(" REQUIRE SSL"),
        SslRequirement::X509 => sql.push_str(" REQUIRE X509"),
        SslRequirement::Specified {
            cipher,
            issuer,
            subject,
        } => {
            let mut parts = Vec::new();
            if let Some(cipher) = cipher {
                parts.push(format!("CIPHER '{}'", sql_quote(cipher)));
            }
            if let Some(issuer) = issuer {
                parts.push(format!("ISSUER '{}'", sql_quote(issuer)));
            }
            if let Some(subject) = subject {
                parts.push(format!("SUBJECT '{}'", sql_quote(subject)));
            }
            if !parts.is_empty() {
                sql.push_str(" REQUIRE ");
                sql.push_str(&parts.join(" AND "));
            }
        }
    }
    let mut resource = Vec::new();
    if user.max_questions != 0 {
        resource.push(format!("MAX_QUERIES_PER_HOUR {}", user.max_questions));
    }
    if user.max_updates != 0 {
        resource.push(format!("MAX_UPDATES_PER_HOUR {}", user.max_updates));
    }
    if user.max_connections != 0 {
        resource.push(format!("MAX_CONNECTIONS_PER_HOUR {}", user.max_connections));
    }
    if user.max_user_connections != 0 {
        resource.push(format!(
            "MAX_USER_CONNECTIONS {}",
            user.max_user_connections
        ));
    }
    if !resource.is_empty() {
        sql.push_str(" WITH ");
        sql.push_str(&resource.join(" "));
    }
    // G9: round-trip password lifecycle / history / require-current /
    // failed-login-locking / attribute clauses so SHOW CREATE USER output
    // is a runnable CREATE USER statement covering the full account state.
    if let Some(lifetime) = user.password_lifetime_days {
        if lifetime == 0 {
            sql.push_str(" PASSWORD EXPIRE NEVER");
        } else {
            sql.push_str(&format!(" PASSWORD EXPIRE INTERVAL {lifetime} DAY"));
        }
    } else if !user.password_expired {
        sql.push_str(" PASSWORD EXPIRE DEFAULT");
    }
    if let Some(history) = user.password_reuse_history {
        sql.push_str(&format!(" PASSWORD HISTORY {history}"));
    } else {
        sql.push_str(" PASSWORD HISTORY DEFAULT");
    }
    if let Some(days) = user.password_reuse_time_days {
        sql.push_str(&format!(" PASSWORD REUSE INTERVAL {days} DAY"));
    } else {
        sql.push_str(" PASSWORD REUSE INTERVAL DEFAULT");
    }
    if let Some(require) = user.password_require_current {
        sql.push_str(if require {
            " PASSWORD REQUIRE CURRENT"
        } else {
            " PASSWORD REQUIRE CURRENT OPTIONAL"
        });
    } else {
        sql.push_str(" PASSWORD REQUIRE CURRENT DEFAULT");
    }
    if user.failed_login_attempts != 0 {
        sql.push_str(&format!(
            " FAILED_LOGIN_ATTEMPTS {}",
            user.failed_login_attempts
        ));
    }
    if let Some(days) = user.password_lock_time_days {
        if days == u16::MAX {
            sql.push_str(" PASSWORD_LOCK_TIME UNBOUNDED");
        } else {
            sql.push_str(&format!(" PASSWORD_LOCK_TIME {days}"));
        }
    }
    if user.password_expired {
        sql.push_str(" PASSWORD EXPIRE");
    }
    if user.account_locked {
        sql.push_str(" ACCOUNT LOCK");
    } else {
        sql.push_str(" ACCOUNT UNLOCK");
    }
    if let Some(attrs) = &user.user_attributes_json {
        sql.push_str(&format!(" ATTRIBUTE '{}'", sql_quote(attrs)));
    }
    Ok(QueryResponse::ResultSet(ResultSet {
        columns: vec![col(
            &format!("CREATE USER for {}", account.current_user_value()),
            ColumnType::VarString,
        )],
        rows: vec![vec![Value::String(sql)]],
    }))
}

fn sql_quote(value: &str) -> String {
    value.replace('\'', "''")
}

pub(crate) fn show_privileges_result() -> QueryResponse {
    // The full MySQL 8.0 catalog of static privileges plus the dynamic
    // privileges mydb actually implements. Context strings are verbatim
    // from the reference manual so client tooling that grep-matches on
    // them keeps working. Capitalisation (`Alter routine`, `Show databases`,
    // …) is the manual's; protocol-visible casing matters for ORMs.
    let row = |privilege: &str, context: &str, comment: &str| {
        vec![
            Value::String(privilege.to_string()),
            Value::String(context.to_string()),
            Value::String(comment.to_string()),
        ]
    };
    let rows = vec![
        row("Alter", "Tables", "To alter the table"),
        row(
            "Alter routine",
            "Functions,Procedures",
            "To alter or drop stored functions/procedures",
        ),
        row(
            "Create",
            "Databases,Tables,Indexes",
            "To create new databases and tables",
        ),
        row(
            "Create routine",
            "Functions,Procedures",
            "To use CREATE FUNCTION/PROCEDURE",
        ),
        row("Create role", "Server Admin", "To create new roles"),
        row(
            "Create temporary tables",
            "Databases",
            "To use CREATE TEMPORARY TABLE",
        ),
        row("Create view", "Tables", "To create new views"),
        row("Create user", "Server Admin", "To create new users"),
        row("Delete", "Tables", "To delete existing rows"),
        row(
            "Drop",
            "Databases,Tables",
            "To drop databases, tables, and views",
        ),
        row("Drop role", "Server Admin", "To drop roles"),
        row(
            "Event",
            "Server Admin",
            "To create, alter, drop and execute events",
        ),
        row(
            "Execute",
            "Functions,Procedures",
            "To execute stored routines",
        ),
        row(
            "File",
            "File access on server",
            "To read and write files on the server",
        ),
        row(
            "Grant option",
            "Databases,Tables,Functions,Procedures",
            "To give to other users those privileges you possess",
        ),
        row("Index", "Tables", "To create or drop indexes"),
        row("Insert", "Tables", "To insert data into tables"),
        row(
            "Lock tables",
            "Databases",
            "To use LOCK TABLES (together with SELECT privilege)",
        ),
        row(
            "Process",
            "Server Admin",
            "To view the plain text of currently executing queries",
        ),
        row("Proxy", "Server Admin", "To make proxy user possible"),
        row(
            "References",
            "Databases,Tables",
            "To have references on tables",
        ),
        row(
            "Reload",
            "Server Admin",
            "To reload or refresh tables, logs and privileges",
        ),
        row(
            "Replication client",
            "Server Admin",
            "To ask where the slave or master servers are",
        ),
        row(
            "Replication slave",
            "Server Admin",
            "To read binary log events from the master",
        ),
        row("Select", "Tables", "To retrieve rows from table"),
        row(
            "Show databases",
            "Server Admin",
            "To see all databases with SHOW DATABASES",
        ),
        row("Show view", "Tables", "To see views with SHOW CREATE VIEW"),
        row("Shutdown", "Server Admin", "To shut down the server"),
        row(
            "Super",
            "Server Admin",
            "To use KILL thread, SET GLOBAL, CHANGE MASTER, etc.",
        ),
        row("Trigger", "Tables", "To use triggers"),
        row(
            "Create tablespace",
            "Server Admin",
            "To create/alter/drop tablespaces",
        ),
        row("Update", "Tables", "To update existing rows"),
        row(
            "Usage",
            "Server Admin",
            "No privileges - allow connect only",
        ),
        // Dynamic privileges mydb recognises.  These mirror the MySQL 8.0
        // dynamic-privilege catalogue and stay in lock-step with what
        // Privilege::parse() accepts.
        row(
            "APPLICATION_PASSWORD_ADMIN",
            "Server Admin",
            "Dynamic privilege",
        ),
        row("AUDIT_ABORT_EXEMPT", "Server Admin", "Dynamic privilege"),
        row("AUDIT_ADMIN", "Server Admin", "Dynamic privilege"),
        row(
            "AUTHENTICATION_POLICY_ADMIN",
            "Server Admin",
            "Dynamic privilege",
        ),
        row("BACKUP_ADMIN", "Server Admin", "Dynamic privilege"),
        row("BINLOG_ADMIN", "Server Admin", "Dynamic privilege"),
        row(
            "BINLOG_ENCRYPTION_ADMIN",
            "Server Admin",
            "Dynamic privilege",
        ),
        row("CLONE_ADMIN", "Server Admin", "Dynamic privilege"),
        row("CONNECTION_ADMIN", "Server Admin", "Dynamic privilege"),
        row("ENCRYPTION_KEY_ADMIN", "Server Admin", "Dynamic privilege"),
        row("FIREWALL_ADMIN", "Server Admin", "Dynamic privilege"),
        row("FIREWALL_EXEMPT", "Server Admin", "Dynamic privilege"),
        row("FIREWALL_USER", "Server Admin", "Dynamic privilege"),
        row("FLUSH_OPTIMIZER_COSTS", "Server Admin", "Dynamic privilege"),
        row("FLUSH_STATUS", "Server Admin", "Dynamic privilege"),
        row("FLUSH_TABLES", "Server Admin", "Dynamic privilege"),
        row("FLUSH_USER_RESOURCES", "Server Admin", "Dynamic privilege"),
        row(
            "GROUP_REPLICATION_ADMIN",
            "Server Admin",
            "Dynamic privilege",
        ),
        row(
            "GROUP_REPLICATION_STREAM",
            "Server Admin",
            "Dynamic privilege",
        ),
        row(
            "INNODB_REDO_LOG_ARCHIVE",
            "Server Admin",
            "Dynamic privilege",
        ),
        row(
            "INNODB_REDO_LOG_ENABLE",
            "Server Admin",
            "Dynamic privilege",
        ),
        row(
            "PASSWORDLESS_USER_ADMIN",
            "Server Admin",
            "Dynamic privilege",
        ),
        row(
            "PERSIST_RO_VARIABLES_ADMIN",
            "Server Admin",
            "Dynamic privilege",
        ),
        row("REPLICATION_APPLIER", "Server Admin", "Dynamic privilege"),
        row(
            "REPLICATION_SLAVE_ADMIN",
            "Server Admin",
            "Dynamic privilege",
        ),
        row("RESOURCE_GROUP_ADMIN", "Server Admin", "Dynamic privilege"),
        row("RESOURCE_GROUP_USER", "Server Admin", "Dynamic privilege"),
        row("ROLE_ADMIN", "Server Admin", "Dynamic privilege"),
        row(
            "SENSITIVE_VARIABLES_OBSERVER",
            "Server Admin",
            "Dynamic privilege",
        ),
        row(
            "SERVICE_CONNECTION_ADMIN",
            "Server Admin",
            "Dynamic privilege",
        ),
        row(
            "SESSION_VARIABLES_ADMIN",
            "Server Admin",
            "Dynamic privilege",
        ),
        row("SET_USER_ID", "Server Admin", "Dynamic privilege"),
        row("SHOW_ROUTINE", "Server Admin", "Dynamic privilege"),
        row("SYSTEM_USER", "Server Admin", "Dynamic privilege"),
        row(
            "SYSTEM_VARIABLES_ADMIN",
            "Server Admin",
            "Dynamic privilege",
        ),
        row(
            "TABLE_ENCRYPTION_ADMIN",
            "Server Admin",
            "Dynamic privilege",
        ),
        row("TELEMETRY_LOG_ADMIN", "Server Admin", "Dynamic privilege"),
        row("XA_RECOVER_ADMIN", "Server Admin", "Dynamic privilege"),
    ];
    QueryResponse::ResultSet(ResultSet {
        columns: vec![
            col("Privilege", ColumnType::VarString),
            col("Context", ColumnType::VarString),
            col("Comment", ColumnType::VarString),
        ],
        rows,
    })
}
