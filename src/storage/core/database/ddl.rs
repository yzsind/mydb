fn set_column_default_in_candidate(
    table: &mut TableSchema,
    idx: usize,
    column_name: &str,
    default: Option<crate::parser::Expr>,
    preserve_backfill: bool,
) -> Result<(), QueryError> {
    if table.columns[idx].generated_expr.is_some() && default.is_some() {
        return Err(error(
            crate::error::catalog::ER_NON_DEFAULT_VALUE_FOR_GENERATED_COLUMN,
            "generated column cannot have a DEFAULT value",
        ));
    }
    if preserve_backfill && table.columns[idx].read_backfill_default.is_none() {
        table.columns[idx].read_backfill_default = Some(
            crate::engine::executor::insert::column_read_backfill_default(&table.columns[idx]),
        );
    }
    apply_column_default(table, idx, column_name, default)
}

fn set_check_enforced_in_candidate<const INSENSITIVE: bool>(
    table: &mut TableSchema,
    name: &str,
    enforced: bool,
) -> Result<(), QueryError> {
    let check = table
        .check_constraints
        .iter_mut()
        .find(|check| {
            if INSENSITIVE {
                check.name.eq_ignore_ascii_case(name)
            } else {
                check.name == name
            }
        })
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_CONSTRAINT_NOT_FOUND,
                format!("Check constraint '{name}' does not exist."),
            )
        })?;
    check.enforced = enforced;
    Ok(())
}

fn apply_column_default(
    table: &mut TableSchema,
    idx: usize,
    column_name: &str,
    default: Option<crate::parser::Expr>,
) -> Result<(), QueryError> {
    match default {
        Some(expr) if is_current_timestamp_expr(&expr) => {
            table.columns[idx].default = None;
            table.columns[idx].default_expr = Some("CURRENT_TIMESTAMP".to_string());
        }
        Some(expr) => {
            let value = literal_default_value(&expr)?;
            if !matches!(value, Value::Null) {
                if let Some(err) = enum_set_default_error(
                    column_name,
                    &table.columns[idx].data_type,
                    &value,
                    table.columns[idx].collation.as_deref(),
                ) {
                    return Err(err);
                }
            }
            if !matches!(value, Value::Null)
                && matches!(
                    table.columns[idx].data_type,
                    DataTypeKind::Blob { .. }
                        | DataTypeKind::Text { .. }
                        | DataTypeKind::Geometry { .. }
                        | DataTypeKind::Json
                )
            {
                return Err(error(
                    crate::error::catalog::ER_BLOB_CANT_HAVE_DEFAULT,
                    format!(
                        "BLOB, TEXT, GEOMETRY or JSON column '{}' can't have a default value",
                        column_name
                    ),
                ));
            }
            table.columns[idx].default = Some(value);
            table.columns[idx].default_expr = None;
        }
        None => {
            table.columns[idx].default = None;
            table.columns[idx].default_expr = None;
        }
    }
    Ok(())
}

fn validate_index_prefix_columns(
    table: &TableSchema,
    index: &IndexSchema,
    prefix_lengths: &[Option<usize>],
) -> Result<(), QueryError> {
    let prefix_check_columns: Vec<String> = (0..index.columns.len())
        .map(|pos| match index.key_part(pos) {
            Some(crate::storage::IndexKeyPart::Column(name)) => name.to_string(),
            _ => String::new(),
        })
        .collect();
    let prefix_check_lengths: Vec<Option<usize>> = prefix_lengths
        .iter()
        .enumerate()
        .map(|(pos, len)| match index.key_part(pos) {
            Some(crate::storage::IndexKeyPart::Expression(_)) => None,
            _ => *len,
        })
        .collect();
    validate_index_key_parts(
        &prefix_check_columns,
        &prefix_check_lengths,
        &table.columns,
        &index.index_type,
    )?;
    Ok(())
}

fn validate_index_column_names(table: &TableSchema, index: &IndexSchema) -> Result<(), QueryError> {
    let column_only_parts: Vec<String> = (0..index.columns.len())
        .filter_map(|pos| match index.key_part(pos) {
            Some(crate::storage::IndexKeyPart::Column(name)) => Some(name.to_string()),
            _ => None,
        })
        .collect();
    validate_columns(&column_only_parts, &table.columns)?;
    Ok(())
}

fn validate_supported_index_type(index: &IndexSchema) -> Result<(), QueryError> {
    if !index.index_type.eq_ignore_ascii_case("BTREE")
        && !index.is_fulltext()
        && !index.index_type.eq_ignore_ascii_case("SPATIAL")
        && !index.is_vector()
    {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            format!("unsupported index type '{}'", index.index_type),
        ));
    }
    Ok(())
}

fn validate_vector_index_column(
    table: &TableSchema,
    index: &IndexSchema,
) -> Result<(), QueryError> {
    if index.is_vector() {
        if index.columns.len() != 1 || index.unique {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "a vector index requires a single non-unique VECTOR column",
            ));
        }
        match table
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(&index.columns[0]))
        {
            Some(c) if matches!(c.data_type, crate::storage::DataTypeKind::Vector { .. }) => {
                let crate::storage::DataTypeKind::Vector { dim, datatype } = c.data_type else {
                    unreachable!()
                };
                if dim.is_none() {
                    return Err(vector::vector_err(
                        "a vector index requires a fixed dimension",
                    ));
                }
                let metric = index.vector_meta_for_table(table).metric_enum();
                if (datatype == crate::vector::VectorDataType::Binary)
                    != matches!(metric, vector::Metric::Hamming | vector::Metric::Jaccard)
                {
                    return Err(vector::vector_err(
                        "vector datatype and index metric are incompatible",
                    ));
                }

                if vector_storage_paged() && index.vector_algo() == VectorAlgo::Hnsw {
                    if let crate::storage::DataTypeKind::Vector {
                        dim: Some(dim),
                        datatype,
                    } = c.data_type
                    {
                        VectorIndexFile::validate_hnsw_layout(
                            if datatype == crate::vector::VectorDataType::Float32 {
                                dim as usize
                            } else {
                                native_vector_geometry(datatype, dim) as usize
                            },
                            index.vector_meta().m as usize,
                        )?;
                    }
                }
                if vector_storage_paged() && index.vector_algo() == VectorAlgo::Ivf {
                    VectorIndexFile::validate_native_ivf_layout(datatype, dim.unwrap())?;
                }
                if c.nullable {
                    return Err(error(
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        "a vector index column must be NOT NULL",
                    ));
                }
            }
            _ => {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "a vector index requires a VECTOR-typed column",
                ))
            }
        }
    }
    Ok(())
}

/// Revalidate in both catalog phases; callers own locking and publication.
fn validate_add_index_definition(
    table: &TableSchema,
    index: &IndexSchema,
) -> Result<(), QueryError> {
    validate_table_index_count(
        table
            .indexes
            .iter()
            .filter(|i| {
                !i.primary
                    && table
                        .pending_primary_key
                        .as_ref()
                        .is_none_or(|p| p.final_index.index_id != i.index_id)
            })
            .count()
            + usize::from(!index.primary),
    )?;
    // Skip expression key parts for column-name validation —
    // their `columns[pos]` slot holds the expression SQL text,
    // not a column name. Functional parts have already passed
    // a stricter check inside `compile_index_key_expressions`.
    validate_index_column_names(table, index)?;
    let prefix_lengths =
        normalized_index_prefix_lengths(index.column_prefix_lengths.clone(), index.columns.len());
    for (pos, len) in prefix_lengths.iter().enumerate() {
        if len.is_some()
            && matches!(
                index.key_part(pos),
                Some(crate::storage::IndexKeyPart::Expression(_))
            )
        {
            return Err(error(
                crate::error::catalog::ER_WRONG_SUB_KEY,
                "prefix length is not allowed for functional index key part",
            ));
        }
    }
    validate_index_prefix_columns(table, index, &prefix_lengths)?;
    index.validate_multi_valued_definition()?;
    validate_supported_index_type(index)?;
    validate_vector_index_column(table, index)?;
    Ok(())
}

static DDL_COPY_TOTAL: AtomicU64 = AtomicU64::new(0);
#[cfg(test)]
thread_local! {
    static DDL_CATALOG_COMMIT_FAILURE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static DDL_CLEANUP_FAILURE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
static DDL_COPY_SCAN_US: AtomicU64 = AtomicU64::new(0);
static DDL_COPY_CONVERT_US: AtomicU64 = AtomicU64::new(0);
static DDL_COPY_INDEX_US: AtomicU64 = AtomicU64::new(0);
static DDL_COPY_REPLAY_US: AtomicU64 = AtomicU64::new(0);
static DDL_COPY_SWAP_US: AtomicU64 = AtomicU64::new(0);
static DDL_COPY_SOURCE_BYTES: AtomicU64 = AtomicU64::new(0);
static DDL_COPY_SHADOW_BYTES: AtomicU64 = AtomicU64::new(0);
static DDL_COPY_SPILL_BYTES: AtomicU64 = AtomicU64::new(0);
static DDL_COPY_DIRECT_HEAP_BUILDS: AtomicU64 = AtomicU64::new(0);
static DDL_COPY_PEAK_BATCH_BYTES: AtomicU64 = AtomicU64::new(0);
static DDL_COPY_INDEX_FEED_US: AtomicU64 = AtomicU64::new(0);
static DDL_COPY_INDEX_BUILD_US: AtomicU64 = AtomicU64::new(0);
static DDL_FINAL_BARRIER_COUNT: AtomicU64 = AtomicU64::new(0);
static DDL_FINAL_BARRIER_TOTAL_US: AtomicU64 = AtomicU64::new(0);
static DDL_FINAL_BARRIER_MAX_US: AtomicU64 = AtomicU64::new(0);
static DDL_FINAL_BARRIER_LE_1MS: AtomicU64 = AtomicU64::new(0);
static DDL_FINAL_BARRIER_LE_10MS: AtomicU64 = AtomicU64::new(0);
static DDL_FINAL_BARRIER_LE_100MS: AtomicU64 = AtomicU64::new(0);
static DDL_FINAL_BARRIER_LE_1S: AtomicU64 = AtomicU64::new(0);
static DDL_FINAL_BARRIER_GT_1S: AtomicU64 = AtomicU64::new(0);

#[inline]
fn ddl_table_not_found(name: &str) -> QueryError {
    error(
        crate::error::catalog::ER_NO_SUCH_TABLE,
        format!("Table '{name}' doesn't exist"),
    )
}

fn table_ddl_schema_is_read_only(database: &str) -> bool {
    database.eq_ignore_ascii_case("information_schema")
        || database.eq_ignore_ascii_case("performance_schema")
}

/// Streaming COPY is the product path.  The switch intentionally remains as
/// a differential oracle for storage regression tests.
fn ddl_copy_streaming_enabled() -> bool {
    #[cfg(not(test))]
    {
        true
    }
    #[cfg(test)]
    {
        std::env::var("MYDB_DDL_COPY_STREAMING")
            .map(|value| {
                !matches!(
                    value.to_ascii_lowercase().as_str(),
                    "0" | "off" | "false" | "no"
                )
            })
            .unwrap_or(true)
    }
}

/// Number of source heap pages transformed in one bounded COPY batch.
fn ddl_copy_batch_pages() -> u64 {
    std::env::var("MYDB_DDL_COPY_BATCH_PAGES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|pages| *pages > 0)
        .unwrap_or(256)
}

/// Common schema pieces derived from a `CREATE TABLE ... LIKE` source,
/// shared by the persistent and TEMPORARY code paths.
struct LikeSchemaParts {
    columns: Vec<ColumnSchema>,
    indexes: Vec<IndexSchema>,
    primary_key: Vec<String>,
    check_constraints: Vec<CheckConstraintSchema>,
    mysql_table_options: Vec<(String, String)>,
    pctfree: u8,
    pctused: u8,
}

impl LikeSchemaParts {
    fn finish(
        self,
        source: &TableSchema,
        id: u64,
        database: String,
        table_name: String,
        partitioning: Option<crate::storage::partitioning::PartitionInfo>,
        now_ms: u64,
    ) -> TableSchema {
        TableSchema {
            id,
            object_id: id,
            active_generation: 1,
            database,
            name: table_name,
            engine: source.engine.clone(),
            charset: source.charset.clone(),
            collation: source.collation.clone(),
            comment: source.comment.clone(),
            created_ms: now_ms,
            updated_ms: now_ms,
            table_type: "BASE TABLE".to_string(),
            columns: self.columns,
            primary_key: self.primary_key,
            indexes: self.indexes,
            check_constraints: self.check_constraints,
            // CREATE TABLE ... LIKE does not copy foreign keys (MySQL).
            foreign_keys: Vec::new(),
            fk_referenced_by: Vec::new(),
            stats: TableStats::empty(),
            system: false,
            schema_version: 1,
            system_time_epoch_us: now_ms.saturating_mul(1_000),
            row_generation_txn_id: 0,
            next_column_id: 0,
            active_layout_id: 1,
            row_layouts: Vec::new(),
            pending_primary_key: None,
            primary_definition_scn: 0,
            next_index_id: 0,
            primary_publication_job_id: 0,
            column_name_map: Default::default(),
            column_id_map: std::collections::HashMap::new(),
            row_codec: Default::default(),
            partitioning,
            pctfree: self.pctfree,
            pctused: self.pctused,
            mysql_table_options: self.mysql_table_options,
        }
    }
}

/// Parser-level CREATE TABLE input that is identical for persistent and
/// session-temporary tables. Persistence, catalog publication, and physical
/// file creation deliberately remain in their respective callers.
struct CreateTableSchemaSpec {
    database: String,
    table_name: String,
    columns: Vec<ColumnDef>,
    primary_key: Vec<String>,
    indexes: Vec<crate::parser::IndexDef>,
    checks: Vec<crate::parser::CheckConstraintDef>,
    partition: Option<crate::parser_partition::PartitionDef>,
    table_charset: String,
    table_collation: String,
    column_comments: Vec<(String, String)>,
    table_comment: String,
    pctfree: Option<u8>,
    pctused: Option<u8>,
    mysql_table_options: Vec<(String, String)>,
    requested_engine: Option<String>,
    temporary: bool,
}

/// CREATE schema after column/index validation but before logical/storage IDs
/// are assigned. Keeping this phase ID-free preserves the existing failure
/// semantics: column and index definition errors do not consume a table ID.
struct PreparedCreateTableSchema {
    database: String,
    table_name: String,
    columns: Vec<ColumnSchema>,
    primary_key: Vec<String>,
    indexes: Vec<IndexSchema>,
    check_defs: Vec<crate::parser::CheckConstraintDef>,
    partition: Option<crate::parser_partition::PartitionDef>,
    table_charset: String,
    table_collation: String,
    column_comments: Vec<(String, String)>,
    table_comment: String,
    pctfree: Option<u8>,
    pctused: Option<u8>,
    mysql_table_options: Vec<(String, String)>,
    requested_engine: Option<String>,
    temporary: bool,
}

/// Crash-safe file exchange shared by ALTER COPY and bulk shadow loads.
struct ShadowGenerationFileSwap {
    old_to_backup: Vec<(PathBuf, PathBuf)>,
    shadow_to_live: Vec<(PathBuf, PathBuf)>,
}

struct AlterCopyProjection {
    columns: Vec<usize>,
    decode: RowDecodePlan,
}

fn alter_copy_projection(
    old_table: &TableSchema,
    candidate: &TableSchema,
    rewrite_mask: &[bool],
) -> Option<AlterCopyProjection> {
    if !candidate.check_constraints.is_empty()
        || candidate
            .columns
            .iter()
            .any(|column| column.generated_expr.is_some() || column.default_expr.is_some())
        || candidate.indexes.iter().any(|index| {
            index.has_expression_parts()
                || index.is_fulltext()
                || index.is_spatial()
                || index.is_vector()
                || index.has_multi_valued_parts()
        })
    {
        return None;
    }
    let mut columns = rewrite_mask
        .iter()
        .enumerate()
        .filter_map(|(idx, rewrite)| (*rewrite && idx < old_table.columns.len()).then_some(idx))
        .collect::<Vec<_>>();
    for index in &candidate.indexes {
        for name in index.effective_columns() {
            let idx = candidate.column_index(name).ok()?;
            if idx < old_table.columns.len() {
                columns.push(idx);
            }
        }
        for name in &index.included_columns {
            let idx = candidate.column_index(name).ok()?;
            if idx < old_table.columns.len() {
                columns.push(idx);
            }
        }
    }
    columns.sort_unstable();
    columns.dedup();
    Some(AlterCopyProjection {
        decode: RowDecodePlan::compile_with_codec(&columns, &old_table.row_codec),
        columns,
    })
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct AlterCopyIntent {
    version: u32,
    table_id: u64,
    database: String,
    table_name: String,
    old_schema_version: u64,
    new_schema_version: u64,
    #[serde(default)]
    new_generation: u64,
    shadow_id: u64,
    old_to_backup: Vec<(String, String)>,
    shadow_to_live: Vec<(String, String)>,
}

const MYSQL_MAX_IDENTIFIER_CHARS: usize = 64;
const MYSQL_MAX_COLUMNS_PER_TABLE: usize = 1017;

fn validate_mysql_identifier(name: &str) -> Result<(), QueryError> {
    if name.chars().count() > MYSQL_MAX_IDENTIFIER_CHARS {
        Err(error(
            crate::error::catalog::ER_TOO_LONG_IDENT,
            format!("Identifier name '{name}' is too long"),
        ))
    } else {
        Ok(())
    }
}

fn validate_vector_declaration(
    dim: Option<u32>,
    datatype: crate::vector::VectorDataType,
) -> Result<(), QueryError> {
    if dim.is_some_and(|n| n == 0 || n > datatype.max_dimension()) {
        return Err(vector::vector_err(format!(
            "{datatype} VECTOR dimension must be in [1,{}]",
            datatype.max_dimension()
        )));
    }
    if dim.is_none() && datatype != crate::vector::VectorDataType::Float32 {
        return Err(vector::vector_err(format!(
            "{datatype} VECTOR requires a declared dimension; use CAST AS VECTOR(n,{datatype})"
        )));
    }
    Ok(())
}

fn validate_create_table_identifiers(
    table_name: &str,
    columns: &[ColumnDef],
) -> Result<(), QueryError> {
    validate_mysql_identifier(table_name)?;
    if columns.len() > MYSQL_MAX_COLUMNS_PER_TABLE {
        return Err(error(
            crate::error::catalog::ER_TOO_MANY_FIELDS,
            "Too many columns",
        ));
    }
    for column in columns {
        validate_mysql_identifier(&column.name)?;
        if let crate::parser::DataType::Vector(dim, datatype) = column.data_type {
            validate_vector_declaration(dim, datatype)?;
        }
    }
    Ok(())
}

fn validate_table_column_identifiers(table: &TableSchema) -> Result<(), QueryError> {
    if table.visible_column_count() > MYSQL_MAX_COLUMNS_PER_TABLE {
        return Err(error(
            crate::error::catalog::ER_TOO_MANY_FIELDS,
            "Too many columns",
        ));
    }
    for (_, column) in table.iter_visible_columns() {
        validate_mysql_identifier(&column.name)?;
        if let DataTypeKind::Vector { dim, datatype } = column.data_type {
            validate_vector_declaration(dim, datatype)?;
        }
    }
    Ok(())
}

fn prepare_create_table_schema(
    spec: CreateTableSchemaSpec,
) -> Result<PreparedCreateTableSchema, QueryError> {
    crate::storage::schema::validate_supported_table_options(&spec.mysql_table_options)?;
    let CreateTableSchemaSpec {
        database,
        table_name,
        columns,
        primary_key,
        indexes,
        checks,
        partition,
        table_charset,
        table_collation,
        column_comments,
        table_comment,
        pctfree,
        pctused,
        mysql_table_options,
        requested_engine,
        temporary,
    } = spec;
    if let Some(engine) = requested_engine.as_deref() {
        if !engine.eq_ignore_ascii_case("mydb") && !engine.eq_ignore_ascii_case("innodb") {
            return Err(error(
                crate::error::catalog::ER_UNKNOWN_STORAGE_ENGINE,
                format!("Unknown storage engine '{engine}'"),
            ));
        }
    }

    if columns.is_empty() {
        return Err(error(
            crate::error::catalog::ER_TABLE_MUST_HAVE_COLUMNS,
            "a table must have at least one column",
        ));
    }

    use crate::storage::schema::{check_comment_len, CommentTarget};
    check_comment_len(CommentTarget::Table, &table_name, &table_comment)?;
    for (column_name, comment) in &column_comments {
        check_comment_len(CommentTarget::Field, column_name, comment)?;
    }

    let mut seen = BTreeMap::new();
    let mut column_schemas = Vec::with_capacity(columns.len());
    let mut column_checks = Vec::new();
    for column in columns {
        let column_key = normalize_name(&column.name);
        if seen.insert(column_key, ()).is_some() {
            return Err(error(
                crate::error::catalog::ER_DUP_FIELDNAME,
                format!("duplicate column '{}'", column.name),
            ));
        }
        column_checks.extend(column.checks.iter().cloned());
        let mut schema = column_def_to_schema(column)?;
        apply_column_charset_defaults(&mut schema, &table_charset, &table_collation)?;
        column_schemas.push(schema);
    }
    apply_legacy_first_timestamp_defaults(&mut column_schemas);

    let mut index_schemas = Vec::new();
    let mut primary_key = primary_key
        .into_iter()
        .map(|column| normalize_name(&column))
        .collect::<Vec<_>>();
    if !primary_key.is_empty() {
        validate_columns(&primary_key, &column_schemas)?;
        validate_index_key_parts(
            &primary_key,
            &vec![None; primary_key.len()],
            &column_schemas,
            "BTREE",
        )?;
        enforce_key_columns_not_null(&primary_key, &mut column_schemas)?;
        index_schemas.push(IndexSchema {
            readable_from_scn: 0,
            index_id: 0,
            name: "PRIMARY".to_string(),
            columns: primary_key.clone(),
            column_prefix_lengths: vec![None; primary_key.len()],
            column_directions: vec![crate::storage::IndexColumnDirection::Asc; primary_key.len()],
            key_expressions: Vec::new(),
            unique: true,
            primary: true,
            included_columns: Vec::new(),
            state: crate::storage::IndexState::Public,
            index_type: "BTREE".to_string(),
            visible: true,
            comment: String::new(),
            fulltext: None,
        });
    }

    for mut index in indexes {
        let prefix_lengths =
            normalized_index_prefix_lengths(index.column_prefix_lengths, index.columns.len());
        let mut directions = index
            .column_descending
            .iter()
            .map(|descending| {
                if *descending {
                    crate::storage::IndexColumnDirection::Desc
                } else {
                    crate::storage::IndexColumnDirection::Asc
                }
            })
            .collect::<Vec<_>>();
        directions.resize(
            index.columns.len(),
            crate::storage::IndexColumnDirection::Asc,
        );
        directions.truncate(index.columns.len());

        // Expression positions contain canonical SQL, not identifiers.
        let parser_key_expressions = index.key_expressions;
        let parser_key_expression_multi_valued = index.key_expression_multi_valued;
        let columns = index
            .columns
            .into_iter()
            .enumerate()
            .map(|(position, column)| {
                if parser_key_expressions
                    .get(position)
                    .and_then(Option::as_ref)
                    .is_some()
                {
                    column
                } else {
                    normalize_name(&column)
                }
            })
            .collect::<Vec<_>>();
        let column_only_parts = columns
            .iter()
            .zip(parser_key_expressions.iter())
            .filter(|(_, expression)| expression.is_none())
            .map(|(column, _)| column.clone())
            .collect::<Vec<_>>();
        validate_columns(&column_only_parts, &column_schemas)?;

        if index.index_type.eq_ignore_ascii_case("BTREE")
            && !index.unique
            && columns.len() == 1
            && parser_key_expressions.iter().all(Option::is_none)
            && prefix_lengths.iter().all(Option::is_none)
            && column_schemas
                .iter()
                .find(|column| column.name == columns[0])
                .is_some_and(|column| {
                    matches!(
                        column.data_type,
                        crate::storage::DataTypeKind::Geometry { .. }
                    )
                })
        {
            index.index_type = "SPATIAL".to_string();
        }

        let prefix_check_columns = columns
            .iter()
            .zip(parser_key_expressions.iter())
            .map(|(column, expression)| {
                if expression.is_some() {
                    String::new()
                } else {
                    column.clone()
                }
            })
            .collect::<Vec<_>>();
        let prefix_check_lengths = prefix_lengths
            .iter()
            .zip(parser_key_expressions.iter())
            .map(|(length, expression)| if expression.is_some() { None } else { *length })
            .collect::<Vec<_>>();
        for (length, expression) in prefix_lengths.iter().zip(parser_key_expressions.iter()) {
            if expression.is_some() && length.is_some() {
                return Err(error(
                    crate::error::catalog::ER_WRONG_SUB_KEY,
                    "prefix length is not allowed for functional index key part",
                ));
            }
        }
        validate_index_key_parts(
            &prefix_check_columns,
            &prefix_check_lengths,
            &column_schemas,
            &index.index_type,
        )?;

        let is_spatial = index.index_type.eq_ignore_ascii_case("SPATIAL");
        let is_vector = index.index_type.eq_ignore_ascii_case("HNSW")
            || index.index_type.eq_ignore_ascii_case("VECTOR")
            || index.index_type.eq_ignore_ascii_case("IVF");
        if !index.index_type.eq_ignore_ascii_case("BTREE")
            && !index.index_type.eq_ignore_ascii_case("FULLTEXT")
            && !is_spatial
            && !is_vector
        {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                format!("unsupported index type '{}'", index.index_type),
            ));
        }

        if is_vector {
            crate::storage::schema::VectorIndexMeta::validate_from_comment(
                &index.comment,
                if index.index_type.eq_ignore_ascii_case("IVF") {
                    crate::storage::schema::VectorAlgo::Ivf
                } else {
                    crate::storage::schema::VectorAlgo::Hnsw
                },
            )?;
            if columns.len() != 1 || index.unique {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "a vector index requires a single non-unique VECTOR column",
                ));
            }
            let vector_column = column_schemas
                .iter()
                .find(|column| column.name == columns[0])
                .is_some_and(|column| {
                    matches!(
                        column.data_type,
                        crate::storage::DataTypeKind::Vector { .. }
                    )
                });
            if !vector_column {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "a vector index requires a VECTOR-typed column",
                ));
            }
            for column in &column_schemas {
                if column.nullable
                    && columns
                        .iter()
                        .any(|name| name.eq_ignore_ascii_case(&column.name))
                {
                    crate::engine::api::push_statement_warning(
                        crate::error::ConditionLevel::Warning,
                        crate::error::catalog::ER_NOT_SUPPORTED_YET,
                        format!(
                            "Vector index requires NOT NULL; column '{}' was made NOT NULL",
                            column.name
                        ),
                    );
                }
            }
            enforce_key_columns_not_null(&columns, &mut column_schemas)?;
        }

        if is_spatial {
            if columns.len() != 1 || index.unique {
                return Err(error(
                    crate::error::catalog::ER_SPATIAL_UNIQUE_INDEX,
                    "a SPATIAL index requires a single non-unique geometry column",
                ));
            }
            if parser_key_expressions.iter().any(Option::is_some)
                || prefix_lengths.iter().any(Option::is_some)
                || directions.iter().any(|direction| {
                    matches!(direction, crate::storage::IndexColumnDirection::Desc)
                })
            {
                return Err(error(
                    crate::error::catalog::ER_SPATIAL_FUNCTIONAL_INDEX,
                    "SPATIAL indexes do not support expressions, prefixes, or descending key parts",
                ));
            }
            let geometry_column = column_schemas
                .iter()
                .find(|column| column.name == columns[0])
                .is_some_and(|column| {
                    matches!(
                        column.data_type,
                        crate::storage::DataTypeKind::Geometry { .. }
                    )
                });
            if !geometry_column {
                return Err(error(
                    crate::error::catalog::ER_SPATIAL_MUST_HAVE_GEOM_COL,
                    "A SPATIAL index may only contain a geometrical type column",
                ));
            }
            validate_spatial_columns_not_null(&columns, &column_schemas)?;
        }

        let primary = index
            .name
            .as_deref()
            .is_some_and(|name| name.eq_ignore_ascii_case("PRIMARY"));
        if primary {
            if !index.visible {
                return Err(error(
                    crate::error::catalog::ER_PK_INDEX_CANT_BE_INVISIBLE,
                    "PRIMARY KEY indexes cannot be invisible",
                ));
            }
            if !primary_key.is_empty()
                || index_schemas
                    .iter()
                    .any(|existing: &IndexSchema| existing.primary)
            {
                return Err(error(
                    crate::error::catalog::ER_MULTIPLE_PRI_KEY,
                    "multiple primary key defined",
                ));
            }
            if !index.unique {
                return Err(error(
                    crate::error::catalog::ER_PARSE_ERROR,
                    "PRIMARY index must be unique",
                ));
            }
            if parser_key_expressions.iter().any(Option::is_some) {
                return Err(error(
                    crate::error::catalog::ER_PARSE_ERROR,
                    "PRIMARY KEY cannot be defined on an expression",
                ));
            }
            enforce_key_columns_not_null(&columns, &mut column_schemas)?;
            primary_key = columns.clone();
        }

        if parser_key_expression_multi_valued
            .iter()
            .any(|value| *value)
        {
            if primary || index.unique {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "UNIQUE or PRIMARY multi-valued indexes are not supported",
                ));
            }
            if !index.index_type.eq_ignore_ascii_case("BTREE") {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "multi-valued indexes require BTREE index type",
                ));
            }
            let multi_position = parser_key_expression_multi_valued
                .iter()
                .position(|value| *value)
                .expect("multi-valued part checked above");
            if prefix_lengths
                .get(multi_position)
                .copied()
                .flatten()
                .is_some()
            {
                return Err(error(
                    crate::error::catalog::ER_WRONG_SUB_KEY,
                    "prefix length is not allowed for multi-valued index key part",
                ));
            }
        }

        let fulltext = index.index_type.eq_ignore_ascii_case("FULLTEXT").then(|| {
            let mut metadata = crate::storage::FulltextMeta {
                parser: index.fulltext_parser.clone(),
                ..Default::default()
            };
            crate::storage::fulltext::snapshot_tokenizer_config_into(&mut metadata);
            metadata
        });
        let included_columns = validate_inline_included_columns(
            &index.included_columns,
            &index.index_type,
            index.unique,
            primary,
            &column_only_parts,
            &column_schemas,
        )?;
        let resolved_name = index.name.clone().unwrap_or_else(|| {
            let base = column_only_parts
                .first()
                .and_then(|name| {
                    column_schemas
                        .iter()
                        .find(|column| column.name.eq_ignore_ascii_case(name))
                })
                .map(|column| column.name.clone())
                .unwrap_or_else(|| "functional_index".to_string());
            let mut name = base.clone();
            let mut suffix = 2;
            while index_schemas
                .iter()
                .any(|i| i.name.eq_ignore_ascii_case(&name))
            {
                name = format!("{base}_{suffix}");
                suffix += 1;
            }
            name
        });
        validate_mysql_identifier(&resolved_name)?;
        check_comment_len(CommentTarget::Index, &resolved_name, &index.comment)?;
        index_schemas.push(IndexSchema {
            readable_from_scn: 0,
            index_id: 0,
            name: resolved_name,
            columns,
            column_prefix_lengths: prefix_lengths,
            column_directions: directions,
            key_expressions: parser_key_expressions
                .into_iter()
                .enumerate()
                .map(|(position, expression)| {
                    expression.map(|expr_sql| crate::storage::IndexExpression {
                        expr_sql,
                        data_type: crate::storage::DataTypeKind::VarChar { len: None },
                        nullable: true,
                        referenced_columns: Vec::new(),
                        multi_valued: parser_key_expression_multi_valued
                            .get(position)
                            .copied()
                            .unwrap_or(false),
                        ast: None,
                        program: None,
                    })
                })
                .collect(),
            unique: index.unique,
            primary,
            included_columns,
            state: crate::storage::IndexState::Public,
            index_type: index.index_type,
            visible: index.visible,
            comment: index.comment,
            fulltext,
        });
    }

    reject_duplicate_index_names(&index_schemas)?;
    validate_table_index_count(index_schemas.iter().filter(|index| !index.primary).count())?;
    column_checks.extend(checks);

    Ok(PreparedCreateTableSchema {
        database,
        table_name,
        columns: column_schemas,
        primary_key,
        indexes: index_schemas,
        check_defs: column_checks,
        partition,
        table_charset,
        table_collation,
        column_comments,
        table_comment,
        pctfree,
        pctused,
        mysql_table_options,
        requested_engine,
        temporary,
    })
}

impl PreparedCreateTableSchema {
    fn partition_count(&self) -> Result<usize, QueryError> {
        partition_count_from_def(self.partition.as_ref())
    }

    fn finish(self, id: u64, partition_storage_ids: &[u64]) -> Result<TableSchema, QueryError> {
        let mut mysql_table_options = Vec::new();
        crate::storage::schema::apply_generic_table_options(
            &mut mysql_table_options,
            self.mysql_table_options,
        );
        let mut table = TableSchema {
            id,
            object_id: id,
            active_generation: 1,
            database: self.database,
            name: self.table_name,
            engine: self
                .requested_engine
                .filter(|engine| !engine.is_empty())
                .unwrap_or_else(|| "InnoDB".to_string()),
            charset: self.table_charset,
            collation: self.table_collation,
            comment: self.table_comment,
            created_ms: current_unix_ms(),
            updated_ms: current_unix_ms(),
            table_type: "BASE TABLE".to_string(),
            columns: self.columns,
            primary_key: self.primary_key,
            indexes: self.indexes,
            check_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            fk_referenced_by: Vec::new(),
            stats: TableStats::empty(),
            system: false,
            schema_version: 1,
            system_time_epoch_us: current_unix_ms().saturating_mul(1_000),
            row_generation_txn_id: 0,
            next_column_id: 0,
            active_layout_id: 1,
            row_layouts: Vec::new(),
            pending_primary_key: None,
            primary_definition_scn: 0,
            next_index_id: 0,
            primary_publication_job_id: 0,
            column_name_map: Default::default(),
            column_id_map: std::collections::HashMap::new(),
            row_codec: Default::default(),
            partitioning: None,
            pctfree: self.pctfree.unwrap_or(0).min(50),
            pctused: self.pctused.unwrap_or(0).min(50),
            mysql_table_options,
        };
        table.check_constraints =
            build_check_constraints_for_table(&table, self.check_defs, None, None)?;
        for (column_name, comment) in self.column_comments {
            let column_index = table.column_index(&column_name)?;
            table.columns[column_index].comment = comment;
        }
        validate_and_prepare_generated_columns(&mut table)?;
        table.build_column_index();
        validate_bit_widths(&table)?;
        validate_row_fits_page(&table)?;
        if let Some(partition) = self.partition.as_ref() {
            let info = build_partition_info_from_def(partition, &table, partition_storage_ids)?;
            table.partitioning = Some(info);
            table.build_column_index();
        }
        table.compile_pending_index_expressions()?;
        validate_mysql80_partition_compat(&table, self.temporary)?;
        Ok(table)
    }
}

impl Database {
    pub(crate) fn ddl_copy_perf_snapshot(&self) -> [u64; 25] {
        [
            DDL_COPY_TOTAL.load(Ordering::Relaxed),
            DDL_COPY_SCAN_US.load(Ordering::Relaxed),
            DDL_COPY_CONVERT_US.load(Ordering::Relaxed),
            DDL_COPY_INDEX_US.load(Ordering::Relaxed),
            DDL_COPY_REPLAY_US.load(Ordering::Relaxed),
            DDL_COPY_SWAP_US.load(Ordering::Relaxed),
            DDL_COPY_SOURCE_BYTES.load(Ordering::Relaxed),
            DDL_COPY_SHADOW_BYTES.load(Ordering::Relaxed),
            DDL_COPY_SPILL_BYTES.load(Ordering::Relaxed),
            DDL_COPY_DIRECT_HEAP_BUILDS.load(Ordering::Relaxed),
            DDL_COPY_PEAK_BATCH_BYTES.load(Ordering::Relaxed),
            DDL_COPY_INDEX_FEED_US.load(Ordering::Relaxed),
            DDL_COPY_INDEX_BUILD_US.load(Ordering::Relaxed),
            DDL_FINAL_BARRIER_COUNT.load(Ordering::Relaxed),
            DDL_FINAL_BARRIER_TOTAL_US.load(Ordering::Relaxed),
            DDL_FINAL_BARRIER_MAX_US.load(Ordering::Relaxed),
            DDL_FINAL_BARRIER_LE_1MS.load(Ordering::Relaxed),
            DDL_FINAL_BARRIER_LE_10MS.load(Ordering::Relaxed),
            DDL_FINAL_BARRIER_LE_100MS.load(Ordering::Relaxed),
            DDL_FINAL_BARRIER_LE_1S.load(Ordering::Relaxed),
            DDL_FINAL_BARRIER_GT_1S.load(Ordering::Relaxed),
            DDL_COPY_BARRIER_WAIT_US.load(Ordering::Relaxed),
            DDL_COPY_LOCK_HOLD_US.load(Ordering::Relaxed),
            DDL_COPY_LOCK_HOLD_MAX_US.load(Ordering::Relaxed),
            DDL_COPY_FINAL_TAIL_ROWS.load(Ordering::Relaxed),
        ]
    }

    pub fn create_table(
        &self,
        name: &str,
        columns: Vec<ColumnDef>,
        primary_key: Vec<String>,
        indexes: Vec<crate::parser::IndexDef>,
    ) -> Result<(), QueryError> {
        self.create_table_with_checks(name, columns, primary_key, indexes, Vec::new(), None)
    }

    pub fn create_table_with_checks(
        &self,
        name: &str,
        columns: Vec<ColumnDef>,
        primary_key: Vec<String>,
        indexes: Vec<crate::parser::IndexDef>,
        checks: Vec<crate::parser::CheckConstraintDef>,
        auto_increment_start: Option<u64>,
    ) -> Result<(), QueryError> {
        self.create_table_with_partition(
            name,
            columns,
            primary_key,
            indexes,
            checks,
            Vec::new(),
            auto_increment_start,
            None,
            None,
            None,
            Vec::new(),
            String::new(),
            None,
            None,
            Vec::new(),
            None,
        )
    }

    /// CREATE TABLE entry that accepts an optional MySQL partition
    /// definition. For unpartitioned tables this collapses to the
    /// previous behaviour. For partitioned tables it:
    ///   - allocates one extra storage id per partition (from the
    ///     same `next_table_id` counter, so partitions never collide
    ///     with another table),
    ///   - creates a separate heap file per partition,
    ///   - attaches the `PartitionInfo` to the persisted `TableSchema`.
    pub fn create_table_with_partition(
        &self,
        name: &str,
        columns: Vec<ColumnDef>,
        primary_key: Vec<String>,
        indexes: Vec<crate::parser::IndexDef>,
        checks: Vec<crate::parser::CheckConstraintDef>,
        foreign_keys: Vec<crate::parser::ForeignKeyDef>,
        auto_increment_start: Option<u64>,
        partition: Option<crate::parser_partition::PartitionDef>,
        default_charset: Option<String>,
        default_collation: Option<String>,
        column_comments: Vec<(String, String)>,
        table_comment: String,
        pctfree: Option<u8>,
        pctused: Option<u8>,
        mysql_table_options: Vec<(String, String)>,
        requested_engine: Option<String>,
    ) -> Result<(), QueryError> {
        let (database, table_name) = split_table_name(DEFAULT_DATABASE, name);
        validate_create_table_identifiers(&table_name, &columns)?;
        let key = table_key(&database, &table_name);
        check_table_ddl_writable(&database)?;
        let (database_charset, database_collation) = {
            let catalog = self.catalog_read()?;
            let Some(schema) = catalog.databases.get(&database) else {
                return Err(error(
                    crate::error::catalog::ER_BAD_DB_ERROR,
                    format!("unknown database '{database}'"),
                ));
            };
            if catalog.tables.contains_key(&key)
                || catalog.views.contains_key(&key)
                || catalog.sequences.contains_key(&key)
            {
                return Err(crate::error::messages::table_exists(&table_name));
            }
            (schema.charset.clone(), schema.collation.clone())
        };
        let (table_charset, table_collation) = resolve_table_charset_collation(
            default_charset.as_deref(),
            default_collation.as_deref(),
            &database_charset,
            &database_collation,
        )?;
        let prepared = prepare_create_table_schema(CreateTableSchemaSpec {
            database,
            table_name,
            columns,
            primary_key,
            indexes,
            checks,
            partition,
            table_charset,
            table_collation,
            column_comments,
            table_comment,
            pctfree,
            pctused,
            mysql_table_options,
            requested_engine,
            temporary: false,
        })?;

        // Allocate the logical table id plus one storage id per partition only
        // after parser-level column/index validation has succeeded.
        let partition_count = prepared.partition_count()?;
        let (id, partition_storage_ids) = {
            let mut catalog = self.catalog_write()?;
            let id = catalog.next_table_id;
            catalog.next_table_id += 1;
            let mut partition_ids = Vec::with_capacity(partition_count);
            for _ in 0..partition_count {
                let partition_id = catalog.next_table_id;
                catalog.next_table_id += 1;
                partition_ids.push(partition_id);
            }
            (id, partition_ids)
        };
        self.ensure_table_id_highwater(self.current_next_table_id()?)?;
        let mut table = prepared.finish(id, &partition_storage_ids)?;
        // Foreign keys are persistent-catalog concerns and therefore remain
        // outside the shared persistent/temporary schema builder.
        if !foreign_keys.is_empty() {
            let (foreign_key_schemas, backing_indexes) = {
                let catalog = self.catalog_read()?;
                build_foreign_keys_for_table(&catalog, &table, foreign_keys)?
            };
            table.foreign_keys = foreign_key_schemas;
            table.indexes.extend(backing_indexes);
            table.build_column_index();
        }
        validate_auto_increment_schema(&table)?;
        let has_auto_increment = table.auto_increment_column_index().is_some();
        let auto_increment_max = if has_auto_increment {
            auto_increment_start.unwrap_or(1).saturating_sub(1)
        } else {
            0
        };
        let partitioned = table.partitioning.is_some();
        let storage_ids = table.storage_ids();
        {
            let mut catalog = self.catalog_write()?;
            // Re-check while holding the writer lock. The early probe is
            // useful for fast failure but cannot close the concurrent-CREATE
            // TOCTOU window by itself.
            if catalog.tables.contains_key(&key)
                || catalog.views.contains_key(&key)
                || catalog.sequences.contains_key(&key)
            {
                return Err(error(
                    crate::error::catalog::ER_TABLE_EXISTS_ERROR,
                    format!("Table '{name}' already exists"),
                ));
            }
            if !table.check_constraints.is_empty() {
                let schema_names =
                    check_constraint_names_in_schema(&catalog, &table.database, None);
                ensure_check_constraint_names_available(&schema_names, &table.check_constraints)?;
            }
            let rebuild_fk_runtime =
                !table.foreign_keys.is_empty() || self.catalog.may_have_foreign_keys();
            catalog.tables.insert(key.clone(), Arc::new(table));
            // Rebuild FK runtime caches so this table's child_col_idx and
            // any parent's reverse `fk_referenced_by` reflect the new FKs.
            if rebuild_fk_runtime {
                build_fk_runtime(&mut catalog);
            }
        }
        self.register_catalog_table_mapping(&key)?;
        let finalize = (|| -> Result<(), QueryError> {
            // Create one heap file per physical storage id. For partitioned
            // tables this is one heap per partition; otherwise table.id.
            if partitioned {
                for sid in &storage_ids {
                    self.create_logged_file(txn_id_for_ddl(), self.table_path(*sid), Vec::new())?;
                    self.remove_heap_cache_entry(*sid)?;
                }
            } else {
                self.create_logged_file(txn_id_for_ddl(), self.table_path(id), Vec::new())?;
                self.remove_heap_cache_entry(id)?;
            }
            if has_auto_increment {
                // B8 — seed cached_max up front for every AUTO_INCREMENT
                // table so the first INSERT after CREATE TABLE never has
                // to fall through `auto_increment_initial_max`'s
                // `read_auto_increment_max` (a NotFound fs::read on the
                // common path) + empty `current_auto_increment_max` scan.
                self.auto_increment.install(id, 1, auto_increment_max)?;
                self.persist_auto_increment_max(id, auto_increment_max)?;
            }
            if let Some(table) = self.table_by_id(id) {
                self.initialize_empty_zone_maps_for_table(&table)?;
            }
            self.note_catalog_table_key(&key)?;
            self.commit_ddl_catalog_txn()
        })();
        if let Err(err) = finalize {
            // CREATE is fail-closed: no in-memory ghost table or orphan heap
            // survives a file/sidecar/DD failure.
            let cleanup = self
                .table_by_id(id)
                .map(|table| self.build_drop_table_intent(&table));
            {
                let mut catalog = self.catalog_write()?;
                if catalog.tables.get(&key).is_some_and(|table| table.id == id) {
                    catalog.tables.remove(&key);
                    build_fk_runtime(&mut catalog);
                }
            }
            if let Some(cleanup) = cleanup {
                self.purge_dropped_table_files(&cleanup);
            }
            self.auto_increment.forget_table(id);
            return Err(err);
        }
        Ok(())
    }

    pub(crate) fn create_temp_table_with_partition(
        &self,
        visible_name: &str,
        connection_id: u32,
        quota: &Arc<crate::storage::temp_quota::SessionTempQuota>,
        columns: Vec<ColumnDef>,
        primary_key: Vec<String>,
        indexes: Vec<crate::parser::IndexDef>,
        checks: Vec<crate::parser::CheckConstraintDef>,
        foreign_keys: Vec<crate::parser::ForeignKeyDef>,
        auto_increment_start: Option<u64>,
        partition: Option<crate::parser_partition::PartitionDef>,
        default_charset: Option<String>,
        default_collation: Option<String>,
        column_comments: Vec<(String, String)>,
        table_comment: String,
        pctfree: Option<u8>,
        pctused: Option<u8>,
        mysql_table_options: Vec<(String, String)>,
        requested_engine: Option<String>,
    ) -> Result<String, QueryError> {
        if !foreign_keys.is_empty() {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "FOREIGN KEY constraints on TEMPORARY tables are not supported",
            ));
        }
        if partition.is_some() {
            return Err(error(
                crate::error::catalog::ER_PARTITION_NO_TEMPORARY,
                "Partitioned TEMPORARY tables are not supported in MySQL 8.0 compatible mode",
            ));
        }
        let (database, table_name) = split_table_name(DEFAULT_DATABASE, visible_name);
        check_table_ddl_writable(&database)?;
        validate_create_table_identifiers(&table_name, &columns)?;
        let (database_charset, database_collation) = {
            let catalog = self.catalog_read()?;
            let Some(schema) = catalog.databases.get(&database) else {
                return Err(error(
                    crate::error::catalog::ER_BAD_DB_ERROR,
                    format!("unknown database '{database}'"),
                ));
            };
            (schema.charset.clone(), schema.collation.clone())
        };
        let (table_charset, table_collation) = resolve_table_charset_collation(
            default_charset.as_deref(),
            default_collation.as_deref(),
            &database_charset,
            &database_collation,
        )?;
        let prepared = prepare_create_table_schema(CreateTableSchemaSpec {
            database,
            table_name,
            columns,
            primary_key,
            indexes,
            checks,
            partition,
            table_charset,
            table_collation,
            column_comments,
            table_comment,
            pctfree,
            pctused,
            mysql_table_options,
            requested_engine,
            temporary: true,
        })?;
        debug_assert_eq!(prepared.partition_count()?, 0);
        let id = self.next_temp_table_id.fetch_add(1, Ordering::AcqRel);
        let internal_key = table_key("__mydb_temp", &format!("{}_{}", connection_id, id));
        let table = prepared.finish(id, &[])?;
        validate_auto_increment_schema(&table)?;
        let has_auto_increment = table.auto_increment_column_index().is_some();
        let auto_increment_max = if has_auto_increment {
            auto_increment_start.unwrap_or(1).saturating_sub(1)
        } else {
            0
        };
        let registration = crate::storage::temp_quota::register(&self.data_dir, id, quota)?;
        let storage_ids = table.storage_ids();
        {
            let mut temp_tables = self.temp_tables.write().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "temporary table lock poisoned",
                )
            })?;
            temp_tables.insert(internal_key.clone(), Arc::new(table));
        }
        let finalize = (|| -> Result<(), QueryError> {
            for sid in &storage_ids {
                self.create_temp_empty_file(self.table_path(*sid))?;
                self.remove_heap_cache_entry(*sid)?;
            }
            if has_auto_increment {
                // Seed the runtime counter; temporary tables never persist
                // AUTO_INCREMENT dynamic metadata.
                self.auto_increment.install(id, 1, auto_increment_max)?;
            }
            Ok(())
        })();
        if let Err(err) = finalize {
            self.temp_tables
                .write()
                .map_err(|_| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "temporary table lock poisoned",
                    )
                })?
                .remove(&internal_key);
            for sid in &storage_ids {
                let _ = std::fs::remove_file(self.table_path(*sid));
                let _ = self.remove_heap_cache_entry(*sid);
            }
            self.auto_increment.forget_table(id);
            return Err(err);
        }
        registration.commit();
        Ok(internal_key)
    }

    /// Shared `CREATE TABLE ... LIKE` schema derivation for both the
    /// persistent and TEMPORARY paths. Copies the source's columns (Public
    /// only, identity/position reset), indexes (Public only, and only when
    /// every key/included column survives), primary key, CHECK constraints
    /// (renamed to the destination + table references retargeted), and the
    /// generic table options / PCTFREE / PCTUSED.
    ///
    /// MySQL 8.0 semantics that this enforces:
    ///   - foreign keys are NOT copied (handled by the callers)
    ///   - `DATA DIRECTORY` / `INDEX DIRECTORY` table options are NOT copied
    ///   - table options (ROW_FORMAT/KEY_BLOCK_SIZE/STATS_*/MAX_ROWS/…),
    ///     PCTFREE and PCTUSED ARE copied
    fn build_like_schema_parts(
        source: &TableSchema,
        table_name: &str,
    ) -> Result<LikeSchemaParts, QueryError> {
        let mut columns = source
            .columns
            .iter()
            .filter(|column| column.state == ColumnState::Public)
            .cloned()
            .collect::<Vec<_>>();
        if columns.is_empty() {
            return Err(error(
                crate::error::catalog::ER_TABLE_MUST_HAVE_COLUMNS,
                "a table must have at least one column",
            ));
        }
        for column in &mut columns {
            column.column_id = 0;
            column.state = ColumnState::Public;
            column.physical_pos = 0;
            column.logical_pos = 0;
            column.ordinal_position = 0;
        }

        let visible_columns = columns
            .iter()
            .map(|column| normalize_name(&column.name))
            .collect::<BTreeSet<_>>();
        let indexes = source
            .indexes
            .iter()
            .filter(|index| {
                index.state == IndexState::Public
                    && index.columns.iter().enumerate().all(|(part, column)| {
                        index.key_expressions.get(part).is_some_and(Option::is_some)
                            || visible_columns.contains(&normalize_name(column))
                    })
                    && index
                        .included_columns
                        .iter()
                        .all(|column| visible_columns.contains(&normalize_name(column)))
            })
            .cloned()
            .map(|mut index| {
                index.state = IndexState::Public;
                index
            })
            .collect::<Vec<_>>();
        let primary_key = indexes
            .iter()
            .find(|index| index.primary)
            .map(|index| index.columns.clone())
            .unwrap_or_default();
        let mut check_constraints = source
            .check_constraints
            .iter()
            .filter(|check| {
                check
                    .column_names
                    .iter()
                    .all(|column| visible_columns.contains(&normalize_name(column)))
            })
            .cloned()
            .collect::<Vec<_>>();
        regenerate_check_constraint_names(&mut check_constraints, table_name);
        retarget_check_constraint_table_references(
            &mut check_constraints,
            &source.name,
            table_name,
        )?;

        // MySQL copies all table options EXCEPT DATA/INDEX DIRECTORY.
        let mysql_table_options = source
            .mysql_table_options
            .iter()
            .filter(|(key, _)| {
                !key.eq_ignore_ascii_case("DATA DIRECTORY")
                    && !key.eq_ignore_ascii_case("INDEX DIRECTORY")
            })
            .cloned()
            .collect::<Vec<_>>();

        Ok(LikeSchemaParts {
            columns,
            indexes,
            primary_key,
            check_constraints,
            mysql_table_options,
            pctfree: source.pctfree,
            pctused: source.pctused,
        })
    }

    pub fn create_table_like(&self, name: &str, source: &TableSchema) -> Result<(), QueryError> {
        let (database, table_name) = split_table_name(DEFAULT_DATABASE, name);
        validate_mysql_identifier(&table_name)?;
        if source.visible_column_count() > MYSQL_MAX_COLUMNS_PER_TABLE {
            return Err(error(
                crate::error::catalog::ER_TOO_MANY_FIELDS,
                "Too many columns",
            ));
        }
        let key = table_key(&database, &table_name);
        if !self.database_exists(&database) {
            return Err(error(
                crate::error::catalog::ER_BAD_DB_ERROR,
                format!("unknown database '{database}'"),
            ));
        }
        check_table_ddl_writable(&database)?;
        {
            let catalog = self.catalog_read()?;
            if catalog.tables.contains_key(&key)
                || catalog.views.contains_key(&key)
                || catalog.sequences.contains_key(&key)
            {
                return Err(error(
                    crate::error::catalog::ER_TABLE_EXISTS_ERROR,
                    format!("Table '{name}' already exists"),
                ));
            }
        }

        let parts = Self::build_like_schema_parts(source, &table_name)?;

        // MySQL 8.0 copies the source's partitioning definition. Clone it,
        // strip the DATA/INDEX DIRECTORY placement options (MySQL does not
        // copy directory placement through LIKE), and drop the source-derived
        // compiled programs — `build_column_index` recompiles them against
        // the destination below.
        let mut partitioning = source.partitioning.as_ref().map(|source_info| {
            let mut info = source_info.clone();
            for partition in &mut info.partitions {
                partition.options.data_directory = None;
                partition.options.index_directory = None;
                for sub in &mut partition.subpartitions {
                    sub.options.data_directory = None;
                    sub.options.index_directory = None;
                }
            }
            info.expr_ast = None;
            info.expr_program = None;
            info.sub_expr_ast = None;
            info.sub_expr_program = None;
            info
        });

        // Allocate the logical table id plus one fresh storage id per
        // partition/subpartition under a single catalog write lock, matching
        // the ordering of the normal CREATE TABLE path.
        let (id, partition_ids) = {
            let mut catalog = self.catalog_write()?;
            let id = catalog.next_table_id;
            catalog.next_table_id += 1;
            if let Some(info) = partitioning.as_mut() {
                for partition in &mut info.partitions {
                    if partition.subpartitions.is_empty() {
                        partition.storage_id = catalog.next_table_id;
                        catalog.next_table_id += 1;
                    } else {
                        partition.storage_id = 0;
                        for sub in &mut partition.subpartitions {
                            sub.storage_id = catalog.next_table_id;
                            catalog.next_table_id += 1;
                        }
                    }
                }
            }
            let ids = partitioning
                .as_ref()
                .map(|info| info.storage_ids())
                .unwrap_or_default();
            (id, ids)
        };
        self.ensure_table_id_highwater(self.current_next_table_id()?)?;
        let mut table = parts.finish(
            source,
            id,
            database,
            table_name,
            partitioning,
            current_unix_ms(),
        );
        table.build_column_index();
        validate_auto_increment_schema(&table)?;
        let has_auto_increment = table.auto_increment_column_index().is_some();
        {
            let mut catalog = self.catalog_write()?;
            // Re-check under the write lock: an existence probe under the
            // earlier read lock is a TOCTOU window for concurrent DDL.
            if catalog.tables.contains_key(&key)
                || catalog.views.contains_key(&key)
                || catalog.sequences.contains_key(&key)
            {
                return Err(error(
                    crate::error::catalog::ER_TABLE_EXISTS_ERROR,
                    format!("Table '{name}' already exists"),
                ));
            }
            if !table.check_constraints.is_empty() {
                let schema_names =
                    check_constraint_names_in_schema(&catalog, &table.database, None);
                ensure_check_constraint_names_available(&schema_names, &table.check_constraints)?;
            }
            catalog.tables.insert(key.clone(), Arc::new(table));
        }
        let finalize = (|| -> Result<(), QueryError> {
            self.register_catalog_table_mapping(&key)?;
            // Initialize one heap per physical partition, or the table heap.
            if partition_ids.is_empty() {
                self.create_logged_file(txn_id_for_ddl(), self.table_path(id), Vec::new())?;
            } else {
                for storage_id in &partition_ids {
                    self.create_logged_file(
                        txn_id_for_ddl(),
                        self.table_path(*storage_id),
                        Vec::new(),
                    )?;
                }
            }
            if has_auto_increment {
                self.persist_auto_increment_max(id, 0)?;
                self.auto_increment.install(id, 1, 0)?;
            }
            self.remove_heap_cache_entry(id)?;
            for storage_id in &partition_ids {
                self.remove_heap_cache_entry(*storage_id)?;
            }
            self.note_catalog_table_key(&key)?;
            self.commit_ddl_catalog_txn()
        })();
        if let Err(err) = finalize {
            let cleanup = self
                .table_by_id(id)
                .map(|table| self.build_drop_table_intent(&table));
            {
                let mut catalog = self.catalog_write()?;
                if catalog.tables.get(&key).is_some_and(|table| table.id == id) {
                    catalog.tables.remove(&key);
                }
            }
            if let Some(cleanup) = cleanup {
                self.purge_dropped_table_files(&cleanup);
            }
            self.auto_increment.forget_table(id);
            return Err(err);
        }
        Ok(())
    }

    fn create_temp_empty_file(&self, path: PathBuf) -> Result<(), QueryError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(io_err)?;
        }
        fs::write(path, Vec::<u8>::new()).map_err(io_err)
    }

    pub(crate) fn create_temp_table_like(
        &self,
        visible_name: &str,
        connection_id: u32,
        quota: &Arc<crate::storage::temp_quota::SessionTempQuota>,
        source: &TableSchema,
    ) -> Result<String, QueryError> {
        let (database, table_name) = split_table_name(DEFAULT_DATABASE, visible_name);
        check_table_ddl_writable(&database)?;
        validate_mysql_identifier(&table_name)?;
        if source.visible_column_count() > MYSQL_MAX_COLUMNS_PER_TABLE {
            return Err(error(
                crate::error::catalog::ER_TOO_MANY_FIELDS,
                "Too many columns",
            ));
        }
        if !self.database_exists(&database) {
            return Err(error(
                crate::error::catalog::ER_BAD_DB_ERROR,
                format!("unknown database '{database}'"),
            ));
        }
        if source.is_partitioned() {
            // MySQL forbids partitioned TEMPORARY tables entirely
            // (ER_PARTITION_NO_TEMPORARY 1562), so LIKE onto a partitioned
            // source can never produce a valid temporary table.
            return Err(error(
                crate::error::catalog::ER_PARTITION_NO_TEMPORARY,
                "Cannot create temporary table with partitions",
            ));
        }
        let parts = Self::build_like_schema_parts(source, &table_name)?;
        let id = self.next_temp_table_id.fetch_add(1, Ordering::AcqRel);
        let internal_key = table_key("__mydb_temp", &format!("{}_{}", connection_id, id));
        let mut table = parts.finish(source, id, database, table_name, None, current_unix_ms());
        table.build_column_index();
        validate_auto_increment_schema(&table)?;
        let has_auto_increment = table.auto_increment_column_index().is_some();
        let registration = crate::storage::temp_quota::register(&self.data_dir, id, quota)?;
        {
            let mut temp_tables = self.temp_tables.write().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "temporary table lock poisoned",
                )
            })?;
            temp_tables.insert(internal_key.clone(), Arc::new(table));
        }
        let finalize = (|| -> Result<(), QueryError> {
            self.create_temp_empty_file(self.table_path(id))?;
            if has_auto_increment {
                self.auto_increment.install(id, 1, 0)?;
            }
            self.remove_heap_cache_entry(id)?;
            Ok(())
        })();
        if let Err(err) = finalize {
            self.drop_temp_table(&internal_key)?;
            return Err(err);
        }
        registration.commit();
        Ok(internal_key)
    }

    fn remove_temp_file(&self, path: PathBuf) -> Result<(), QueryError> {
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(io_err(err)),
        }
        crate::storage::temp_quota::release_file(&path);
        Ok(())
    }

    /// Idempotent internal cleanup used by error recovery and session teardown.
    /// SQL DROP existence checks belong to the session registry in the executor.
    pub fn drop_temp_table(&self, internal_key: &str) -> Result<(), QueryError> {
        let table = self
            .temp_tables
            .write()
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "temporary table lock poisoned",
                )
            })?
            .remove(internal_key);
        let Some(table) = table else {
            return Ok(());
        };
        for sid in table.storage_ids() {
            self.remove_heap_cache_entry(sid)?;
            self.remove_overflow_cache_entry(sid)?;
            self.forget_zone_map(sid)?;
            self.remove_temp_file(self.table_path(sid))?;
            self.remove_temp_file(self.overflow_path(sid))?;
            self.remove_temp_file(self.zone_map_file_path(sid))?;
            for index in &table.indexes {
                self.remove_index_cache_entry(sid, &index.name)?;
                self.remove_temp_file(self.index_path(sid, &index.name))?;
            }
        }
        crate::storage::temp_quota::unregister(&self.data_dir, table.id);
        self.auto_increment.forget_table(table.id);
        self.drop_table_vector_graphs(&table);
        Ok(())
    }

    pub fn truncate_temp_table(&self, internal_key: &str) -> Result<(), QueryError> {
        let table = self
            .temp_tables
            .read()
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "temporary table lock poisoned",
                )
            })?
            .get(internal_key)
            .cloned()
            .ok_or_else(|| ddl_table_not_found(&internal_key))?;
        for sid in table.storage_ids() {
            self.remove_heap_cache_entry(sid)?;
            self.remove_overflow_cache_entry(sid)?;
            self.forget_zone_map(sid)?;
            self.create_temp_empty_file(self.table_path(sid))?;
            self.remove_temp_file(self.overflow_path(sid))?;
            for index in &table.indexes {
                self.remove_index_cache_entry(sid, &index.name)?;
                self.remove_temp_file(self.index_path(sid, &index.name))?;
            }
        }
        crate::storage::temp_quota::release_files(&self.data_dir, table.id);
        if table.auto_increment_column_index().is_some() {
            self.reset_auto_increment(table.id)?;
        }
        self.drop_table_vector_graphs(&table);
        Ok(())
    }

    /// Write the current buffer images, including WAL-free temporary pages.
    /// Normal Memory-mode flush is intentionally a no-op and cannot produce
    /// an ALTER rollback snapshot. This is bounded to one page of scratch.
    fn flush_temp_snapshot_pages(
        &self,
        path: &Path,
        file_key: Arc<BufferPageFileKey>,
        page_count: u64,
        first_page: Option<Vec<u8>>,
    ) -> Result<(), QueryError> {
        crate::storage::temp_quota::reserve_extent(path, page_count * PAGE_SIZE as u64)?;
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(io_err)?;
        for page_id in 0..page_count {
            if page_id == 0 {
                if let Some(image) = &first_page {
                    file.write_all(image).map_err(io_err)?;
                    continue;
                }
            }
            let image = self.buffer_pool.read_raw_page(PageKey {
                file: file_key.clone(),
                page_id,
            })?;
            file.write_all(&image).map_err(io_err)?;
        }
        file.set_len(page_count * PAGE_SIZE as u64).map_err(io_err)
    }

    pub(crate) fn snapshot_temp_table(
        &self,
        internal_key: &str,
    ) -> Result<TempTableSnapshot, QueryError> {
        let table = self
            .temp_tables
            .read()
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "temporary table lock poisoned",
                )
            })?
            .get(internal_key)
            .cloned()
            .ok_or_else(|| ddl_table_not_found(&internal_key))?;
        let directory = crate::engine::executor::select::grace::SpillTmpDir::create_kind(
            &self.data_dir,
            "temp-snapshot",
        )?;
        let snapshot_dir = directory.path();
        for sid in table.storage_ids() {
            self.with_heap_read(sid, |heap| {
                self.flush_temp_snapshot_pages(
                    &heap.path,
                    heap.file_key.clone(),
                    heap.page_count()?,
                    None,
                )
            })?;
            if self.overflow_path(sid).exists() {
                self.with_overflow_read(sid, |lob| {
                    self.flush_temp_snapshot_pages(
                        &lob.path,
                        lob.file_key.clone(),
                        lob.page_count,
                        None,
                    )
                })?;
            }
            for index in &table.indexes {
                self.with_index_read(sid, &index.name, |idx| {
                    let meta = idx.meta_state.lock().meta.clone();
                    self.flush_temp_snapshot_pages(
                        &idx.path,
                        idx.file_key.clone(),
                        meta.next_page_id,
                        Some(encode_index_meta(&meta)),
                    )
                })?;
            }
        }
        let mut files = Vec::new();
        let mut session_charges = Vec::new();
        for (idx, original) in self.temp_table_file_paths(&table).into_iter().enumerate() {
            let snapshot = snapshot_dir.join(format!("{idx}.snap"));
            let existed = original.exists();
            if existed {
                if let Some(parent) = snapshot.parent() {
                    fs::create_dir_all(parent).map_err(io_err)?;
                }
                let bytes = fs::metadata(&original).map_err(io_err)?.len();
                if let Some(charge) =
                    crate::storage::temp_quota::reserve_snapshot(&self.data_dir, table.id, bytes)?
                {
                    session_charges.push(charge);
                }
                let reservation = directory.charger().reserve_write(bytes, bytes)?;
                fs::copy(&original, &snapshot).map_err(io_err)?;
                drop(reservation);
            }
            files.push(TempTableFileSnapshot {
                original,
                snapshot,
                existed,
            });
        }
        Ok(TempTableSnapshot {
            _directory: directory,
            _session_charges: session_charges,
            internal_key: internal_key.to_string(),
            table,
            files,
        })
    }

    pub(crate) fn restore_temp_table_snapshot(
        &self,
        snapshot: TempTableSnapshot,
    ) -> Result<(), QueryError> {
        if let Some(current) = self
            .temp_tables
            .read()
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "temporary table lock poisoned",
                )
            })?
            .get(&snapshot.internal_key)
            .cloned()
        {
            self.remove_temp_table_runtime_files(&current)?;
        }
        self.remove_temp_table_runtime_files(&snapshot.table)?;
        for file in &snapshot.files {
            if let Some(parent) = file.original.parent() {
                fs::create_dir_all(parent).map_err(io_err)?;
            }
            let _ = fs::remove_file(&file.original);
            if file.existed {
                crate::storage::temp_quota::reserve_extent(
                    &file.original,
                    fs::metadata(&file.snapshot).map_err(io_err)?.len(),
                )?;
                fs::copy(&file.snapshot, &file.original).map_err(io_err)?;
            }
        }
        {
            let mut temp_tables = self.temp_tables.write().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "temporary table lock poisoned",
                )
            })?;
            temp_tables.insert(snapshot.internal_key.clone(), snapshot.table.clone());
        }
        self.discard_temp_table_snapshot(snapshot)
    }

    pub(crate) fn discard_temp_table_snapshot(
        &self,
        snapshot: TempTableSnapshot,
    ) -> Result<(), QueryError> {
        drop(snapshot);
        Ok(())
    }

    fn temp_table_file_paths(&self, table: &TableSchema) -> Vec<PathBuf> {
        let mut paths = Vec::new();
        for sid in table.storage_ids() {
            paths.push(self.table_path(sid));
            paths.push(self.overflow_path(sid));
            paths.push(self.zone_map_file_path(sid));
            for index in &table.indexes {
                paths.push(self.index_path(sid, &index.name));
            }
        }
        paths
    }

    fn remove_temp_table_runtime_files(&self, table: &TableSchema) -> Result<(), QueryError> {
        for sid in table.storage_ids() {
            self.remove_heap_cache_entry(sid)?;
            self.remove_overflow_cache_entry(sid)?;
            self.forget_zone_map(sid)?;
            self.remove_temp_file(self.table_path(sid))?;
            self.remove_temp_file(self.overflow_path(sid))?;
            self.remove_temp_file(self.zone_map_file_path(sid))?;
            for index in &table.indexes {
                self.remove_index_cache_entry(sid, &index.name)?;
                self.remove_temp_file(self.index_path(sid, &index.name))?;
            }
        }
        crate::storage::temp_quota::release_files(&self.data_dir, table.id);
        Ok(())
    }

    fn prepare_empty_temp_index_file(
        &self,
        table_id: u64,
        index_name: &str,
    ) -> Result<(), QueryError> {
        self.remove_index_cache_entry(table_id, index_name)?;
        let path = self.index_path(table_id, index_name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(io_err)?;
        }
        self.remove_temp_file(path.clone())?;
        let empty = IndexFile::create_rebuilt_from_sorted_with_durability(
            path,
            Vec::new(),
            self.log.clone(),
            DurabilityMode::Memory,
            self.buffer_pool.clone(),
        )?;
        empty.flush()?;
        self.remove_index_cache_entry(table_id, index_name)
    }

    pub fn add_temp_index(
        &self,
        internal_key: &str,
        mut index: IndexSchema,
    ) -> Result<(), QueryError> {
        index.column_prefix_lengths =
            normalized_index_prefix_lengths(index.column_prefix_lengths, index.columns.len());
        index.column_directions.resize(
            index.columns.len(),
            crate::storage::IndexColumnDirection::Asc,
        );
        index.column_directions.truncate(index.columns.len());
        validate_supported_index_type(&index)?;
        let table = self
            .temp_tables
            .read()
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "temporary table lock poisoned",
                )
            })?
            .get(internal_key)
            .cloned()
            .ok_or_else(|| ddl_table_not_found(&internal_key))?;
        if table
            .indexes
            .iter()
            .any(|existing| existing.name.eq_ignore_ascii_case(&index.name))
        {
            return Err(error(
                crate::error::catalog::ER_DUP_KEYNAME,
                format!("duplicate index '{}'", index.name),
            ));
        }
        validate_table_index_count(table.indexes.iter().filter(|i| !i.primary).count() + 1)?;
        validate_index_column_names(&table, &index)?;
        validate_index_prefix_columns(&table, &index, &index.column_prefix_lengths)?;
        index.validate_multi_valued_definition()?;
        validate_vector_index_column(&table, &index)?;
        if index.primary && table.indexes.iter().any(|existing| existing.primary) {
            return Err(error(
                crate::error::catalog::ER_MULTIPLE_PRI_KEY,
                "multiple primary key defined",
            ));
        }
        if index.primary {
            self.enforce_temp_primary_key_not_null(internal_key, &table, &index.columns)?;
        }
        if index.unique {
            self.validate_temp_unique_index_values(internal_key, &table, &index)?;
        }
        let mut next = table.as_ref().clone();
        if index.primary {
            for column_name in &index.columns {
                if let Some(column) = next
                    .columns
                    .iter_mut()
                    .find(|column| column.name.eq_ignore_ascii_case(column_name))
                {
                    column.nullable = false;
                }
            }
            next.primary_key = index.columns.clone();
        }
        index.state = IndexState::Public;
        self.prepare_empty_temp_index_file(next.id, &index.name)?;
        next.indexes.push(index.clone());
        next.bump_schema_version();
        self.rebuild_index(&next, &index)?;
        self.temp_tables
            .write()
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "temporary table lock poisoned",
                )
            })?
            .insert(internal_key.to_string(), Arc::new(next));
        Ok(())
    }

    pub fn drop_temp_index(&self, internal_key: &str, index_name: &str) -> Result<(), QueryError> {
        let table = self
            .temp_tables
            .read()
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "temporary table lock poisoned",
                )
            })?
            .get(internal_key)
            .cloned()
            .ok_or_else(|| ddl_table_not_found(&internal_key))?;
        let mut next = table.as_ref().clone();
        if let Some(auto_idx) = next.auto_increment_column_index() {
            let auto_col = normalize_name(&next.columns[auto_idx].name);
            let dropping_auto_key = next.indexes.iter().any(|index| {
                !index.primary
                    && index.name.eq_ignore_ascii_case(index_name)
                    && index.state == IndexState::Public
                    && index
                        .columns
                        .first()
                        .is_some_and(|column| normalize_name(column) == auto_col)
            });
            let replacement_auto_key = next.indexes.iter().any(|index| {
                !index.name.eq_ignore_ascii_case(index_name)
                    && index.state == IndexState::Public
                    && index
                        .columns
                        .first()
                        .is_some_and(|column| normalize_name(column) == auto_col)
            });
            if dropping_auto_key && !replacement_auto_key {
                return Err(error(
                    crate::error::catalog::ER_WRONG_AUTO_KEY,
                    "Incorrect table definition; AUTO_INCREMENT column must be defined as a key",
                ));
            }
        }
        let before = next.indexes.len();
        next.indexes
            .retain(|index| !index.name.eq_ignore_ascii_case(index_name) || index.primary);
        if next.indexes.len() == before {
            return Err(error(
                crate::error::catalog::ER_CANT_DROP_FIELD_OR_KEY,
                format!("index '{index_name}' does not exist"),
            ));
        }
        next.bump_schema_version();
        for sid in table.storage_ids() {
            self.remove_index_cache_entry(sid, index_name)?;
            self.remove_temp_file(self.index_path(sid, index_name))?;
        }
        self.temp_tables
            .write()
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "temporary table lock poisoned",
                )
            })?
            .insert(internal_key.to_string(), Arc::new(next));
        Ok(())
    }

    pub fn rename_temp_index(
        &self,
        internal_key: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<(), QueryError> {
        let table = self
            .temp_tables
            .read()
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "temporary table lock poisoned",
                )
            })?
            .get(internal_key)
            .cloned()
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_NO_SUCH_TABLE,
                    "temporary table does not exist",
                )
            })?;
        if table
            .indexes
            .iter()
            .any(|index| index.name.eq_ignore_ascii_case(new_name))
        {
            return Err(error(
                crate::error::catalog::ER_DUP_KEYNAME,
                format!("duplicate index '{new_name}'"),
            ));
        }
        let index = table
            .indexes
            .iter()
            .find(|index| index.name.eq_ignore_ascii_case(old_name))
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_CANT_DROP_FIELD_OR_KEY,
                    format!("index '{old_name}' does not exist"),
                )
            })?;
        if index.primary {
            return Err(error(
                crate::error::catalog::ER_WRONG_NAME_FOR_INDEX,
                "cannot rename PRIMARY KEY index",
            ));
        }
        self.flush_dirty_data()?;
        self.remove_index_cache_entry(table.id, old_name)?;
        let old_path = self.index_path(table.id, old_name);
        let new_path = self.index_path(table.id, new_name);
        if old_path.exists() {
            fs::rename(&old_path, &new_path).map_err(io_err)?;
        }
        crate::storage::temp_quota::rename_file(&old_path, &new_path);
        self.mutate_temp_table_schema(internal_key, |table| {
            let index = table
                .indexes
                .iter_mut()
                .find(|index| index.name.eq_ignore_ascii_case(old_name))
                .expect("temporary index validated before rename");
            index.name = new_name.to_string();
            Ok(())
        })?;
        let table = self
            .temp_tables
            .read()
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "temporary table lock poisoned",
                )
            })?
            .get(internal_key)
            .cloned()
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_NO_SUCH_TABLE,
                    "temporary table does not exist",
                )
            })?;
        let index = table
            .indexes
            .iter()
            .find(|index| index.name.eq_ignore_ascii_case(new_name))
            .expect("renamed temporary index is present");
        self.rebuild_index(&table, index)
    }

    /// INSTANT ADD COLUMN for a session temporary table. Temporary rows use
    /// the same versioned row-layout codec as persistent rows, so existing
    /// short rows receive the catalog backfill default without a heap scan.
    pub fn add_temp_column(&self, internal_key: &str, column: ColumnDef) -> Result<(), QueryError> {
        let check_defs = column.checks.clone();
        let mut new_column = column_def_to_schema(column)?;
        if new_column.generated_expr.is_some() {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "adding a generated column to a temporary table requires COPY",
            ));
        }
        let table_id;
        {
            let mut tables = self.temp_tables.write().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "temporary table lock poisoned",
                )
            })?;
            let table_arc = tables
                .get_mut(internal_key)
                .ok_or_else(|| ddl_table_not_found(&internal_key))?;
            let table = Arc::make_mut(table_arc);
            apply_column_charset_defaults(&mut new_column, &table.charset, &table.collation)?;
            if table
                .iter_visible_columns()
                .any(|(_, existing)| existing.name.eq_ignore_ascii_case(&new_column.name))
            {
                return Err(error(
                    crate::error::catalog::ER_DUP_FIELDNAME,
                    format!("duplicate column '{}'", new_column.name),
                ));
            }
            if new_column.auto_increment {
                return Err(error(
                    crate::error::catalog::ER_WRONG_AUTO_KEY,
                    "Incorrect table definition; there can be only one auto column and it must be defined as a key",
                ));
            }
            if !new_column.nullable
                && new_column.default.is_none()
                && new_column.default_expr.is_none()
                && crate::engine::executor::insert::implicit_default_for_type(&new_column.data_type)
                    .is_none()
            {
                return Err(error(
                    crate::error::catalog::ER_NO_DEFAULT_FOR_FIELD,
                    format!(
                        "column '{}' added without DEFAULT requires NULL allowed",
                        new_column.name
                    ),
                ));
            }
            if new_column.default.is_none() && new_column.default_expr.is_some() {
                let frozen = crate::engine::expr::resolve_column_default_value(&new_column);
                if !matches!(frozen, Value::Null) {
                    new_column.default = Some(frozen);
                }
            }
            new_column.column_id = table.allocate_column_id();
            new_column.physical_pos = table.next_physical_pos();
            new_column.logical_pos = table.visible_column_count() as u32;
            new_column.state = ColumnState::Public;
            if table.visible_column_count() >= MYSQL_MAX_COLUMNS_PER_TABLE {
                return Err(error(
                    crate::error::catalog::ER_TOO_MANY_FIELDS,
                    "Too many columns",
                ));
            }
            table.columns.push(new_column);
            validate_and_prepare_generated_columns(table)?;
            if !check_defs.is_empty() {
                let existing_checks = table.check_constraints.clone();
                let mut checks = build_check_constraints_for_table(
                    table,
                    check_defs,
                    Some(&existing_checks),
                    None,
                )?;
                table.check_constraints.append(&mut checks);
            }
            table.publish_new_row_layout();
            table.build_column_index();
            table.bump_schema_version();
            validate_row_fits_page(table)?;
            table_id = table.id;
        }
        self.refresh_heap_codec(table_id)
    }

    pub fn drop_temp_column(
        &self,
        internal_key: &str,
        column_name: &str,
    ) -> Result<(), QueryError> {
        let snapshot = self
            .temp_tables
            .read()
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "temporary table lock poisoned",
                )
            })?
            .get(internal_key)
            .cloned()
            .ok_or_else(|| ddl_table_not_found(&internal_key))?;
        if snapshot.visible_column_count() <= 1 {
            return Err(error(
                crate::error::catalog::ER_CANT_REMOVE_ALL_FIELDS,
                "cannot drop the only column in a table",
            ));
        }
        let target = snapshot
            .columns
            .iter()
            .position(|column| {
                column.state != ColumnState::Dropped
                    && column.name.eq_ignore_ascii_case(column_name)
            })
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_BAD_FIELD_ERROR,
                    format!("unknown column '{column_name}'"),
                )
            })?;
        reject_column_referenced_by_check(&snapshot, column_name)?;
        reject_column_referenced_by_generated(&snapshot, column_name)?;
        if snapshot
            .primary_key
            .iter()
            .any(|column| column.eq_ignore_ascii_case(column_name))
        {
            return Err(error(
                crate::error::catalog::ER_CANT_REMOVE_ALL_FIELDS,
                "cannot drop column that is part of PRIMARY KEY",
            ));
        }
        let (drop_indexes, rebuild_indexes) =
            plan_index_cascade_for_drop_column(&snapshot, column_name)?;
        for index in drop_indexes {
            self.drop_temp_index(internal_key, &index)?;
        }
        for index in rebuild_indexes {
            self.drop_temp_index(internal_key, &index.name)?;
            self.add_temp_index(internal_key, index)?;
        }
        let table_id;
        {
            let mut tables = self.temp_tables.write().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "temporary table lock poisoned",
                )
            })?;
            let table = Arc::make_mut(
                tables
                    .get_mut(internal_key)
                    .ok_or_else(|| ddl_table_not_found(&internal_key))?,
            );
            table.columns[target].state = ColumnState::Dropped;
            table.publish_new_row_layout();
            table.build_column_index();
            table.bump_schema_version();
            table_id = table.id;
        }
        self.refresh_heap_codec(table_id)
    }

    fn rewrite_temp_table_candidate(
        &self,
        internal_key: &str,
        old_table: Arc<TableSchema>,
        mut candidate: TableSchema,
        strict: bool,
    ) -> Result<Vec<crate::engine::api::SqlWarning>, QueryError> {
        let txn = TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted);
        let source_rows = self.scan_rows(internal_key, txn)?;
        let mut warnings = Vec::new();
        let mut rows = Vec::with_capacity(source_rows.len());
        for row in source_rows {
            let mut values = row.values;
            while values.len() < candidate.columns.len() {
                let column = &candidate.columns[values.len()];
                values.push(crate::engine::executor::insert::column_read_backfill_default(column));
            }
            coerce_row_for_alter_copy(&candidate, &mut values, strict, &mut warnings)?;
            materialize_generated_columns(&candidate, &mut values, None)?;
            self.check_not_null_constraints(&candidate, &values)?;
            self.check_check_constraints(&candidate, &values, None)?;
            rows.push(values);
        }
        candidate.reset_row_layouts_for_rewrite();
        candidate.build_column_index();
        validate_row_fits_page(&candidate)?;
        for sid in old_table.storage_ids() {
            self.remove_heap_cache_entry(sid)?;
            self.remove_overflow_cache_entry(sid)?;
            self.forget_zone_map(sid)?;
            self.remove_temp_file(self.table_path(sid))?;
            self.remove_temp_file(self.overflow_path(sid))?;
            self.remove_temp_file(self.zone_map_file_path(sid))?;
            for index in &old_table.indexes {
                self.remove_index_cache_entry(sid, &index.name)?;
                self.remove_temp_file(self.index_path(sid, &index.name))?;
            }
        }
        self.create_temp_empty_file(self.table_path(candidate.id))?;
        self.temp_tables
            .write()
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "temporary table lock poisoned",
                )
            })?
            .insert(internal_key.to_string(), Arc::new(candidate.clone()));
        self.set_cached_heap_codec(candidate.id, candidate.row_codec.clone())?;
        if !rows.is_empty() {
            let write_txn = self.begin_txn()?;
            if let Err(err) = self.insert_alter_copy_rewritten_rows(write_txn, &candidate, rows) {
                let _ = self.rollback_txn(write_txn.txn_id);
                return Err(err);
            }
            self.commit_txn(write_txn.txn_id)?;
        }
        Ok(warnings)
    }

    pub fn modify_temp_column(
        &self,
        internal_key: &str,
        column: ColumnDef,
        strict: bool,
    ) -> Result<Vec<crate::engine::api::SqlWarning>, QueryError> {
        let old = self
            .temp_tables
            .read()
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "temporary table lock poisoned",
                )
            })?
            .get(internal_key)
            .cloned()
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_NO_SUCH_TABLE,
                    "temporary table does not exist",
                )
            })?;
        let mut schema = column_def_to_schema(column)?;
        let position = old.column_index(&schema.name)?;
        preserve_primary_key_not_null(&old, position, &mut schema);
        apply_column_charset_for_alter(
            &mut schema,
            &old.columns[position],
            &old.charset,
            &old.collation,
        )?;
        self.reject_generated_storedness_change(&old, position, &schema)?;
        self.reject_functional_index_type_change(&old, position, &schema)?;
        let mut candidate = old.as_ref().clone();
        apply_column_def_preserving_identity(&mut candidate.columns[position], schema);
        candidate.publish_new_row_layout();
        validate_and_prepare_generated_columns(&mut candidate)?;
        candidate.build_column_index();
        candidate.bump_schema_version();
        ensure_table_keeps_visible_column(&candidate)?;
        self.rewrite_temp_table_candidate(internal_key, old, candidate, strict)
    }

    pub fn change_temp_column(
        &self,
        internal_key: &str,
        old_name: &str,
        column: ColumnDef,
        strict: bool,
    ) -> Result<Vec<crate::engine::api::SqlWarning>, QueryError> {
        let old = self
            .temp_tables
            .read()
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "temporary table lock poisoned",
                )
            })?
            .get(internal_key)
            .cloned()
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_NO_SUCH_TABLE,
                    "temporary table does not exist",
                )
            })?;
        let position = old.column_index(old_name)?;
        let mut schema = column_def_to_schema(column)?;
        preserve_primary_key_not_null(&old, position, &mut schema);
        apply_column_charset_for_alter(
            &mut schema,
            &old.columns[position],
            &old.charset,
            &old.collation,
        )?;
        if !old_name.eq_ignore_ascii_case(&schema.name) && old.column_index(&schema.name).is_ok() {
            return Err(error(
                crate::error::catalog::ER_DUP_FIELDNAME,
                format!("duplicate column '{}'", schema.name),
            ));
        }
        let mut candidate = old.as_ref().clone();
        let new_name = schema.name.clone();
        apply_column_def_preserving_identity(&mut candidate.columns[position], schema);
        if !old_name.eq_ignore_ascii_case(&new_name) {
            for key in &mut candidate.primary_key {
                if key.eq_ignore_ascii_case(old_name) {
                    *key = new_name.clone();
                }
            }
            for index in &mut candidate.indexes {
                for column in index.columns.iter_mut().chain(&mut index.included_columns) {
                    if column.eq_ignore_ascii_case(old_name) {
                        *column = new_name.clone();
                    }
                }
            }
            rename_check_constraint_column_references(&mut candidate, old_name, &new_name)?;
            rename_generated_column_references(&mut candidate, old_name, &new_name)?;
        }
        candidate.publish_new_row_layout();
        validate_and_prepare_generated_columns(&mut candidate)?;
        candidate.build_column_index();
        candidate.bump_schema_version();
        ensure_table_keeps_visible_column(&candidate)?;
        self.rewrite_temp_table_candidate(internal_key, old, candidate, strict)
    }

    pub fn mutate_temp_table_schema(
        &self,
        internal_key: &str,
        mutate: impl FnOnce(&mut TableSchema) -> Result<(), QueryError>,
    ) -> Result<(), QueryError> {
        let table_id = {
            let mut tables = self.temp_tables.write().map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "temporary table lock poisoned",
                )
            })?;
            let table = Arc::make_mut(tables.get_mut(internal_key).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_NO_SUCH_TABLE,
                    "temporary table does not exist",
                )
            })?);
            let mut candidate = table.clone();
            mutate(&mut candidate)?;
            candidate.build_column_index();
            candidate.bump_schema_version();
            ensure_table_keeps_visible_column(&candidate)?;
            *table = candidate;
            table.id
        };
        self.refresh_heap_codec(table_id)
    }

    pub fn rename_temp_column(
        &self,
        internal_key: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<(), QueryError> {
        validate_mysql_identifier(new_name)?;
        self.mutate_temp_table_schema(internal_key, |table| {
            let position = table.column_index(old_name)?;
            if !old_name.eq_ignore_ascii_case(new_name) && table.column_index(new_name).is_ok() {
                return Err(error(
                    crate::error::catalog::ER_DUP_FIELDNAME,
                    format!("duplicate column '{new_name}'"),
                ));
            }
            self.reject_functional_index_rename(table, old_name)?;
            table.columns[position].name = new_name.to_string();
            for column in &mut table.primary_key {
                if column.eq_ignore_ascii_case(old_name) {
                    *column = new_name.to_string();
                }
            }
            for index in &mut table.indexes {
                for column in index.columns.iter_mut().chain(&mut index.included_columns) {
                    if column.eq_ignore_ascii_case(old_name) {
                        *column = new_name.to_string();
                    }
                }
            }
            rename_check_constraint_column_references(table, old_name, new_name)?;
            rename_generated_column_references(table, old_name, new_name)?;
            rename_partition_column_references(table, old_name, new_name)?;
            validate_and_prepare_generated_columns(table)?;
            Ok(())
        })
    }

    pub fn move_temp_column_position(
        &self,
        internal_key: &str,
        column_name: &str,
        position: crate::parser::ColumnPosition,
    ) -> Result<(), QueryError> {
        self.mutate_temp_table_schema(internal_key, |table| {
            move_column_in_candidate(table, column_name, &position)?;
            Ok(())
        })
    }

    pub fn set_temp_column_comment(
        &self,
        internal_key: &str,
        column_name: &str,
        comment: &str,
    ) -> Result<(), QueryError> {
        crate::storage::schema::check_comment_len(
            crate::storage::schema::CommentTarget::Field,
            column_name,
            comment,
        )?;
        self.mutate_temp_table_schema(internal_key, |table| {
            let idx = table.column_index(column_name)?;
            table.columns[idx].comment = comment.to_string();
            Ok(())
        })
    }

    pub fn set_temp_column_default(
        &self,
        internal_key: &str,
        column_name: &str,
        default: Option<crate::parser::Expr>,
    ) -> Result<(), QueryError> {
        self.mutate_temp_table_schema(internal_key, |table| {
            let idx = table.column_index(column_name)?;
            set_column_default_in_candidate(table, idx, column_name, default, false)?;
            Ok(())
        })
    }

    fn temp_ddl_schema(&self, internal_key: &str) -> Result<Arc<TableSchema>, QueryError> {
        self.temp_tables
            .read()
            .map_err(|_| {
                error(
                    crate::error::catalog::ER_GET_ERRNO,
                    "temporary table lock poisoned",
                )
            })?
            .get(internal_key)
            .cloned()
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_NO_SUCH_TABLE,
                    "temporary table does not exist",
                )
            })
    }

    pub fn add_temp_check_constraint(
        &self,
        internal_key: &str,
        check: crate::parser::CheckConstraintDef,
    ) -> Result<(), QueryError> {
        let existing = self.temp_ddl_schema(internal_key)?;
        let mut candidate = existing.as_ref().clone();
        let mut additions = build_check_constraints_for_table(
            &candidate,
            vec![check],
            Some(&candidate.check_constraints),
            None,
        )?;
        let added = additions.pop().expect("one temporary CHECK was built");
        candidate.check_constraints.push(added.clone());
        candidate.build_column_index();
        self.validate_temp_check_constraints(internal_key, &candidate)?;
        self.mutate_temp_table_schema(internal_key, |table| {
            table.check_constraints.push(added);
            Ok(())
        })
    }

    pub fn drop_temp_check_constraint(
        &self,
        internal_key: &str,
        name: &str,
        if_exists: bool,
    ) -> Result<(), QueryError> {
        self.mutate_temp_table_schema(internal_key, |table| {
            let before = table.check_constraints.len();
            table
                .check_constraints
                .retain(|check| !check.name.eq_ignore_ascii_case(name));
            if before == table.check_constraints.len() && !if_exists {
                return Err(error(
                    crate::error::catalog::ER_CONSTRAINT_NOT_FOUND,
                    format!("Check constraint '{name}' does not exist."),
                ));
            }
            Ok(())
        })
    }

    pub fn alter_temp_check_constraint(
        &self,
        internal_key: &str,
        name: &str,
        enforced: bool,
    ) -> Result<(), QueryError> {
        let existing = self.temp_ddl_schema(internal_key)?;
        let mut candidate = existing.as_ref().clone();
        set_check_enforced_in_candidate::<true>(&mut candidate, name, enforced)?;
        candidate.build_column_index();
        if enforced {
            self.validate_temp_check_constraints(internal_key, &candidate)?;
        }
        self.mutate_temp_table_schema(internal_key, |table| {
            set_check_enforced_in_candidate::<true>(table, name, enforced)?;
            Ok(())
        })
    }

    fn validate_temp_check_constraints(
        &self,
        internal_key: &str,
        candidate: &TableSchema,
    ) -> Result<(), QueryError> {
        self.scan_rows_ordered_streaming(
            internal_key,
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
            |values| self.check_check_constraints(candidate, values, None),
        )
    }

    pub fn rename_temp_table(
        &self,
        internal_key: &str,
        visible_name: &str,
    ) -> Result<(), QueryError> {
        let (database, table_name) = split_table_name(DEFAULT_DATABASE, visible_name);
        validate_mysql_identifier(&table_name)?;
        check_table_ddl_writable(&database)?;
        let mut temp_tables = self.temp_tables.write().map_err(|_| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                "temporary table lock poisoned",
            )
        })?;
        let table_arc = temp_tables
            .get_mut(internal_key)
            .ok_or_else(|| ddl_table_not_found(&internal_key))?;
        let table = Arc::make_mut(table_arc);
        table.database = database;
        table.name = table_name;
        table.bump_schema_version();
        Ok(())
    }

    fn enforce_temp_primary_key_not_null(
        &self,
        internal_key: &str,
        table: &TableSchema,
        columns: &[String],
    ) -> Result<(), QueryError> {
        let column_indexes = columns
            .iter()
            .map(|column| table.column_index(column))
            .collect::<Result<Vec<_>, _>>()?;
        self.scan_rows_ordered_streaming(
            internal_key,
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
            |values| {
                if column_indexes
                    .iter()
                    .any(|idx| matches!(values.get(*idx), Some(Value::Null)))
                {
                    return Err(error(
                        crate::error::catalog::ER_INVALID_USE_OF_NULL,
                        "invalid use of NULL value for primary key",
                    ));
                }
                Ok(())
            },
        )
    }

    fn validate_temp_unique_index_values(
        &self,
        internal_key: &str,
        table: &TableSchema,
        index: &IndexSchema,
    ) -> Result<(), QueryError> {
        use crate::engine::executor::mem_budget::{
            ControlledMemoryCategory, ControlledMemoryCharge,
        };
        let mut charge = ControlledMemoryCharge::try_new(ControlledMemoryCategory::DdlCopy, 0)?;
        let mut seen = HashSet::new();
        self.scan_rows_ordered_streaming(
            internal_key,
            TxnView::read_only(u64::MAX, IsolationLevel::ReadCommitted),
            |values| {
                let key = table.index_key(index, values)?;
                if key.0.iter().any(|part| matches!(part, KeyPart::Null)) && !index.primary {
                    return Ok(());
                }
                // Account retained key payload plus conservative hash-table growth
                // before allocating. The streaming scan never materializes all rows.
                let payload = key
                    .0
                    .iter()
                    .map(|part| match part {
                        KeyPart::Bytes(v) => v.capacity(),
                        KeyPart::String(v) | KeyPart::CollatedString { value: v, .. } => {
                            v.capacity()
                        }
                        KeyPart::DecimalWide { value, .. } => {
                            (value.bits() as usize / 8).saturating_add(8)
                        }
                        _ => 0,
                    })
                    .sum::<usize>();
                charge.try_grow(payload.saturating_add(
                    2 * (std::mem::size_of::<IndexKey>()
                        + key.0.len() * std::mem::size_of::<KeyPart>()
                        + 16),
                ))?;
                seen.try_reserve(1).map_err(|_| {
                    error(
                        crate::error::catalog::ER_OUTOFMEMORY,
                        "Temporary unique-index validation exhausted memory",
                    )
                })?;
                if !seen.insert(key) {
                    return Err(error(
                        crate::error::catalog::ER_DUP_ENTRY,
                        format!("duplicate entry for key '{}'", index.name),
                    ));
                }
                Ok(())
            },
        )
    }

    pub fn drop_table(&self, name: &str) -> Result<(), QueryError> {
        let (database, table_name) = split_table_name(DEFAULT_DATABASE, name);
        check_table_ddl_writable(&database)?;
        let key = table_key(&database, &table_name);
        // Resolve a complete descriptor before entering any catalog critical
        // section. After restart `catalog.tables` may contain only a compact
        // header; path/FK/index cleanup needs the lazily decoded descriptor,
        // and the intent path helpers need the catalog row to remain live.
        let Some(table) = self.table_by_key(&key) else {
            return Err(error(
                crate::error::catalog::ER_BAD_TABLE_ERROR,
                format!("unknown table '{name}'"),
            ));
        };
        // M2.2 - DDL X lock semantics: ensure no active DML holds IX/S/SIX.
        self.check_ddl_table_lock(table.id)?;
        // Refuse to drop a table another table references via FK — unless the
        // session set `foreign_key_checks=0`, which MySQL 8.0 honours for DROP
        // (the child's constraint is left dangling). This is the standard
        // mysqldump / ordered-rebuild idiom.
        if fk_checks_enabled() {
            self.fk_reject_table_referenced(&table, false)?;
        }
        // Crash-safe DROP (commit-first, see drop_table_recovery.rs): record a
        // durable intent listing every physical file, commit the catalog change
        // (the atomic point), then unlink the files. A crash before the commit
        // leaves table + files + intent intact (recovery drops the stale
        // intent); a crash after it is finished by `recover_drop_tables`.
        let intent = self.build_drop_table_intent(&table);
        let intent_path = self.write_drop_table_intent(&intent)?;
        // Catalog commit point.
        {
            let mut catalog = self.catalog_write()?;
            if catalog.tables.remove(&key).is_none() {
                // A concurrent drop won the race and already purged the files.
                self.remove_drop_table_intent(intent_path.as_ref());
                return Ok(());
            }
            catalog.triggers.retain(|_, trigger| {
                let keep = normalize_name(&trigger.table_database) != database
                    || normalize_name(&trigger.table_name) != table_name;
                if !keep {
                    self.note_catalog_trigger(&trigger.database, &trigger.name);
                }
                keep
            });
            // Rebuild reverse FK caches only when the dropped table referenced
            // other tables (child side): that is the sole case that leaves a
            // stale `fk_referenced_by` entry on a surviving parent. Dropping a
            // table with no outbound FK cannot invalidate any other table's
            // caches, so we skip the O(catalog) rebuild entirely (PF1).
            if !table.foreign_keys.is_empty() {
                build_fk_runtime(&mut catalog);
            }
        }
        self.note_catalog_table(table.id);
        self.commit_ddl_catalog_txn()?;
        // Post-commit physical purge (idempotent; recovery redoes it on crash).
        let caches_evicted = self.evict_dropped_table_caches(&table).is_ok();
        self.catalog.forget_dropped_table(&key, &table);
        self.table_write_epochs.write().remove(&table.id);
        self.drop_table_modification_delta_without_epoch(table.id);
        if caches_evicted && self.purge_dropped_table_files(&intent) {
            self.remove_drop_table_intent(intent_path.as_ref());
        }
        Ok(())
    }

    pub fn drop_tables(&self, names: &[String], if_exists: bool) -> Result<(), QueryError> {
        let mut resolved = Vec::with_capacity(names.len());
        let mut drop_set = BTreeSet::new();
        for name in names {
            let (database, table_name) = split_table_name(DEFAULT_DATABASE, name);
            check_table_ddl_writable(&database)?;
            let key = table_key(&database, &table_name);
            let Some(table) = self.table_by_key(&key) else {
                if if_exists {
                    // MySQL 8.0 emits a Note (visible in SHOW WARNINGS) for
                    // each non-existent table skipped by IF EXISTS rather
                    // than silently dropping it.
                    crate::engine::api::push_statement_warning(
                        crate::error::ConditionLevel::Note,
                        crate::error::catalog::ER_BAD_TABLE_ERROR,
                        format!("Unknown table '{database}.{table_name}'"),
                    );
                    continue;
                }
                return Err(error(
                    crate::error::catalog::ER_BAD_TABLE_ERROR,
                    format!("Unknown table '{database}.{table_name}'"),
                ));
            };
            self.check_ddl_table_lock(table.id)?;
            drop_set.insert(key.clone());
            resolved.push((key, table));
        }
        // `foreign_key_checks=0` disables the inbound-reference rejection
        // (MySQL 8.0 leaves the child FK dangling); a table dropped in the
        // same statement as its referencing child is always allowed.
        if fk_checks_enabled() {
            for (_, table) in &resolved {
                for inbound in &table.fk_referenced_by {
                    if inbound.child_table_id == table.id
                        || drop_set.contains(&inbound.child_table_key)
                    {
                        continue;
                    }
                    return Err(error(crate::error::catalog::ER_FK_CANNOT_DROP_PARENT,
                        format!(
                            "Cannot drop table '{}' referenced by a foreign key constraint '{}' on table '{}'.",
                            table.name, inbound.fk_name, inbound.child_table_key
                        ),
                    ));
                }
            }
        }
        // Snapshot the tables to drop and their intent file lists while the
        // catalog still points at them (the intent builder resolves paths via
        // `table_loc_fs`, which needs the live catalog row). Crash-safe DROP is
        // commit-first (see drop_table_recovery.rs): write every intent, commit
        // the catalog change once, then purge the files.
        let mut dropped: Vec<(Arc<TableSchema>, DropTableIntent, Option<PathBuf>)> =
            Vec::with_capacity(resolved.len());
        for (_, table) in &resolved {
            let intent = self.build_drop_table_intent(table);
            dropped.push((table.clone(), intent, None));
        }
        if dropped.is_empty() {
            return Ok(());
        }
        for entry in &mut dropped {
            entry.2 = self.write_drop_table_intent(&entry.1)?;
        }
        // Catalog commit point — all tables removed atomically in one flush.
        // Rebuild reverse FK caches only if at least one dropped table had an
        // outbound FK (the only case leaving a stale `fk_referenced_by` on a
        // surviving parent) — otherwise skip the O(catalog) rebuild (PF1).
        let needs_fk_rebuild = dropped.iter().any(|(t, _, _)| !t.foreign_keys.is_empty());
        {
            let mut catalog = self.catalog_write()?;
            for (key, _) in &resolved {
                if let Some(table) = catalog.tables.remove(key) {
                    let db = normalize_name(&table.database);
                    let name = normalize_name(&table.name);
                    catalog.triggers.retain(|_, trigger| {
                        let keep = normalize_name(&trigger.table_database) != db
                            || normalize_name(&trigger.table_name) != name;
                        if !keep {
                            self.note_catalog_trigger(&trigger.database, &trigger.name);
                        }
                        keep
                    });
                }
            }
            if needs_fk_rebuild {
                build_fk_runtime(&mut catalog);
            }
        }
        for (table, _, _) in &dropped {
            self.note_catalog_table(table.id);
        }
        self.commit_ddl_catalog_txn()?;
        // Post-commit physical purge (idempotent; recovery redoes it on crash).
        for (table, intent, intent_path) in &dropped {
            let caches_evicted = self.evict_dropped_table_caches(table).is_ok();
            let key = table_key(&table.database, &table.name);
            self.catalog.forget_dropped_table(&key, table);
            self.table_write_epochs.write().remove(&table.id);
            self.drop_table_modification_delta_without_epoch(table.id);
            if caches_evicted && self.purge_dropped_table_files(intent) {
                self.remove_drop_table_intent(intent_path.as_ref());
            }
        }
        Ok(())
    }

    pub fn truncate_table(&self, name: &str) -> Result<(), QueryError> {
        let (database, table_name) = split_table_name(DEFAULT_DATABASE, name);
        check_table_ddl_writable(&database)?;
        let key = table_key(&database, &table_name);
        // T3 — TRUNCATE on a view returns 1347 in MySQL 8.0
        // ('db.v' is not BASE TABLE), not the generic 1146.
        let is_view = {
            let catalog = self.catalog_read()?;
            catalog.views.contains_key(&key)
        };
        if is_view {
            return Err(error(
                crate::error::catalog::ER_WRONG_OBJECT,
                format!("'{database}.{table_name}' is not BASE TABLE"),
            ));
        }
        let table = self
            .table_by_key(&key)
            .ok_or_else(|| ddl_table_not_found(&name))?;
        // M2.2 - DDL X lock check.
        self.check_ddl_table_lock(table.id)?;
        // MySQL allows self-referencing FKs here, but rejects references
        // from other tables. This is checked *regardless* of
        // `foreign_key_checks`, matching InnoDB's 1701 behaviour.
        self.fk_reject_table_referenced(&table, true)?;

        // MDL protects SQL callers; retain storage X locks as well so direct
        // storage writers cannot commit into the retired identity after cutover.
        let ddl_txn = self.begin_read_only_txn()?;
        let _storage_guard = OnlineIndexTxnGuard {
            db: self,
            txn_id: ddl_txn.txn_id,
        };
        let mut lock_ids = table.storage_ids();
        lock_ids.push(table.id);
        lock_ids.sort_unstable();
        lock_ids.dedup();
        for id in lock_ids {
            self.acquire_table_exclusive(ddl_txn.txn_id, id)?;
        }

        // Never wipe a live storage identity. Publish a new empty object in
        // one dictionary transaction, then retire the old physical generation.
        // A retained cleanup intent can only address the retired IDs, even if
        // the client has already committed inserts into the replacement.
        let mut candidate = table.as_ref().clone();
        {
            let mut catalog = self.catalog_write()?;
            candidate.id = catalog.next_table_id;
            catalog.next_table_id += 1;
            if let Some(partitioning) = &mut candidate.partitioning {
                for partition in &mut partitioning.partitions {
                    if partition.subpartitions.is_empty() {
                        partition.storage_id = catalog.next_table_id;
                        catalog.next_table_id += 1;
                    } else {
                        for leaf in &mut partition.subpartitions {
                            leaf.storage_id = catalog.next_table_id;
                            catalog.next_table_id += 1;
                        }
                    }
                }
                partitioning.runtime = std::sync::OnceLock::new();
                partitioning.metadata_runtime = std::sync::OnceLock::new();
            }
        }
        self.ensure_table_id_highwater(self.current_next_table_id()?)?;
        candidate.active_generation = 1;
        candidate.row_generation_txn_id = 0;
        candidate.stats = TableStats::empty();
        candidate.bump_schema_version();
        candidate.build_column_index();
        let retired = self.build_drop_table_intent(&table);
        let retired_path = self.write_drop_table_intent(&retired)?;
        let private = DropTableIntent {
            retired_index: None,
            table_id: candidate.id,
            had_auto_increment: candidate.auto_increment_column_index().is_some(),
            database: database.clone(),
            table_name: table_name.clone(),
            files: candidate
                .storage_ids()
                .iter()
                .map(|id| {
                    self.relative_path(&crate::storage::naming::table_object_generation_path(
                        &self.data_dir,
                        *id,
                        1,
                        "heap",
                    ))
                })
                .collect(),
            vector_files: Vec::new(),
        };
        let private_path = self.write_drop_table_intent(&private)?;
        for path in &private.files {
            self.create_logged_file(txn_id_for_ddl(), self.data_dir.join(path), Vec::new())?;
        }
        let candidate = Arc::new(candidate);
        let charge = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
            CatalogManager::descriptor_bytes(&candidate),
        )?;
        {
            let mut catalog = self.catalog_write()?;
            *self
                .catalog
                .ddl_pinned_ids
                .write()
                .entry(candidate.id)
                .or_default() += 1;
            catalog.tables.insert(key.clone(), candidate.clone());
            if !candidate.foreign_keys.is_empty() {
                build_fk_runtime(&mut catalog);
            }
        }
        let _lease = DdlTableLease {
            db: self,
            table: candidate.clone(),
            _charge: charge,
        };
        self.note_catalog_table(table.id);
        self.note_catalog_table(candidate.id);
        if let Err(err) = self.commit_ddl_catalog_txn_recoverable() {
            self.catalog_write()?
                .tables
                .insert(key.clone(), table.clone());
            Self::take_catalog_mutations();
            // Preserve both intents if commit durability is ambiguous.
            if self.ensure_operational().is_ok() && self.purge_dropped_table_files(&private) {
                self.remove_drop_table_intent(private_path.as_ref());
                self.remove_drop_table_intent(retired_path.as_ref());
            }
            return Err(err);
        }
        self.remove_drop_table_intent(private_path.as_ref());
        if self.evict_dropped_table_caches(&table).is_ok()
            && self.purge_dropped_table_files(&retired)
        {
            self.remove_drop_table_intent(retired_path.as_ref());
        }
        self.catalog.forget_dropped_table(&key, &table);
        self.table_write_epochs.write().remove(&table.id);
        self.drop_table_modification_delta_without_epoch(table.id);
        Ok(())
    }

    pub fn set_auto_increment(&self, name: &str, next_value: u64) -> Result<(), QueryError> {
        if next_value == 0 {
            // S28 — MySQL treats `AUTO_INCREMENT = 0` as a no-op (the counter
            // can never be set below the current max), not an error. Previously
            // mydb rejected it with 1064.
            return Ok(());
        }
        let (database, table_name) = split_table_name(DEFAULT_DATABASE, name);
        check_table_ddl_writable(&database)?;
        let key = table_key(&database, &table_name);
        let table = self
            .catalog_read()?
            .tables
            .get(&key)
            .cloned()
            .ok_or_else(|| ddl_table_not_found(&name))?;
        validate_auto_increment_schema(&table)?;
        if table.auto_increment_column_index().is_none() {
            return Err(error(
                crate::error::catalog::ER_WRONG_AUTO_KEY,
                "table has no AUTO_INCREMENT column",
            ));
        }
        let current_max = self.current_auto_increment_max(&table)?;
        let requested_max = next_value.saturating_sub(1);
        let max_value = current_max.max(requested_max);
        let state = self.auto_increment_state(table.id, current_max)?;
        state.max_value.fetch_max(max_value, Ordering::AcqRel);
        // B1 — drop any idle reservations any connection had for
        // this table. Their `next` is below the new counter and
        // would collide with values about to be allocated.
        self.persist_auto_increment_max(table.id, max_value)
    }

    pub fn validate_alter_table_actions(
        &self,
        table_name: &str,
        actions: &[crate::parser::AlterAction],
    ) -> Result<(), QueryError> {
        use crate::parser::AlterAction as A;
        Self::validate_alter_identifiers(actions)?;
        if actions.len() <= 1 {
            return Ok(());
        }
        let (database, table_only) = split_table_name(DEFAULT_DATABASE, table_name);
        let key = table_key(&database, &table_only);
        let mut candidate = self
            .catalog_read()?
            .tables
            .get(&key)
            .cloned()
            .ok_or_else(|| ddl_table_not_found(&table_name))?
            .as_ref()
            .clone();
        for action in actions {
            match action {
                A::AddColumn(column) => {
                    if candidate
                        .iter_visible_columns()
                        .any(|(_, c)| c.name.eq_ignore_ascii_case(&column.name))
                    {
                        return Err(error(
                            crate::error::catalog::ER_DUP_FIELDNAME,
                            format!("duplicate column '{}'", column.name),
                        ));
                    }
                    let mut schema = column_def_to_schema(column.clone())?;
                    apply_column_charset_defaults(
                        &mut schema,
                        &candidate.charset,
                        &candidate.collation,
                    )?;
                    if schema.auto_increment
                        && !matches!(
                            schema.data_type,
                            DataTypeKind::TinyInt
                                | DataTypeKind::SmallInt
                                | DataTypeKind::MediumInt
                                | DataTypeKind::Int
                                | DataTypeKind::BigInt
                        )
                    {
                        return Err(error(
                            crate::error::catalog::ER_WRONG_FIELD_SPEC,
                            "Incorrect column specifier for AUTO_INCREMENT column",
                        ));
                    }
                    schema.column_id = candidate.allocate_column_id();
                    schema.physical_pos = candidate.next_physical_pos();
                    schema.logical_pos = candidate.columns.len() as u32;
                    candidate.columns.push(schema);
                    candidate.build_column_index();
                }
                A::DropColumn(column_name) => {
                    if candidate.visible_column_count() <= 1 {
                        return Err(error(
                            crate::error::catalog::ER_CANT_REMOVE_ALL_FIELDS,
                            "cannot drop the only column in a table",
                        ));
                    }
                    candidate.column_index(column_name)?;
                    reject_column_referenced_by_check(&candidate, column_name)?;
                    reject_column_referenced_by_generated(&candidate, column_name)?;
                    // P0-8 — same partition-column guard as the single-action
                    // DROP COLUMN path (see drop_column) so multi-action ALTER
                    // TABLE fails early before any physical work is scheduled.
                    reject_column_referenced_by_partitioning(&candidate, column_name)?;
                    self.fk_reject_column_referenced(&candidate, column_name)?;
                    if candidate
                        .primary_key
                        .iter()
                        .any(|c| c.eq_ignore_ascii_case(column_name))
                    {
                        return Err(error(
                            crate::error::catalog::ER_CANT_REMOVE_ALL_FIELDS,
                            "cannot drop column that is part of PRIMARY KEY",
                        ));
                    }
                    // Cascade the drop through referencing indexes so the
                    // candidate schema stays consistent (issue #83): a
                    // single-column / functional / special index is removed,
                    // a plain composite index loses just this column. FK
                    // backing indexes are already guarded by
                    // fk_reject_column_referenced above.
                    let (drop_names, rebuilt) =
                        plan_index_cascade_for_drop_column(&candidate, column_name)?;
                    candidate
                        .indexes
                        .retain(|index| !drop_names.contains(&index.name));
                    for rb in rebuilt {
                        if let Some(existing) =
                            candidate.indexes.iter_mut().find(|ix| ix.name == rb.name)
                        {
                            *existing = rb;
                        }
                    }
                    let idx = candidate.column_index(column_name)?;
                    candidate.columns[idx].state = ColumnState::Dropped;
                    candidate.build_column_index();
                }
                A::MoveColumn { column, position } => {
                    candidate.column_index(column)?;
                    if let crate::parser::ColumnPosition::After(after) = position {
                        candidate.column_index(after)?;
                    }
                }
                A::SetColumnComment { column, .. } | A::SetColumnDefault { column, .. } => {
                    candidate.column_index(column)?;
                }
                A::SetTableOptions {
                    default_charset,
                    default_collation,
                    ..
                } => {
                    let _ = resolve_table_charset_collation(
                        default_charset.as_deref(),
                        default_collation.as_deref(),
                        &candidate.charset,
                        &candidate.collation,
                    )?;
                }
                A::AddIndex(index) => {
                    let name = index
                        .name
                        .clone()
                        .unwrap_or_else(|| index.columns.join("_"));
                    if name.eq_ignore_ascii_case("PRIMARY")
                        && (!candidate.primary_key.is_empty()
                            || candidate.pending_primary_key.is_some())
                    {
                        return Err(error(
                            crate::error::catalog::ER_MULTIPLE_PRI_KEY,
                            "Multiple primary key defined",
                        ));
                    }
                    if candidate
                        .indexes
                        .iter()
                        .any(|existing| existing.name.eq_ignore_ascii_case(&name))
                    {
                        return Err(error(
                            crate::error::catalog::ER_DUP_KEYNAME,
                            format!("duplicate index '{name}'"),
                        ));
                    }
                    for (pos, column) in index.columns.iter().enumerate() {
                        if index
                            .key_expressions
                            .get(pos)
                            .and_then(Option::as_ref)
                            .is_none()
                        {
                            candidate.column_index(column)?;
                        }
                    }
                }
                A::DropIndex(index_name) => {
                    self.fk_reject_index_drop(&candidate, index_name)?;
                    let before = candidate.indexes.len();
                    candidate.indexes.retain(|index| {
                        !index.name.eq_ignore_ascii_case(index_name) || index.primary
                    });
                    if candidate.indexes.len() == before {
                        return Err(error(
                            crate::error::catalog::ER_CANT_DROP_FIELD_OR_KEY,
                            format!("index '{index_name}' does not exist"),
                        ));
                    }
                }
                A::DropPrimaryKey => {
                    self.fk_reject_index_drop(&candidate, "PRIMARY")?;
                    if !candidate.indexes.iter().any(|index| index.primary) {
                        return Err(error(
                            crate::error::catalog::ER_CANT_DROP_FIELD_OR_KEY,
                            "PRIMARY KEY does not exist",
                        ));
                    }
                    candidate.indexes.retain(|index| !index.primary);
                    candidate.primary_key.clear();
                }
                A::RenameIndex { old_name, new_name } => {
                    if candidate
                        .indexes
                        .iter()
                        .any(|index| index.name.eq_ignore_ascii_case(new_name))
                    {
                        return Err(error(
                            crate::error::catalog::ER_DUP_KEYNAME,
                            format!("duplicate index '{new_name}'"),
                        ));
                    }
                    let index = candidate
                        .indexes
                        .iter_mut()
                        .find(|index| index.name.eq_ignore_ascii_case(old_name))
                        .ok_or_else(|| {
                            error(
                                crate::error::catalog::ER_CANT_DROP_FIELD_OR_KEY,
                                format!("index '{old_name}' does not exist"),
                            )
                        })?;
                    if index.primary {
                        return Err(error(
                            crate::error::catalog::ER_WRONG_NAME_FOR_INDEX,
                            "cannot rename PRIMARY KEY index",
                        ));
                    }
                    index.name = new_name.clone();
                }
                A::RenameColumn { old_name, new_name } => {
                    let idx = candidate.column_index(old_name)?;
                    if candidate.column_index(new_name).is_ok() {
                        return Err(error(
                            crate::error::catalog::ER_DUP_FIELDNAME,
                            format!("duplicate column '{new_name}'"),
                        ));
                    }
                    candidate.columns[idx].name = new_name.clone();
                    candidate.build_column_index();
                }
                A::RenameTable { new_name } => {
                    let (new_database, new_table) = split_table_name(&database, new_name);
                    let new_key = table_key(&new_database, &new_table);
                    let catalog = self.catalog_read()?;
                    if !catalog.databases.contains_key(&new_database) {
                        return Err(error(
                            crate::error::catalog::ER_BAD_DB_ERROR,
                            format!("unknown database '{new_database}'"),
                        ));
                    }
                    if catalog.tables.contains_key(&new_key) || catalog.views.contains_key(&new_key)
                    {
                        return Err(error(
                            crate::error::catalog::ER_TABLE_EXISTS_ERROR,
                            format!("Table '{new_name}' already exists"),
                        ));
                    }
                }
                A::ModifyColumn(column) => {
                    let idx = candidate.column_index(&column.name)?;
                    // Mirror prepare_modify_column_candidate's up-front
                    // validation so a MODIFY that must fail (FK-involved
                    // column, functional-index type change, invalid column
                    // definition) is rejected in pre-flight — before any
                    // earlier action in this multi-action ALTER mutates and
                    // flushes the catalog with no rollback.
                    let mut new_schema = column_def_to_schema(column.clone())?;
                    preserve_primary_key_not_null(&candidate, idx, &mut new_schema);
                    apply_column_charset_for_alter(
                        &mut new_schema,
                        &candidate.columns[idx],
                        &candidate.charset,
                        &candidate.collation,
                    )?;
                    self.reject_functional_index_type_change(&candidate, idx, &new_schema)?;
                    self.reject_foreign_key_column_change(&candidate, idx, &new_schema)?;
                    self.reject_vector_index_column_change(&candidate, idx, &new_schema)?;
                    candidate.columns[idx].invisible = column.invisible;
                }
                A::ChangeColumn { old_name, new_def } => {
                    let idx = candidate.column_index(old_name)?;
                    if !old_name.eq_ignore_ascii_case(&new_def.name)
                        && candidate.column_index(&new_def.name).is_ok()
                    {
                        return Err(error(
                            crate::error::catalog::ER_DUP_FIELDNAME,
                            format!("duplicate column '{}'", new_def.name),
                        ));
                    }
                    // Mirror prepare_change_column_candidate's up-front
                    // validation (see ModifyColumn above for the rationale).
                    let mut new_schema = column_def_to_schema(new_def.clone())?;
                    preserve_primary_key_not_null(&candidate, idx, &mut new_schema);
                    apply_column_charset_for_alter(
                        &mut new_schema,
                        &candidate.columns[idx],
                        &candidate.charset,
                        &candidate.collation,
                    )?;
                    self.reject_foreign_key_column_change(&candidate, idx, &new_schema)?;
                    self.reject_vector_index_column_change(&candidate, idx, &new_schema)?;
                    if old_name.eq_ignore_ascii_case(&new_def.name) {
                        self.reject_functional_index_type_change(&candidate, idx, &new_schema)?;
                    } else {
                        self.reject_functional_index_rename(&candidate, old_name)?;
                    }
                    candidate.columns[idx].name = new_def.name.clone();
                    candidate.columns[idx].invisible = new_def.invisible;
                    candidate.build_column_index();
                }
                A::SetColumnVisibility { column, invisible } => {
                    let idx = candidate.column_index(column)?;
                    candidate.columns[idx].invisible = *invisible;
                }
                A::ConvertToCharset { charset, collation } => {
                    let charset_info = crate::charset::resolve_charset(charset)?;
                    if let Some(coll) = collation {
                        let info = crate::charset::resolve_collation(coll)?;
                        crate::charset::validate_charset_collation_pair(
                            charset_info.name,
                            info.name,
                        )?;
                    }
                }
                A::Rebuild
                | A::SetAutoIncrement(_)
                | A::AddCheck(_)
                | A::AddForeignKey(_)
                | A::DropCheck { .. }
                | A::DropForeignKey { .. }
                | A::AlterCheck { .. }
                | A::AlterIndexVisibility { .. }
                | A::SetStorageProperties { .. }
                | A::Noop { .. } => {}
            }
        }
        validate_table_column_identifiers(&candidate)?;
        // MySQL 8.0.23+: a table must keep at least one visible column
        // (`ER_TABLE_MUST_HAVE_A_VISIBLE_COLUMN`) — covers MODIFY/CHANGE flipping the last one and
        // ALTER COLUMN … SET INVISIBLE.
        if !candidate
            .columns
            .iter()
            .any(|c| c.state != ColumnState::Dropped && !c.invisible)
        {
            return Err(error(
                crate::error::catalog::ER_TABLE_MUST_HAVE_A_VISIBLE_COLUMN,
                "A table must have at least one visible column.",
            ));
        }
        Ok(())
    }

    /// Validate requested names before any action, including temporary rename
    /// session bookkeeping. Candidate checks also cover direct storage callers.
    pub(crate) fn validate_alter_identifiers(
        actions: &[crate::parser::AlterAction],
    ) -> Result<(), QueryError> {
        use crate::parser::AlterAction as A;
        for action in actions {
            match action {
                A::AddColumn(column) | A::ModifyColumn(column) => {
                    validate_mysql_identifier(&column.name)?
                }
                A::ChangeColumn { new_def, .. } => validate_mysql_identifier(&new_def.name)?,
                A::RenameColumn { new_name, .. } | A::RenameIndex { new_name, .. } => {
                    validate_mysql_identifier(new_name)?
                }
                A::RenameTable { new_name } => {
                    let (database, table) = split_table_name(DEFAULT_DATABASE, new_name);
                    validate_mysql_identifier(&database)?;
                    validate_mysql_identifier(&table)?;
                }
                A::AddIndex(index) => {
                    if let Some(name) = &index.name {
                        validate_mysql_identifier(name)?;
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// C1 (atomic DDL): whether an ALTER action's effect is confined to
    /// this table's catalog entry (plus its `commit_catalog_txn`) with **no**
    /// physical file create/delete/rename, no heap rewrite, and no mutation
    /// of any *other* table — i.e. it is reversible simply by re-publishing
    /// the pre-ALTER schema. Used to make an all-metadata multi-action ALTER
    /// statement atomic (snapshot up front, restore on any mid-loop
    /// failure). Anything that rewrites the heap (COPY), builds/drops/renames
    /// an index / heap / LOB file, or propagates into another table
    /// (RENAME COLUMN rewrites inbound FK `referenced_columns`) is **not**
    /// reversible this way and deliberately keeps the sequential path — a
    /// schema-only restore after an irreversible physical mutation would
    /// corrupt, which is exactly the failure mode we refuse to introduce.
    pub fn alter_action_is_metadata_reversible(
        &self,
        table_name: &str,
        action: &crate::parser::AlterAction,
    ) -> Result<bool, QueryError> {
        use crate::parser::AlterAction as A;
        Ok(match action {
            // Pure catalog mutations, single-table.
            A::SetColumnComment { .. }
            | A::SetColumnDefault { .. }
            | A::SetColumnVisibility { .. }
            | A::SetTableOptions { .. }
            | A::SetStorageProperties { .. }
            | A::MoveColumn { .. }
            | A::Noop { .. }
            | A::AlterIndexVisibility { .. }
            | A::RenameIndex { .. }
            | A::AddCheck(_)
            | A::DropCheck { .. }
            | A::AlterCheck { .. } => true,
            // S27 — SetAutoIncrement publishes non-transactional counter redo,
            // which the schema-snapshot rollback does not restore. Treat it as
            // non-reversible so a failing multi-action ALTER can't leave the
            // schema and the counter diverged; it falls to the sequential path.
            A::SetAutoIncrement(_) => false,
            // Column add / change is reversible only while it stays INSTANT
            // (catalog + row-layout only). A COPY rewrite is irreversible by
            // schema restore.
            A::AddColumn(_) | A::ModifyColumn(_) | A::ChangeColumn { .. } => {
                !self.alter_action_requires_copy(table_name, action)?
                    || self.alter_action_validation_only(table_name, action)?
            }
            A::DropColumn(name) => {
                let table = self.table(table_name)?;
                let (drop, rebuild) = plan_index_cascade_for_drop_column(&table, name)?;
                drop.is_empty() && rebuild.is_empty()
            }
            A::RenameColumn { .. } => {
                let table = self.table(table_name)?;
                table.foreign_keys.is_empty() && table.fk_referenced_by.is_empty()
            }
            // Everything else touches a physical file or another table.
            _ => false,
        })
    }

    /// C1: snapshot a table's current schema Arc for statement-level
    /// rollback of an all-metadata multi-action ALTER.
    pub fn snapshot_table_schema(&self, table_name: &str) -> Result<Arc<TableSchema>, QueryError> {
        let (database, table_only) = split_table_name(DEFAULT_DATABASE, table_name);
        let key = table_key(&database, &table_only);
        let missing = || ddl_table_not_found(&table_name);
        let header = self
            .catalog_read()?
            .tables
            .get(&key)
            .cloned()
            .ok_or_else(missing)?;
        if !header.columns.is_empty() || header.system {
            return Ok(header);
        }
        // A cold catalog entry is only a lightweight header. Rollback must
        // retain the complete current descriptor, independently of cache
        // warmup and without applying a historical read-schema projection.
        let descriptor = self
            .load_table_descriptor_from_dd(header.id)?
            .ok_or_else(missing)?;
        self.catalog
            .install_descriptor(key, Arc::clone(&descriptor));
        Ok(descriptor)
    }

    /// C1: restore a table to a schema snapshot taken before an all-metadata
    /// multi-action ALTER, reverting every partially-applied action. Goes
    /// through the same commit primitive as a normal metadata ALTER
    /// (`replace_table_schema_in_catalog`: re-publish schema + refresh heap
    /// codec + flush, which bumps the global cache epoch), so the table ends
    /// byte-identical to its pre-ALTER state. Sound only when every executed
    /// action was `alter_action_is_metadata_reversible` — the caller
    /// guarantees that gate.
    pub fn restore_table_schema_from_snapshot(
        &self,
        table_name: &str,
        snapshot: Arc<TableSchema>,
    ) -> Result<(), QueryError> {
        let _internal_compensation = crate::binlog::suspend_ddl_txn_context();
        let current = self.snapshot_table_schema(table_name)?;
        self.rebind_index_runtime_names(&current, &snapshot);
        if !current.foreign_keys.is_empty() {
            // RENAME INDEX also changes derived inbound backing-index names.
            // Restore them together with the child's descriptor on rollback.
            let mut catalog = self.catalog_write()?;
            catalog.tables.insert(
                table_key(&snapshot.database, &snapshot.name),
                snapshot.clone(),
            );
            build_fk_runtime(&mut catalog);
        }
        let result = self.replace_table_schema_in_catalog(table_name, snapshot.as_ref().clone());
        if let Err(err) = &result {
            self.quarantine_mvcc(format!("metadata ALTER rollback failed: {}", err.message()));
        }
        result
    }

    #[cfg(test)]
    pub(crate) fn fail_next_catalog_commit_for_test(&self) {
        DDL_CATALOG_COMMIT_FAILURE.with(|fail| fail.set(true));
    }

    #[cfg(test)]
    pub(crate) fn fail_next_ddl_cleanup_for_test(&self) {
        DDL_CLEANUP_FAILURE.with(|fail| fail.set(true));
    }

    pub fn add_index(&self, table_name: &str, index: IndexSchema) -> Result<(), QueryError> {
        self.add_index_offline_bulk(table_name, index)
    }

    /// Data-dependent ADD INDEX preflight used by compound ALTER TABLE. It
    /// runs before any sibling action mutates the table, closing the common
    /// atomicity hole where an earlier COPY succeeded and a later UNIQUE/PK
    /// build then discovered duplicate or NULL values.
    pub fn validate_add_index_existing_rows(
        &self,
        table_name: &str,
        index: &IndexSchema,
    ) -> Result<(), QueryError> {
        let table = self.table(table_name)?;
        if index.primary && !index.unique {
            enforce_existing_primary_key_not_null(self, &table, &index.columns)?;
        }
        if index.unique || index.primary {
            validate_existing_unique_index_values(self, &table, index)?;
        }
        Ok(())
    }

    #[cfg(test)]
    /// Multi-index variant of [`Database::add_index`]. Locks the
    /// target table X exclusive once, publishes every index as
    /// Building under that lock, runs a single fan-out heap scan
    /// + per-index bulk-load, then flips all Building → Public in
    /// one catalog write. The big win: only one heap pass even when
    /// the caller adds many indexes to a wide table (TPC-H lineitem
    /// has 7 secondary indexes, each previously paying for a
    /// separate ~800 MB sequential read).
    pub fn add_indexes(
        self: &Arc<Self>,
        table_name: &str,
        indexes: Vec<IndexSchema>,
    ) -> Result<(), QueryError> {
        self.add_indexes_offline_bulk(table_name, indexes)
    }

    #[cfg(test)]
    fn add_indexes_offline_bulk(
        self: &Arc<Self>,
        table_name: &str,
        indexes: Vec<IndexSchema>,
    ) -> Result<(), QueryError> {
        self.add_indexes_offline_bulk_with_options(
            table_name,
            indexes,
            IndexBuildOptions::default(),
        )
    }

    fn add_indexes_offline_bulk_with_options(
        self: &Arc<Self>,
        table_name: &str,
        indexes: Vec<IndexSchema>,
        options: IndexBuildOptions,
    ) -> Result<(), QueryError> {
        if indexes.is_empty() {
            return Ok(());
        }
        // Optional background derived-data scans must yield while ADD INDEX
        // owns its collector reservation. Otherwise they can refill the
        // buffer pool after the builder deliberately evicts it and consume
        // the headroom required for leaf/internal-page construction.
        let _foreground_bulk_ddl = self.enter_foreground_bulk_ddl();
        // CREATE INDEX and a single-action ALTER TABLE ADD INDEX must stay on
        // this identical build path. The batch path below exists only to share
        // one heap scan and one worker budget across multiple indexes.
        if indexes.len() == 1 {
            return self.add_index_offline_bulk_with_options(
                table_name,
                indexes.into_iter().next().unwrap(),
                options,
            );
        }
        let ddl_txn = self.begin_read_only_txn()?;
        let result = (|| {
            // Phase 1 for every index. The table X lock is acquired
            // by the first phase1 call; subsequent calls pass `None`
            // because the same DDL txn already holds the lock and
            // `acquire_table_exclusive` would re-enter pointlessly.
            // `prepare_empty_file=false` matches the single-index
            // offline path: offline ADD INDEX rewrites the file end
            // to end inside the lock, no need to lay down an empty
            // catalog-published placeholder first.
            let mut keys: Vec<(String, String)> = Vec::with_capacity(indexes.len());
            let mut last_table: Option<TableSchema> = None;
            for (i, index) in indexes.into_iter().enumerate() {
                let lock_arg = if i == 0 { Some(ddl_txn.txn_id) } else { None };
                let (key, table, index_name) =
                    self.add_index_phase1_with_table_lock(table_name, index, lock_arg, false)?;
                keys.push((key, index_name));
                last_table = Some(table);
            }
            let table = last_table.expect("at least one index processed");
            let index_names: Vec<String> = keys.iter().map(|(_, n)| n.clone()).collect();
            // The cloned `table` reflects the schema after *every*
            // phase1 wrote its Building entry, so every requested
            // index is present in `table.indexes`.
            if let Err(err) =
                self.rebuild_indexes_offline_bulk_batch_with_options(&table, &index_names, options)
            {
                for (k, n) in &keys {
                    let _ = self.scrub_failed_index_build(k, n);
                }
                return Err(err);
            }
            // CREATE INDEX takes effect immediately for any vector index in
            // the batch (see the single-index path). Under the DDL X lock.
            let vector_names: Vec<&str> = index_names.iter().map(String::as_str).collect();
            if let Err(err) = self.warm_vector_graphs_after_ddl(&table, &vector_names) {
                for (key, index_name) in &keys {
                    let _ = self.scrub_failed_index_build(key, index_name);
                }
                return Err(err);
            }
            let modified_rows = table.stats.row_count.max(1);
            {
                let mut catalog = self.catalog_write()?;
                for (key, index_name) in &keys {
                    if let Some(table_arc) = catalog.tables.get_mut(key) {
                        let table = Arc::make_mut(table_arc);
                        if let Some(slot) = table
                            .indexes
                            .iter_mut()
                            .find(|existing| existing.name.eq_ignore_ascii_case(index_name))
                        {
                            slot.state = crate::storage::IndexState::Public;
                        }
                        table.stats.modified_rows_since_analyze = table
                            .stats
                            .modified_rows_since_analyze
                            .saturating_add(modified_rows);
                        table.bump_schema_version();
                    }
                }
            }
            self.note_catalog_table(table.id);
            self.commit_ddl_catalog_txn()?;
            Ok(())
        })();
        let release = self.rollback_txn(ddl_txn.txn_id);
        match (result, release) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(err), _) => Err(err),
            (Ok(()), Err(err)) => Err(err),
        }
    }

    fn add_index_offline_bulk(
        &self,
        table_name: &str,
        index: IndexSchema,
    ) -> Result<(), QueryError> {
        self.add_index_offline_bulk_with_options(table_name, index, IndexBuildOptions::default())
    }

    fn add_index_offline_bulk_with_options(
        &self,
        table_name: &str,
        index: IndexSchema,
        options: IndexBuildOptions,
    ) -> Result<(), QueryError> {
        let _foreground_bulk_ddl = self.enter_foreground_bulk_ddl();
        let ddl_txn = self.begin_read_only_txn()?;
        let result = (|| {
            let (key, building_table, index_name) = self.add_index_phase1_with_table_lock(
                table_name,
                index,
                Some(ddl_txn.txn_id),
                false,
            )?;
            let index_for_build = building_table
                .indexes
                .iter()
                .find(|existing| existing.name.eq_ignore_ascii_case(&index_name))
                .expect("newly-added index should exist in cloned schema");
            #[cfg(test)]
            self.pause_ddl_backfill_for_test();
            if let Err(err) = self.rebuild_index_offline_bulk_with_options(
                &building_table,
                index_for_build,
                options,
            ) {
                let _ = self.scrub_failed_index_build(&key, &index_name);
                return Err(err);
            }
            // CREATE INDEX takes effect immediately: build the HNSW graph
            // from the just-backfilled heap so the first KNN query doesn't pay
            // the full build cost. Runs under the DDL table X lock (offline
            // ADD INDEX), so the heap is stable and the graph is complete.
            // No-op for non-vector indexes and empty heaps.
            if let Err(err) =
                self.warm_vector_graphs_after_ddl(&building_table, &[index_name.as_str()])
            {
                let _ = self.scrub_failed_index_build(&key, &index_name);
                return Err(err);
            }
            let modified_rows = building_table.stats.row_count.max(1);
            {
                let mut catalog = self.catalog_write()?;
                if let Some(table_arc) = catalog.tables.get_mut(&key) {
                    let table = Arc::make_mut(table_arc);
                    if let Some(slot) = table
                        .indexes
                        .iter_mut()
                        .find(|existing| existing.name.eq_ignore_ascii_case(&index_name))
                    {
                        slot.state = crate::storage::IndexState::Public;
                    }
                    table.stats.modified_rows_since_analyze = table
                        .stats
                        .modified_rows_since_analyze
                        .saturating_add(modified_rows);
                    table.bump_schema_version();
                }
            }
            self.note_catalog_table(building_table.id);
            self.commit_ddl_catalog_txn()?;
            Ok(())
        })();
        let release = self.rollback_txn(ddl_txn.txn_id);
        match (result, release) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(err), _) => Err(err),
            (Ok(()), Err(err)) => Err(err),
        }
    }

    /// Phase 1 of ADD INDEX. Validates the request, locks the catalog
    /// briefly to publish the new entry as [`IndexState::Building`],
    /// flushes the catalog so a crash here is observable on restart,
    /// and returns the qualified key, the cloned post-publish
    /// `TableSchema`, and the index name. The caller is responsible
    /// for running [`Database::add_index_phase23`] — either inline
    /// (synchronous ADD INDEX) or on a background thread (async
    /// ADD INDEX, see `add_index_with_async`).
    fn add_index_phase1(
        &self,
        table_name: &str,
        index: IndexSchema,
    ) -> Result<(String, TableSchema, String), QueryError> {
        self.add_index_phase1_with_table_lock(table_name, index, None, true)
    }

    fn add_index_phase1_with_table_lock(
        &self,
        table_name: &str,
        index: IndexSchema,
        ddl_txn_id: Option<u64>,
        prepare_empty_file: bool,
    ) -> Result<(String, TableSchema, String), QueryError> {
        self.prepare_index_build(table_name, index, ddl_txn_id, prepare_empty_file, false)
    }

    fn prepare_index_build(
        &self,
        table_name: &str,
        mut index: IndexSchema,
        ddl_txn_id: Option<u64>,
        prepare_empty_file: bool,
        capture_online: bool,
    ) -> Result<(String, TableSchema, String), QueryError> {
        if index.primary {
            index.unique = true;
            index.name = "PRIMARY".into();
            if !index.visible {
                return Err(error(
                    crate::error::catalog::ER_PK_INDEX_CANT_BE_INVISIBLE,
                    "PRIMARY KEY indexes cannot be invisible",
                ));
            }
            if index.has_expression_parts() || index.has_multi_valued_parts() {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "PRIMARY KEY requires column key parts",
                ));
            }
        }
        let (database, table_only) = split_table_name(DEFAULT_DATABASE, table_name);
        check_table_ddl_writable(&database)?;
        let key = table_key(&database, &table_only);
        // ALTER TABLE ... ADD INDEX ... COMMENT is subject to the same
        // MySQL 8.0 1024-char index-comment cap as CREATE TABLE. This is the
        // shared phase-1 entry for the offline single/bulk and async paths.
        crate::storage::schema::check_comment_len(
            crate::storage::schema::CommentTarget::Index,
            &index.name,
            &index.comment,
        )?;
        if index.is_vector() {
            crate::storage::schema::VectorIndexMeta::validate_from_comment(
                &index.comment,
                index.vector_algo(),
            )?;
        }
        index.column_prefix_lengths =
            normalized_index_prefix_lengths(index.column_prefix_lengths, index.columns.len());
        index.column_directions.resize(
            index.columns.len(),
            crate::storage::IndexColumnDirection::Asc,
        );
        index.column_directions.truncate(index.columns.len());
        let mut index_name = index.name.clone();
        // Cache eviction leaves a lightweight object header in the
        // compatibility catalog. Materialize the immutable descriptor before
        // phase 1 validates columns and index metadata under the table MDL.
        self.table_by_key(&key)
            .ok_or_else(|| ddl_table_not_found(&table_name))?;
        let existing_table = {
            let catalog = self.catalog_read()?;
            let table = catalog
                .tables
                .get(&key)
                .ok_or_else(|| ddl_table_not_found(&table_name))?;
            if index.primary
                && (table.indexes.iter().any(|existing| existing.primary)
                    || table.pending_primary_key.is_some())
            {
                return Err(error(
                    crate::error::catalog::ER_MULTIPLE_PRI_KEY,
                    "Multiple primary key defined",
                ));
            }
            if table
                .indexes
                .iter()
                .any(|existing| existing.name.eq_ignore_ascii_case(&index.name))
            {
                return Err(error(
                    crate::error::catalog::ER_DUP_KEYNAME,
                    format!("duplicate index '{}'", index.name),
                ));
            }
            validate_add_index_definition(table, &index)?;
            if !index.included_columns.is_empty() {
                if index.unique {
                    return Err(error(
                        crate::error::catalog::ER_PARSE_ERROR,
                        "UNIQUE INDEX cannot declare INCLUDE columns",
                    ));
                }
                validate_columns(&index.included_columns, &table.columns)?;
            }
            if table.is_partitioned() {
                let mut candidate = table.as_ref().clone();
                if index.primary {
                    for column_name in &index.columns {
                        if let Some(column) = candidate
                            .columns
                            .iter_mut()
                            .find(|column| column.name.eq_ignore_ascii_case(column_name))
                        {
                            column.nullable = false;
                        }
                    }
                    candidate.primary_key = index.columns.clone();
                }
                candidate.indexes.push(index.clone());
                validate_mysql80_partition_compat(&candidate, false)?;
            }
            table.clone()
        };
        if let Some(txn_id) = ddl_txn_id {
            if capture_online {
                self.acquire_table_shared(txn_id, existing_table.id)?;
            } else {
                self.acquire_table_exclusive(txn_id, existing_table.id)?;
            }
        }
        // PRIMARY is UNIQUE, so the projected unique scan below also checks
        // NULL first and preserves error 1138 precedence. Avoid a redundant
        // full-table materialisation pass in that common case.
        if index.primary && !index.unique {
            enforce_existing_primary_key_not_null(self, &existing_table, &index.columns)?;
        }
        // Online ADD INDEX exposes Building to concurrent DML and therefore
        // keeps the pre-scan. Offline ADD INDEX holds the table X lock; its
        // ordered bulk stream validates UNIQUE/PRIMARY without a second heap
        // scan or an ungoverned whole-table HashSet.
        if prepare_empty_file && index.unique && !capture_online {
            validate_existing_unique_index_values(self, &existing_table, &index)?;
        }
        // Catalog write lock briefly: insert the new index entry as
        // `Building`. Concurrent DML now maintains it through the
        // standard write paths; SELECTs do NOT consider it because the
        // planner filters Building indexes. The lock is released before
        // the heap scan so other DDL on other tables — and any catalog
        // read — can proceed.
        let table = {
            let mut catalog = self.catalog_write()?;
            let table_arc = catalog
                .tables
                .get_mut(&key)
                .ok_or_else(|| ddl_table_not_found(&table_name))?;
            let table = Arc::make_mut(table_arc);
            if index.index_id == 0 {
                index.index_id = table.next_index_id.max(1);
                table.next_index_id = index.index_id.checked_add(1).ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        "physical index id exhausted",
                    )
                })?;
            } else if table
                .indexes
                .iter()
                .any(|existing| existing.index_id == index.index_id)
            {
                return Err(error(
                    crate::error::catalog::ER_DUP_KEYNAME,
                    format!("duplicate physical index id {}", index.index_id),
                ));
            }
            if table
                .indexes
                .iter()
                .any(|existing| existing.name.eq_ignore_ascii_case(&index.name))
            {
                return Err(error(
                    crate::error::catalog::ER_DUP_KEYNAME,
                    format!("duplicate index '{}'", index.name),
                ));
            }
            table.next_index_id = table.next_index_id.max(index.index_id.saturating_add(1));
            validate_add_index_definition(table, &index)?;
            if index.primary
                && (table.indexes.iter().any(|existing| existing.primary)
                    || table.pending_primary_key.is_some())
            {
                return Err(error(
                    crate::error::catalog::ER_MULTIPLE_PRI_KEY,
                    "multiple primary key defined",
                ));
            }
            if table.is_partitioned() {
                let mut candidate = table.clone();
                if index.primary {
                    for column_name in &index.columns {
                        if let Some(column) = candidate
                            .columns
                            .iter_mut()
                            .find(|column| column.name.eq_ignore_ascii_case(column_name))
                        {
                            column.nullable = false;
                        }
                    }
                    candidate.primary_key = index.columns.clone();
                }
                candidate.indexes.push(index.clone());
                validate_mysql80_partition_compat(&candidate, false)?;
            }
            if index.primary && !capture_online {
                for column_name in &index.columns {
                    if let Some(column) = table
                        .columns
                        .iter_mut()
                        .find(|column| column.name.eq_ignore_ascii_case(column_name))
                    {
                        column.nullable = false;
                    }
                }
                table.primary_key = index.columns.clone();
            }
            if index.primary && capture_online {
                let final_index = index.clone();
                let job_id = ddl_txn_id.expect("online prepare owns a transaction");
                let mut suffix = 0u64;
                loop {
                    index_name = format!("__mydb_pk_{job_id}_{suffix}");
                    if !table
                        .indexes
                        .iter()
                        .any(|i| i.name.eq_ignore_ascii_case(&index_name))
                    {
                        break;
                    }
                    suffix += 1;
                }
                let column_ids = index
                    .columns
                    .iter()
                    .map(|name| table.column_index(name).map(|i| table.columns[i].column_id))
                    .collect::<Result<Vec<_>, _>>()?;
                table.pending_primary_key = Some(crate::storage::schema::PendingPrimaryKey {
                    job_id,
                    source_generation: table.active_generation,
                    column_ids,
                    capture_name: index_name.clone(),
                    final_index,
                });
                index.name = index_name.clone();
                index.primary = false;
                index.unique = false;
            }
            // The index file must exist before the Building catalog
            // entry becomes visible. Concurrent DML reads the catalog
            // after this lock is released and will immediately route
            // writes into every listed index, including Building ones.
            if prepare_empty_file && !capture_online {
                self.prepare_empty_index_file_at_path(
                    table.id,
                    &index.name,
                    crate::storage::naming::index_object_path_by_id(
                        &self.data_dir,
                        table.id,
                        table.active_generation.max(1),
                        index.index_id,
                        "idx",
                    ),
                )?;
            }
            let mut building_index = index.clone();
            building_index.state = crate::storage::IndexState::Building;
            table.indexes.push(building_index);
            table.bump_schema_version();
            table.clone()
        };
        // Online ADD INDEX must persist the Building entry so a crash
        // here is observable on restart and the recovery path can scrub
        // the half-built index. Offline ADD INDEX holds the table X lock
        // and rebuilds the file synchronously, so it can skip this extra
        // catalog fsync and publish only the final Public entry.
        if capture_online {
            self.pin_online_index_descriptor(&table)?;
        }
        if prepare_empty_file {
            self.note_catalog_table(table.id);
            self.commit_catalog_txn()?;
        }
        if prepare_empty_file && capture_online {
            // The durable intent precedes private-file creation. Source MDL X
            // and data X still exclude writers until capture is installed.
            self.prepare_empty_index_file_at_path(
                table.id,
                &index_name,
                self.index_path_for_table_schema(&table, table.id, &index_name),
            )?;
        }
        Ok((key, table, index_name))
    }

    /// Phase 2/3 of ADD INDEX: run the heap-scan backfill and flip the
    /// index from `Building` to `Public`. Safe to run on a background
    /// thread because it touches only the per-index file lock and the
    /// catalog `RwLock` — no `&self` reentrancy guarantees are needed
    /// beyond what `Arc<Database>` already provides. On rebuild
    /// failure (UNIQUE conflict, I/O error) the catalog entry is
    /// scrubbed via [`Database::scrub_failed_index_build`] so the user
    /// can re-issue without a stale Building tombstone. The error is
    /// then returned to the caller; a synchronous ADD INDEX surfaces
    /// it directly, an async ADD INDEX has the spawning thread log it
    /// (the connection has already returned successfully, so the
    /// failure is observable only via `mydb_ddl_jobs` going empty
    /// without a corresponding Public entry, plus the slow-DDL log).
    pub(crate) fn add_index_phase23(
        &self,
        table_key: &str,
        building_table: &TableSchema,
        index_name: &str,
        options: IndexBuildOptions,
    ) -> Result<(), QueryError> {
        #[cfg(test)]
        self.pause_ddl_backfill_for_test();

        let index_for_build = building_table
            .indexes
            .iter()
            .find(|existing| existing.name.eq_ignore_ascii_case(index_name))
            .expect("newly-added index should exist in cloned schema");
        if index_for_build.index_type.eq_ignore_ascii_case("BTREE") {
            INDEX_BUILD_REQUESTED_WORKERS.fetch_add(
                options.requested_degree.min(u64::MAX as usize) as u64,
                Ordering::Relaxed,
            );
            INDEX_BUILD_EFFECTIVE_WORKERS.fetch_add(
                options.effective_degree.min(u64::MAX as usize) as u64,
                Ordering::Relaxed,
            );
            if options.effective_degree > 1 {
                INDEX_BUILD_PARALLEL_BUILDS.fetch_add(1, Ordering::Relaxed);
            } else {
                INDEX_BUILD_SERIAL_BUILDS.fetch_add(1, Ordering::Relaxed);
            }
            if options.effective_degree < options.requested_degree {
                INDEX_BUILD_DEGREE_DOWNGRADES.fetch_add(1, Ordering::Relaxed);
            }
        }
        if let Err(err) = self.rebuild_index_with_options(building_table, index_for_build, options)
        {
            let _ = self.scrub_failed_index_build(table_key, index_name);
            return Err(err);
        }
        if let Err(err) = self.warm_vector_graphs_after_ddl(building_table, &[index_name]) {
            let _ = self.scrub_failed_index_build(table_key, index_name);
            return Err(err);
        }
        // Catalog write lock briefly: flip Building → Public.
        // Subsequent SELECTs may now use the index.
        {
            let mut catalog = self.catalog_write()?;
            if let Some(table_arc) = catalog.tables.get_mut(table_key) {
                let table = Arc::make_mut(table_arc);
                if let Some(slot) = table
                    .indexes
                    .iter_mut()
                    .find(|existing| existing.name.eq_ignore_ascii_case(index_name))
                {
                    slot.state = crate::storage::IndexState::Public;
                }
                table.bump_schema_version();
            }
        }
        self.mark_table_stats_stale(table_key, building_table.stats.row_count.max(1))?;
        self.note_catalog_table(building_table.id);
        self.commit_ddl_catalog_txn()?;
        Ok(())
    }

    /// Roll back a failed ADD INDEX: remove the half-built index from
    /// the catalog and delete its on-disk file. Best-effort — if the
    /// catalog or filesystem mutation fails we still want the original
    /// build error to reach the user, so this returns a Result that
    /// `add_index` deliberately ignores at the call site.
    fn scrub_failed_index_build(
        &self,
        table_key: &str,
        index_name: &str,
    ) -> Result<(), QueryError> {
        // Resolve physical identity before removing the descriptor. Afterwards
        // index_path() can only produce the missing-name fallback id.
        let physical_files = self
            .table_by_key(table_key)
            .map(|table| {
                let mut ids = table
                    .partitioning
                    .as_ref()
                    .map(|p| p.storage_ids())
                    .unwrap_or_default();
                ids.push(table.id);
                ids.into_iter()
                    .map(|id| (id, self.index_path(id, index_name)))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let table_id = {
            let mut catalog = self.catalog_write()?;
            let Some(table_arc) = catalog.tables.get_mut(table_key) else {
                return Ok(());
            };
            let table = Arc::make_mut(table_arc);
            let before = table.indexes.len();
            table
                .indexes
                .retain(|i| !i.name.eq_ignore_ascii_case(index_name));
            if table.indexes.len() == before {
                return Ok(());
            }
            if table
                .pending_primary_key
                .as_ref()
                .is_some_and(|p| p.capture_name.eq_ignore_ascii_case(index_name))
            {
                table.pending_primary_key = None;
            }
            table.bump_schema_version();
            table.id
        };
        self.sync_online_index_descriptor(table_id)?;
        if let Some(schema) = self.table_by_id(table_id) {
            self.table_locks_sharded
                .set_schema_floor(table_id, schema.schema_version);
            for id in schema.storage_ids() {
                self.table_locks_sharded
                    .set_schema_floor(id, schema.schema_version);
            }
        }
        self.note_catalog_table(table_id);
        self.commit_catalog_txn()?;
        for (storage_id, path) in physical_files {
            self.remove_index_cache_entry(storage_id, index_name)?;
            match std::fs::remove_file(path) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(io_err(err)),
            }
        }
        Ok(())
    }

    /// INSTANT ADD COLUMN — appends a new column to the catalog without
    /// rewriting any heap rows.
    ///
    /// Heap rows written before this DDL retain their original payload
    /// (a shorter encoded column count). The read path
    /// (`Database::pad_full_row_to_schema` / `backfill_projected_with_defaults`)
    /// substitutes the new column's catalog DEFAULT when those rows are
    /// projected, so existing reads keep working unchanged. Subsequent
    /// INSERT/UPDATE writes serialize the full schema-length value
    /// vector and naturally migrate the row to the latest layout, so
    /// no background backfill is needed.
    ///
    /// Validation matches the previous behaviour: NOT NULL without
    /// DEFAULT is rejected (the read-path default would otherwise be
    /// NULL, violating the constraint for pre-existing rows).
    pub fn add_column(&self, table_name: &str, column: ColumnDef) -> Result<(), QueryError> {
        self.add_column_positioned(table_name, column, None)
    }

    fn add_column_positioned(
        &self,
        table_name: &str,
        column: ColumnDef,
        position: Option<&crate::parser::ColumnPosition>,
    ) -> Result<(), QueryError> {
        let (database, table_only) = split_table_name(DEFAULT_DATABASE, table_name);
        check_table_ddl_writable(&database)?;
        let key = table_key(&database, &table_only);
        let check_defs = column.checks.clone();
        let mut new_column_def = column_def_to_schema(column)?;
        let mut catalog = self.catalog_write()?;
        let schema_check_names = check_constraint_names_in_schema(&catalog, &database, Some(&key));
        let table_arc = catalog
            .tables
            .get_mut(&key)
            .ok_or_else(|| ddl_table_not_found(&table_name))?;
        // Readers may bind while this DDL is running. Build/validate the whole
        // new descriptor privately so a failed ADD cannot expose a half-built
        // column between this helper returning and the executor's rollback.
        let mut candidate = table_arc.as_ref().clone();
        let table = &mut candidate;
        apply_column_charset_defaults(&mut new_column_def, &table.charset, &table.collation)?;
        if table
            .iter_visible_columns()
            .any(|(_, c)| c.name.eq_ignore_ascii_case(&new_column_def.name))
        {
            // Tombstone (Dropped) columns are deliberately ignored:
            // ADD COLUMN x after DROP COLUMN x should succeed and
            // allocate a fresh column_id at a new physical_pos.
            return Err(error(
                crate::error::catalog::ER_DUP_FIELDNAME,
                format!("duplicate column '{}'", new_column_def.name),
            ));
        }
        if new_column_def.auto_increment
            && !matches!(
                new_column_def.data_type,
                crate::storage::schema::DataTypeKind::TinyInt
                    | crate::storage::schema::DataTypeKind::SmallInt
                    | crate::storage::schema::DataTypeKind::MediumInt
                    | crate::storage::schema::DataTypeKind::Int
                    | crate::storage::schema::DataTypeKind::BigInt
                    | crate::storage::schema::DataTypeKind::Bool
            )
        {
            // MySQL diagnoses an invalid AUTO_INCREMENT data type before it
            // checks whether the column is a key. Keeping that precedence is
            // important for compound ALTER statements as well as CREATE.
            return Err(error(
                crate::error::catalog::ER_WRONG_FIELD_SPEC,
                "Incorrect column specifier for AUTO_INCREMENT column",
            ));
        }
        if new_column_def.auto_increment {
            return Err(error(crate::error::catalog::ER_WRONG_AUTO_KEY,
                "Incorrect table definition; there can be only one auto column and it must be defined as a key",
            ));
        }
        if !new_column_def.nullable
            && new_column_def.default.is_none()
            && new_column_def.default_expr.is_none()
            && !new_column_def.auto_increment
            && new_column_def.generated_expr.is_none()
        {
            // MySQL: ADD COLUMN NOT NULL without DEFAULT backfills existing
            // rows with the type's implicit default (INT→0, VARCHAR→'', …) —
            // an established ALTER relaxation distinct from INSERT's 1364, and
            // it applies even to an empty table. The value is supplied by the
            // read path (`column_read_backfill_default`), so INSERT-omit still
            // errors 1364. Only types with no implicit default (JSON /
            // GEOMETRY / VECTOR) genuinely can't be filled → keep 1364.
            if crate::engine::executor::insert::implicit_default_for_type(&new_column_def.data_type)
                .is_none()
            {
                return Err(error(
                    crate::error::catalog::ER_NO_DEFAULT_FOR_FIELD,
                    format!(
                        "column '{}' added without DEFAULT requires NULL allowed",
                        new_column_def.name
                    ),
                ));
            }
        }
        let row_default = new_column_def
            .default_expr
            .as_deref()
            .map(crate::parser::parse_catalog_expr)
            .transpose()
            .map_err(|err| error(crate::error::catalog::ER_PARSE_ERROR, err.to_string()))?
            .is_some_and(|expr| {
                table
                    .columns
                    .iter()
                    .any(|column| expr_references_column(&expr, &column.name))
            });
        if new_column_def.generated_expr.is_some() || row_default {
            // A generated value must be materialized into every existing
            // heap row (the instant path can't, since the projected read
            // path fills catalog DEFAULT rather than recomputing the
            // expression, and `stats.row_count` is unreliable/lazy so it
            // can't gate emptiness). Route through the partition-aware
            // COPY rewrite unconditionally — an empty table is a cheap
            // no-op rewrite. Both VIRTUAL and STORED generated columns
            // are physically stored in mydb's heap, so both need this.
            let old_table = catalog
                .tables
                .get(&key)
                .cloned()
                .ok_or_else(|| ddl_table_not_found(&table_name))?;
            drop(catalog);
            return self.add_generated_column_via_rewrite(
                table_name,
                old_table,
                new_column_def,
                check_defs,
                schema_check_names,
            );
        }
        // INSTANT ADD of a `DEFAULT (<expr>)` / `DEFAULT CURRENT_TIMESTAMP`
        // column: pre-existing short rows are served by
        // `column_read_backfill_default`, which only consults `default` and
        // returns NULL for an expr-only default — so old rows would read NULL
        // (violating NOT NULL for `NOT NULL DEFAULT CURRENT_TIMESTAMP`, and
        // diverging from MySQL, which backfills the ALTER-time value). Freeze
        // the ALTER-time evaluation into `default`. New INSERTs are unaffected:
        // both `resolve_column_default_value` (write path) and SHOW CREATE /
        // IS.COLUMNS prefer `default_expr`, so per-row semantics (a fresh
        // CURRENT_TIMESTAMP per insert) are preserved.
        if new_column_def.default.is_none() && new_column_def.default_expr.is_some() {
            let frozen = crate::engine::expr::resolve_column_default_value(&new_column_def);
            if !matches!(frozen, Value::Null) {
                new_column_def.default = Some(frozen);
            }
        }
        // Allocate stable identity now so the new column slot is never
        // reused by future ADD/DROP cycles. `physical_pos` is the next
        // free heap-slot index — it determines which slot in the
        // encoded row this column will occupy when a write actually
        // emits it. `logical_pos` controls SHOW COLUMNS / SELECT *
        // ordering and matches MySQL's "append at end" semantics.
        let column_id = table.allocate_column_id();
        let physical_pos = table.next_physical_pos();
        let logical_pos = table.columns.len() as u32;
        let mut new_column = new_column_def;
        new_column.read_backfill_default =
            Some(crate::engine::executor::insert::column_read_backfill_default(&new_column));
        new_column.column_id = column_id;
        new_column.state = ColumnState::Public;
        new_column.physical_pos = physical_pos;
        new_column.logical_pos = logical_pos;
        let table_id = table.id;
        table.columns.push(new_column);
        validate_table_column_identifiers(table)?;
        validate_and_prepare_generated_columns(table)?;
        if !check_defs.is_empty() {
            let mut new_checks = build_check_constraints_for_table(
                table,
                check_defs,
                Some(&table.check_constraints),
                None,
            )?;
            ensure_check_constraint_names_available(&schema_check_names, &new_checks)?;
            if table.stats.row_count > 0 {
                let default_values = table
                    .columns
                    .iter()
                    .map(crate::engine::expr::resolve_column_default_value)
                    .collect::<Vec<_>>();
                for check in &mut new_checks {
                    table.check_constraints.push(check.clone());
                    table.build_column_index();
                    let check = table.check_constraints.last().unwrap();
                    if check.enforced && !check_constraint_passes(table, check, &default_values)? {
                        return Err(check_constraint_error(&check.name));
                    }
                    table.check_constraints.pop();
                }
            }
            table.check_constraints.append(&mut new_checks);
        }
        if let Some(position) = position {
            let name = table
                .columns
                .last()
                .expect("new column was appended")
                .name
                .clone();
            table.build_column_index();
            move_column_in_candidate(table, &name, position)?;
        }
        table.publish_new_row_layout();
        table.build_column_index();
        table.bump_schema_version();
        // Reject a column whose addition pushes the fixed row past a page
        // (e.g. `ADD COLUMN v VECTOR(over-page-dim)`) here, with the 1118 +
        // dimension-cap hint, instead of deferring the failure to the first
        // INSERT/UPDATE that tries to encode a full-width row.
        validate_row_fits_page(table)?;
        *table_arc = Arc::new(candidate);
        drop(catalog);
        // Refresh the cached HeapFile's codec snapshot to match the
        // post-ADD schema. `remove_heap_cache_entry` would also work
        // but it evicts dirty heap pages along with the entry, which
        // would lose any uncommitted row writes still in the buffer
        // pool.
        self.refresh_heap_codec(table_id)?;
        // Heap data is intentionally untouched: existing rows are short
        // by one slot, and the read path supplies the new column's
        // DEFAULT on projection. Stats are still valid (row count
        // unchanged); we only mark them stale in case downstream
        // selectivity wants to re-collect distribution for the new
        // column.
        self.mark_table_stats_stale(&key, 0)?;
        self.note_catalog_table(table_id);
        self.commit_ddl_catalog_txn()
    }

    /// ADD COLUMN of a generated column onto a non-empty table. Builds
    /// the post-ADD candidate schema and routes it through the
    /// partition-aware COPY rewrite so the computed value is materialized
    /// into every existing heap row. Both VIRTUAL and STORED generated
    /// columns are physically stored in mydb's heap, so both take this
    /// path. `new_column_def` already has charset defaults applied;
    /// `schema_check_names` holds the check-constraint names used by
    /// other tables in the database (cross-table name uniqueness).
    fn add_generated_column_via_rewrite(
        &self,
        table_name: &str,
        old_table: Arc<TableSchema>,
        new_column_def: ColumnSchema,
        check_defs: Vec<crate::parser::CheckConstraintDef>,
        schema_check_names: std::collections::BTreeSet<String>,
    ) -> Result<(), QueryError> {
        let mut candidate = old_table.as_ref().clone();
        let column_id = candidate.allocate_column_id();
        let physical_pos = candidate.next_physical_pos();
        let logical_pos = candidate.columns.len() as u32;
        let mut new_column = new_column_def;
        new_column.column_id = column_id;
        new_column.state = ColumnState::Public;
        new_column.physical_pos = physical_pos;
        new_column.logical_pos = logical_pos;
        candidate.columns.push(new_column);
        validate_and_prepare_generated_columns(&mut candidate)?;
        if !check_defs.is_empty() {
            let mut new_checks = build_check_constraints_for_table(
                &candidate,
                check_defs,
                Some(&candidate.check_constraints),
                None,
            )?;
            ensure_check_constraint_names_available(&schema_check_names, &new_checks)?;
            candidate.check_constraints.append(&mut new_checks);
        }
        candidate.publish_new_row_layout();
        candidate.build_column_index();
        candidate.bump_schema_version();
        // strict=true: existing columns are unchanged (no lossy coercion
        // of stored data), and the generated value is computed fresh per
        // row by `materialize_generated_columns` inside the rewrite.
        self.rewrite_table_for_alter_copy(table_name, old_table, candidate, true)?;
        Ok(())
    }

    /// INSTANT DROP COLUMN — flips the target column's lifecycle state
    /// to [`ColumnState::Dropped`] (tombstone) without rewriting any
    /// heap data. Existing row layouts remain readable by `layout_id`,
    /// while the newly-published active layout omits the dropped column
    /// so future writes stop carrying the tombstone payload. SQL
    /// surfaces — name lookup, SHOW COLUMNS, SELECT *,
    /// information_schema.columns, INSERT-without-column-list arity —
    /// all skip Dropped columns.
    ///
    /// PRIMARY/foreign-key and expression dependencies are validated before
    /// mutation. Secondary-index cascades build private replacement trees and
    /// publish them with the tombstone; the heap is never copied.
    pub fn drop_column(&self, table_name: &str, column_name: &str) -> Result<(), QueryError> {
        let (database, table_only) = split_table_name(DEFAULT_DATABASE, table_name);
        check_table_ddl_writable(&database)?;
        let key = table_key(&database, &table_only);
        // Pre-checks + index cascade planning on a read snapshot, so the
        // cascade (drop_index / add_index) can take their own catalog locks
        // before we grab the write lock for the column tombstone.
        let snapshot = self
            .table_by_key(&key)
            .ok_or_else(|| ddl_table_not_found(&table_name))?;
        if snapshot.visible_column_count() <= 1 {
            return Err(error(
                crate::error::catalog::ER_CANT_REMOVE_ALL_FIELDS,
                "cannot drop the only column in a table",
            ));
        }
        if !snapshot
            .columns
            .iter()
            .any(|c| c.state != ColumnState::Dropped && c.name.eq_ignore_ascii_case(column_name))
        {
            return Err(error(
                crate::error::catalog::ER_BAD_FIELD_ERROR,
                format!("unknown column '{column_name}'"),
            ));
        }
        reject_column_referenced_by_check(&snapshot, column_name)?;
        reject_column_referenced_by_generated(&snapshot, column_name)?;
        // P0-8 — a column referenced by the table's PARTITION BY /
        // SUBPARTITION BY clause cannot be dropped without desynchronizing
        // the partition definition. Reject cleanly before any physical
        // change is scheduled.
        reject_column_referenced_by_partitioning(&snapshot, column_name)?;
        self.fk_reject_column_referenced(&snapshot, column_name)?;
        if snapshot
            .primary_key
            .iter()
            .any(|c| c.eq_ignore_ascii_case(column_name))
        {
            return Err(error(
                crate::error::catalog::ER_CANT_REMOVE_ALL_FIELDS,
                "cannot drop column that is part of PRIMARY KEY",
            ));
        }
        // MySQL: dropping a column referenced by an index cascades to the
        // index — a single-column / functional / special index is dropped,
        // and a plain composite index drops just that column (issue #83).
        // Only foreign-key backing indexes still block the drop (handled by
        // fk_reject_column_referenced above). Execute the cascade via the
        // self-locking helpers before the tombstone.
        let (indexes_to_drop, indexes_to_rebuild) =
            plan_index_cascade_for_drop_column(&snapshot, column_name)?;
        let mut candidate = snapshot.as_ref().clone();
        candidate
            .indexes
            .retain(|index| !indexes_to_drop.contains(&index.name));
        // Give rebuilt indexes private object ids. Reusing the live id/name
        // before validation could truncate the old tree on a duplicate error.
        for mut rebuilt in indexes_to_rebuild {
            rebuilt.index_id = 0;
            let index = candidate
                .indexes
                .iter_mut()
                .find(|i| i.name == rebuilt.name)
                .expect("cascade index exists");
            *index = rebuilt;
        }
        let pos = candidate.column_index(column_name)?;
        candidate.columns[pos].state = ColumnState::Dropped;
        candidate.publish_new_row_layout();
        candidate.build_column_index();
        candidate.bump_schema_version();
        let replacements = candidate
            .indexes
            .iter()
            .filter(|index| {
                snapshot
                    .indexes
                    .iter()
                    .any(|old| old.name == index.name && old.index_id != index.index_id)
            })
            .cloned()
            .collect::<Vec<_>>();
        let mut built_indexes = Vec::new();
        if replacements.is_empty() {
            self.replace_table_schema_in_catalog(table_name, candidate.clone())?;
        } else {
            let txn = self.begin_txn()?;
            let _txn = OnlineCopyTxnGuard {
                db: self,
                txn_id: txn.txn_id,
            };
            let mut lock_ids = snapshot.storage_ids();
            lock_ids.push(snapshot.id);
            lock_ids.sort_unstable();
            lock_ids.dedup();
            self.try_online_index_data_barrier(txn.txn_id, &lock_ids)?;
            let mut paths = Vec::new();
            let result = (|| {
                let _lsn = btree_logical_wal::LsnScope::new(Some(self.log.lock().last_lsn()));
                for sid in snapshot.storage_ids() {
                    let mut leaf = candidate.clone();
                    leaf.id = sid;
                    leaf.partitioning = None;
                    leaf.indexes = replacements.clone();
                    for index in &leaf.indexes {
                        paths.push(self.index_path_for_table_schema(&leaf, sid, &index.name));
                    }
                    let mut builders = DdlCopyIndexBuilders::new(&leaf)?;
                    let plans = builders.key_plans(&leaf);
                    let pages = self.with_heap_read(sid, |heap| heap.page_count())?;
                    for page in 0..pages {
                        crate::engine::cancel::check_cancelled()?;
                        let rows =
                            self.scan_rows_storage_range(&snapshot, sid, txn, page, page + 1)?;
                        let mut ids = Vec::with_capacity(rows.len());
                        let mut batch = Vec::with_capacity(rows.len());
                        for mut row in rows {
                            ids.push(row.row_id);
                            let keys =
                                DdlCopyIndexBuilders::prepare_keys(&leaf, &plans, &mut row.values)?;
                            batch.push(DdlCopyPreparedRow {
                                source_row: row.row_id,
                                values: None,
                                payload: Vec::new(),
                                keys,
                            });
                        }
                        builders.push_prepared_batch(self.data_dir(), batch, &ids)?;
                    }
                    for (name, file) in builders.finish_private(self, &leaf)? {
                        built_indexes.push((
                            (sid, normalize_name(&name)),
                            Arc::new(parking_lot::RwLock::new(file)),
                        ));
                    }
                }
                let key = table_key(&candidate.database, &candidate.name);
                self.catalog_write()?
                    .tables
                    .insert(key.clone(), Arc::new(candidate.clone()));
                self.note_catalog_table(candidate.id);
                if let Err(err) = self.commit_catalog_txn_existing(txn) {
                    self.catalog_write()?.tables.insert(key, snapshot.clone());
                    return Err(err);
                }
                Ok(())
            })();
            if result.is_err() {
                // Publication failure may be an ambiguous I/O error. Keep private
                // trees if storage is quarantined so recovery can resolve it.
                if self.ensure_operational().is_ok() {
                    for path in paths {
                        let _ = fs::remove_file(path);
                    }
                }
                return result;
            }
            self.refresh_heap_codec(candidate.id)?;
        }
        // Only reclaim old derived files after the complete schema is durable.
        for old in &snapshot.indexes {
            if candidate.indexes.iter().any(|i| i.index_id == old.index_id) {
                continue;
            }
            for sid in snapshot.storage_ids() {
                self.remove_index_cache_entry(sid, &old.name)?;
                let path = self.index_path_for_table_schema(&snapshot, sid, &old.name);
                let _ = self.delete_logged_file(txn_id_for_ddl(), path);
                if old.is_vector() {
                    for path in self.vector_file_paths(&snapshot, &old.name, sid) {
                        let _ = self.buffer_pool.discard_pages_for_path(&path);
                        let _ = fs::remove_file(path);
                    }
                }
            }
            if old.is_vector() {
                self.drop_vector_graph(snapshot.id, &old.name);
            }
        }
        self.index_cache.write().extend(built_indexes);
        self.mark_table_stats_stale(&key, 0)?;
        Ok(())
    }

    pub fn drop_index(&self, table_name: &str, index_name: &str) -> Result<(), QueryError> {
        let _definition = self.pin_ddl_table(table_name)?;
        let _role = self.zone_map_gate.read_recursive();
        self.ensure_writable()?;
        let source = self.table(table_name)?;
        let lock_txn = self.begin_read_only_txn()?;
        let _data_barrier = OnlineIndexTxnGuard {
            db: self,
            txn_id: lock_txn.txn_id,
        };
        let mut lock_ids = source.storage_ids();
        lock_ids.push(source.id);
        lock_ids.sort_unstable();
        lock_ids.dedup();
        self.try_online_index_data_barrier(lock_txn.txn_id, &lock_ids)?;
        let (database, table_only) = split_table_name(DEFAULT_DATABASE, table_name);
        check_table_ddl_writable(&database)?;
        let key = table_key(&database, &table_only);
        let (table_snapshot, dropped_index) = {
            let mut catalog = self.catalog_write()?;
            let table_arc = catalog
                .tables
                .get_mut(&key)
                .ok_or_else(|| ddl_table_not_found(&table_name))?;
            let table = Arc::make_mut(table_arc);
            // Refuse to drop an index that is the only one able to back a
            // foreign key (child backing index or parent referenced key).
            self.fk_reject_index_drop(table, index_name)?;
            if let Some(auto_idx) = table.auto_increment_column_index() {
                let auto_col = normalize_name(&table.columns[auto_idx].name);
                let dropping_auto_key = table.indexes.iter().any(|index| {
                    !index.primary
                        && index.name.eq_ignore_ascii_case(index_name)
                        && index.state == IndexState::Public
                        && index
                            .columns
                            .first()
                            .is_some_and(|column| normalize_name(column) == auto_col)
                });
                let replacement_auto_key = table.indexes.iter().any(|index| {
                    !index.name.eq_ignore_ascii_case(index_name)
                        && index.state == IndexState::Public
                        && index
                            .columns
                            .first()
                            .is_some_and(|column| normalize_name(column) == auto_col)
                });
                if dropping_auto_key && !replacement_auto_key {
                    return Err(error(crate::error::catalog::ER_WRONG_AUTO_KEY,
                        "Incorrect table definition; AUTO_INCREMENT column must be defined as a key",
                    ));
                }
            }
            let dropped_index = table
                .indexes
                .iter()
                .find(|index| index.name.eq_ignore_ascii_case(index_name) && !index.primary)
                .cloned()
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_CANT_DROP_FIELD_OR_KEY,
                        format!("index '{index_name}' does not exist"),
                    )
                })?;
            let table_snapshot = table.clone();
            let before = table.indexes.len();
            table
                .indexes
                .retain(|index| !index.name.eq_ignore_ascii_case(index_name) || index.primary);
            if table.indexes.len() == before {
                return Err(error(
                    crate::error::catalog::ER_CANT_DROP_FIELD_OR_KEY,
                    format!("index '{index_name}' does not exist"),
                ));
            }
            table.stats.indexes.remove(&normalize_name(index_name));
            table.bump_schema_version();
            (table_snapshot, dropped_index)
        };
        let mut retired = DropTableIntent {
            table_id: table_snapshot.id,
            retired_index: Some((
                dropped_index.index_id,
                table_snapshot.active_generation.max(1),
            )),
            had_auto_increment: false,
            database: database.clone(),
            table_name: table_only,
            files: Vec::new(),
            vector_files: Vec::new(),
        };
        for storage_id in table_snapshot.storage_ids() {
            retired.files.push(self.relative_path(
                &crate::storage::naming::index_object_path_by_id(
                    &self.data_dir,
                    storage_id,
                    table_snapshot.active_generation.max(1),
                    dropped_index.index_id,
                    "idx",
                ),
            ));
            if dropped_index.is_vector() {
                retired.vector_files.extend(
                    self.vector_file_paths(&table_snapshot, index_name, storage_id)
                        .iter()
                        .map(|path| self.relative_path(path)),
                );
            }
        }
        self.note_catalog_table(table_snapshot.id);
        let catalog_txn = match self.prepare_index_retirement(&retired) {
            Ok(txn) => txn,
            Err(err) => {
                self.catalog_write()?
                    .tables
                    .insert(key, Arc::new(table_snapshot));
                Self::take_catalog_mutations();
                return Err(err);
            }
        };
        // Retire cache handles before the committed manifest becomes visible
        // to maintenance, preventing later writeback from resurrecting files.
        for storage_id in table_snapshot.storage_ids() {
            if let Err(err) = self.remove_index_cache_entry(storage_id, index_name) {
                self.quarantine_mvcc(format!(
                    "index retirement cache flush failed: {}",
                    err.message()
                ));
                return Err(err);
            }
        }
        // Drop the in-memory HNSW graph if this was a vector index (no-op
        // otherwise); the registry B-tree file is removed above.
        self.drop_vector_graph(table_snapshot.id, index_name);
        if let Err(err) = catalog_txn.commit_ddl() {
            self.quarantine_mvcc(format!(
                "index retirement catalog commit failed: {}",
                err.message()
            ));
            return Err(err);
        }
        for id in lock_ids {
            self.table_locks_sharded
                .set_schema_floor(id, table_snapshot.schema_version.saturating_add(1));
        }
        if self.config.durability != DurabilityMode::Relaxed {
            self.purge_dropped_table_files(&retired);
        }
        Ok(())
    }

    pub fn alter_index_visibility(
        &self,
        table_name: &str,
        index_name: &str,
        visible: bool,
    ) -> Result<(), QueryError> {
        let (database, table_only) = split_table_name(DEFAULT_DATABASE, table_name);
        check_table_ddl_writable(&database)?;
        let key = table_key(&database, &table_only);
        let mut changed = false;
        {
            let mut catalog = self.catalog_write()?;
            let table_arc = catalog
                .tables
                .get_mut(&key)
                .ok_or_else(|| ddl_table_not_found(&table_name))?;
            let table = Arc::make_mut(table_arc);
            let index = table
                .indexes
                .iter_mut()
                .find(|index| index.name.eq_ignore_ascii_case(index_name))
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_CANT_DROP_FIELD_OR_KEY,
                        format!("index '{index_name}' does not exist"),
                    )
                })?;
            if index.primary && !visible {
                // S22 — MySQL 8.0 reports ER_PK_INDEX_CANT_BE_INVISIBLE
                // (3522) here, matching the inline `PRIMARY KEY ... INVISIBLE`
                // path; this ALTER branch used to return a generic 1235.
                return Err(error(
                    crate::error::catalog::ER_PK_INDEX_CANT_BE_INVISIBLE,
                    "A primary key index cannot be invisible.",
                ));
            }
            if index.visible != visible {
                index.visible = visible;
                table.bump_schema_version();
                changed = true;
            }
        }
        if changed {
            self.note_catalog_table_key(&key)?;
            self.commit_ddl_catalog_txn()?;
        }
        Ok(())
    }

    pub fn rename_index(
        &self,
        table_name: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<(), QueryError> {
        validate_mysql_identifier(new_name)?;
        let _descriptor = self.pin_ddl_table(table_name)?;
        let (database, table_only) = split_table_name(DEFAULT_DATABASE, table_name);
        check_table_ddl_writable(&database)?;
        let key = table_key(&database, &table_only);
        // Validate against the complete descriptor before touching runtime handles.
        let table_snapshot = {
            let catalog = self.catalog_read()?;
            let table = catalog
                .tables
                .get(&key)
                .ok_or_else(|| ddl_table_not_found(&table_name))?;
            if table.indexes.iter().any(|index| {
                index.name.eq_ignore_ascii_case(new_name)
                    && !index.name.eq_ignore_ascii_case(old_name)
            }) {
                return Err(error(
                    crate::error::catalog::ER_DUP_KEYNAME,
                    format!("duplicate index '{new_name}'"),
                ));
            }
            let index = table
                .indexes
                .iter()
                .find(|index| index.name.eq_ignore_ascii_case(old_name))
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_CANT_DROP_FIELD_OR_KEY,
                        format!("index '{old_name}' does not exist"),
                    )
                })?;
            if index.state != IndexState::Public {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "cannot rename an index while it is being built",
                ));
            }
            if index.primary {
                return Err(error(
                    crate::error::catalog::ER_WRONG_NAME_FOR_INDEX,
                    "cannot rename PRIMARY KEY index",
                ));
            }
            table.clone()
        };
        // The stable physical index identity is unchanged by publication.
        {
            let mut catalog = self.catalog_write()?;
            let mut backing_index_renamed = false;
            {
                let table_arc = catalog
                    .tables
                    .get_mut(&key)
                    .ok_or_else(|| ddl_table_not_found(&table_name))?;
                let table = Arc::make_mut(table_arc);
                let Some(index) = table
                    .indexes
                    .iter_mut()
                    .find(|index| index.name.eq_ignore_ascii_case(old_name))
                else {
                    return Err(error(
                        crate::error::catalog::ER_CANT_DROP_FIELD_OR_KEY,
                        format!("index '{old_name}' does not exist"),
                    ));
                };
                index.name = new_name.to_string();
                // S4 — a foreign key on this table records the *name* of its
                // child-side backing index, and parent-side DELETE/UPDATE
                // enforcement (`fk_enforce_parent_*`) looks the child index up
                // by that name. If we rename the index without updating the
                // FK, that lookup fails and CASCADE/SET NULL/RESTRICT are
                // silently skipped, producing orphan rows. Keep it in sync.
                for fk in &mut table.foreign_keys {
                    if fk.backing_index.eq_ignore_ascii_case(old_name) {
                        fk.backing_index = new_name.to_string();
                        backing_index_renamed = true;
                    }
                }
                if let Some(stats) = table.stats.indexes.remove(&normalize_name(old_name)) {
                    table.stats.indexes.insert(normalize_name(new_name), stats);
                }
                table.bump_schema_version();
            }
            // Propagate the renamed backing index into every parent table's
            // inbound-FK cache (`fk_referenced_by.child_backing_index`),
            // which is derived from the child's `foreign_keys[].backing_index`.
            if backing_index_renamed {
                build_fk_runtime(&mut catalog);
            }
        }
        let renamed = self.catalog_read()?.tables[&key].clone();
        self.rebind_index_runtime_names(&table_snapshot, &renamed);
        self.note_catalog_table_key(&key)?;
        self.commit_ddl_catalog_txn()?;
        Ok(())
    }

    pub fn drop_primary_key(&self, table_name: &str) -> Result<(), QueryError> {
        let (database, table_only) = split_table_name(DEFAULT_DATABASE, table_name);
        check_table_ddl_writable(&database)?;
        let key = table_key(&database, &table_only);
        let source = self.table(table_name)?;
        self.check_ddl_table_lock(source.id)?;
        let files = source
            .storage_ids()
            .into_iter()
            .map(|id| (id, self.index_path(id, "PRIMARY")))
            .collect::<Vec<_>>();
        {
            let mut catalog = self.catalog_write()?;
            let table_arc = catalog
                .tables
                .get_mut(&key)
                .ok_or_else(|| ddl_table_not_found(&table_name))?;
            let table = Arc::make_mut(table_arc);
            self.fk_reject_index_drop(table, "PRIMARY")?;
            if !table.indexes.iter().any(|index| index.primary) {
                return Err(error(
                    crate::error::catalog::ER_CANT_DROP_FIELD_OR_KEY,
                    "PRIMARY KEY does not exist",
                ));
            }
            if let Some(auto_idx) = table.auto_increment_column_index() {
                let auto_col = normalize_name(&table.columns[auto_idx].name);
                let primary_owns_auto = table
                    .primary_key
                    .first()
                    .is_some_and(|column| normalize_name(column) == auto_col);
                let replacement_auto_key = table.indexes.iter().any(|index| {
                    !index.primary
                        && index.state == IndexState::Public
                        && index
                            .columns
                            .first()
                            .is_some_and(|column| normalize_name(column) == auto_col)
                });
                if primary_owns_auto && !replacement_auto_key {
                    return Err(error(crate::error::catalog::ER_WRONG_AUTO_KEY,
                        "Incorrect table definition; AUTO_INCREMENT column must be defined as a key",
                    ));
                }
            }
            table.indexes.retain(|index| !index.primary);
            table.primary_key.clear();
            table.stats.indexes.remove("primary");
            table.bump_schema_version();
        }
        self.note_catalog_table_key(&key)?;
        self.commit_ddl_catalog_txn()?;
        for (sid, path) in files {
            self.remove_index_cache_entry(sid, "PRIMARY")?;
            self.delete_logged_file(txn_id_for_ddl(), path)?;
        }
        Ok(())
    }

    /// `ALTER TABLE … PCTFREE=N PCTUSED=N`: persist Oracle-style
    /// physical-storage knobs on the table's catalog entry. `None` for
    /// either side means "do not change". Bumps `schema_version` (the
    /// per-table HeapFile cache rereads `update_reserve_bytes` on next
    /// open; existing handles are refreshed in place without discarding
    /// buffered rows). Persists into the
    /// `mysql.tables.table_options_json` column on the next dictionary
    /// flush.
    pub fn set_table_storage_properties(
        &self,
        table_name: &str,
        pctfree: Option<u8>,
        pctused: Option<u8>,
    ) -> Result<(), QueryError> {
        let (database, table_only) = split_table_name(DEFAULT_DATABASE, table_name);
        check_table_ddl_writable(&database)?;
        let key = table_key(&database, &table_only);
        let table_id;
        {
            let mut catalog = self.catalog_write()?;
            let table_arc = catalog
                .tables
                .get_mut(&key)
                .ok_or_else(|| ddl_table_not_found(&table_name))?;
            let table = Arc::make_mut(table_arc);
            table_id = table.id;
            if let Some(pf) = pctfree {
                table.pctfree = pf.min(50);
            }
            if let Some(pu) = pctused {
                table.pctused = pu.min(50);
            }
            table.bump_schema_version();
        }
        // Metadata changes must preserve dirty heap pages, particularly in
        // Memory mode. Refresh the cached codec/reserve for all partition leaves.
        self.refresh_heap_codec(table_id)?;
        self.note_catalog_table(table_id);
        self.commit_ddl_catalog_txn()
    }

    pub fn set_table_options(
        &self,
        table_name: &str,
        engine: Option<String>,
        default_charset: Option<String>,
        default_collation: Option<String>,
        comment: Option<String>,
        generic: Vec<(String, String)>,
    ) -> Result<(), QueryError> {
        let (database, table_only) = split_table_name(DEFAULT_DATABASE, table_name);
        check_table_ddl_writable(&database)?;
        let key = table_key(&database, &table_only);
        let zone_options_changed = generic.iter().any(|(key, _)| {
            key.eq_ignore_ascii_case("ZONE_MAP") || key.eq_ignore_ascii_case("ZONE_SIZE")
        });
        let mut zone_storage_ids = Vec::new();
        {
            let mut catalog = self.catalog_write()?;
            let table_arc = catalog
                .tables
                .get_mut(&key)
                .ok_or_else(|| ddl_table_not_found(&table_name))?;
            let table = Arc::make_mut(table_arc);
            if zone_options_changed {
                zone_storage_ids = table.storage_ids();
            }
            // mydb is single-engine: the declared ENGINE is metadata only,
            // recorded so SHOW CREATE TABLE / SHOW TABLE STATUS echo the
            // requested engine while the table physically uses mydb's MVCC
            // engine. Empty string keeps the field's default sentinel.
            if let Some(engine) = engine {
                if !engine.is_empty() {
                    table.engine = engine;
                }
            }
            if default_charset.is_some() || default_collation.is_some() {
                let (charset, collation) = resolve_table_charset_collation(
                    default_charset.as_deref(),
                    default_collation.as_deref(),
                    &table.charset,
                    &table.collation,
                )?;
                table.charset = charset;
                table.collation = collation;
            }
            if let Some(comment) = comment {
                crate::storage::schema::check_comment_len(
                    crate::storage::schema::CommentTarget::Table,
                    &table_only,
                    &comment,
                )?;
                table.comment = comment;
            }
            crate::storage::schema::apply_generic_table_options(
                &mut table.mysql_table_options,
                generic,
            );
            table.bump_schema_version();
        }
        if zone_options_changed {
            for storage_id in zone_storage_ids {
                self.forget_zone_map(storage_id)?;
                let _ = fs::remove_file(self.zone_map_file_path(storage_id));
            }
            if let Ok(table) = self.table(table_name) {
                self.invalidate_zone_maps_for_table(&table)?;
            }
        }
        self.note_catalog_table_key(&key)?;
        self.commit_ddl_catalog_txn()
    }

    /// `ALTER TABLE t CONVERT TO CHARACTER SET cs [COLLATE coll]`.
    /// Although mydb stores strings internally as UTF-8, conversion is a
    /// physical COPY operation: existing values must be representable in the
    /// destination character set and every collation-bearing index key must
    /// be rebuilt atomically with the schema change.
    pub fn convert_table_charset(
        &self,
        table_name: &str,
        charset: &str,
        collation: Option<&str>,
        strict: bool,
    ) -> Result<Vec<crate::engine::api::SqlWarning>, QueryError> {
        let (database, table_only) = split_table_name(DEFAULT_DATABASE, table_name);
        check_table_ddl_writable(&database)?;
        let charset_info = crate::charset::resolve_charset(charset)?;
        let collation_name = match collation {
            Some(coll) => {
                let info = crate::charset::resolve_collation(coll)?;
                crate::charset::validate_charset_collation_pair(charset_info.name, info.name)?;
                info.name.to_string()
            }
            None => crate::charset::default_collation_for_charset(charset_info.name)?.to_string(),
        };
        let key = table_key(&database, &table_only);
        let old_table = self
            .catalog_read()?
            .tables
            .get(&key)
            .cloned()
            .ok_or_else(|| ddl_table_not_found(&table_name))?;
        let mut candidate = old_table.as_ref().clone();
        candidate.charset = charset_info.name.to_string();
        candidate.collation = collation_name.clone();
        let mut changed = false;
        for column in &mut candidate.columns {
            if column.state == ColumnState::Dropped {
                continue;
            }
            use crate::storage::schema::DataTypeKind as K;
            if matches!(
                column.data_type,
                K::Char { .. }
                    | K::VarChar { .. }
                    | K::Text { .. }
                    | K::Enum { .. }
                    | K::Set { .. }
            ) {
                changed |= column.charset.as_deref() != Some(charset_info.name)
                    || column.collation.as_deref() != Some(collation_name.as_str());
                column.charset = Some(charset_info.name.to_string());
                column.collation = Some(collation_name.clone());
            }
        }
        if !changed
            && old_table.charset.eq_ignore_ascii_case(charset_info.name)
            && old_table.collation.eq_ignore_ascii_case(&collation_name)
        {
            return Ok(Vec::new());
        }
        candidate.bump_schema_version();
        candidate.build_column_index();
        self.rewrite_table_for_alter_copy(table_name, old_table, candidate, strict)
    }

    /// MySQL 8.0.23+ `ALTER TABLE t ALTER COLUMN c SET {VISIBLE|INVISIBLE}`
    /// — catalog-only flip; `validate_alter_table_actions` has already
    /// guaranteed at least one visible column remains.
    /// Metadata-only mutation under the existing catalog transaction. Callers
    /// retain responsibility for DDL locking and action-specific validation.
    fn mutate_catalog_table_schema(
        &self,
        table_name: &str,
        mutate: impl FnOnce(&mut TableSchema) -> Result<(), QueryError>,
    ) -> Result<(), QueryError> {
        let (database, table_only) = split_table_name(DEFAULT_DATABASE, table_name);
        check_table_ddl_writable(&database)?;
        let key = table_key(&database, &table_only);
        {
            let mut catalog = self.catalog_write()?;
            let table = catalog
                .tables
                .get_mut(&key)
                .ok_or_else(|| ddl_table_not_found(&table_name))?;
            let table = Arc::make_mut(table);
            mutate(table)?;
            table.bump_schema_version();
        }
        self.note_catalog_table_key(&key)?;
        self.commit_ddl_catalog_txn()
    }

    pub fn set_column_visibility(
        &self,
        table_name: &str,
        column_name: &str,
        invisible: bool,
    ) -> Result<(), QueryError> {
        self.mutate_catalog_table_schema(table_name, |table| {
            if invisible {
                let table = &*table;
                let idx = table.column_index(column_name)?;
                let other_visible = table
                    .columns
                    .iter()
                    .enumerate()
                    .any(|(i, c)| i != idx && c.state != ColumnState::Dropped && !c.invisible);
                if !other_visible {
                    return Err(error(
                        crate::error::catalog::ER_TABLE_MUST_HAVE_A_VISIBLE_COLUMN,
                        "A table must have at least one visible column.",
                    ));
                }
            }
            let idx = table.column_index(column_name)?;
            table.columns[idx].invisible = invisible;
            Ok(())
        })
    }

    pub fn set_column_comment(
        &self,
        table_name: &str,
        column_name: &str,
        comment: &str,
    ) -> Result<(), QueryError> {
        self.mutate_catalog_table_schema(table_name, |table| {
            let idx = table.column_index(column_name)?;
            crate::storage::schema::check_comment_len(
                crate::storage::schema::CommentTarget::Field,
                column_name,
                comment,
            )?;
            table.columns[idx].comment = comment.to_string();
            Ok(())
        })
    }

    pub fn move_column_position(
        &self,
        table_name: &str,
        column_name: &str,
        position: crate::parser::ColumnPosition,
    ) -> Result<(), QueryError> {
        self.mutate_catalog_table_schema(table_name, |table| {
            move_column_in_candidate(table, column_name, &position)?;
            table.build_column_index();
            Ok(())
        })
    }

    pub fn set_column_default(
        &self,
        table_name: &str,
        column_name: &str,
        default: Option<crate::parser::Expr>,
    ) -> Result<(), QueryError> {
        self.mutate_catalog_table_schema(table_name, |table| {
            let idx = table.column_index(column_name)?;
            set_column_default_in_candidate(table, idx, column_name, default, true)?;
            Ok(())
        })
    }

    pub fn add_check_constraint(
        &self,
        table_name: &str,
        check: crate::parser::CheckConstraintDef,
    ) -> Result<(), QueryError> {
        let (database, table_only) = split_table_name(DEFAULT_DATABASE, table_name);
        check_table_ddl_writable(&database)?;
        let key = table_key(&database, &table_only);
        let ddl_txn = self.begin_read_only_txn()?;
        let result = (|| {
            let existing = self
                .catalog_read()?
                .tables
                .get(&key)
                .cloned()
                .ok_or_else(|| ddl_table_not_found(&table_name))?;
            // CHECK validation reads rows and only needs to exclude writers.
            // This can also run inside a read-compatible MODIFY's S barrier.
            self.acquire_table_shared(ddl_txn.txn_id, existing.id)?;
            let mut candidate = (*existing).clone();
            let mut new_checks = build_check_constraints_for_table(
                &candidate,
                vec![check],
                Some(&candidate.check_constraints),
                None,
            )?;
            let schema_names = {
                let catalog = self.catalog_read()?;
                check_constraint_names_in_schema(&catalog, &database, Some(&key))
            };
            ensure_check_constraint_names_available(&schema_names, &new_checks)?;
            candidate.check_constraints.append(&mut new_checks);
            candidate.build_column_index();
            let added = candidate.check_constraints.last().cloned().unwrap();
            validate_existing_check_constraint_values(self, &candidate, &added)?;
            {
                let mut catalog = self.catalog_write()?;
                let schema_names =
                    check_constraint_names_in_schema(&catalog, &database, Some(&key));
                ensure_check_constraint_names_available(
                    &schema_names,
                    std::slice::from_ref(&added),
                )?;
                let table_arc = catalog
                    .tables
                    .get_mut(&key)
                    .ok_or_else(|| ddl_table_not_found(&table_name))?;
                let table = Arc::make_mut(table_arc);
                if table
                    .check_constraints
                    .iter()
                    .any(|existing| existing.name == added.name)
                {
                    return Err(error(
                        crate::error::catalog::ER_CHECK_CONSTRAINT_DUP_NAME,
                        format!("Duplicate check constraint name '{}'.", added.name),
                    ));
                }
                table.check_constraints.push(added);
                table.build_column_index();
                table.bump_schema_version();
            }
            self.note_catalog_table_key(&key)?;
            self.commit_ddl_catalog_txn()
        })();
        let release = self.rollback_txn(ddl_txn.txn_id);
        match (result, release) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(err), _) => Err(err),
            (Ok(()), Err(err)) => Err(err),
        }
    }

    pub fn drop_check_constraint(
        &self,
        table_name: &str,
        name: &str,
        if_exists: bool,
        include_foreign_keys: bool,
    ) -> Result<(), QueryError> {
        let (database, table_only) = split_table_name(DEFAULT_DATABASE, table_name);
        check_table_ddl_writable(&database)?;
        let key = table_key(&database, &table_only);
        let removed_check = {
            let mut catalog = self.catalog_write()?;
            let table_arc = catalog
                .tables
                .get_mut(&key)
                .ok_or_else(|| ddl_table_not_found(&table_name))?;
            let table = Arc::make_mut(table_arc);
            let before = table.check_constraints.len();
            table.check_constraints.retain(|check| check.name != name);
            if table.check_constraints.len() != before {
                table.build_column_index();
                table.bump_schema_version();
                true
            } else {
                false
            }
        };
        if removed_check {
            self.note_catalog_table_key(&key)?;
            return self.commit_ddl_catalog_txn();
        }
        // A CHECK-only operation must never silently remove a foreign key.
        if include_foreign_keys && self.drop_foreign_key_if_present(table_name, name)? {
            return Ok(());
        }
        if if_exists {
            return Ok(());
        }
        Err(error(
            crate::error::catalog::ER_CONSTRAINT_NOT_FOUND,
            format!("Check constraint '{name}' does not exist."),
        ))
    }

    pub fn alter_check_constraint(
        &self,
        table_name: &str,
        name: &str,
        enforced: bool,
    ) -> Result<(), QueryError> {
        let (database, table_only) = split_table_name(DEFAULT_DATABASE, table_name);
        check_table_ddl_writable(&database)?;
        let key = table_key(&database, &table_only);
        if enforced {
            let ddl_txn = self.begin_read_only_txn()?;
            let result = (|| {
                let existing = self
                    .catalog_read()?
                    .tables
                    .get(&key)
                    .cloned()
                    .ok_or_else(|| ddl_table_not_found(&table_name))?;
                self.acquire_table_exclusive(ddl_txn.txn_id, existing.id)?;
                let mut candidate = (*existing).clone();
                set_check_enforced_in_candidate::<false>(&mut candidate, name, true)?;
                candidate.build_column_index();
                let check = candidate
                    .check_constraints
                    .iter()
                    .find(|check| check.name == name)
                    .unwrap()
                    .clone();
                validate_existing_check_constraint_values(self, &candidate, &check)?;
                self.set_check_constraint_enforced(&key, name, true)
            })();
            let release = self.rollback_txn(ddl_txn.txn_id);
            match (result, release) {
                (Ok(()), Ok(())) => Ok(()),
                (Err(err), _) => Err(err),
                (Ok(()), Err(err)) => Err(err),
            }
        } else {
            self.set_check_constraint_enforced(&key, name, false)
        }
    }

    fn set_check_constraint_enforced(
        &self,
        table_key: &str,
        name: &str,
        enforced: bool,
    ) -> Result<(), QueryError> {
        {
            let mut catalog = self.catalog_write()?;
            let table_arc = catalog.tables.get_mut(table_key).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_NO_SUCH_TABLE,
                    "table does not exist",
                )
            })?;
            let table = Arc::make_mut(table_arc);
            set_check_enforced_in_candidate::<false>(table, name, enforced)?;
            table.build_column_index();
            table.bump_schema_version();
        }
        self.note_catalog_table_key(table_key)?;
        self.commit_ddl_catalog_txn()
    }

    pub(crate) fn acquire_alter_execution_mdl(
        &self,
        owner: u64,
        table_name: &str,
        mode: MdlLockType,
        timeout: Duration,
    ) -> Result<(), QueryError> {
        self.acquire_alter_execution_mdl_duration(
            owner,
            table_name,
            mode,
            timeout,
            MdlDuration::Statement,
        )
    }

    fn acquire_alter_execution_mdl_duration(
        &self,
        owner: u64,
        table_name: &str,
        mode: MdlLockType,
        timeout: Duration,
        duration: MdlDuration,
    ) -> Result<(), QueryError> {
        self.acquire_alter_execution_mdl_checked(owner, table_name, mode, timeout, duration, || {
            Ok(())
        })
    }

    fn acquire_alter_execution_mdl_checked(
        &self,
        owner: u64,
        table_name: &str,
        mode: MdlLockType,
        timeout: Duration,
        duration: MdlDuration,
        check: impl Fn() -> Result<(), QueryError>,
    ) -> Result<(), QueryError> {
        let (database, table) = split_table_name(DEFAULT_DATABASE, table_name);
        let started = Instant::now();
        let mut stage = None;
        let result = (|| loop {
            check()?;
            self.ensure_operational()?;
            crate::engine::cancel::check_cancelled()?;
            match self.acquire_mdl(
                owner,
                Some(owner),
                duration,
                vec![MdlRequest {
                    key: MdlObjectKey::table(&database, &table),
                    lock_type: mode,
                }],
                "ddl/alter/execution-plan",
            ) {
                Ok(()) => return Ok(()),
                Err(err) if is_lock_wait_pending_error(&err) => {
                    stage.get_or_insert_with(|| {
                        crate::engine::cancel::enter_process_stage(
                            crate::engine::session_registry::ProcessState::WaitingTableMetadataLock,
                        )
                    });
                    if started.elapsed() >= timeout {
                        return Err(error(
                            crate::error::catalog::ER_LOCK_WAIT_TIMEOUT,
                            "ALTER timed out waiting for metadata lock",
                        ));
                    }
                    self.wait_for_ddl_signal(timeout.saturating_sub(started.elapsed()))?;
                }
                Err(err) => return Err(err),
            }
        })();
        if result.is_err() {
            self.mdl.clear_pending_for_owner(owner);
        }
        result
    }

    pub(crate) fn alter_drop_column_rebuilds_index(
        &self,
        table_name: &str,
        column: &str,
    ) -> Result<bool, QueryError> {
        let table = self.table(table_name)?;
        Ok(!plan_index_cascade_for_drop_column(&table, column)?
            .1
            .is_empty())
    }

    pub(crate) fn alter_action_validation_only(
        &self,
        table_name: &str,
        action: &crate::parser::AlterAction,
    ) -> Result<bool, QueryError> {
        use crate::parser::AlterAction as A;
        let (source, target, name) = match action {
            A::ModifyColumn(column) => {
                let (source, target, _) =
                    self.prepare_modify_column_candidate(table_name, column.clone())?;
                (source, target, column.name.as_str())
            }
            A::ChangeColumn { old_name, new_def } => {
                let (source, target, _) =
                    self.prepare_change_column_candidate(table_name, old_name, new_def.clone())?;
                (source, target, old_name.as_str())
            }
            _ => return Ok(false),
        };
        let pos = source.column_index(name)?;
        Ok(column_change_is_validation_only(
            &source.columns[pos],
            &target.columns[pos],
        ))
    }

    fn validate_column_candidate(
        &self,
        table_name: &str,
        source: Arc<TableSchema>,
        mut candidate: TableSchema,
        owner: Option<u64>,
    ) -> Result<(), QueryError> {
        let txn = self.begin_read_only_txn()?;
        let _txn = OnlineIndexTxnGuard {
            db: self,
            txn_id: txn.txn_id,
        };
        let mut ids = source.storage_ids();
        ids.push(source.id);
        ids.sort_unstable();
        ids.dedup();
        for id in &ids {
            if owner.is_some() {
                self.acquire_table_shared(txn.txn_id, *id)?;
            } else {
                self.acquire_table_exclusive(txn.txn_id, *id)?;
            }
        }
        #[cfg(test)]
        self.pause_ddl_backfill_for_test();
        let columns = source
            .columns
            .iter()
            .zip(&candidate.columns)
            .enumerate()
            .filter_map(|(pos, (old, new))| {
                column_change_requires_validation(old, new).then_some(pos)
            })
            .collect::<Vec<_>>();
        // Decode only constrained columns and visit one row at a time; unrelated
        // large LOBs must not turn a NOT NULL check into a full-row materialization.
        self.scan_rows_columns_streaming(table_name, txn, &columns, |values| {
            crate::engine::cancel::check_cancelled()?;
            for (value, pos) in values.iter().zip(&columns) {
                let column = &candidate.columns[*pos];
                if !column.nullable && matches!(value, Value::Null) {
                    return Err(crate::error::messages::bad_null(&column.name));
                }
                coerce_column_value_for_schema(value.clone(), column)?;
            }
            Ok(())
        })?;
        if let Some(owner) = owner {
            // Validation preserves physical slots/encoding. Exclude writers
            // through the final catalog batch commit without draining readers.
            self.acquire_alter_execution_mdl(
                owner,
                table_name,
                MdlLockType::SharedNoWrite,
                crate::storage::txn::metadata_wait_timeout(),
            )?;
        }
        let actual = self.table(table_name)?;
        if actual.id != source.id || actual.schema_version != source.schema_version {
            return Err(error(
                crate::error::catalog::ER_TABLE_DEF_CHANGED,
                "table changed during column validation",
            ));
        }
        let boundary = self.next_scn_atomic.load(Ordering::Acquire);
        for (old, new) in source.columns.iter().zip(&mut candidate.columns) {
            if old.nullable && !new.nullable {
                new.not_null_from_scn = boundary;
                self.read_definition_highwater
                    .fetch_max(boundary, Ordering::Release);
            }
        }
        self.replace_table_schema_in_catalog(table_name, candidate)
    }

    pub fn alter_action_requires_copy(
        &self,
        table_name: &str,
        action: &crate::parser::AlterAction,
    ) -> Result<bool, QueryError> {
        use crate::parser::AlterAction as A;
        // S4: enumerate every action explicitly so a new variant that
        // needs COPY cannot silently take the default `false` branch
        // and get mis-routed as INSTANT. The `_ => Ok(false)` pattern
        // this replaces would have made e.g. a future "reorder heap"
        // action look INSTANT to the algorithm validator and the atomic
        // rollback gate, which then wouldn't guard against corruption.
        match action {
            A::ModifyColumn(column) => {
                let (_, _, copy_required) =
                    self.prepare_modify_column_candidate(table_name, column.clone())?;
                Ok(copy_required)
            }
            A::ChangeColumn { old_name, new_def } => {
                let (_, _, copy_required) =
                    self.prepare_change_column_candidate(table_name, old_name, new_def.clone())?;
                Ok(copy_required)
            }
            A::AddColumn(column) => Ok(column.generated.is_some()
                || column.checks.iter().any(|check| check.enforced)
                || column.default.as_ref().is_some_and(|expr| {
                    crate::parser::visit::try_for_each_expr(expr, |node| {
                        if matches!(node, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
                            Err(())
                        } else {
                            Ok(true)
                        }
                    })
                    .is_err()
                })),
            // These actions do not rewrite heap rows (some still scan/build indexes).
            A::MoveColumn { .. }
            | A::SetColumnComment { .. }
            | A::SetColumnDefault { .. }
            | A::SetColumnVisibility { .. }
            | A::SetTableOptions { .. }
            | A::SetStorageProperties { .. }
            | A::SetAutoIncrement(_)
            | A::AddIndex(_)
            | A::AddCheck(_)
            | A::AddForeignKey(_)
            | A::DropColumn(_)
            | A::DropIndex(_)
            | A::DropPrimaryKey
            | A::DropCheck { .. }
            | A::DropForeignKey { .. }
            | A::AlterIndexVisibility { .. }
            | A::AlterCheck { .. }
            | A::RenameColumn { .. }
            | A::RenameIndex { .. }
            | A::RenameTable { .. }
            | A::Noop { .. } => Ok(false),
            A::Rebuild | A::ConvertToCharset { .. } => Ok(true),
        }
    }

    pub(crate) fn alter_column_positioned_metadata(
        &self,
        table_name: &str,
        action: &crate::parser::AlterAction,
        position: &crate::parser::ColumnPosition,
        owner: u64,
    ) -> Result<(), QueryError> {
        use crate::parser::AlterAction as A;
        let (source, mut candidate, copy_required, name) = match action {
            A::AddColumn(column) => {
                if self.alter_action_requires_copy(table_name, action)? {
                    return Err(error(
                        crate::error::catalog::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON,
                        "positioned metadata ADD requires an unchanged heap generation",
                    ));
                }
                return self.add_column_positioned(table_name, column.clone(), Some(position));
            }
            A::ModifyColumn(column) => {
                let (source, candidate, copy) =
                    self.prepare_modify_column_candidate(table_name, column.clone())?;
                (source, candidate, copy, column.name.as_str())
            }
            A::ChangeColumn { old_name, new_def }
                if old_name.eq_ignore_ascii_case(&new_def.name) =>
            {
                let (source, candidate, copy) =
                    self.prepare_change_column_candidate(table_name, old_name, new_def.clone())?;
                (source, candidate, copy, new_def.name.as_str())
            }
            _ => {
                return Err(error(
                    crate::error::catalog::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON,
                    "unsupported positioned metadata column operation",
                ))
            }
        };
        if source
            .columns
            .iter()
            .zip(&candidate.columns)
            .any(|(old, new)| column_change_requires_physical_copy(old, new))
        {
            return Err(error(
                crate::error::catalog::ER_ALTER_OPERATION_NOT_SUPPORTED_REASON,
                "positioned metadata MODIFY requires an unchanged heap generation",
            ));
        }
        move_column_in_candidate(&mut candidate, name, position)?;
        candidate.build_column_index();
        if copy_required {
            self.validate_column_candidate(table_name, source, candidate, Some(owner))
        } else {
            self.replace_table_schema_in_catalog(table_name, candidate)
        }
    }

    pub fn modify_column_with_policy_for_session(
        self: &Arc<Self>,
        table_name: &str,
        column: ColumnDef,
        strict: bool,
        lock: crate::parser::AlterLock,
        statement_mdl_owner: u64,
    ) -> Result<Vec<crate::engine::api::SqlWarning>, QueryError> {
        let (old_table, candidate, copy_required) =
            self.prepare_modify_column_candidate(table_name, column)?;
        self.apply_column_candidate_for_session(
            table_name,
            old_table,
            candidate,
            copy_required,
            strict,
            lock,
            statement_mdl_owner,
        )
    }

    pub fn change_column_with_policy_for_session(
        self: &Arc<Self>,
        table_name: &str,
        old_name: &str,
        column: ColumnDef,
        strict: bool,
        lock: crate::parser::AlterLock,
        statement_mdl_owner: u64,
    ) -> Result<Vec<crate::engine::api::SqlWarning>, QueryError> {
        let (old_table, candidate, copy_required) =
            self.prepare_change_column_candidate(table_name, old_name, column)?;
        self.apply_column_candidate_for_session(
            table_name,
            old_table,
            candidate,
            copy_required,
            strict,
            lock,
            statement_mdl_owner,
        )
    }

    fn column_candidate_needs_validation_only(
        old: &TableSchema,
        new: &TableSchema,
        copy_required: bool,
        strict: bool,
    ) -> bool {
        strict
            && copy_required
            && old
                .columns
                .iter()
                .zip(&new.columns)
                .all(|(old, new)| !column_change_requires_physical_copy(old, new))
    }

    fn apply_column_candidate_policy(
        &self,
        table_name: &str,
        old_table: Arc<TableSchema>,
        candidate: TableSchema,
        copy_required: bool,
        strict: bool,
        statement_mdl_owner: Option<u64>,
    ) -> Result<Vec<crate::engine::api::SqlWarning>, QueryError> {
        if Self::column_candidate_needs_validation_only(
            &old_table,
            &candidate,
            copy_required,
            strict,
        ) {
            self.validate_column_candidate(table_name, old_table, candidate, statement_mdl_owner)?;
        } else if copy_required {
            return self.rewrite_table_for_alter_copy(table_name, old_table, candidate, strict);
        } else {
            self.replace_table_schema_in_catalog(table_name, candidate)?;
        }
        Ok(Vec::new())
    }

    fn apply_column_candidate_for_session(
        self: &Arc<Self>,
        table_name: &str,
        old_table: Arc<TableSchema>,
        candidate: TableSchema,
        copy_required: bool,
        strict: bool,
        lock: crate::parser::AlterLock,
        statement_mdl_owner: u64,
    ) -> Result<Vec<crate::engine::api::SqlWarning>, QueryError> {
        if copy_required
            && !Self::column_candidate_needs_validation_only(
                &old_table,
                &candidate,
                copy_required,
                strict,
            )
            && !old_table.is_partitioned()
            && self.should_run_async_alter_copy(strict, lock)
        {
            self.spawn_alter_copy_job(
                table_name.to_string(),
                old_table,
                candidate,
                strict,
                lock,
                statement_mdl_owner,
            )?;
            Ok(Vec::new())
        } else {
            self.apply_column_candidate_policy(
                table_name,
                old_table,
                candidate,
                copy_required,
                strict,
                Some(statement_mdl_owner),
            )
        }
    }

    fn should_run_async_alter_copy(&self, strict: bool, lock: crate::parser::AlterLock) -> bool {
        self.async_ddl_enabled()
            && strict
            && matches!(
                lock,
                crate::parser::AlterLock::None | crate::parser::AlterLock::Shared
            )
    }

    /// Plan a supported compound physical ALTER against one immutable schema
    /// and execute exactly one build-then-swap COPY. `None` means the action
    /// mix has no atomic candidate implementation; SQL must reject it before
    /// applying any action rather than falling back to sequential mutations.
    pub fn rewrite_compound_alter_once(
        &self,
        table_name: &str,
        actions: &[crate::parser::AlterAction],
        mut added_indexes: Vec<IndexSchema>,
        strict: bool,
        online_owner: Option<(u64, crate::parser::AlterLock)>,
    ) -> Result<Option<Vec<crate::engine::api::SqlWarning>>, QueryError> {
        use crate::parser::AlterAction as A;
        if actions.iter().any(|action| {
            !matches!(
                action,
                A::Rebuild
                    | A::AddColumn(_)
                    | A::DropColumn(_)
                    | A::ModifyColumn(_)
                    | A::ChangeColumn { .. }
                    | A::AddIndex(_)
                    | A::DropIndex(_)
                    | A::DropPrimaryKey
                    | A::ConvertToCharset { .. }
                    | A::MoveColumn { .. }
            )
        }) {
            return Ok(None);
        }
        let old_table = self.table(table_name)?;
        let mut candidate = old_table.as_ref().clone();
        let mut changes_row_shape = false;
        for action in actions {
            match action {
                A::Rebuild => {}
                A::AddColumn(column) => {
                    if candidate
                        .iter_visible_columns()
                        .any(|(_, existing)| existing.name.eq_ignore_ascii_case(&column.name))
                    {
                        return Err(error(
                            crate::error::catalog::ER_DUP_FIELDNAME,
                            format!("duplicate column '{}'", column.name),
                        ));
                    }
                    let mut schema = column_def_to_schema(column.clone())?;
                    apply_column_charset_defaults(
                        &mut schema,
                        &candidate.charset,
                        &candidate.collation,
                    )?;
                    if schema.default.is_none() && schema.default_expr.is_some() {
                        let frozen = crate::engine::expr::resolve_column_default_value(&schema);
                        if !matches!(frozen, Value::Null) {
                            schema.default = Some(frozen);
                        }
                    }
                    schema.column_id = candidate.allocate_column_id();
                    schema.physical_pos = candidate.next_physical_pos();
                    schema.logical_pos = candidate.columns.len() as u32;
                    schema.ordinal_position = schema.logical_pos.saturating_add(1);
                    candidate.columns.push(schema);
                    candidate.build_column_index();
                    changes_row_shape = true;
                }
                A::DropColumn(column_name) => {
                    if candidate.visible_column_count() <= 1 {
                        return Err(error(
                            crate::error::catalog::ER_CANT_REMOVE_ALL_FIELDS,
                            "cannot drop the only column in a table",
                        ));
                    }
                    reject_column_referenced_by_check(&candidate, column_name)?;
                    reject_column_referenced_by_generated(&candidate, column_name)?;
                    reject_column_referenced_by_partitioning(&candidate, column_name)?;
                    self.fk_reject_column_referenced(&candidate, column_name)?;
                    let (drop_names, rebuilt) =
                        plan_index_cascade_for_drop_column(&candidate, column_name)?;
                    candidate
                        .indexes
                        .retain(|index| !drop_names.contains(&index.name));
                    for replacement in rebuilt {
                        if let Some(index) = candidate
                            .indexes
                            .iter_mut()
                            .find(|index| index.index_id == replacement.index_id)
                        {
                            *index = replacement;
                        }
                    }
                    let position = candidate.column_index(column_name)?;
                    candidate.columns[position].state = ColumnState::Dropped;
                    changes_row_shape = true;
                }
                A::ModifyColumn(column) => {
                    let mut schema = column_def_to_schema(column.clone())?;
                    let position = candidate.column_index(&schema.name)?;
                    preserve_primary_key_not_null(&candidate, position, &mut schema);
                    apply_column_charset_for_alter(
                        &mut schema,
                        &candidate.columns[position],
                        &candidate.charset,
                        &candidate.collation,
                    )?;
                    self.reject_generated_storedness_change(&candidate, position, &schema)?;
                    self.reject_functional_index_type_change(&candidate, position, &schema)?;
                    self.reject_foreign_key_column_change(&candidate, position, &schema)?;
                    changes_row_shape |=
                        column_change_requires_copy(&candidate.columns[position], &schema);
                    apply_column_def_preserving_identity(&mut candidate.columns[position], schema);
                }
                A::ChangeColumn { old_name, new_def } => {
                    let position = candidate.column_index(old_name)?;
                    let old_column = candidate.columns[position].clone();
                    let mut schema = column_def_to_schema(new_def.clone())?;
                    preserve_primary_key_not_null(&candidate, position, &mut schema);
                    apply_column_charset_for_alter(
                        &mut schema,
                        &old_column,
                        &candidate.charset,
                        &candidate.collation,
                    )?;
                    if !old_name.eq_ignore_ascii_case(&schema.name)
                        && candidate.column_index(&schema.name).is_ok()
                    {
                        return Err(error(
                            crate::error::catalog::ER_DUP_FIELDNAME,
                            format!("duplicate column '{}'", schema.name),
                        ));
                    }
                    self.reject_generated_storedness_change(&candidate, position, &schema)?;
                    self.reject_foreign_key_column_change(&candidate, position, &schema)?;
                    if old_name.eq_ignore_ascii_case(&schema.name) {
                        self.reject_functional_index_type_change(&candidate, position, &schema)?;
                    } else {
                        self.reject_functional_index_rename(&candidate, old_name)?;
                    }
                    changes_row_shape |= column_change_requires_copy(&old_column, &schema);
                    let new_name = schema.name.clone();
                    apply_column_def_preserving_identity(&mut candidate.columns[position], schema);
                    if !old_name.eq_ignore_ascii_case(&new_name) {
                        for key in &mut candidate.primary_key {
                            if key.eq_ignore_ascii_case(old_name) {
                                *key = new_name.clone();
                            }
                        }
                        for index in &mut candidate.indexes {
                            for column in
                                index.columns.iter_mut().chain(&mut index.included_columns)
                            {
                                if column.eq_ignore_ascii_case(old_name) {
                                    *column = new_name.clone();
                                }
                            }
                        }
                        rename_check_constraint_column_references(
                            &mut candidate,
                            old_name,
                            &new_name,
                        )?;
                        rename_generated_column_references(&mut candidate, old_name, &new_name)?;
                        rename_partition_column_references(&mut candidate, old_name, &new_name)?;
                    }
                }
                A::MoveColumn { column, position } => {
                    move_column_in_candidate(&mut candidate, column, position)?;
                }
                A::AddIndex(_) => {
                    let mut index = added_indexes.remove(0);
                    if candidate
                        .indexes
                        .iter()
                        .any(|existing| existing.name.eq_ignore_ascii_case(&index.name))
                    {
                        return Err(error(
                            crate::error::catalog::ER_DUP_KEYNAME,
                            format!("duplicate index '{}'", index.name),
                        ));
                    }
                    if index.primary {
                        if !candidate.primary_key.is_empty() {
                            return Err(error(
                                crate::error::catalog::ER_MULTIPLE_PRI_KEY,
                                "Multiple primary key defined",
                            ));
                        }
                        candidate.primary_key = index.columns.clone();
                        for name in &index.columns {
                            let pos = candidate.column_index(name)?;
                            candidate.columns[pos].nullable = false;
                        }
                    }
                    index.state = IndexState::Public;
                    candidate.indexes.push(index);
                }
                A::DropPrimaryKey => {
                    self.fk_reject_index_drop(&candidate, "PRIMARY")?;
                    if candidate.primary_key.is_empty() {
                        return Err(error(
                            crate::error::catalog::ER_CANT_DROP_FIELD_OR_KEY,
                            "Can't DROP PRIMARY; check that column/key exists",
                        ));
                    }
                    candidate.primary_key.clear();
                    candidate.indexes.retain(|index| !index.primary);
                }
                A::DropIndex(name) => {
                    self.fk_reject_index_drop(&candidate, name)?;
                    let before = candidate.indexes.len();
                    candidate
                        .indexes
                        .retain(|index| !index.name.eq_ignore_ascii_case(name));
                    if before == candidate.indexes.len() {
                        return Err(error(
                            crate::error::catalog::ER_CANT_DROP_FIELD_OR_KEY,
                            format!("index '{name}' does not exist"),
                        ));
                    }
                }
                A::ConvertToCharset { charset, collation } => {
                    let info = crate::charset::resolve_charset(charset)?;
                    let collation = match collation {
                        Some(collation) => {
                            let coll = crate::charset::resolve_collation(collation)?;
                            crate::charset::validate_charset_collation_pair(info.name, coll.name)?;
                            coll.name.to_string()
                        }
                        None => {
                            crate::charset::default_collation_for_charset(info.name)?.to_string()
                        }
                    };
                    candidate.charset = info.name.to_string();
                    candidate.collation = collation.clone();
                    for column in &mut candidate.columns {
                        if matches!(
                            column.data_type,
                            DataTypeKind::Char { .. }
                                | DataTypeKind::VarChar { .. }
                                | DataTypeKind::Text { .. }
                                | DataTypeKind::Enum { .. }
                                | DataTypeKind::Set { .. }
                        ) {
                            column.charset = Some(info.name.to_string());
                            column.collation = Some(collation.clone());
                        }
                    }
                }
                _ => unreachable!("compound action gate"),
            }
            candidate.build_column_index();
            let checks = match action {
                A::AddColumn(column) | A::ModifyColumn(column) => &column.checks,
                A::ChangeColumn { new_def, .. } => &new_def.checks,
                _ => continue,
            };
            if !checks.is_empty() {
                let key = table_key(&candidate.database, &candidate.name);
                let names = {
                    let catalog = self.catalog_read()?;
                    check_constraint_names_in_schema(&catalog, &candidate.database, Some(&key))
                };
                let mut checks = build_check_constraints_for_table(
                    &candidate,
                    checks.clone(),
                    Some(&candidate.check_constraints),
                    None,
                )?;
                ensure_check_constraint_names_available(&names, &checks)?;
                candidate.check_constraints.append(&mut checks);
            }
        }
        if changes_row_shape {
            candidate.publish_new_row_layout();
        }
        validate_table_column_identifiers(&candidate)?;
        validate_and_prepare_generated_columns(&mut candidate)?;
        validate_auto_increment_schema(&candidate)?;
        ensure_table_keeps_visible_column(&candidate)?;
        candidate.compile_pending_index_expressions()?;
        validate_table_index_count(
            candidate
                .indexes
                .iter()
                .filter(|index| !index.primary)
                .count(),
        )?;
        validate_bit_widths(&candidate)?;
        validate_mysql80_partition_compat(&candidate, false)?;
        candidate.build_column_index();
        candidate.bump_schema_version();
        if let Some((owner, lock)) = online_owner {
            return self
                .run_online_type_copy(table_name, old_table, candidate, strict, owner, lock)
                .map(Some);
        }
        self.rewrite_table_for_alter_copy(table_name, old_table, candidate, strict)
            .map(Some)
    }

    fn reject_vector_index_column_change(
        &self,
        table: &TableSchema,
        pos: usize,
        new: &ColumnSchema,
    ) -> Result<(), QueryError> {
        if table.indexes.iter().any(|index| {
            index.is_vector()
                && index
                    .columns
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case(&table.columns[pos].name))
        }) && (new.nullable || !matches!(new.data_type, DataTypeKind::Vector { .. }))
        {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "a vector index column must remain VECTOR and NOT NULL; drop the index first",
            ));
        }
        Ok(())
    }

    fn table_for_column_change(&self, table_name: &str) -> Result<Arc<TableSchema>, QueryError> {
        let (database, table_only) = split_table_name(DEFAULT_DATABASE, table_name);
        check_table_ddl_writable(&database)?;
        let key = table_key(&database, &table_only);
        let table = self
            .catalog_read()?
            .tables
            .get(&key)
            .cloned()
            .ok_or_else(|| ddl_table_not_found(&table_name))?;
        Ok(table)
    }

    fn prepare_modify_column_candidate(
        &self,
        table_name: &str,
        column: ColumnDef,
    ) -> Result<(Arc<TableSchema>, TableSchema, bool), QueryError> {
        let table = self.table_for_column_change(table_name)?;
        let mut new_schema = column_def_to_schema(column)?;
        let pos = table
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&new_schema.name))
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_BAD_FIELD_ERROR,
                    format!("unknown column '{}'", new_schema.name),
                )
            })?;
        apply_column_charset_for_alter(
            &mut new_schema,
            &table.columns[pos],
            &table.charset,
            &table.collation,
        )?;
        preserve_primary_key_not_null(&table, pos, &mut new_schema);
        self.reject_generated_storedness_change(&table, pos, &new_schema)?;
        self.reject_functional_index_type_change(&table, pos, &new_schema)?;
        self.reject_foreign_key_column_change(&table, pos, &new_schema)?;
        self.reject_vector_index_column_change(&table, pos, &new_schema)?;
        let copy_required = column_change_requires_copy(&table.columns[pos], &new_schema);
        let mut candidate = table.as_ref().clone();
        apply_column_def_preserving_identity(&mut candidate.columns[pos], new_schema.clone());
        finish_column_change_candidate(&mut candidate, copy_required, new_schema.auto_increment)?;
        Ok((table, candidate, copy_required))
    }

    fn prepare_change_column_candidate(
        &self,
        table_name: &str,
        old_name: &str,
        column: ColumnDef,
    ) -> Result<(Arc<TableSchema>, TableSchema, bool), QueryError> {
        let table = self.table_for_column_change(table_name)?;
        let mut new_schema = column_def_to_schema(column)?;
        let pos = table
            .columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(old_name))
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_BAD_FIELD_ERROR,
                    format!("unknown column '{old_name}'"),
                )
            })?;
        apply_column_charset_for_alter(
            &mut new_schema,
            &table.columns[pos],
            &table.charset,
            &table.collation,
        )?;
        preserve_primary_key_not_null(&table, pos, &mut new_schema);
        self.reject_generated_storedness_change(&table, pos, &new_schema)?;
        self.reject_foreign_key_column_change(&table, pos, &new_schema)?;
        self.reject_vector_index_column_change(&table, pos, &new_schema)?;
        let old_col_name = table.columns[pos].name.clone();
        if !old_col_name.eq_ignore_ascii_case(&new_schema.name)
            && table
                .columns
                .iter()
                .any(|c| c.name.eq_ignore_ascii_case(&new_schema.name))
        {
            return Err(error(
                crate::error::catalog::ER_DUP_FIELDNAME,
                format!("duplicate column '{}'", new_schema.name),
            ));
        }
        if !new_schema.name.eq_ignore_ascii_case(&old_col_name) {
            self.reject_functional_index_rename(&table, &old_col_name)?;
        } else {
            self.reject_functional_index_type_change(&table, pos, &new_schema)?;
        }
        let copy_required = column_change_requires_copy(&table.columns[pos], &new_schema);
        let mut candidate = table.as_ref().clone();
        apply_column_def_preserving_identity(&mut candidate.columns[pos], new_schema.clone());
        let new_col_name = candidate.columns[pos].name.clone();
        if !old_col_name.eq_ignore_ascii_case(&new_col_name) {
            for pk in &mut candidate.primary_key {
                if pk.eq_ignore_ascii_case(&old_col_name) {
                    *pk = new_col_name.clone();
                }
            }
            for index in &mut candidate.indexes {
                for col in index.columns.iter_mut().chain(&mut index.included_columns) {
                    if col.eq_ignore_ascii_case(&old_col_name) {
                        *col = new_col_name.clone();
                    }
                }
            }
            rename_check_constraint_column_references(
                &mut candidate,
                &old_col_name,
                &new_col_name,
            )?;
            rename_generated_column_references(&mut candidate, &old_col_name, &new_col_name)?;
            // P1-11 — same partition-column propagation as rename_column.
            rename_partition_column_references(&mut candidate, &old_col_name, &new_col_name)?;
        }
        finish_column_change_candidate(&mut candidate, copy_required, new_schema.auto_increment)?;
        Ok((table, candidate, copy_required))
    }

    /// MySQL forbids flipping a generated column's storedness in place
    /// (`STORED` ↔ `VIRTUAL`) via `MODIFY`/`CHANGE`, rejecting with
    /// ER_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN (3106). Without this guard
    /// mydb silently accepted the flip: `column_change_requires_copy` merely
    /// treats the differing `generated_stored` as "needs a COPY rewrite" and
    /// rebuilds the table, converting the column's kind behind the user's
    /// back. We only reject when both the old and new definitions are
    /// generated and the storedness actually differs; converting to/from a
    /// non-generated column is governed by the surrounding validation paths.
    fn reject_generated_storedness_change(
        &self,
        table: &TableSchema,
        pos: usize,
        new_schema: &ColumnSchema,
    ) -> Result<(), QueryError> {
        let old = &table.columns[pos];
        if old.generated_expr.is_some()
            && new_schema.generated_expr.is_some()
            && old.generated_stored != new_schema.generated_stored
        {
            return Err(error(
                crate::error::catalog::ER_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN,
                "'Changing the STORED status' is not supported for generated columns.".to_string(),
            ));
        }
        Ok(())
    }

    fn reject_functional_index_type_change(
        &self,
        table: &TableSchema,
        pos: usize,
        new_schema: &ColumnSchema,
    ) -> Result<(), QueryError> {
        let old = &table.columns[pos];
        let type_changed =
            old.data_type != new_schema.data_type || old.unsigned != new_schema.unsigned;
        if !type_changed {
            return Ok(());
        }
        let col_lower = old.name.to_ascii_lowercase();
        if table.indexes.iter().any(|idx| {
            idx.key_expressions
                .iter()
                .flatten()
                .any(|expr| expr.referenced_columns.iter().any(|c| c == &col_lower))
        }) {
            return Err(error(crate::error::catalog::ER_DEPENDENT_BY_PARTITION_FUNC,
                format!(
                    "column '{}' has a functional index dependency; cannot change its declared type",
                    old.name
                ),
            ));
        }
        Ok(())
    }

    fn reject_functional_index_rename(
        &self,
        table: &TableSchema,
        old_col_name: &str,
    ) -> Result<(), QueryError> {
        let old_lower = old_col_name.to_ascii_lowercase();
        if table.indexes.iter().any(|idx| {
            idx.key_expressions
                .iter()
                .flatten()
                .any(|expr| expr.referenced_columns.iter().any(|c| c == &old_lower))
        }) {
            return Err(error(crate::error::catalog::ER_DEPENDENT_BY_PARTITION_FUNC,
                format!(
                    "column '{old_col_name}' has a functional index dependency and cannot be renamed",
                ),
            ));
        }
        Ok(())
    }

    fn reject_foreign_key_column_change(
        &self,
        table: &TableSchema,
        pos: usize,
        new_schema: &ColumnSchema,
    ) -> Result<(), QueryError> {
        let old = &table.columns[pos];
        let type_changed =
            old.data_type != new_schema.data_type || old.unsigned != new_schema.unsigned;
        let renamed = !old.name.eq_ignore_ascii_case(&new_schema.name);
        if !type_changed && !renamed {
            return Ok(());
        }
        for fk in &table.foreign_keys {
            if fk.references_column(&old.name) {
                return Err(error(
                    crate::error::catalog::ER_FK_COLUMN_CANNOT_CHANGE,
                    format!(
                        "Cannot change column '{}': used in a foreign key constraint '{}'",
                        old.name, fk.name
                    ),
                ));
            }
        }
        for inbound in &table.fk_referenced_by {
            if inbound.referenced_col_idx.iter().any(|&ri| ri == pos) {
                return Err(error(crate::error::catalog::ER_FK_COLUMN_CANNOT_CHANGE_CHILD,
                    format!(
                        "Cannot change column '{}': used in a foreign key constraint '{}' of table '{}'",
                        old.name, inbound.fk_name, inbound.child_table_key
                    ),
                ));
            }
        }
        Ok(())
    }

    fn replace_table_schema_in_catalog(
        &self,
        table_name: &str,
        candidate: TableSchema,
    ) -> Result<(), QueryError> {
        // M7 — the unified commit gate for ALTER: validate that the altered row
        // still fits one page (intercepts changing/adding an over-page `VECTOR(n)` etc.), the same gate as CREATE, with a clear error before commit.
        validate_bit_widths(&candidate)?;
        validate_table_column_identifiers(&candidate)?;
        validate_row_fits_page(&candidate)?;
        let (database, table_only) = split_table_name(DEFAULT_DATABASE, table_name);
        let key = table_key(&database, &table_only);
        let table_id = candidate.id;
        {
            let mut catalog = self.catalog_write()?;
            let table_arc = catalog
                .tables
                .get_mut(&key)
                .ok_or_else(|| ddl_table_not_found(&table_name))?;
            *Arc::make_mut(table_arc) = candidate;
        }
        self.refresh_heap_codec(table_id)?;
        self.note_catalog_table(table_id);
        self.commit_ddl_catalog_txn()
    }

    fn alter_copy_job_path(&self, table_id: u64) -> PathBuf {
        alter_copy_job_dir(&self.data_dir).join(format!("{table_id}.job"))
    }

    #[cfg(test)]
    fn begin_alter_copy_crash_marker(
        &self,
        old_table: &TableSchema,
        candidate: &TableSchema,
    ) -> Result<(), QueryError> {
        if self.config.durability == DurabilityMode::Memory {
            return Ok(());
        }
        let path = self.alter_copy_job_path(old_table.id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(io_err)?;
        }
        let bytes = format!(
            "version=1\nkind=ALTER_TABLE_COPY\ntable_id={}\ntable_schema={}\ntable_name={}\nold_schema_version={}\nnew_schema_version={}\nstarted_unix_ms={}\n",
            old_table.id,
            old_table.database,
            old_table.name,
            old_table.schema_version,
            candidate.schema_version,
            current_unix_ms(),
        )
        .into_bytes();
        {
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&path)
                .map_err(io_err)?;
            file.write_all(&bytes).map_err(io_err)?;
            file.sync_data().map_err(io_err)?;
        }
        let lsn = self.append_wal(
            txn_id_for_ddl(),
            WalPayload::FileCreate {
                path: self.relative_path(&path),
                after: bytes,
            },
        )?;
        if self.config.durability == DurabilityMode::Strict {
            self.sync_commit_lsn(lsn)?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn finish_alter_copy_crash_marker(&self, table_id: u64) -> Result<(), QueryError> {
        if self.config.durability == DurabilityMode::Memory {
            return Ok(());
        }
        let path = self.alter_copy_job_path(table_id);
        let lsn = self.append_wal(
            txn_id_for_ddl(),
            WalPayload::FileDelete {
                path: self.relative_path(&path),
                before: Vec::new(),
            },
        )?;
        if self.config.durability == DurabilityMode::Strict {
            self.sync_commit_lsn(lsn)?;
        }
        let _ = fs::remove_file(path);
        Ok(())
    }

    fn register_alter_copy_job(&self, table: &TableSchema) -> Arc<AlterCopyJob> {
        self.register_ddl_job(table, "COPY", true)
    }

    fn register_ddl_job(
        &self,
        table: &TableSchema,
        operation: &'static str,
        capture_rows: bool,
    ) -> Arc<AlterCopyJob> {
        self.register_ddl_job_with_subject(table, operation, capture_rows, operation.to_string())
    }

    fn register_ddl_job_with_subject(
        &self,
        table: &TableSchema,
        operation: &'static str,
        capture_rows: bool,
        object_name: String,
    ) -> Arc<AlterCopyJob> {
        let job = Arc::new(AlterCopyJob {
            job_id: self.allocate_nontransaction_id(),
            table_id: table.id,
            database: table.database.clone(),
            table: table.name.clone(),
            operation,
            object_name,
            capture_rows,
            cancel_token: crate::engine::cancel::current_token()
                .unwrap_or_else(crate::engine::cancel::QueryCancelToken::noop)
                .fork_local(),
            progress: Mutex::new(AlterCopyJobProgress {
                state: "COPYING".to_string(),
                copied_rows: 0,
                total_rows: None,
                replay_watermark: None,
                error_message: None,
            }),
            changes: Mutex::new(AlterCopyChangeLog::new(self.data_dir())),
            cancel_requested: AtomicBool::new(false),
            retired_vector_graphs: Mutex::new(Vec::new()),
            retired_files: Mutex::new(Vec::new()),
        });
        if let Ok(mut jobs) = self.alter_copy_jobs.lock() {
            jobs.insert(table.id, Arc::clone(&job));
        }
        job
    }

    fn alter_copy_job_snapshot(job: &AlterCopyJob) -> AlterCopyJobSnapshot {
        let progress = job
            .progress
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        AlterCopyJobSnapshot {
            job_id: job.job_id,
            table_id: job.table_id,
            database: job.database.clone(),
            table: job.table.clone(),
            operation: job.operation,
            object_name: job.object_name.clone(),
            state: progress.state.clone(),
            copied_rows: progress.copied_rows,
            total_rows: progress.total_rows,
            replay_watermark: progress.replay_watermark,
            error_message: progress.error_message.clone(),
            cancel_allowed: matches!(
                progress.state.as_str(),
                "COPYING" | "INDEX_BUILD" | "VALIDATING" | "CATCH_UP"
            ),
        }
    }

    fn unregister_alter_copy_job(&self, table_id: u64, failure: Option<&QueryError>) {
        let job = self
            .alter_copy_jobs
            .lock()
            .ok()
            .and_then(|mut jobs| jobs.remove(&table_id));
        let Some(job) = job else { return };
        {
            let mut progress = job
                .progress
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            if let Some(err) = failure {
                progress.state = if err.is(crate::error::catalog::ER_QUERY_INTERRUPTED) {
                    "CANCELLED".to_string()
                } else {
                    "FAILED".to_string()
                };
                progress.error_message = Some(err.message().to_string());
            } else {
                progress.state = "COMMITTED".to_string();
                progress.error_message = None;
            }
        }
        let mut cleanup_failed = false;
        for path in job
            .retired_files
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .drain(..)
        {
            if cleanup_failed && path.extension().is_some_and(|ext| ext == "job") {
                continue;
            }
            if let Err(err) = fs::remove_file(&path) {
                if err.kind() != std::io::ErrorKind::NotFound {
                    cleanup_failed = true;
                    log_message(format!(
                        "ALTER COPY deferred cleanup {}: {err}",
                        path.display()
                    ));
                }
            }
        }
        let snapshot = Self::alter_copy_job_snapshot(&job);
        let history_bytes = std::mem::size_of::<AlterCopyJobHistoryEntry>()
            .saturating_add(snapshot.database.capacity())
            .saturating_add(snapshot.table.capacity())
            .saturating_add(snapshot.object_name.capacity())
            .saturating_add(snapshot.state.capacity())
            .saturating_add(
                snapshot
                    .error_message
                    .as_ref()
                    .map(String::capacity)
                    .unwrap_or(0),
            );
        if let Some(charge) =
            crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new_optional(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::SystemMetadata,
                history_bytes,
            )
        {
            if let Ok(mut history) = self.alter_copy_job_history.lock() {
                if history.len() == 64 {
                    history.pop_front();
                }
                history.push_back(AlterCopyJobHistoryEntry {
                    snapshot,
                    _memory_charge: charge,
                });
            }
        }
    }

    fn set_alter_copy_job_total(job: &Arc<AlterCopyJob>, total_rows: u64) {
        if let Ok(mut progress) = job.progress.lock() {
            progress.total_rows = Some(total_rows);
        }
    }

    fn set_alter_copy_job_state(job: &Arc<AlterCopyJob>, state: &str) {
        if let Ok(mut progress) = job.progress.lock() {
            progress.state = state.to_string();
        }
    }

    fn set_alter_copy_job_replay_watermark(job: &Arc<AlterCopyJob>, watermark: u64) {
        if let Ok(mut progress) = job.progress.lock() {
            progress.replay_watermark = Some(watermark);
        }
    }

    #[cfg(test)]
    fn advance_alter_copy_job_copied(job: &Arc<AlterCopyJob>) {
        if let Ok(mut progress) = job.progress.lock() {
            progress.copied_rows = progress.copied_rows.saturating_add(1);
        }
    }

    fn advance_alter_copy_job_copied_by(job: &Arc<AlterCopyJob>, rows: u64) {
        if let Ok(mut progress) = job.progress.lock() {
            progress.copied_rows = progress.copied_rows.saturating_add(rows);
        }
    }

    fn active_alter_copy_job(&self, table_id: u64) -> Option<Arc<AlterCopyJob>> {
        match self.alter_copy_jobs.lock() {
            Ok(jobs) => jobs.get(&table_id).cloned(),
            Err(poison) => poison.into_inner().get(&table_id).cloned(),
        }
    }

    pub(crate) fn record_alter_copy_touched_rows<I>(
        &self,
        txn_id: u64,
        table_id: u64,
        storage_id: u64,
        rows: I,
    ) where
        I: IntoIterator<Item = RowId>,
    {
        let Some(job) = self.active_alter_copy_job(table_id) else {
            return;
        };
        if !job.capture_rows {
            return;
        }
        let mut changes = job
            .changes
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        for row_id in rows {
            if changes.record((txn_id, storage_id, row_id)).is_err() {
                changes.invalidated = true;
                break;
            }
        }
    }

    pub fn alter_copy_job_snapshots(&self) -> Vec<AlterCopyJobSnapshot> {
        let jobs = match self.alter_copy_jobs.lock() {
            Ok(jobs) => jobs.values().cloned().collect::<Vec<_>>(),
            Err(poison) => poison.into_inner().values().cloned().collect::<Vec<_>>(),
        };
        let mut snapshots = jobs
            .into_iter()
            .map(|job| Self::alter_copy_job_snapshot(&job))
            .collect::<Vec<_>>();
        if let Ok(history) = self.alter_copy_job_history.lock() {
            snapshots.extend(history.iter().map(|entry| entry.snapshot.clone()));
        }
        snapshots.sort_by_key(|snapshot| snapshot.job_id);
        snapshots
    }

    /// Request cancellation of an online COPY job. Cancellation is accepted
    /// only while the job is still building or catching up; once FINALIZING
    /// begins the catalog/data switch must run to completion.
    pub fn cancel_ddl_job(&self, job_id: u64) -> Result<(), QueryError> {
        let jobs = self
            .alter_copy_jobs
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let job = jobs.values().find(|job| job.job_id == job_id).cloned();
        drop(jobs);
        if let Some(job) = job {
            let mut progress = job
                .progress
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            if progress.state == "FINALIZING" {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    format!("DDL job {job_id} can no longer be cancelled after finalization began"),
                ));
            }
            job.cancel_requested.store(true, Ordering::Release);
            job.cancel_token.cancel_local();
            progress.state = "CANCEL_REQUESTED".to_string();
            self.notify_non_row_lock_waiters();
            return Ok(());
        }
        if self.cancel_partition_ddl_job(job_id)? {
            return Ok(());
        }
        Err(error(
            crate::error::catalog::ER_NO_SUCH_TABLE,
            format!("DDL job {job_id} does not exist"),
        ))
    }

    fn check_alter_copy_cancelled(job: &AlterCopyJob) -> Result<(), QueryError> {
        if job.cancel_requested.load(Ordering::Acquire) {
            Err(error(
                crate::error::catalog::ER_QUERY_INTERRUPTED,
                format!("DDL job {} was cancelled", job.job_id),
            ))
        } else {
            Ok(())
        }
    }

    /// Close cancellation under the same latch used to accept CANCEL DDL JOB.
    /// This gate is mandatory even when there are no tail rows to replay.
    fn enter_alter_copy_finalizing(job: &AlterCopyJob) -> Result<(), QueryError> {
        let mut progress = job.progress.lock().unwrap_or_else(|p| p.into_inner());
        Self::check_alter_copy_cancelled(job)?;
        crate::engine::cancel::check_cancelled()?;
        progress.state = "FINALIZING".to_string();
        Ok(())
    }

    /// Rebuild into a private generation while ordinary DML remains online.
    /// The caller holds statement-duration SharedUpgradable MDL; the shared
    /// online COPY protocol acquires exclusive MDL only at prepare/publish.
    pub(crate) fn optimize_table_online(
        &self,
        table_name: &str,
        owner: u64,
    ) -> Result<(), QueryError> {
        let source = self.table(table_name)?;
        if is_system_database(&source.database) {
            return Err(error(
                crate::error::catalog::ER_DBACCESS_DENIED_ERROR,
                "OPTIMIZE TABLE cannot rebuild system tables",
            ));
        }
        if self.config.durability == DurabilityMode::Memory {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "online OPTIMIZE TABLE requires a durable storage backend",
            ));
        }
        if source.indexes.iter().any(|index| {
            !["BTREE", "FULLTEXT", "SPATIAL"]
                .iter()
                .any(|kind| index.index_type.eq_ignore_ascii_case(kind))
                && !index.is_vector()
        }) {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "online OPTIMIZE TABLE does not support this index type",
            ));
        }
        // FULLTEXT candidates currently carry only RowId, which is local to
        // one partition. Compaction can make previously distinct local ids
        // collide in that query path. Reject before changing any files until
        // the fulltext executor carries partition-qualified document ids.
        if source.is_partitioned() && source.indexes.iter().any(IndexSchema::is_fulltext) {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "online OPTIMIZE TABLE does not support partitioned FULLTEXT indexes",
            ));
        }
        let mut target = source.as_ref().clone();
        target.bump_schema_version();
        // Physical maintenance must never replicate its private row writes.
        let _binlog = crate::binlog::SessionSqlLogBinGuard::new(false);
        self.run_online_type_copy(
            table_name,
            source,
            target,
            true,
            owner,
            crate::parser::AlterLock::None,
        )?;
        let current = self.table(table_name)?;
        // Concurrent replay can free freshly copied LOB chains. Reclaim its
        // free tail too, after releasing publish MDL and honoring shrink GUCs.
        for storage_id in current.storage_ids() {
            self.shrink_table_lobs(storage_id)?;
        }
        // Reopen the already-built paged graphs under their published paths.
        // This also makes maintenance diagnostics immediately reflect the new
        // generation; memory graphs were transferred by the publish step.
        for index in current.indexes.iter().filter(|index| index.is_vector()) {
            for storage_id in current.storage_ids() {
                self.vector_graph_get_or_build_for_partition(&current, index, storage_id)?;
            }
        }
        let key = table_key(&current.database, &current.name);
        self.refresh_table_stats_with_zone_errors(&key, true, true)?;
        self.commit_catalog_txn()
    }

    fn rewrite_table_for_alter_copy(
        &self,
        table_name: &str,
        old_table: Arc<TableSchema>,
        mut candidate: TableSchema,
        strict: bool,
    ) -> Result<Vec<crate::engine::api::SqlWarning>, QueryError> {
        #[cfg(test)]
        if !ddl_copy_streaming_enabled() {
            return self.rewrite_table_for_alter_copy_legacy_oracle(
                table_name, old_table, candidate, strict,
            );
        }
        candidate.reset_row_layouts_for_rewrite();
        candidate.build_column_index();
        let _binlog = crate::binlog::SessionSqlLogBinGuard::new(false);
        if old_table.is_partitioned() {
            let job = self.register_alter_copy_job(&old_table);
            let id = old_table.id;
            let result =
                self.rewrite_partitioned_type_copy(old_table, candidate, strict, job, None, None);
            self.unregister_alter_copy_job(id, result.as_ref().err());
            result
        } else {
            self.rewrite_table_for_alter_copy_streaming_direct(
                table_name, old_table, candidate, strict, None, None,
            )
        }
    }

    // Differential oracle only: production never destructively rewrites a live heap.
    #[cfg(test)]
    fn rewrite_table_for_alter_copy_legacy_oracle(
        &self,
        table_name: &str,
        old_table: Arc<TableSchema>,
        mut candidate: TableSchema,
        strict: bool,
    ) -> Result<Vec<crate::engine::api::SqlWarning>, QueryError> {
        // V4 — a COPY rewrite truncates the heap and re-inserts every row
        // under the current active layout, so all prior (Readable) layouts
        // are provably dead. Collapse `row_layouts` down to just that
        // Active layout here, the single choke point every COPY path
        // funnels through, so accumulated INSTANT layouts never leak into
        // the rewritten table. The active layout id is kept unchanged so
        // the candidate's already-built row codec (and the ids it stamps
        // on the rewritten rows) stay consistent with what the schema
        // persists.
        candidate.reset_row_layouts_for_rewrite();
        // `reset_row_layouts_for_rewrite` mutates the persisted layout set;
        // rebuild the derived codec before a direct shadow writer stamps the
        // active layout id into row payloads.
        candidate.build_column_index();
        // The ALTER TABLE DDL itself will be binlogged by the executor as a
        // QUERY_EVENT; the per-row WRITE_ROWS from re-inserting under the new
        // layout must be suppressed or the replica applies the DDL (creating
        // rows via engine mechanics) *and* then hits duplicate-key on the
        // WRITE_ROWS events (ER_DUP_ENTRY 1062). partition_reorg does the
        // same at src/storage/core/database/partition_reorg.rs:363.
        let _suppress_binlog = crate::binlog::SessionSqlLogBinGuard::new(false);
        if !old_table.is_partitioned()
            && ddl_copy_streaming_enabled()
            && self.config.durability != DurabilityMode::Memory
            && !table_has_externalizable_column(&old_table)
            && !table_has_externalizable_column(&candidate)
            && !candidate.indexes.iter().any(IndexSchema::is_vector)
        {
            return self.rewrite_table_for_alter_copy_streaming_direct(
                table_name, old_table, candidate, strict, None, None,
            );
        }
        let job = self.register_alter_copy_job(&old_table);
        let ddl_txn = self.begin_txn()?;
        let ddl_txn_id = ddl_txn.txn_id;
        // Tracks whether the new schema was durably flushed. Until it is, a
        // failure has changed nothing on disk and the crash marker (written
        // just before the destructive heap rewrite) can be cleaned so it does
        // not brick the next startup. Past the flush the heap is truncated and
        // re-inserted, so any failure leaves a genuine inconsistency and the
        // marker must stay for recovery to detect.
        let schema_committed = std::cell::Cell::new(false);
        let result = (|| {
            Self::set_alter_copy_job_state(&job, "COPYING");
            self.acquire_table_exclusive(ddl_txn_id, old_table.id)?;
            let key = table_key(&candidate.database, &candidate.name);
            // Fail before any destructive work if the new schema can't fit a
            // row on a page (e.g. `MODIFY … VECTOR(over-page-dim)` or a widen
            // pushing the fixed row past ROW_FIT_BUDGET). Without this the
            // rewrite truncates the heap first and only discovers the encoding
            // failure while re-inserting — wiping a non-empty table's data.
            validate_row_fits_page(&candidate)?;
            let source_rows = self.scan_rows(&old_table.qualified_name(), ddl_txn)?;
            Self::set_alter_copy_job_total(&job, source_rows.len() as u64);
            #[cfg(test)]
            self.pause_ddl_backfill_for_test();
            let mut warnings = Vec::new();
            let mut rewritten_rows = Vec::with_capacity(source_rows.len());
            let mut unique_keys = candidate
                .indexes
                .iter()
                .filter(|index| index.unique)
                .map(|index| (index.name.clone(), crate::util::fast_hash::ah_hash_set()))
                .collect::<BTreeMap<_, crate::util::fast_hash::AhHashSet<IndexKey>>>();
            for row in source_rows {
                Self::check_alter_copy_cancelled(&job)?;
                let mut values = row.values;
                // ADD COLUMN rewrite: pre-existing rows are short by the
                // newly appended slot(s). Pad with each new column's
                // catalog DEFAULT — generated slots get a placeholder that
                // `materialize_generated_columns` immediately overwrites
                // with the computed value. A no-op when the candidate has
                // the same column count (MODIFY / CHANGE rewrites).
                while values.len() < candidate.columns.len() {
                    let col = &candidate.columns[values.len()];
                    values.push(crate::engine::executor::insert::column_read_backfill_default(col));
                }
                coerce_row_for_alter_copy(&candidate, &mut values, strict, &mut warnings)?;
                materialize_generated_columns(&candidate, &mut values, None)?;
                self.check_not_null_constraints(&candidate, &values)?;
                self.check_check_constraints(&candidate, &values, None)?;
                for index in candidate.indexes.iter().filter(|index| index.unique) {
                    let key = candidate.index_key(index, &values)?;
                    if key.0.iter().any(|part| matches!(part, KeyPart::Null)) && !index.primary {
                        continue;
                    }
                    let seen = unique_keys
                        .get_mut(&index.name)
                        .expect("unique index batch should exist");
                    if !seen.insert(key) {
                        return Err(error(
                            crate::error::catalog::ER_DUP_ENTRY,
                            format!("duplicate entry for key '{}'", index.name),
                        ));
                    }
                }
                rewritten_rows.push(values);
                Self::advance_alter_copy_job_copied(&job);
            }

            Self::enter_alter_copy_finalizing(&job)?;
            self.begin_alter_copy_crash_marker(&old_table, &candidate)?;
            {
                let mut catalog = self.catalog_write()?;
                let table_arc = catalog
                    .tables
                    .get_mut(&key)
                    .ok_or_else(|| ddl_table_not_found(&table_name))?;
                *Arc::make_mut(table_arc) = candidate.clone();
            }
            self.note_catalog_table(candidate.id);
            self.commit_ddl_catalog_txn()?;
            schema_committed.set(true);

            // Partitioned tables keep one heap (+ indexes/LOB) per
            // partition — wipe every storage id, exactly like TRUNCATE.
            for sid in old_table.storage_ids() {
                let _ = self.with_heap_exclusive(sid, |heap| heap.flush_dirty_pages());
                self.remove_heap_cache_entry(sid)?;
                self.remove_overflow_cache_entry(sid)?;
                self.create_logged_file(txn_id_for_ddl(), self.table_path(sid), Vec::new())?;
                let _ = self.delete_logged_file(txn_id_for_ddl(), self.overflow_path(sid));
                self.forget_zone_map(sid)?;
                let _ = self.delete_logged_file(txn_id_for_ddl(), self.zone_map_file_path(sid));
                for index in &old_table.indexes {
                    self.remove_index_cache_entry(sid, &index.name)?;
                    let _ = self
                        .delete_logged_file(txn_id_for_ddl(), self.index_path(sid, &index.name));
                }
            }
            self.drop_table_vector_graphs(&old_table);

            if !rewritten_rows.is_empty() {
                if candidate.is_partitioned() {
                    // Route every rewritten row to its partition through
                    // the standard partition-aware insert (handles
                    // per-partition heap/index/undo/binlog).
                    self.insert_rows_partitioned_detailed(
                        ddl_txn,
                        &std::sync::Arc::new(candidate.clone()),
                        rewritten_rows,
                    )?;
                } else {
                    self.insert_alter_copy_rewritten_rows(ddl_txn, &candidate, rewritten_rows)?;
                }
            }
            self.commit_txn(ddl_txn_id)?;
            self.mark_table_stats_stale(&key, old_table.stats.row_count.max(1))?;
            self.finish_alter_copy_crash_marker(old_table.id)?;
            Ok(warnings)
        })();
        if result.is_err() {
            let _ = self.rollback_txn(ddl_txn_id);
            if !schema_committed.get() {
                // The rewrite failed before the new schema became durable and
                // before the heap was touched: clear the crash marker so a
                // recoverable early failure (validation, unique-dup, I/O while
                // scanning) does not reject the next startup (errno 1105).
                let _ = self.finish_alter_copy_crash_marker(old_table.id);
            }
        }
        self.unregister_alter_copy_job(old_table.id, result.as_ref().err());
        result
    }

    /// Non-partitioned COPY fast path: stream source pages into a private
    /// generation, bulk-build its indexes, then replace the live files under
    /// the already-held table X lock.  The legacy implementation below remains
    /// the differential oracle and the bounded fallback for off-page/vector
    /// shapes.
    fn rewrite_table_for_alter_copy_streaming_direct(
        &self,
        table_name: &str,
        old_table: Arc<TableSchema>,
        mut candidate: TableSchema,
        strict: bool,
        existing_job: Option<Arc<AlterCopyJob>>,
        online_barrier: Option<(u64, crate::parser::AlterLock)>,
    ) -> Result<Vec<crate::engine::api::SqlWarning>, QueryError> {
        let _copying = crate::engine::cancel::enter_process_stage(
            crate::engine::session_registry::ProcessState::CopyToTmpTable,
        );
        DDL_COPY_TOTAL.fetch_add(1, Ordering::Relaxed);
        let owns_job = existing_job.is_none();
        let job = existing_job.unwrap_or_else(|| self.register_alter_copy_job(&old_table));
        let _job_cancel_scope = crate::engine::cancel::enter(job.cancel_token.clone());
        let online_rows =
            parking_lot::Mutex::new(OnlineCopyRows::new(self.data_dir(), job.job_id)?);
        let mut _final_hold = None;
        // RC writer transactions do not retain undo for the source scan.
        // Register before choosing the writer view, atomically with purge.
        let _source_snapshot = StorageReadSnapshot::new(self);
        let ddl_txn = self.begin_txn()?;
        let ddl_txn_id = ddl_txn.txn_id;
        candidate.row_generation_txn_id = ddl_txn_id;
        let shadow_id = {
            let mut catalog = self.catalog_write()?;
            let id = catalog.next_table_id;
            catalog.next_table_id = catalog.next_table_id.saturating_add(1);
            id
        };
        self.ensure_table_id_highwater(self.current_next_table_id()?)?;
        candidate.active_generation = old_table.active_generation.saturating_add(1).max(1);
        self.register_reorg_storage_definition(&[shadow_id], &candidate);
        let mut shadow_table = candidate.clone();
        shadow_table.id = shadow_id;
        let shadow_heap_path = self.table_path(shadow_id);
        let intent =
            match self.write_alter_copy_intent(&old_table, &candidate, shadow_id, job.job_id) {
                Ok(intent) => intent,
                Err(err) => {
                    let _ = self.rollback_txn(ddl_txn_id);
                    self.unregister_reorg_storage_locs(&[shadow_id]);
                    if owns_job {
                        self.unregister_alter_copy_job(old_table.id, Some(&err));
                    }
                    return Err(err);
                }
            };
        let mut published = false;
        let result = (|| {
            self.acquire_table_exclusive(ddl_txn_id, old_table.id)?;
            if let Some((owner, lock)) = online_barrier {
                // The read view was opened under the prepare MDL barrier.
                // Keep it alive through publish to pin source versions/RowIds.
                self.table_locks_sharded
                    .downgrade_exclusive_to_shared_mode(
                        ddl_txn_id,
                        old_table.id,
                        if lock == crate::parser::AlterLock::Shared {
                            TableLockMode::Shared
                        } else {
                            TableLockMode::IntentShared
                        },
                    )?;
                self.release_mdl_duration(owner, MdlDuration::Transaction);
                self.notify_non_row_lock_waiters();
            }
            validate_row_fits_page(&candidate)?;
            if let Some(parent) = shadow_heap_path.parent() {
                fs::create_dir_all(parent).map_err(io_err)?;
            }
            let _ = fs::remove_file(&shadow_heap_path);
            OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&shadow_heap_path)
                .map_err(io_err)?;
            self.remove_heap_cache_entry(shadow_id)?;
            self.set_cached_heap_codec(shadow_id, candidate.row_codec.clone())?;
            if table_has_externalizable_column(&candidate) {
                self.with_overflow_read(shadow_id, |_| Ok(()))?;
            }

            let page_count = self.with_heap_read(old_table.id, |heap| heap.page_count())?;
            Self::set_alter_copy_job_total(
                &job,
                old_table
                    .stats
                    .row_count
                    .saturating_add_signed(self.table_live_row_delta(old_table.id)),
            );
            #[cfg(test)]
            self.pause_ddl_backfill_for_test();
            let mut warnings = Vec::new();
            let mut copied_rows = 0_u64;
            let build_lsn = self.log.lock().last_lsn();
            let spill_before = spill_bytes_written();
            let index_builders = DdlCopyIndexBuilders::new(&shadow_table)?;
            let index_key_plans = index_builders.key_plans(&shadow_table);
            let index_builders = parking_lot::Mutex::new(index_builders);
            let coercion_mask = candidate
                .columns
                .iter()
                .enumerate()
                .map(|(idx, column)| {
                    old_table.columns.get(idx).is_none_or(|old| {
                        match (serde_json::to_vec(old), serde_json::to_vec(column)) {
                            (Ok(old), Ok(new)) => old != new,
                            _ => true,
                        }
                    })
                })
                .collect::<Vec<_>>();
            let projection = alter_copy_projection(&old_table, &candidate, &coercion_mask);
            let mut index_elapsed_us = 0_u64;
            let external = table_has_externalizable_column(&old_table)
                || table_has_externalizable_column(&candidate);
            let configured_pages = if external { 1 } else { ddl_copy_batch_pages() };
            let mut lo = 0_u64;
            std::thread::scope(|scope| -> Result<(), QueryError> {
                // One ordered heap writer trails the scanner by one batch.
                // This overlaps page encoding/checksumming/pwrite with the
                // next source scan without changing source-page order or the
                // 256-page logical memory bound. Both live batches keep their
                // independent DdlCopy reservations until fan-out completes.
                let mut pending_write: Option<
                    std::thread::ScopedJoinHandle<
                        '_,
                        Result<
                            (
                                u64,
                                u64,
                                crate::engine::executor::mem_budget::ControlledMemoryCharge,
                            ),
                            QueryError,
                        >,
                    >,
                > = None;
                let mut pending_bytes = 0usize;
                while lo < page_count {
                    Self::check_alter_copy_cancelled(&job)?;
                    let mut pages = configured_pages.min(page_count - lo).max(1);
                    let (hi, mut batch_charge) = loop {
                        let reserved = usize::try_from(pages)
                            .unwrap_or(usize::MAX)
                            .saturating_mul(PAGE_SIZE)
                            .saturating_mul(3);
                        match crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                            crate::engine::executor::mem_budget::ControlledMemoryCategory::DdlCopy,
                            reserved,
                        ) {
                            Ok(charge) => break ((lo + pages).min(page_count), charge),
                            Err(_err) if pages > 1 => pages = (pages / 2).max(1),
                            Err(err) => return Err(err),
                        }
                    };

                    let (mut batch, batch_warnings, scan_us, convert_us) = self
                        .scan_transform_alter_copy_batch_parallel(
                            old_table.id,
                            lo,
                            hi,
                            ddl_txn,
                            &old_table,
                            &candidate,
                            strict,
                            &job,
                            ddl_txn_id,
                            &index_key_plans,
                            &coercion_mask,
                            projection.as_ref(),
                        )?;
                    let actual_batch_bytes =
                        batch
                            .capacity()
                            .saturating_mul(std::mem::size_of::<DdlCopyPreparedRow>())
                            .saturating_add(
                                batch
                                    .iter()
                                    .map(DdlCopyPreparedRow::capacity_bytes)
                                    .fold(0usize, usize::saturating_add),
                            )
                            .saturating_add(batch_warnings.capacity().saturating_mul(
                                std::mem::size_of::<crate::engine::api::SqlWarning>(),
                            ));
                    DDL_COPY_SCAN_US.fetch_add(scan_us, Ordering::Relaxed);
                    DDL_COPY_SOURCE_BYTES.fetch_add(
                        hi.saturating_sub(lo).saturating_mul(PAGE_SIZE as u64),
                        Ordering::Relaxed,
                    );
                    warnings.extend(batch_warnings);
                    DDL_COPY_CONVERT_US.fetch_add(convert_us, Ordering::Relaxed);

                    if !batch.is_empty() {
                        let payloads = batch
                            .iter()
                            .map(|row| row.payload.as_slice())
                            .collect::<Vec<_>>();
                        let direct_heap_bytes = payloads
                            .capacity()
                            .saturating_mul(std::mem::size_of::<&[u8]>())
                            .saturating_add(
                                HeapFile::shadow_payload_batch_direct_scratch_capacity(&payloads),
                            );
                        let batch_bytes = actual_batch_bytes.saturating_add(direct_heap_bytes);
                        if batch_bytes > batch_charge.bytes() {
                            batch_charge.try_grow(batch_bytes - batch_charge.bytes())?;
                        }
                    } else if actual_batch_bytes > batch_charge.bytes() {
                        batch_charge.try_grow(actual_batch_bytes - batch_charge.bytes())?;
                    }
                    DDL_COPY_PEAK_BATCH_BYTES.fetch_max(
                        pending_bytes.saturating_add(batch_charge.bytes()) as u64,
                        Ordering::Relaxed,
                    );

                    if let Some(handle) = pending_write.take() {
                        let (batch_len, feed_us, _completed_charge) =
                            handle.join().map_err(|_| {
                                error(
                                    crate::error::catalog::ER_UNKNOWN_ERROR,
                                    "ALTER COPY shadow-heap worker panicked",
                                )
                            })??;
                        index_elapsed_us = index_elapsed_us.saturating_add(feed_us);
                        copied_rows = copied_rows.saturating_add(batch_len);
                        Self::advance_alter_copy_job_copied_by(&job, batch_len);
                        pending_bytes = 0;
                    }

                    if batch.is_empty() {
                        drop(batch_charge);
                    } else {
                        let batch_len = batch.len() as u64;
                        pending_bytes = batch_charge.bytes();
                        let index_builders_ref = &index_builders;
                        let online_rows_ref = &online_rows;
                        let shadow_table_codec = Arc::clone(&shadow_table.row_codec);
                        pending_write = Some(scope.spawn(move || {
                            let payloads = batch
                                .iter()
                                .map(|row| row.payload.as_slice())
                                .collect::<Vec<_>>();
                            let row_ids = if external {
                                drop(payloads);
                                let values = batch
                                    .iter_mut()
                                    .map(|row| row.values.take().expect("external COPY values"))
                                    .collect::<Vec<_>>();
                                let headers = vec![RowHeader::visible(ddl_txn_id); values.len()];
                                self.insert_online_copy_batch(
                                    ddl_txn_id,
                                    shadow_id,
                                    &shadow_table_codec,
                                    &headers,
                                    &values,
                                )?
                            } else {
                                self.with_heap_read(shadow_id, |heap| {
                                    heap.append_shadow_payload_batch_direct(build_lsn, &payloads)
                                })?
                            };
                            if online_barrier
                                .is_some_and(|(_, lock)| lock != crate::parser::AlterLock::Shared)
                            {
                                let mut mapping = online_rows_ref.lock();
                                for (row, shadow) in batch.iter().zip(&row_ids) {
                                    mapping.insert(
                                        row.source_row,
                                        crate::storage::DmlRowRef {
                                            storage_id: shadow_id,
                                            row_id: *shadow,
                                        },
                                    )?;
                                }
                            }
                            let index_feed_started = Instant::now();
                            index_builders_ref.lock().push_prepared_batch(
                                self.data_dir(),
                                batch,
                                &row_ids,
                            )?;
                            let feed_us = index_feed_started
                                .elapsed()
                                .as_micros()
                                .min(u128::from(u64::MAX))
                                as u64;
                            Ok::<_, QueryError>((batch_len, feed_us, batch_charge))
                        }));
                    }
                    lo = hi;
                }
                if let Some(handle) = pending_write {
                    let (batch_len, feed_us, _completed_charge) =
                        handle.join().map_err(|_| {
                            error(
                                crate::error::catalog::ER_UNKNOWN_ERROR,
                                "ALTER COPY shadow-heap worker panicked",
                            )
                        })??;
                    index_elapsed_us = index_elapsed_us.saturating_add(feed_us);
                    copied_rows = copied_rows.saturating_add(batch_len);
                    Self::advance_alter_copy_job_copied_by(&job, batch_len);
                }
                Ok(())
            })?;
            let index_builders = index_builders.into_inner();
            Self::set_alter_copy_job_total(&job, copied_rows);
            DDL_COPY_DIRECT_HEAP_BUILDS.fetch_add(1, Ordering::Relaxed);
            Self::set_alter_copy_job_replay_watermark(&job, copied_rows);

            Self::set_alter_copy_job_state(&job, "INDEX_BUILD");
            DDL_COPY_INDEX_FEED_US.fetch_add(index_elapsed_us, Ordering::Relaxed);
            let index_started = Instant::now();
            let previous_index_lsn = btree_logical_wal::current();
            let previous_deferred_sync = btree_logical_wal::bulk_sync_deferred();
            btree_logical_wal::set(Some(build_lsn));
            btree_logical_wal::set_bulk_sync_deferred(
                self.config.durability == DurabilityMode::Strict,
            );
            let index_result = index_builders.finish(self, &shadow_table);
            btree_logical_wal::set_bulk_sync_deferred(previous_deferred_sync);
            btree_logical_wal::set(previous_index_lsn);
            index_result?;
            self.build_online_copy_vectors(&shadow_table, shadow_id)?;
            self.flush_online_copy_vectors(&shadow_table, shadow_id)?;
            if external {
                self.flush_online_copy_shadow(&shadow_table)?;
            }
            if self.config.durability == DurabilityMode::Strict {
                std::thread::scope(|scope| {
                    let heap_path = self.table_path(shadow_id);
                    let heap_sync = scope.spawn(move || {
                        OpenOptions::new()
                            .read(true)
                            .open(heap_path)
                            .map_err(io_err)?
                            .sync_all()
                            .map_err(io_err)
                    });
                    let handles = candidate
                        .indexes
                        .iter()
                        .map(|index| {
                            let path = self.index_path_for_table_schema(
                                &candidate,
                                shadow_id,
                                &index.name,
                            );
                            scope.spawn(move || {
                                OpenOptions::new()
                                    .read(true)
                                    .open(path)
                                    .map_err(io_err)?
                                    .sync_data()
                                    .map_err(io_err)
                            })
                        })
                        .collect::<Vec<_>>();
                    heap_sync.join().map_err(|_| {
                        error(
                            crate::error::catalog::ER_UNKNOWN_ERROR,
                            "ALTER COPY heap-sync worker panicked",
                        )
                    })??;
                    for handle in handles {
                        handle.join().map_err(|_| {
                            error(
                                crate::error::catalog::ER_UNKNOWN_ERROR,
                                "ALTER COPY index-sync worker panicked",
                            )
                        })??;
                    }
                    Ok::<_, QueryError>(())
                })?;
            } else {
                self.with_heap_read(shadow_id, |heap| heap.sync_shadow_direct())?;
            }
            let index_build_us = index_started
                .elapsed()
                .as_micros()
                .min(u128::from(u64::MAX)) as u64;
            DDL_COPY_INDEX_BUILD_US.fetch_add(index_build_us, Ordering::Relaxed);
            index_elapsed_us = index_elapsed_us.saturating_add(index_build_us);
            DDL_COPY_INDEX_US.fetch_add(index_elapsed_us, Ordering::Relaxed);
            DDL_COPY_SPILL_BYTES.fetch_add(
                spill_bytes_written().saturating_sub(spill_before),
                Ordering::Relaxed,
            );
            let shadow_bytes = fs::metadata(self.table_path(shadow_id))
                .map(|meta| meta.len())
                .unwrap_or(0)
                .saturating_add(candidate.indexes.iter().fold(0_u64, |sum, index| {
                    sum.saturating_add(
                        fs::metadata(self.index_path_for_table_schema(
                            &candidate,
                            shadow_id,
                            &index.name,
                        ))
                        .map(|meta| meta.len())
                        .unwrap_or(0),
                    )
                }));
            DDL_COPY_SHADOW_BYTES.fetch_add(shadow_bytes, Ordering::Relaxed);
            if let Some((owner, _)) = online_barrier {
                Self::set_alter_copy_job_state(&job, "CATCH_UP");
                let _replay_timer = OnlineIndexTimer(&DDL_COPY_REPLAY_US, Instant::now());
                let mut mapping = online_rows.lock();
                let mut attempts = 0;
                loop {
                    Self::check_alter_copy_cancelled(&job)?;
                    self.replay_online_copy_rows(
                        &old_table,
                        &shadow_table,
                        ddl_txn,
                        &job,
                        &mut mapping,
                        strict,
                        &mut warnings,
                        None,
                    )?;
                    self.flush_online_copy_shadow(&shadow_table)?;
                    self.flush_online_copy_shadow(&old_table)?;
                    #[cfg(test)]
                    {
                        let hook = self.online_copy_publish_test_hook.lock().unwrap().take();
                        if let Some(hook) = hook {
                            hook.pause();
                        }
                    }
                    mapping.prepare_final_tail()?;
                    let barrier_started = Instant::now();
                    self.online_index_barrier_checked(
                        &old_table,
                        owner,
                        false,
                        MdlLockType::Exclusive,
                        || Self::check_alter_copy_cancelled(&job),
                    )?;
                    DDL_COPY_BARRIER_WAIT_US.fetch_add(
                        barrier_started.elapsed().as_micros() as u64,
                        Ordering::Relaxed,
                    );
                    let held = OnlineCopyHoldTimer(Instant::now());
                    self.acquire_table_exclusive(ddl_txn_id, old_table.id)?;
                    let tail = job.changes.lock().unwrap_or_else(|p| p.into_inner()).len();
                    if tail <= 1024 {
                        DDL_COPY_FINAL_TAIL_ROWS.fetch_add(tail as u64, Ordering::Relaxed);
                        self.replay_online_copy_rows(
                            &old_table,
                            &shadow_table,
                            ddl_txn,
                            &job,
                            &mut mapping,
                            strict,
                            &mut warnings,
                            Some(online_copy_deadline()),
                        )?;
                        self.flush_online_copy_shadow(&shadow_table)?;
                        _final_hold = Some(held);
                        break;
                    }
                    self.table_locks_sharded
                        .downgrade_exclusive_to_intent_shared(ddl_txn_id, old_table.id)?;
                    self.release_mdl_duration(owner, MdlDuration::Transaction);
                    self.notify_non_row_lock_waiters();
                    attempts += 1;
                    if attempts >= 32 {
                        return Err(error(
                            crate::error::catalog::ER_LOCK_WAIT_TIMEOUT,
                            "online ALTER could not converge within its bounded final-tail budget",
                        ));
                    }
                }
            }
            Self::enter_alter_copy_finalizing(&job)?;
            let swap_started = Instant::now();
            let memory_graphs = if !vector_storage_paged() {
                let graphs = self.vector_indexes.read();
                candidate
                    .indexes
                    .iter()
                    .filter(|index| index.is_vector())
                    .filter_map(|index| {
                        graphs
                            .get(&(
                                shadow_id,
                                shadow_id,
                                candidate.active_generation.max(1),
                                normalize_name(&index.name),
                            ))
                            .cloned()
                            .map(|graph| (normalize_name(&index.name), graph))
                    })
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            if online_barrier.is_some() {
                let graphs = self.vector_indexes.read();
                job.retired_vector_graphs
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .extend(
                        graphs
                            .iter()
                            .filter(|((tid, _, _, _), _)| *tid == old_table.id || *tid == shadow_id)
                            .map(|(_, graph)| graph.clone()),
                    );
            }
            // Memory indexes/LOBs live in buffer frames, not reopenable files.
            // Publish their private handles instead of evicting and reopening
            // them as empty trees. All fallible build work has finished.
            if self.config.durability == DurabilityMode::Memory {
                self.publish_memory_copy(&old_table, &candidate, shadow_id, ddl_txn)?;
                published = true;
                self.evict_table_vector_graphs(old_table.id);
                for (name, graph) in memory_graphs {
                    self.vector_indexes.write().insert(
                        (
                            old_table.id,
                            old_table.id,
                            candidate.active_generation.max(1),
                            name,
                        ),
                        graph,
                    );
                }
                self.finish_shadow_generation_intent(&intent)?;
                return Ok(warnings);
            }
            self.evict_table_vector_graphs(old_table.id);
            self.evict_table_vector_graphs(shadow_id);
            let file_swap =
                self.swap_shadow_generation_files(&old_table, &candidate, shadow_id, job.job_id)?;

            let key = table_key(&candidate.database, &candidate.name);
            {
                let mut catalog = self.catalog_write()?;
                let table_arc = catalog
                    .tables
                    .get_mut(&key)
                    .ok_or_else(|| ddl_table_not_found(&table_name))?;
                *Arc::make_mut(table_arc) = candidate.clone();
            }
            self.note_catalog_table(candidate.id);
            // The old and private pathnames must never be reconstructed from
            // row deltas after their bulk-built base generation is retired.
            let retired_paths = file_swap
                .old_to_backup
                .iter()
                .map(|(old, _)| old)
                .chain(file_swap.shadow_to_live.iter().map(|(shadow, _)| shadow))
                .map(|path| self.relative_path(path))
                .collect();
            let publish = self
                .append_wal(
                    ddl_txn_id,
                    WalPayload::DdlCommit {
                        job_id: job.job_id,
                        retired_paths,
                    },
                )
                .and_then(|_| self.commit_catalog_txn_existing(ddl_txn));
            if let Err(err) = publish {
                if let Ok(mut catalog) = self.catalog_write() {
                    if let Some(table_arc) = catalog.tables.get_mut(&key) {
                        *Arc::make_mut(table_arc) = old_table.as_ref().clone();
                    }
                }
                if let Err(rollback) = self.rollback_shadow_generation_file_swap(&file_swap) {
                    self.quarantine_mvcc(format!(
                        "ALTER COPY catalog rollback needs recovery: {}",
                        rollback.message()
                    ));
                }
                return Err(err);
            }
            published = true;
            // A background opener can have cached the source path between
            // the first eviction and catalog publication. Drain it before
            // releasing MDL; in-flight cold opens are fenced by the epoch.
            self.invalidate_generation_handles(&old_table)?;
            for (name, graph) in memory_graphs {
                self.vector_indexes.write().insert(
                    (
                        old_table.id,
                        old_table.id,
                        candidate.active_generation.max(1),
                        name,
                    ),
                    graph,
                );
            }
            self.table_locks_sharded
                .set_schema_floor(old_table.id, candidate.schema_version);
            self.primary_definition_highwater
                .fetch_max(self.visible_scn(), Ordering::Release);
            if online_barrier.is_some() {
                self.forget_zone_map(old_table.id)?;
                let mut retired = job.retired_files.lock().unwrap_or_else(|p| p.into_inner());
                retired.extend(
                    file_swap
                        .old_to_backup
                        .into_iter()
                        .flat_map(|(old, backup)| [old, backup]),
                );
                retired.extend(
                    file_swap
                        .shadow_to_live
                        .into_iter()
                        .map(|(shadow, _)| shadow),
                );
                retired.push(intent.clone());
            } else {
                self.finish_shadow_generation_file_swap(&old_table, file_swap)?;
                self.finish_shadow_generation_intent(&intent)?;
            }
            DDL_COPY_SWAP_US.fetch_add(
                swap_started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
                Ordering::Relaxed,
            );
            self.mark_table_stats_stale(&key, old_table.stats.row_count.max(1))?;
            Ok(warnings)
        })();

        if result.is_err() {
            let active = self
                .txns
                .read()
                .slot(ddl_txn_id)
                .is_some_and(|slot| slot.status == TxnStatus::Active);
            if active {
                let _ = self.rollback_txn(ddl_txn_id);
            }
        }
        if let Some((owner, _)) = online_barrier {
            self.release_mdl_duration(owner, MdlDuration::Transaction);
            self.notify_non_row_lock_waiters();
        }
        drop(_final_hold);
        let _ = self.remove_heap_cache_entry(shadow_id);
        for index in &candidate.indexes {
            let _ = self.remove_index_cache_entry(shadow_id, &index.name);
        }
        self.drop_table_vector_graphs(&shadow_table);
        let shadow_lob = self.overflow_path(shadow_id);
        let _ = self.remove_overflow_cache_entry(shadow_id);
        self.unregister_reorg_storage_locs(&[shadow_id]);
        if result.is_err() {
            let _ = fs::remove_file(shadow_lob);
            let _ = fs::remove_file(&shadow_heap_path);
            for index in &candidate.indexes {
                let _ = fs::remove_file(self.index_path_for_table_schema(
                    &candidate,
                    shadow_id,
                    &index.name,
                ));
            }
            if !published && self.ensure_operational().is_ok() {
                let _ = self.finish_shadow_generation_intent(&intent);
            }
        }
        if owns_job {
            self.unregister_alter_copy_job(old_table.id, result.as_ref().err());
        }
        result
    }

    fn publish_memory_copy(
        &self,
        source: &TableSchema,
        target: &TableSchema,
        shadow_id: u64,
        txn: TxnView,
    ) -> Result<(), QueryError> {
        let heap = self.heap_handle(shadow_id)?;
        let indexes = target
            .indexes
            .iter()
            .map(|index| {
                self.index_handle(shadow_id, &index.name)
                    .map(|handle| (normalize_name(&index.name), handle))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let key = table_key(&target.database, &target.name);
        self.catalog_write()?
            .tables
            .insert(key.clone(), Arc::new(target.clone()));
        self.note_catalog_table(target.id);
        if let Err(err) = self.commit_catalog_txn_existing(txn) {
            self.catalog_write()?
                .tables
                .insert(key, Arc::new(source.clone()));
            return Err(err);
        }
        // Statement MDL and the storage X lock exclude observers throughout
        // the catalog/handle switch. There are no I/O operations below.
        heap.write().table_id = source.id;
        {
            let mut cache = self.heap_cache.write();
            self.invalidate_storage_handle_open_epoch(source.id);
            self.invalidate_storage_handle_open_epoch(shadow_id);
            cache.remove(&shadow_id);
            cache.insert(source.id, heap);
        }
        {
            let mut cache = self.overflow_cache.write();
            self.invalidate_storage_handle_open_epoch(source.id);
            self.invalidate_storage_handle_open_epoch(shadow_id);
            let overflow = cache.remove(&shadow_id);
            cache.remove(&source.id);
            if let Some(overflow) = overflow {
                overflow.write().table_id = source.id;
                cache.insert(source.id, overflow);
            }
        }
        {
            let mut cache = self.index_cache.write();
            self.invalidate_storage_handle_open_epoch(source.id);
            self.invalidate_storage_handle_open_epoch(shadow_id);
            cache.retain(|(id, _), _| *id != source.id && *id != shadow_id);
            for (name, handle) in indexes {
                cache.insert((source.id, name), handle);
            }
        }
        self.spatial_rtree_cache
            .write()
            .retain(|(id, _), _| *id != source.id && *id != shadow_id);
        self.index_mutation_generations
            .write()
            .retain(|(id, _), _| *id != source.id && *id != shadow_id);
        self.forget_zone_map(source.id)?;
        self.table_locks_sharded
            .set_schema_floor(source.id, target.schema_version);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn scan_transform_alter_copy_batch_parallel(
        &self,
        storage_id: u64,
        page_lo: u64,
        page_hi: u64,
        txn: TxnView,
        old_table: &TableSchema,
        candidate: &TableSchema,
        strict: bool,
        job: &Arc<AlterCopyJob>,
        ddl_txn_id: u64,
        index_key_plans: &[DdlCopyIndexKeyPlan],
        coercion_mask: &[bool],
        projection: Option<&AlterCopyProjection>,
    ) -> Result<
        (
            Vec<DdlCopyPreparedRow>,
            Vec<crate::engine::api::SqlWarning>,
            u64,
            u64,
        ),
        QueryError,
    > {
        let defaults = candidate
            .columns
            .iter()
            .enumerate()
            .skip(old_table.columns.len())
            .filter_map(|(idx, column)| column.default_expr.as_deref().map(|sql| (idx, sql)))
            .map(|(idx, sql)| {
                crate::parser::parse_catalog_expr(sql)
                    .map(|expr| (idx, expr))
                    .map_err(|err| error(crate::error::catalog::ER_PARSE_ERROR, err.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|(_, expr)| {
                crate::parser::visit::try_for_each_expr(expr, |node| {
                    if matches!(node, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) {
                        Err(())
                    } else {
                        Ok(true)
                    }
                })
                .is_err()
            })
            .collect::<Vec<_>>();
        if table_has_externalizable_column(old_table) || table_has_externalizable_column(candidate)
        {
            let started = Instant::now();
            let source_rows =
                self.scan_rows_storage_range(old_table, storage_id, txn, page_lo, page_hi)?;
            let scan_us = started.elapsed().as_micros() as u64;
            let started = Instant::now();
            let mut warnings = Vec::new();
            let mut rows = Vec::with_capacity(source_rows.len());
            for row in source_rows {
                Self::check_alter_copy_cancelled(job)?;
                let mut values = self.prepare_alter_copy_values_masked(
                    candidate,
                    row.values,
                    strict,
                    &mut warnings,
                    coercion_mask,
                    &defaults,
                )?;
                let keys =
                    DdlCopyIndexBuilders::prepare_keys(candidate, index_key_plans, &mut values)?;
                rows.push(DdlCopyPreparedRow {
                    source_row: row.row_id,
                    values: Some(values),
                    payload: Vec::new(),
                    keys,
                });
            }
            return Ok((
                rows,
                warnings,
                scan_us,
                started.elapsed().as_micros() as u64,
            ));
        }
        let pages = page_hi.saturating_sub(page_lo).max(1);
        let degree = (index_sort_parallel_degree() as u64).min(pages) as usize;
        let pages_per_worker = pages.div_ceil(degree as u64);
        let ranges = (0..degree)
            .filter_map(|worker| {
                let lo = page_lo.saturating_add(worker as u64 * pages_per_worker);
                let hi = (lo + pages_per_worker).min(page_hi);
                (lo < hi).then_some((lo, hi))
            })
            .collect::<Vec<_>>();
        let session_info = crate::engine::expr::current_session_expr_info();
        // Schema coercion also emits statement diagnostics through TLS. Share a
        // batched sink with workers so those warnings survive their exit.
        let warning_sink = Arc::new(parking_lot::Mutex::new(
            crate::engine::api::SqlWarnings::new(),
        ));
        let result = std::thread::scope(|scope| {
            let defaults = &defaults;
            let session_info = &session_info;
            let handles = ranges
                    .into_iter()
                    .map(|(lo, hi)| {
                        let warning_sink = warning_sink.clone();
                        scope.spawn(move || {
                            let run = || crate::engine::expr::with_session_expr_info_option(session_info, || {
                            let mut rows = Vec::new();
                            let mut warnings = Vec::new();
                            let mut projected_values = projection
                                .map(|projection| Vec::with_capacity(projection.columns.len()))
                                .unwrap_or_default();
                            let mut candidate_values = Vec::with_capacity(candidate.columns.len());
                            let scan_started = Instant::now();
                            self.with_heap_read(storage_id, |heap| {
                                if let Some(projection) = projection {
                                    return heap.scan_alter_copy_raw_pages_range_until(
                                        lo,
                                        hi,
                                        |row_id, header, original, decoded_values| {
                                            Self::check_alter_copy_cancelled(job)?;
                                            let current_payload = original.is_some()
                                                && self.row_version_visible(header, txn)?;
                                            if current_payload {
                                                if header.is_deleted() {
                                                    return Ok(false);
                                                }
                                                let payload = original
                                                    .expect("current inline row must have a payload");
                                                match decode_heap_payload_columns_into_with_plan(
                                                    payload,
                                                    &projection.decode,
                                                    &old_table.row_codec,
                                                    &mut projected_values,
                                                )? {
                                                    HeapPayloadOutcome::Row(_, _) => {}
                                                    _ => {
                                                        return Err(error(
                                                            crate::error::catalog::ER_GET_ERRNO,
                                                            "ALTER COPY projected decoder saw a non-row payload",
                                                        ))
                                                    }
                                                }
                                                candidate_values.clear();
                                                // Unprojected slots can still be absent from an old
                                                // physical row. Preserve their old read value, never
                                                // the new DEFAULT introduced by this ALTER.
                                                candidate_values.extend(candidate.columns.iter().enumerate().map(
                                                    |(idx, column)| crate::engine::executor::insert::column_read_backfill_default(
                                                        old_table.columns.get(idx).unwrap_or(column)
                                                    ),
                                                ));
                                                for (column, value) in projection
                                                    .columns
                                                    .iter()
                                                    .copied()
                                                    .zip(projected_values.drain(..))
                                                {
                                                    candidate_values[column] = value;
                                                }
                                                self.prepare_alter_copy_projected_values_in_place(
                                                    candidate,
                                                    &mut candidate_values,
                                                    strict,
                                                    &mut warnings,
                                                    coercion_mask,
                                                )?;
                                            } else {
                                                let values = match decoded_values {
                                                    Some(values) => values,
                                                    None => match decode_heap_payload(
                                                        original.expect("inline row must have a payload"),
                                                        &old_table.row_codec,
                                                    )? {
                                                        HeapPayload::Row(_, values) => values,
                                                        _ => return Ok(false),
                                                    },
                                                };
                                                let (visible, _) = self.visible_row_with_current_payload(
                                                    MvccStoredRow {
                                                        row_id,
                                                        header,
                                                        values,
                                                    },
                                                    txn,
                                                )?;
                                                let Some(mut visible) = visible else {
                                                    return Ok(false);
                                                };
                                                Self::pad_full_row_to_schema(
                                                    &mut visible.values,
                                                    old_table,
                                                );
                                                while visible.values.len() < candidate.columns.len() {
                                                    let column =
                                                        &candidate.columns[visible.values.len()];
                                                    visible.values.push(
                                                        crate::engine::executor::insert::column_read_backfill_default(column),
                                                    );
                                                }
                                                candidate_values = self.prepare_alter_copy_values_masked(
                                                    candidate,
                                                    visible.values,
                                                    strict,
                                                    &mut warnings,
                                                    coercion_mask,
                        &defaults,
                                                )?;
                                            }
                                            let payload = if current_payload {
                                                transcode_visible_row_payload(
                                                    original.expect("current row payload must exist"),
                                                    &old_table.row_codec,
                                                    &candidate.row_codec,
                                                    &candidate_values,
                                                    coercion_mask,
                                                    ddl_txn_id,
                                                )?
                                            } else {
                                                encode_visible_row_payload(
                                                    &candidate_values,
                                                    &candidate.row_codec,
                                                    ddl_txn_id,
                                                )?
                                            };
                                            let keys = DdlCopyIndexBuilders::prepare_keys(
                                                candidate,
                                                index_key_plans,
                                                &mut candidate_values,
                                            )?;
                                            rows.push(DdlCopyPreparedRow { source_row: row_id, values: None, payload, keys });
                                            Ok(false)
                                        },
                                    );
                                }
                                heap.scan_mvcc_pages_range_with_payload_until(lo, hi, |row, original| {
                                    Self::check_alter_copy_cancelled(job)?;
                                    let (visible, current_payload) =
                                        self.visible_row_with_current_payload(row, txn)?;
                                    let Some(mut visible) = visible else {
                                        return Ok(false);
                                    };
                                    let can_transcode = original.is_some() && current_payload;
                                    Self::pad_full_row_to_schema(&mut visible.values, old_table);
                                    while visible.values.len() < candidate.columns.len() {
                                        let column = &candidate.columns[visible.values.len()];
                                        visible
                                            .values
                                            .push(crate::engine::executor::insert::column_read_backfill_default(column));
                                    }
                                    let mut values = self.prepare_alter_copy_values_masked(
                                        candidate,
                                        visible.values,
                                        strict,
                                        &mut warnings,
                                        coercion_mask,
                        &defaults,
                                    )?;
                                    let payload = if can_transcode {
                                        transcode_visible_row_payload(
                                            original.expect("ordinary row payload must be present"),
                                            &old_table.row_codec,
                                            &candidate.row_codec,
                                            &values,
                                            coercion_mask,
                                            ddl_txn_id,
                                        )?
                                    } else {
                                        encode_visible_row_payload(
                                            &values,
                                            &candidate.row_codec,
                                            ddl_txn_id,
                                        )?
                                    };
                                    let keys = DdlCopyIndexBuilders::prepare_keys(
                                        candidate,
                                        index_key_plans,
                                        &mut values,
                                    )?;
                                    rows.push(DdlCopyPreparedRow { source_row: visible.row_id, values: None, payload, keys });
                                    Ok(false)
                                })
                            })?;
                            let scan_us = scan_started
                                .elapsed()
                                .as_micros()
                                .min(u128::from(u64::MAX)) as u64;
                            Ok::<_, QueryError>((rows, warnings, scan_us, 0_u64))
                            });
                            crate::engine::api::with_batched_statement_warning_sink(warning_sink, run)
                        })
                    })
                    .collect::<Vec<_>>();
            let mut rows = Vec::new();
            let mut warnings = Vec::new();
            let mut scan_us = 0_u64;
            let mut convert_us = 0_u64;
            for handle in handles {
                let (mut chunk_rows, mut chunk_warnings, worker_scan_us, worker_convert_us) =
                    handle.join().map_err(|_| {
                        error(
                            crate::error::catalog::ER_UNKNOWN_ERROR,
                            "ALTER COPY conversion worker panicked",
                        )
                    })??;
                rows.append(&mut chunk_rows);
                warnings.append(&mut chunk_warnings);
                scan_us = scan_us.max(worker_scan_us);
                convert_us = convert_us.max(worker_convert_us);
            }
            Ok((rows, warnings, scan_us, convert_us))
        });
        crate::engine::api::append_statement_warnings(std::mem::take(&mut *warning_sink.lock()));
        result
    }

    fn sync_shadow_generation_directories(
        &self,
        swap: &ShadowGenerationFileSwap,
    ) -> Result<(), QueryError> {
        if self.config.durability == DurabilityMode::Memory {
            return Ok(());
        }
        let mut directories = std::collections::BTreeSet::new();
        for (left, right) in swap.old_to_backup.iter().chain(&swap.shadow_to_live) {
            for path in [left, right] {
                if let Some(parent) = path.parent() {
                    directories.extend(
                        parent
                            .ancestors()
                            .take_while(|dir| dir.starts_with(&self.data_dir))
                            .map(Path::to_path_buf),
                    );
                }
            }
        }
        // New index-generation and private-object directories must themselves
        // be durable, not just the files or the live heap's immediate parent.
        crate::platform::fs::sync_paths(
            directories
                .iter()
                .rev()
                .filter(|directory| directory.exists()),
        )
        .map_err(io_err)
    }

    fn swap_shadow_generation_files(
        &self,
        old_table: &TableSchema,
        candidate: &TableSchema,
        shadow_id: u64,
        job_id: u64,
    ) -> Result<ShadowGenerationFileSwap, QueryError> {
        // Source LOB/vector tails must also survive a failed swap. Flush all
        // authoritative source files before dropping their cached pages.
        self.flush_online_copy_shadow(old_table)?;
        self.remove_heap_cache_entry(old_table.id)?;
        self.remove_heap_cache_entry(shadow_id)?;
        for index in &old_table.indexes {
            self.remove_index_cache_entry(old_table.id, &index.name)?;
        }
        for index in &candidate.indexes {
            self.remove_index_cache_entry(shadow_id, &index.name)?;
        }

        self.remove_overflow_cache_entry(old_table.id)?;
        self.remove_overflow_cache_entry(shadow_id)?;
        let (old_to_backup, shadow_to_live) =
            self.shadow_generation_swap_paths(old_table, candidate, shadow_id, job_id);

        let mut performed = ShadowGenerationFileSwap {
            old_to_backup: Vec::new(),
            shadow_to_live: Vec::new(),
        };
        let result = (|| {
            let new_generation = old_table.active_generation != candidate.active_generation;
            for (live, backup) in &old_to_backup {
                // A distinct generation never overwrites the old pathname.
                // Keep its synced base available to REDO until commit decides.
                if !new_generation && live.exists() {
                    fs::rename(live, backup).map_err(io_err)?;
                    performed.old_to_backup.push((live.clone(), backup.clone()));
                }
            }
            for (shadow, live) in &shadow_to_live {
                if shadow
                    .extension()
                    .is_some_and(|ext| matches!(ext.to_str(), Some("lob" | "hnsw" | "hvec")))
                    && !shadow.exists()
                {
                    continue;
                }
                if let Some(parent) = live.parent() {
                    fs::create_dir_all(parent).map_err(io_err)?;
                }
                if new_generation {
                    // Keep private WAL paths reachable for loser-transaction
                    // UNDO. Both links name the already-synced private file.
                    fs::hard_link(shadow, live).map_err(io_err)?;
                } else {
                    fs::rename(shadow, live).map_err(io_err)?;
                }
                performed
                    .shadow_to_live
                    .push((shadow.clone(), live.clone()));
            }
            // LOAD can replace an inode at the same physical generation.
            // Invalidate opens started after the initial cache drain as well.
            self.invalidate_generation_handles(old_table)?;
            self.sync_shadow_generation_directories(&performed)?;
            Ok::<_, QueryError>(())
        })();
        if let Err(err) = result {
            if let Err(rollback) = self.rollback_shadow_generation_file_swap(&performed) {
                self.quarantine_mvcc(format!(
                    "ALTER COPY file swap needs recovery: {}",
                    rollback.message()
                ));
            }
            return Err(err);
        }
        Ok(ShadowGenerationFileSwap {
            old_to_backup,
            shadow_to_live,
        })
    }

    fn rollback_shadow_generation_file_swap(
        &self,
        swap: &ShadowGenerationFileSwap,
    ) -> Result<(), QueryError> {
        let mut failure = None;
        for (shadow, live) in swap.shadow_to_live.iter().rev() {
            if live.exists() {
                let result = if shadow.exists() {
                    fs::remove_file(live)
                } else {
                    fs::rename(live, shadow)
                };
                if let Err(err) = result {
                    failure = Some(io_err(err));
                }
            }
        }
        for (live, backup) in swap.old_to_backup.iter().rev() {
            if backup.exists() {
                if let Err(err) = fs::rename(backup, live) {
                    failure = Some(io_err(err));
                }
            }
        }
        if failure.is_none() {
            failure = self.sync_shadow_generation_directories(swap).err();
        }
        if let Some(err) = failure {
            self.quarantine_mvcc(format!(
                "storage generation rollback requires recovery: {}",
                err.message()
            ));
            Err(err)
        } else {
            Ok(())
        }
    }

    fn finish_shadow_generation_file_swap(
        &self,
        old_table: &TableSchema,
        swap: ShadowGenerationFileSwap,
    ) -> Result<(), QueryError> {
        let remove = |path: &Path| -> Result<(), QueryError> {
            match fs::remove_file(path) {
                Ok(()) => Ok(()),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(err) => Err(io_err(err)),
            }
        };
        for (old, backup) in &swap.old_to_backup {
            if !swap.shadow_to_live.iter().any(|(_, live)| live == old) {
                remove(old)?;
            }
            remove(backup)?;
        }
        for (shadow, _) in &swap.shadow_to_live {
            remove(shadow)?;
        }
        self.sync_shadow_generation_directories(&swap)?;
        self.forget_zone_map(old_table.id)?;
        let _ = fs::remove_file(self.zone_map_file_path(old_table.id));
        Ok(())
    }

    fn shadow_generation_swap_paths(
        &self,
        old_table: &TableSchema,
        candidate: &TableSchema,
        shadow_id: u64,
        job_id: u64,
    ) -> (Vec<(PathBuf, PathBuf)>, Vec<(PathBuf, PathBuf)>) {
        let old_live_heap = crate::storage::naming::table_object_generation_path(
            &self.data_dir,
            old_table.id,
            old_table.active_generation.max(1),
            "heap",
        );
        let new_live_heap = crate::storage::naming::table_object_generation_path(
            &self.data_dir,
            old_table.id,
            candidate.active_generation.max(1),
            "heap",
        );
        let mut old_to_backup = vec![(
            old_live_heap.clone(),
            old_live_heap.with_extension(format!("heap.alter-old-{job_id}")),
        )];
        old_to_backup.extend(old_table.indexes.iter().map(|index| {
            let live = self.index_path_for_table_schema(old_table, old_table.id, &index.name);
            let backup = live.with_extension(format!("idx.alter-old-{job_id}"));
            (live, backup)
        }));
        let mut shadow_to_live = vec![(self.table_path(shadow_id), new_live_heap)];
        shadow_to_live.extend(candidate.indexes.iter().map(|index| {
            (
                self.index_path_for_table_schema(candidate, shadow_id, &index.name),
                self.index_path_for_table_schema(candidate, old_table.id, &index.name),
            )
        }));
        if table_has_externalizable_column(old_table) {
            let live = crate::storage::naming::table_object_generation_path(
                &self.data_dir,
                old_table.id,
                old_table.active_generation.max(1),
                "lob",
            );
            old_to_backup.push((
                live.clone(),
                live.with_extension(format!("lob.alter-old-{job_id}")),
            ));
        }
        if table_has_externalizable_column(candidate) {
            let live = crate::storage::naming::table_object_generation_path(
                &self.data_dir,
                old_table.id,
                candidate.active_generation.max(1),
                "lob",
            );
            shadow_to_live.push((self.overflow_path(shadow_id), live));
        }
        for index in old_table.indexes.iter().filter(|index| index.is_vector()) {
            for live in self.vector_file_paths(old_table, &index.name, old_table.id) {
                let backup = live.with_extension(format!(
                    "{}.alter-old-{job_id}",
                    live.extension().unwrap().to_string_lossy()
                ));
                old_to_backup.push((live, backup));
            }
        }
        for index in candidate.indexes.iter().filter(|index| index.is_vector()) {
            let shadows = self.vector_file_paths(candidate, &index.name, shadow_id);
            let lives = self.vector_file_paths(candidate, &index.name, old_table.id);
            shadow_to_live.extend(shadows.into_iter().zip(lives));
        }
        (old_to_backup, shadow_to_live)
    }

    fn write_alter_copy_intent(
        &self,
        old_table: &TableSchema,
        candidate: &TableSchema,
        shadow_id: u64,
        job_id: u64,
    ) -> Result<PathBuf, QueryError> {
        let (old_to_backup, shadow_to_live) =
            self.shadow_generation_swap_paths(old_table, candidate, shadow_id, job_id);
        let relative_pairs = |pairs: Vec<(PathBuf, PathBuf)>| {
            pairs
                .into_iter()
                .map(|(from, to)| (self.relative_path(&from), self.relative_path(&to)))
                .collect::<Vec<_>>()
        };
        let intent = AlterCopyIntent {
            version: 2,
            table_id: old_table.id,
            database: old_table.database.clone(),
            table_name: old_table.name.clone(),
            old_schema_version: old_table.schema_version,
            new_schema_version: candidate.schema_version,
            new_generation: candidate.active_generation.max(1),
            shadow_id,
            old_to_backup: relative_pairs(old_to_backup),
            shadow_to_live: relative_pairs(shadow_to_live),
        };
        let path = self.alter_copy_job_path(old_table.id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(io_err)?;
        }
        let bytes = serde_json::to_vec(&intent).map_err(|err| {
            error(
                crate::error::catalog::ER_GET_ERRNO,
                format!("ALTER COPY intent encode failed: {err}"),
            )
        })?;
        // A crash while writing must leave only an unpublished staging file.
        // Never expose an empty/truncated journal as an authoritative intent.
        let staging = path.with_extension("job.tmp");
        {
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&staging)
                .map_err(io_err)?;
            file.write_all(&bytes).map_err(io_err)?;
            if self.config.durability != DurabilityMode::Memory {
                crate::platform::fs::sync_before_atomic_rename(&file).map_err(io_err)?;
            }
        }
        fs::rename(&staging, &path).map_err(io_err)?;
        if self.config.durability != DurabilityMode::Memory {
            if let Some(parent) = path.parent() {
                crate::platform::fs::sync_path(parent).map_err(io_err)?;
            }
        }
        Ok(path)
    }

    fn finish_shadow_generation_intent(&self, path: &Path) -> Result<(), QueryError> {
        match fs::remove_file(path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(io_err(err)),
        }
        if self.config.durability != DurabilityMode::Memory {
            if let Some(parent) = path.parent() {
                crate::platform::fs::sync_path(parent).map_err(io_err)?;
            }
        }
        Ok(())
    }

    fn recover_alter_copy_intents(&self) -> Result<(), QueryError> {
        for path in incomplete_alter_copy_job_paths(&self.data_dir)? {
            let bytes = fs::read(&path).map_err(io_err)?;
            let intent: AlterCopyIntent = serde_json::from_slice(&bytes).map_err(|_| {
                error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    format!(
                        "incomplete legacy ALTER TABLE COPY job detected at startup ({})",
                        path.display()
                    ),
                )
            })?;
            if intent.version != 2 {
                return Err(error(
                    crate::error::catalog::ER_UNKNOWN_ERROR,
                    format!("unsupported ALTER COPY intent version {}", intent.version),
                ));
            }
            let committed = self.table_by_id(intent.table_id).is_some_and(|table| {
                if intent.new_generation == 0 {
                    table.schema_version == intent.new_schema_version
                } else {
                    table.schema_version >= intent.new_schema_version
                        && table.active_generation.max(1) >= intent.new_generation
                }
            });
            recover_generation_files(
                &self.data_dir,
                &intent.old_to_backup,
                &intent.shadow_to_live,
                committed,
                true,
                "invalid path in ALTER COPY intent",
            )?;
            self.finish_shadow_generation_intent(&path)?;
        }
        Ok(())
    }

    fn spawn_alter_copy_job(
        self: &Arc<Self>,
        table_name: String,
        old_table: Arc<TableSchema>,
        candidate: TableSchema,
        strict: bool,
        lock: crate::parser::AlterLock,
        statement_mdl_owner: u64,
    ) -> Result<(), QueryError> {
        let _barrier_guard = OnlineIndexMdlGuard {
            db: self,
            owner: statement_mdl_owner,
        };
        if old_table.is_partitioned() {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "ALTER TABLE COPY rewrite for partitioned tables is not supported yet",
            ));
        }
        let job = self.register_alter_copy_job(&old_table);
        let old_table_id = old_table.id;
        let result = self.rewrite_table_for_alter_copy_online_worker(
            &table_name,
            old_table,
            candidate,
            strict,
            lock,
            job,
            statement_mdl_owner,
        );
        drop(_barrier_guard);
        self.unregister_alter_copy_job(old_table_id, result.as_ref().err());
        result
    }

    fn rewrite_table_for_alter_copy_online_worker(
        &self,
        table_name: &str,
        old_table: Arc<TableSchema>,
        mut candidate: TableSchema,
        strict: bool,
        lock: crate::parser::AlterLock,
        job: Arc<AlterCopyJob>,
        background_owner: u64,
    ) -> Result<(), QueryError> {
        let _binlog = crate::binlog::SessionSqlLogBinGuard::new(false);
        self.online_index_barrier_checked(
            &old_table,
            background_owner,
            false,
            MdlLockType::Exclusive,
            || Self::check_alter_copy_cancelled(&job),
        )?;
        candidate.reset_row_layouts_for_rewrite();
        candidate.build_column_index();
        self.rewrite_table_for_alter_copy_streaming_direct(
            table_name,
            old_table,
            candidate,
            strict,
            Some(job),
            Some((background_owner, lock)),
        )?;
        Ok(())
    }

    fn prepare_alter_copy_values(
        &self,
        candidate: &TableSchema,
        mut values: Vec<Value>,
        strict: bool,
        warnings: &mut Vec<crate::engine::api::SqlWarning>,
    ) -> Result<Vec<Value>, QueryError> {
        while values.len() < candidate.columns.len() {
            let column = &candidate.columns[values.len()];
            values.push(crate::engine::executor::insert::column_read_backfill_default(column));
        }
        coerce_row_for_alter_copy(candidate, &mut values, strict, warnings)?;
        materialize_generated_columns(candidate, &mut values, None)?;
        self.check_not_null_constraints(candidate, &values)?;
        self.check_check_constraints(candidate, &values, None)?;
        Ok(values)
    }

    fn prepare_alter_copy_values_masked(
        &self,
        candidate: &TableSchema,
        mut values: Vec<Value>,
        strict: bool,
        warnings: &mut Vec<crate::engine::api::SqlWarning>,
        coercion_mask: &[bool],
        defaults: &[(usize, Expr)],
    ) -> Result<Vec<Value>, QueryError> {
        while values.len() < candidate.columns.len() {
            let column = &candidate.columns[values.len()];
            values.push(crate::engine::executor::insert::column_read_backfill_default(column));
        }
        for (idx, expr) in defaults {
            values[*idx] = crate::engine::expr::eval_expr(expr, candidate, &values)?;
        }
        coerce_row_for_alter_copy_masked(
            candidate,
            &mut values,
            strict,
            warnings,
            Some(coercion_mask),
        )?;
        materialize_generated_columns(candidate, &mut values, None)?;
        self.check_not_null_constraints(candidate, &values)?;
        self.check_check_constraints(candidate, &values, None)?;
        Ok(values)
    }

    fn prepare_alter_copy_projected_values_in_place(
        &self,
        candidate: &TableSchema,
        values: &mut Vec<Value>,
        strict: bool,
        warnings: &mut Vec<crate::engine::api::SqlWarning>,
        coercion_mask: &[bool],
    ) -> Result<(), QueryError> {
        coerce_row_for_alter_copy_masked(candidate, values, strict, warnings, Some(coercion_mask))?;
        for (idx, column) in candidate.columns.iter().enumerate() {
            if coercion_mask.get(idx).copied().unwrap_or(true)
                && !column.nullable
                && matches!(values.get(idx), Some(Value::Null))
            {
                return Err(crate::error::messages::bad_null(&column.name));
            }
        }
        Ok(())
    }

    fn insert_alter_copy_rewritten_rows(
        &self,
        txn: TxnView,
        table: &TableSchema,
        rows: Vec<Vec<Value>>,
    ) -> Result<(), QueryError> {
        let row_headers = vec![RowHeader::visible(txn.txn_id); rows.len()];
        let inserted = self.insert_mvcc_batch_externalized(
            txn.txn_id,
            table.id,
            &table.row_codec,
            &row_headers,
            &rows,
        )?;
        self.lock_rows_nonblocking(txn.txn_id, table.id, inserted.iter().copied())?;
        self.push_insert_batch_undo_deferred(txn.txn_id, table.id, &inserted)?;
        let mut index_entries = table
            .indexes
            .iter()
            .map(|index| (index.name.clone(), Vec::new()))
            .collect::<BTreeMap<_, Vec<(IndexKey, RowId)>>>();
        for (values, row_id) in rows.iter().zip(inserted.iter().copied()) {
            for index in &table.indexes {
                let entries = index_entries
                    .get_mut(&index.name)
                    .expect("index entry batch should exist");
                for key in table.index_keys(index, values)? {
                    entries.push((key, row_id));
                }
            }
            self.note_vector_inserts(table, values, row_id, table.id);
        }
        self.insert_index_batches_deferred(txn.txn_id, table, index_entries)?;
        crate::binlog::record_write_rows(txn.txn_id, table, rows)?;
        Ok(())
    }

    pub fn rename_column(
        &self,
        table_name: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<(), QueryError> {
        validate_mysql_identifier(new_name)?;
        let (database, table_only) = split_table_name(DEFAULT_DATABASE, table_name);
        check_table_ddl_writable(&database)?;
        let key = table_key(&database, &table_only);
        let source = self.table(table_name)?;
        let dependent_keys = source
            .fk_referenced_by
            .iter()
            .map(|edge| edge.child_table_key.clone())
            .collect::<BTreeSet<_>>();
        let _dependent_definitions = dependent_keys
            .iter()
            .map(|name| self.pin_ddl_table(name))
            .collect::<Result<Vec<_>, _>>()?;
        let mut changed_table_ids = Vec::new();
        {
            let mut catalog = self.catalog_write()?;
            let table_arc = catalog
                .tables
                .get_mut(&key)
                .ok_or_else(|| ddl_table_not_found(&table_name))?;
            let table = Arc::make_mut(table_arc);
            let pos = table
                .columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(old_name))
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_BAD_FIELD_ERROR,
                        format!("unknown column '{old_name}'"),
                    )
                })?;
            let old_col_name = table.columns[pos].name.clone();
            // S7 — reject renaming to a name another column already uses
            // (1060). A single-action `RENAME COLUMN` skips the multi-action
            // pre-flight in `validate_alter_table_actions`, so without this
            // check it silently produced two columns with the same name.
            if !old_col_name.eq_ignore_ascii_case(new_name)
                && table
                    .columns
                    .iter()
                    .any(|c| c.name.eq_ignore_ascii_case(new_name))
            {
                return Err(error(
                    crate::error::catalog::ER_DUP_FIELDNAME,
                    format!("duplicate column name '{new_name}'"),
                ));
            }
            // Block rename when a functional index references the
            // column — the persisted expression SQL still names the
            // old column, and silently rewriting expressions is
            // fragile. MySQL surfaces error 3855 here; we mirror that.
            let old_lower = old_col_name.to_ascii_lowercase();
            if table.indexes.iter().any(|idx| {
                idx.key_expressions
                    .iter()
                    .flatten()
                    .any(|expr| expr.referenced_columns.iter().any(|c| c == &old_lower))
            }) {
                return Err(error(crate::error::catalog::ER_DEPENDENT_BY_PARTITION_FUNC,
                    format!(
                        "column '{old_col_name}' has a functional index dependency and cannot be renamed",
                    ),
                ));
            }
            table.columns[pos].name = new_name.to_string();
            for pk in &mut table.primary_key {
                if pk.eq_ignore_ascii_case(&old_col_name) {
                    *pk = new_name.to_string();
                }
            }
            for index in &mut table.indexes {
                for (col_pos, col) in index.columns.iter_mut().enumerate() {
                    // Only rewrite plain column parts; expression parts
                    // hold expression SQL in `columns[col_pos]`.
                    if index
                        .key_expressions
                        .get(col_pos)
                        .and_then(Option::as_ref)
                        .is_none()
                        && col.eq_ignore_ascii_case(&old_col_name)
                    {
                        *col = new_name.to_string();
                    }
                }
            }
            for index in &mut table.indexes {
                for column in &mut index.included_columns {
                    if column.eq_ignore_ascii_case(&old_col_name) {
                        *column = new_name.to_string();
                    }
                }
            }
            // Propagate the rename into this table's own outbound foreign
            // keys (child-side columns). MySQL 8.0 updates FK definitions
            // when a participating column is renamed; without this the FK's
            // `columns` string dangles, `build_fk_runtime`'s `column_index`
            // lookup silently fails, and enforcement breaks (plus the
            // persisted DD keeps the stale name across restart).
            for fk in &mut table.foreign_keys {
                for col in &mut fk.columns {
                    if col.eq_ignore_ascii_case(&old_col_name) {
                        *col = new_name.to_string();
                    }
                }
            }
            rename_check_constraint_column_references(table, &old_col_name, new_name)?;
            rename_generated_column_references(table, &old_col_name, new_name)?;
            // P1-11: propagate the rename into PARTITION BY / SUBPARTITION BY.
            // Previously `partitioning.columns` and `expr_sql` kept the old
            // name, so SHOW CREATE TABLE printed a stale definition, the
            // runtime router failed to resolve the column, and the persisted
            // expression could not reparse against the renamed columns.
            rename_partition_column_references(table, &old_col_name, new_name)?;
            validate_and_prepare_generated_columns(table)?;
            table.build_column_index();
            table.bump_schema_version();
            changed_table_ids.push(table.id);
            // If this table is referenced as a parent, other tables' FK
            // `referenced_columns` name the just-renamed column and must be
            // updated in lock-step. Stable column IDs/positions preserve every
            // resolved FK index. Touch only reverse dependencies; rebuilding
            // the entire catalog would erase edges owned by cold headers.
            let db_norm = normalize_name(&database);
            let tbl_norm = normalize_name(&table_only);
            for dependent in &dependent_keys {
                let Some(other_arc) = catalog.tables.get_mut(dependent) else {
                    continue;
                };
                let other = Arc::make_mut(other_arc);
                let mut changed = false;
                for fk in &mut other.foreign_keys {
                    if normalize_name(&fk.referenced_database) == db_norm
                        && normalize_name(&fk.referenced_table) == tbl_norm
                    {
                        for col in &mut fk.referenced_columns {
                            if col.eq_ignore_ascii_case(&old_col_name) {
                                *col = new_name.to_string();
                                changed = true;
                            }
                        }
                    }
                }
                if changed {
                    other.bump_schema_version();
                    changed_table_ids.push(other.id);
                }
            }
        }
        for table_id in changed_table_ids {
            self.note_catalog_table(table_id);
        }
        self.commit_ddl_catalog_txn()
    }

    /// Multi-pair `RENAME TABLE a TO b, c TO d, …`. Each pair is applied in
    /// declaration order against the in-memory catalog, and the single durable
    /// catalog transaction is deferred to the very end, so the whole statement
    /// commits atomically at the durability boundary (one DD generation bump
    /// for all pairs) rather than one commit per pair. A swap
    /// `a TO tmp, b TO a, tmp TO b` therefore can no longer leave a
    /// half-committed pair persisted if a later pair fails: on any error every
    /// already-applied pair is reversed in memory, and
    /// because nothing was flushed yet the persisted data dictionary is still
    /// pristine. Physical paths and handles retain their immutable object IDs;
    /// a logical rename never moves files or flushes unrelated dirty data.
    pub fn rename_tables_atomic(&self, pairs: &[(String, String)]) -> Result<(), QueryError> {
        let mut definitions = Vec::new();
        let mut seen = HashSet::new();
        for (name, _) in pairs {
            if let Some(table) = self.table_by_key(&normalize_table_key(DEFAULT_DATABASE, name)) {
                for key in std::iter::once(table.qualified_name()).chain(
                    table
                        .fk_referenced_by
                        .iter()
                        .map(|fk| fk.child_table_key.clone()),
                ) {
                    if seen.insert(key.clone()) {
                        definitions.push(self.pin_ddl_table(&key)?);
                    }
                }
            }
        }
        // `done`: (current_new_name, original_old_name) — reverse-rename each
        // on rollback. `renamed`: (old_database, old_table) of every applied
        // pair, for post-commit dependent-view invalidation.
        let mut done: Vec<(String, String, Option<PathBuf>)> = Vec::with_capacity(pairs.len());
        let mut renamed: Vec<(String, String)> = Vec::with_capacity(pairs.len());
        for (old, new) in pairs {
            match self.rename_one(old, new, false) {
                Ok((old_db, old_tbl, intent_path)) => {
                    done.push((new.clone(), old.clone(), intent_path));
                    renamed.push((old_db, old_tbl));
                }
                Err(err) => {
                    // Nothing has been flushed yet, so the persisted data
                    // dictionary is still pristine. Reverse every applied pair
                    // in memory so the in-memory
                    // catalog matches disk again, then propagate.
                    for (current_new, original_old, intent_path) in done.into_iter().rev() {
                        if let Ok((_, _, reverse_intent)) =
                            self.rename_one(&current_new, &original_old, false)
                        {
                            self.remove_rename_table_intent(reverse_intent.as_ref());
                        }
                        self.remove_rename_table_intent(intent_path.as_ref());
                    }
                    return Err(err);
                }
            }
        }
        // Single durable commit for all pairs.
        self.commit_ddl_catalog_txn()?;
        for (_, _, intent_path) in &done {
            self.remove_rename_table_intent(intent_path.as_ref());
        }
        // Invalidate views that referenced a name which no longer resolves to
        // any table/view after the whole statement. A name freed and then
        // re-occupied within the same statement (the swap idiom) still exists,
        // so its dependent views stay valid — matching MySQL, which only fails
        // a view lazily when its referent is truly gone.
        for (old_db, old_tbl) in renamed {
            let still_exists = {
                let catalog = self.catalog_read()?;
                let key = table_key(&old_db, &old_tbl);
                catalog.tables.contains_key(&key) || catalog.views.contains_key(&key)
            };
            if !still_exists {
                let _ = self.invalidate_views_referencing(&old_db, &old_tbl);
            }
        }
        Ok(())
    }

    /// Single-object `RENAME TABLE old TO new` / `ALTER TABLE old RENAME TO
    /// new`. Persists immediately and invalidates dependent views.
    pub fn rename_table(&self, table_name: &str, new_name: &str) -> Result<(), QueryError> {
        self.rename_tables_atomic(&[(table_name.to_owned(), new_name.to_owned())])
    }

    /// Rename one base table or view. `flush` controls whether the change is
    /// persisted here (single-object callers pass true; the multi-pair driver
    /// passes false and flushes once after all pairs). Returns the renamed
    /// object's `(old_database, old_table)` so callers can invalidate
    /// dependent views. Does *not* itself invalidate views.
    fn rename_one(
        &self,
        table_name: &str,
        new_name: &str,
        flush: bool,
    ) -> Result<(String, String, Option<PathBuf>), QueryError> {
        let (old_database, old_table) = split_table_name(DEFAULT_DATABASE, table_name);
        let (new_database, new_table) = split_table_name(&old_database, new_name);
        validate_mysql_identifier(&new_database)?;
        validate_mysql_identifier(&new_table)?;
        if table_ddl_schema_is_read_only(&old_database)
            || table_ddl_schema_is_read_only(&new_database)
        {
            return Err(error(
                crate::error::catalog::ER_DBACCESS_DENIED_ERROR,
                "access denied for system database",
            ));
        }
        let old_key = table_key(&old_database, &old_table);
        let new_key = table_key(&new_database, &new_table);
        // Classify the source and run every pre-flight check under a read
        // lock. Order matches MySQL: a missing source (1146) is reported
        // before an occupied target (1050). `RENAME TABLE` operates on both
        // base tables and views (8.0 allows renaming a view via RENAME TABLE);
        // a sequence is not a base table, so it earns ER_WRONG_OBJECT (1347).
        enum RenameKind {
            Table {
                id: u64,
                index_names: Vec<String>,
                // Every physical storage id backing the table. For a
                // partitioned table these are the per-partition leaf ids
                // that own the actual heap/index files (named
                // `<owner>__<storage_id>__…`); a non-partitioned table
                // has a single storage id equal to `id`.
                storage_ids: Vec<u64>,
            },
            View,
        }
        let kind = {
            let catalog = self.catalog_read()?;
            let kind = if let Some(table) = catalog.tables.get(&old_key) {
                self.check_ddl_table_lock(table.id)?;
                RenameKind::Table {
                    id: table.id,
                    index_names: table.indexes.iter().map(|i| i.name.clone()).collect(),
                    storage_ids: table.storage_ids(),
                }
            } else if catalog.views.contains_key(&old_key) {
                if old_database != new_database {
                    return Err(error(
                        crate::error::catalog::ER_FORBID_SCHEMA_CHANGE,
                        format!("Changing schema from '{old_database}' to '{new_database}' is not allowed"),
                    ));
                }
                RenameKind::View
            } else if catalog.sequences.contains_key(&old_key) {
                return Err(error(
                    crate::error::catalog::ER_WRONG_OBJECT,
                    format!("'{old_database}.{old_table}' is not of type 'BASE TABLE'"),
                ));
            } else {
                return Err(ddl_table_not_found(&table_name));
            };
            if !catalog.databases.contains_key(&new_database) {
                return Err(error(
                    crate::error::catalog::ER_BAD_DB_ERROR,
                    format!("unknown database '{new_database}'"),
                ));
            }
            if catalog.tables.contains_key(&new_key)
                || catalog.views.contains_key(&new_key)
                || catalog.sequences.contains_key(&new_key)
            {
                return Err(error(
                    crate::error::catalog::ER_TABLE_EXISTS_ERROR,
                    format!("Table '{new_name}' already exists"),
                ));
            }
            // A3 — MySQL forbids moving a table that owns triggers to a
            // different schema: a trigger lives in the same schema as its
            // table, so a cross-db rename would strand it. Reject with 1435
            // rather than silently leaving `trigger.database` inconsistent.
            if matches!(kind, RenameKind::Table { .. })
                && old_database != new_database
                && catalog.triggers.values().any(|trigger| {
                    normalize_name(&trigger.table_database) == old_database
                        && normalize_name(&trigger.table_name) == old_table
                })
            {
                return Err(error(
                    crate::error::catalog::ER_TRG_IN_WRONG_SCHEMA,
                    "Trigger in wrong schema",
                ));
            }
            kind
        };
        // A view has no sidecar/index files — a pure catalog rename. Renaming
        // it may strand *other* views that depend on it, which the caller
        // invalidates lazily (matching MySQL).
        let (table_id, index_names, storage_ids) = match kind {
            RenameKind::View => {
                {
                    let mut catalog = self.catalog_write()?;
                    if catalog.tables.contains_key(&new_key)
                        || catalog.views.contains_key(&new_key)
                        || catalog.sequences.contains_key(&new_key)
                    {
                        return Err(error(
                            crate::error::catalog::ER_TABLE_EXISTS_ERROR,
                            format!("Table '{new_name}' already exists"),
                        ));
                    }
                    let Some(mut view) = catalog.views.remove(&old_key) else {
                        return Err(ddl_table_not_found(&table_name));
                    };
                    view.database = new_database.clone();
                    view.name = new_table.clone();
                    view.bump_schema_version();
                    catalog.views.insert(new_key.clone(), view);
                }
                self.note_catalog_view(&old_database, &old_table);
                self.note_catalog_view(&new_database, &new_table);
                if flush {
                    self.commit_ddl_catalog_txn()?;
                }
                return Ok((old_database, old_table, None));
            }
            RenameKind::Table {
                id,
                index_names,
                storage_ids,
            } => (id, index_names, storage_ids),
        };
        // Storage paths are owned by immutable IDs/generations, never names.
        // Renaming is a catalog transaction, with no data flush or file move.
        let _ = (index_names, storage_ids);
        let rename_intent_path: Option<PathBuf> = None;
        // Multi-pair RENAME changes live names before the ID/name cache is
        // published. Resolve its already-pinned descriptor from this batch.
        let source = self
            .catalog_read()?
            .tables
            .get(&old_key)
            .cloned()
            .ok_or_else(|| ddl_table_not_found(table_name))?;
        let dependent_keys = source
            .fk_referenced_by
            .iter()
            .map(|edge| edge.child_table_key.clone())
            .chain(
                source
                    .foreign_keys
                    .iter()
                    .map(|fk| table_key(&fk.referenced_database, &fk.referenced_table)),
            )
            .collect::<BTreeSet<_>>();
        let _dependent_definitions = dependent_keys
            .iter()
            .filter(|key| *key != &old_key)
            .map(|name| self.pin_ddl_table(name))
            .collect::<Result<Vec<_>, _>>()?;
        let mut changed_table_ids = vec![table_id];
        let mut changed_triggers = Vec::new();
        {
            let mut catalog = self.catalog_write()?;
            // Re-check: nothing should have changed between the read
            // above and here under DDL serialization, but a paranoid
            // check is cheap and produces a clear error if it did.
            if catalog.tables.contains_key(&new_key)
                || catalog.views.contains_key(&new_key)
                || catalog.sequences.contains_key(&new_key)
            {
                // Reject the occupied target before changing either name.
                self.remove_rename_table_intent(rename_intent_path.as_ref());
                return Err(error(
                    crate::error::catalog::ER_TABLE_EXISTS_ERROR,
                    format!("Table '{new_name}' already exists"),
                ));
            }
            let Some(table_arc) = catalog.tables.remove(&old_key) else {
                self.remove_rename_table_intent(rename_intent_path.as_ref());
                return Err(ddl_table_not_found(&table_name));
            };
            let mut table = (*table_arc).clone();
            table.database = new_database.clone();
            table.name = new_table.clone();
            table.bump_schema_version();
            catalog.tables.insert(new_key.clone(), Arc::new(table));
            for trigger in catalog.triggers.values_mut() {
                if normalize_name(&trigger.table_database) == old_database
                    && normalize_name(&trigger.table_name) == old_table
                {
                    trigger.table_database = new_database.clone();
                    trigger.table_name = new_table.clone();
                    trigger.bump_schema_version();
                    changed_triggers.push((trigger.database.clone(), trigger.name.clone()));
                }
            }
            for dependent in &dependent_keys {
                let dependent = if dependent == &old_key {
                    &new_key
                } else {
                    dependent
                };
                let Some(table_arc) = catalog.tables.get_mut(dependent) else {
                    continue;
                };
                let table = Arc::make_mut(table_arc);
                let mut changed = false;
                for edge in &mut table.fk_referenced_by {
                    if edge.child_table_id == table_id {
                        edge.child_table_key = new_key.clone();
                        changed = true;
                    }
                }
                for fk in &mut table.foreign_keys {
                    if normalize_name(&fk.referenced_database) == old_database
                        && normalize_name(&fk.referenced_table) == old_table
                    {
                        fk.referenced_database = new_database.clone();
                        fk.referenced_table = new_table.clone();
                        changed = true;
                    }
                }
                if changed {
                    table.bump_schema_version();
                    changed_table_ids.push(table.id);
                }
            }
        }
        changed_table_ids.sort_unstable();
        changed_table_ids.dedup();
        for changed in changed_table_ids {
            self.note_catalog_table(changed);
        }
        for (schema, trigger) in changed_triggers {
            self.note_catalog_trigger(&schema, &trigger);
        }
        if flush {
            self.commit_ddl_catalog_txn()?;
            self.remove_rename_table_intent(rename_intent_path.as_ref());
            return Ok((old_database, old_table, None));
        }
        Ok((old_database, old_table, rename_intent_path))
    }
}

/// Normalize and validate the `INCLUDE (...)` columns of an inline /
/// ALTER secondary index in a CREATE TABLE. INCLUDE is only legal on a
/// non-unique BTREE key; the columns must exist and must not duplicate
/// the declared key columns. Returns the normalized include list (empty
/// when the index declared none). The ALTER `add_index` path enforces the
/// same rules independently in `validate_index_definition`.
/// Validate the schema-derived fixed row footprint before publication. Large
/// vector payloads use varlen/overflow lanes; remaining fixed lanes must fit.
/// MySQL restricts `BIT(M)` to `1 <= M <= 64`; a wider declaration is
/// `ERROR 1439 (42000): Display width out of range for column '<c>' (max = 64)`.
/// mydb previously accepted any width and then *panicked* on the first
/// INSERT (the row codec assumes `ceil(M/8) <= 8` bytes). Rejecting at
/// CREATE/ALTER closes both the compatibility gap and the DoS. `BIT(0)` is
/// invalid with ER_INVALID_FIELD_SIZE (3013); a bare `BIT` (len `None`) means
/// `BIT(1)`.
fn validate_bit_widths(table: &TableSchema) -> Result<(), QueryError> {
    for col in &table.columns {
        if let DataTypeKind::Bit { len: Some(n) } = col.data_type {
            if n == 0 {
                return Err(error(
                    crate::error::catalog::ER_INVALID_FIELD_SIZE,
                    format!("Invalid size for column '{}'", col.name),
                ));
            }
            if n > 64 {
                return Err(error(
                    crate::error::catalog::ER_TOO_BIG_DISPLAYWIDTH,
                    format!(
                        "Display width out of range for column '{}' (max = 64)",
                        col.name
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// Reject two indexes that resolve to the same (case-insensitive) name,
/// including the reserved PRIMARY — MySQL errors 1061. CREATE must enforce
/// this like ALTER's `add_index`: two indexes with the same name map to the
/// same `<table>__<id>__<index>.idx` file (and the same cache slot) and would
/// silently alias / corrupt one another.
fn reject_duplicate_index_names(index_schemas: &[IndexSchema]) -> Result<(), QueryError> {
    let mut seen = std::collections::HashSet::with_capacity(index_schemas.len());
    for idx in index_schemas {
        if !seen.insert(normalize_name(&idx.name)) {
            return Err(error(
                crate::error::catalog::ER_DUP_KEYNAME,
                format!("duplicate index '{}'", idx.name),
            ));
        }
    }
    Ok(())
}

fn validate_row_fits_page(table: &TableSchema) -> Result<(), QueryError> {
    const ROW_FIT_BUDGET: usize = PAGE_SIZE - PAGE_HEADER_SIZE - SLOT_SIZE;
    let footprint = table.row_codec.min_fixed_row_footprint();
    if footprint <= ROW_FIT_BUDGET {
        return Ok(());
    }
    Err(error(
        crate::error::catalog::ER_TOO_BIG_ROWSIZE,
        format!(
            "row of fixed columns ({footprint} bytes + header) exceeds the single-page \
             limit ({ROW_FIT_BUDGET} bytes); reduce fixed-width column sizes"
        ),
    ))
}

fn validate_inline_included_columns(
    included: &[String],
    index_type: &str,
    unique: bool,
    primary: bool,
    key_columns: &[String],
    column_schemas: &[ColumnSchema],
) -> Result<Vec<String>, QueryError> {
    if included.is_empty() {
        return Ok(Vec::new());
    }
    if unique || primary {
        return Err(error(
            crate::error::catalog::ER_PARSE_ERROR,
            "UNIQUE INDEX cannot declare INCLUDE columns",
        ));
    }
    if !index_type.eq_ignore_ascii_case("BTREE") {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            "INCLUDE columns are only supported on BTREE indexes",
        ));
    }
    let normalized: Vec<String> = included.iter().map(|c| normalize_name(c)).collect();
    validate_columns(&normalized, column_schemas)?;
    for inc in &normalized {
        if key_columns.iter().any(|key| key == inc) {
            return Err(error(
                crate::error::catalog::ER_DUP_FIELDNAME,
                format!("column '{inc}' appears in both the index key and INCLUDE list"),
            ));
        }
    }
    Ok(normalized)
}

fn resolve_table_charset_collation(
    explicit_charset: Option<&str>,
    explicit_collation: Option<&str>,
    database_charset: &str,
    database_collation: &str,
) -> Result<(String, String), QueryError> {
    let collation_info = explicit_collation
        .map(crate::charset::resolve_collation)
        .transpose()?;
    let charset_name = match explicit_charset {
        Some(charset) => crate::charset::resolve_charset(charset)?.name,
        None => collation_info
            .map(|collation| collation.charset)
            .unwrap_or(database_charset),
    };
    let collation_name = match collation_info {
        Some(collation) => collation.name,
        None if explicit_charset.is_some() => {
            crate::charset::default_collation_for_charset(charset_name)?
        }
        None => database_collation,
    };
    crate::charset::validate_charset_collation_pair(charset_name, collation_name)?;
    Ok((charset_name.to_string(), collation_name.to_string()))
}

fn apply_column_charset_for_alter(
    column: &mut crate::storage::ColumnSchema,
    existing: &crate::storage::ColumnSchema,
    table_charset: &str,
    table_collation: &str,
) -> Result<(), QueryError> {
    if is_character_column_kind(&column.data_type)
        && column.charset.is_none()
        && column.collation.is_none()
    {
        column.charset = existing.charset.clone();
        column.collation = existing.collation.clone();
    }
    apply_column_charset_defaults(column, table_charset, table_collation)
}

fn apply_column_charset_defaults(
    column: &mut crate::storage::ColumnSchema,
    table_charset: &str,
    table_collation: &str,
) -> Result<(), QueryError> {
    if !is_character_column_kind(&column.data_type) {
        if column.charset.is_some() || column.collation.is_some() {
            return Err(error(
                crate::error::catalog::ER_COLLATION_CHARSET_MISMATCH,
                format!(
                    "CHARACTER SET or COLLATE is not valid for non-character column '{}'",
                    column.name
                ),
            ));
        }
        column.charset = None;
        column.collation = None;
        return Ok(());
    }

    let explicit_charset = column.charset.as_deref();
    let binary_attribute =
        column.collation.as_deref() == Some(crate::parser::BINARY_ATTRIBUTE_COLLATION);
    let collation_info = column
        .collation
        .as_deref()
        .filter(|_| !binary_attribute)
        .map(crate::charset::resolve_collation)
        .transpose()?;
    let charset_name = match explicit_charset {
        Some(charset) => crate::charset::resolve_charset(charset)?.name,
        None => collation_info
            .map(|collation| collation.charset)
            .unwrap_or(table_charset),
    };
    let binary_collation;
    let collation_name = match collation_info {
        Some(collation) => collation.name,
        None if binary_attribute => {
            binary_collation = if charset_name.eq_ignore_ascii_case("binary") {
                "binary".to_string()
            } else {
                format!("{charset_name}_bin")
            };
            crate::charset::resolve_collation(&binary_collation)?.name
        }
        None if explicit_charset.is_some() => {
            crate::charset::default_collation_for_charset(charset_name)?
        }
        None => table_collation,
    };
    crate::charset::validate_charset_collation_pair(charset_name, collation_name)?;
    // MySQL normalizes CHARACTER SET binary to a byte-string domain.
    // Do this before catalog persistence, so storage, comparisons and SHOW
    // all see the same type (also for ALTER and table charset inheritance).
    if charset_name.eq_ignore_ascii_case("binary") {
        let binary_type = match column.data_type {
            DataTypeKind::Char { len } => Some(DataTypeKind::Binary { len }),
            DataTypeKind::VarChar { len } => Some(DataTypeKind::VarBinary { len }),
            DataTypeKind::Text { max_len } => Some(DataTypeKind::Blob { max_len }),
            _ => None,
        };
        if let Some(data_type) = binary_type {
            column.data_type = data_type;
            column.charset = None;
            column.collation = None;
            return Ok(());
        }
    }
    column.charset = Some(charset_name.to_string());
    column.collation = Some(collation_name.to_string());
    enforce_varchar_encoded_width(column, charset_name)?;
    Ok(())
}

/// Validate VARCHAR's 16-bit encoded byte domain after table-level charset
/// inheritance has been resolved. Column conversion cannot decide this when
/// the declaration omits an explicit charset (`VARCHAR(65500) CHARSET
/// latin1` is legal, while the same width under utf8mb4 is not).
fn enforce_varchar_encoded_width(
    column: &mut crate::storage::ColumnSchema,
    charset_name: &str,
) -> Result<(), QueryError> {
    let DataTypeKind::VarChar { len: Some(length) } = column.data_type else {
        return Ok(());
    };
    let max_bytes = (length as u64).saturating_mul(u64::from(
        crate::charset::resolve_charset(charset_name)?.max_len,
    ));
    if max_bytes <= 65_535 {
        return Ok(());
    }
    if crate::storage::core::coercion_strict() {
        return Err(error(
            crate::error::catalog::ER_TOO_BIG_FIELDLENGTH,
            format!(
                "Column length too big for column '{}' (max = 65535); use BLOB or TEXT instead",
                column.name
            ),
        ));
    }
    let max_len = match max_bytes {
        0..=255 => crate::storage::schema::TINY_LOB_MAX,
        256..=65_535 => crate::storage::schema::DEFAULT_LOB_MAX,
        65_536..=16_777_215 => crate::storage::schema::MEDIUM_LOB_MAX,
        _ => crate::storage::schema::LONG_LOB_MAX,
    };
    crate::engine::api::push_statement_warning(
        crate::error::ConditionLevel::Note,
        crate::error::catalog::ER_AUTO_CONVERT,
        format!("Converting column '{}' from VARCHAR to TEXT", column.name),
    );
    column.data_type = DataTypeKind::Text { max_len };
    Ok(())
}

fn is_character_column_kind(data_type: &DataTypeKind) -> bool {
    matches!(
        data_type,
        DataTypeKind::Char { .. }
            | DataTypeKind::VarChar { .. }
            | DataTypeKind::Text { .. }
            | DataTypeKind::Enum { .. }
            | DataTypeKind::Set { .. }
            | DataTypeKind::Json
    )
}

/// Overwrite a column slot's declarable attributes from a new
/// `column_def_to_schema` result while preserving its stable identity
/// (`column_id`, `physical_pos`, `logical_pos`, `state`). The
/// `column_def_to_schema` builder fills the identity fields with
/// zeros (it is used by both ALTER ADD/MODIFY/CHANGE and CREATE
/// TABLE), so we cannot just assign `slot = new` — that would clear
/// the identity allocated when the column was first created and
/// break already-stored rows that index by physical_pos.
fn column_change_requires_copy(old: &ColumnSchema, new: &ColumnSchema) -> bool {
    column_change_requires_validation(old, new) || column_change_requires_physical_copy(old, new)
}

fn column_change_requires_validation(old: &ColumnSchema, new: &ColumnSchema) -> bool {
    (old.nullable && !new.nullable)
        || (!old.unsigned && new.unsigned)
        || matches!((&old.data_type, &new.data_type),
        (DataTypeKind::Decimal { precision: old_p, .. }, DataTypeKind::Decimal { precision: new_p, .. }) if new_p < old_p)
        || match (&old.data_type, &new.data_type) {
            (
                DataTypeKind::VarBinary { len: old_len },
                DataTypeKind::VarBinary { len: new_len },
            ) => optional_len_decreases(*old_len, *new_len),
            (DataTypeKind::Text { max_len: old_len }, DataTypeKind::Text { max_len: new_len })
            | (DataTypeKind::Blob { max_len: old_len }, DataTypeKind::Blob { max_len: new_len }) => {
                new_len < old_len
            }
            _ => false,
        }
}

fn column_change_is_validation_only(old: &ColumnSchema, new: &ColumnSchema) -> bool {
    column_change_requires_validation(old, new) && !column_change_requires_physical_copy(old, new)
}

fn column_change_requires_physical_copy(old: &ColumnSchema, new: &ColumnSchema) -> bool {
    // This ALTER materializes a new timestamp into historical NULL rows. It
    // cannot use validation-only publication even when the encoding is equal.
    if old.nullable
        && !new.nullable
        && matches!(new.data_type, DataTypeKind::Timestamp { .. })
        && (new.default.is_some() || new.default_expr.is_some())
    {
        return true;
    }
    // ENUM/SET member-list APPEND is a pure metadata change when the
    // storage width stays the same (MySQL 8.0 INSTANT ADDition). Old
    // rows keep their ordinal / bitmask verbatim — the new members
    // never appear until fresh DML writes them — so we can skip the
    // whole-table COPY. Any other kind of change (reorder, rename,
    // shrink) still falls through to the strict `column_kind_for`
    // inequality check and forces a COPY.
    if is_enum_set_instant_append(&old.data_type, &new.data_type)
        && old.generated_expr == new.generated_expr
        && old.generated_stored == new.generated_stored
        && old.charset == new.charset
        && old.collation == new.collation
    {
        return false;
    }
    if column_kind_for(&old.data_type, old.unsigned)
        != column_kind_for(&new.data_type, new.unsigned)
    {
        return true;
    }
    if old.generated_expr != new.generated_expr || old.generated_stored != new.generated_stored {
        return true;
    }
    // S3 — a charset/collation change on a character column alters the
    // sort order baked into every B-tree key that references it
    // (`KeyPart::CollatedString` embeds the collation id). The
    // metadata-only INSTANT path would leave those keys encoded under the
    // old collation, silently corrupting index lookups. Force a COPY
    // rewrite (which drops + rebuilds all indexes) whenever the effective
    // charset or collation differs. Must run before the length-only fast
    // paths below, which short-circuit on equal lengths and would
    // otherwise mask a pure collation change. `apply_column_charset_for_alter`
    // has already resolved an omitted COLLATE to the existing column's
    // value, so an unrelated MODIFY does not spuriously trip this.
    if is_character_column_kind(&new.data_type)
        && (old.charset != new.charset || old.collation != new.collation)
    {
        return true;
    }
    match (&old.data_type, &new.data_type) {
        (DataTypeKind::VarChar { len: old_len }, DataTypeKind::VarChar { len: new_len })
        | (DataTypeKind::Char { len: old_len }, DataTypeKind::Char { len: new_len })
        | (DataTypeKind::Binary { len: old_len }, DataTypeKind::Binary { len: new_len }) => {
            return optional_len_decreases(*old_len, *new_len);
        }
        // These bounds never change bytes when validation succeeds. VARCHAR
        // stays on COPY because shrinking can legally trim trailing spaces.
        (DataTypeKind::VarBinary { .. }, DataTypeKind::VarBinary { .. })
        | (DataTypeKind::Text { .. }, DataTypeKind::Text { .. })
        | (DataTypeKind::Blob { .. }, DataTypeKind::Blob { .. }) => return false,
        (
            DataTypeKind::Float {
                precision: old_p,
                scale: old_s,
            },
            DataTypeKind::Float {
                precision: new_p,
                scale: new_s,
            },
        )
        | (
            DataTypeKind::Double {
                precision: old_p,
                scale: old_s,
            },
            DataTypeKind::Double {
                precision: new_p,
                scale: new_s,
            },
        ) => {
            // M/D changes may round stored values or narrow their range even
            // though the IEEE storage width stays the same. A pure M widening
            // with unchanged scale preserves every existing value and key.
            return old_s != new_s
                || match (old_p, new_p) {
                    (Some(old), Some(new)) => new < old,
                    (None, Some(_)) => true,
                    _ => false,
                };
        }
        (
            DataTypeKind::Decimal {
                scale: old_scale, ..
            },
            DataTypeKind::Decimal {
                scale: new_scale, ..
            },
        ) => {
            // S23 — reaching the match means `column_kind_for` already matched,
            // i.e. both decimals landed in the same fixed-width bucket (≤9→4B,
            // ≤18→8B, ≤38→16B). Since the physical width bucket encodes the
            // scale too, a *precision-only* increase (e.g. DECIMAL(5,2) →
            // DECIMAL(8,2)) is byte-identical on disk and can run INSTANT
            // rather than a full COPY. A scale change is caught defensively.
            // (Integer widening, by contrast, changes the fixed byte width, so
            // it necessarily differs in `column_kind_for` and never reaches
            // here — it correctly still requires a COPY.)
            return old_scale != new_scale;
        }
        _ => {}
    }
    old.data_type != new.data_type || old.unsigned != new.unsigned
}

/// Detect the safe INSTANT append case for ENUM/SET: `new` extends
/// `old` by appending zero or more members at the tail, and the
/// storage width bucket does not change. Returns false for any other
/// shape (reorder, rename, deletion, width change) — those still need
/// the COPY rewrite.
fn is_enum_set_instant_append(old: &DataTypeKind, new: &DataTypeKind) -> bool {
    match (old, new) {
        (DataTypeKind::Enum { values: o }, DataTypeKind::Enum { values: n }) => {
            n.len() >= o.len()
                && n[..o.len()] == o[..]
                && crate::storage::enum_storage_width(o.len())
                    == crate::storage::enum_storage_width(n.len())
        }
        (DataTypeKind::Set { values: o }, DataTypeKind::Set { values: n }) => {
            n.len() >= o.len()
                && n[..o.len()] == o[..]
                && crate::storage::set_storage_width(o.len())
                    == crate::storage::set_storage_width(n.len())
        }
        _ => false,
    }
}

fn optional_len_decreases(old: Option<usize>, new: Option<usize>) -> bool {
    match (old, new) {
        (Some(old), Some(new)) => new < old,
        (None, Some(_)) => true,
        _ => false,
    }
}

fn coerce_row_for_alter_copy(
    table: &TableSchema,
    values: &mut [Value],
    strict: bool,
    warnings: &mut Vec<crate::engine::api::SqlWarning>,
) -> Result<(), QueryError> {
    coerce_row_for_alter_copy_masked(table, values, strict, warnings, None)
}

/// Format the overwhelmingly common exact integer -> CHAR/VARCHAR ALTER
/// conversion into `Value`'s inline short-string lane.  The regular schema
/// coercer necessarily returns an owned `String`, which turns a 10M-row COPY
/// into 10M allocator round trips even when every rendered value is only a
/// handful of ASCII digits.  This fast path is semantically narrow: it only
/// fires when the complete decimal representation fits the declared target,
/// so truncation, warnings and strict-mode errors remain with the oracle.
fn alter_copy_exact_integer_string(value: &Value, column: &ColumnSchema) -> Option<Value> {
    let (negative, mut magnitude) = match value {
        Value::Int(value) => (*value < 0, value.unsigned_abs()),
        Value::UInt(value) => (false, *value),
        _ => return None,
    };
    let max_chars = match column.data_type {
        DataTypeKind::Char { len } | DataTypeKind::VarChar { len } => len,
        _ => return None,
    };
    let mut scratch = [0_u8; 21];
    let mut cursor = scratch.len();
    loop {
        cursor -= 1;
        scratch[cursor] = b'0' + (magnitude % 10) as u8;
        magnitude /= 10;
        if magnitude == 0 {
            break;
        }
    }
    if negative {
        cursor -= 1;
        scratch[cursor] = b'-';
    }
    let rendered = &scratch[cursor..];
    if max_chars.is_some_and(|max_chars| rendered.len() > max_chars) {
        return None;
    }
    Some(Value::SmallString(SmallStringBuf::from_slice(rendered)))
}

fn coerce_row_for_alter_copy_masked(
    table: &TableSchema,
    values: &mut [Value],
    strict: bool,
    warnings: &mut Vec<crate::engine::api::SqlWarning>,
    coercion_mask: Option<&[bool]>,
) -> Result<(), QueryError> {
    if values.len() != table.columns.len() {
        return Err(error(
            crate::error::catalog::ER_WRONG_VALUE_COUNT_ON_ROW,
            "column count does not match value count",
        ));
    }
    for (idx, (value, column)) in values.iter_mut().zip(&table.columns).enumerate() {
        if coercion_mask.is_some_and(|mask| !mask.get(idx).copied().unwrap_or(true)) {
            continue;
        }
        let mut current = std::mem::replace(value, Value::Null);
        // MySQL's TIMESTAMP ALTER semantics apply the new explicit default
        // while rewriting pre-existing NULL rows. In particular, converting
        // a nullable BIGINT/TIMESTAMP to
        // `TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP` must not run the
        // NOT NULL check against the historical NULL before the new default
        // has been materialized.
        if matches!(current, Value::Null)
            && !column.nullable
            && matches!(column.data_type, DataTypeKind::Timestamp { .. })
            && (column.default.is_some() || column.default_expr.is_some())
        {
            current = crate::engine::expr::resolve_column_default_value(column);
        }
        if !strict && matches!(current, Value::Null) && !column.nullable {
            if let Some(default) =
                crate::engine::executor::insert::implicit_default_for_type(&column.data_type)
            {
                current = default;
                push_alter_copy_warning(
                    warnings,
                    crate::error::catalog::ER_BAD_NULL_ERROR,
                    format!("Column '{}' cannot be null", column.name),
                );
            }
        }
        // The ENUM row codec keeps the invalid ordinal-zero sentinel as a
        // heap `String("")`, while a legitimately declared empty member is
        // `SmallString("")`. Preserve the sentinel across ALTER COPY instead
        // of feeding it through ordinary string lookup, which would silently
        // turn ordinal 0 into the declared empty member's ordinal.
        if matches!(
            (&current, &column.data_type),
            (Value::Bytes(bytes), DataTypeKind::Enum { .. }) if bytes.is_empty()
        ) {
            *value = current;
            continue;
        }
        if let Some(compact) = alter_copy_exact_integer_string(&current, column) {
            *value = compact;
            continue;
        }
        match coerce_column_value_for_schema(current.clone(), column) {
            Ok(coerced) => *value = coerced,
            Err(err) if !strict => {
                *value = coerce_column_value_for_alter_copy_lossy(current, column, err, warnings)?;
            }
            Err(err) => return Err(err),
        }
    }
    if let Some(mask) = coercion_mask {
        for (idx, (value, column)) in values.iter_mut().zip(&table.columns).enumerate() {
            if !mask.get(idx).copied().unwrap_or(true)
                || !matches!(column.data_type, DataTypeKind::Char { .. })
            {
                continue;
            }
            match value {
                Value::String(text) | Value::Decimal(text) => {
                    text.truncate(crate::util::pad_space::pad_space_trim_end_str(text).len());
                }
                Value::Bytes(bytes) => {
                    bytes.truncate(crate::util::pad_space::trim_trailing_spaces_len(bytes));
                }
                _ => {}
            }
        }
    } else {
        compact_char_columns_for_storage(table, values);
    }
    Ok(())
}

fn push_alter_copy_warning(
    warnings: &mut Vec<crate::engine::api::SqlWarning>,
    spec: crate::error::ErrorSpec,
    message: impl Into<String>,
) {
    warnings.push(crate::engine::api::SqlWarning::known(
        crate::error::ConditionLevel::Warning,
        spec,
        message,
    ));
}

/// Warning text for a lossy numeric coercion during ALTER COPY, keyed by
/// the registered strict-path condition that triggered the fallback.
fn alter_copy_numeric_warning(error: &QueryError, column: &str) -> String {
    if error.is(crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD) {
        format!("incorrect integer value for column '{column}'")
    } else if error.is(crate::error::catalog::WARN_DATA_TRUNCATED) {
        format!("data truncated for column '{column}'")
    } else {
        format!("out of range value for column '{column}'")
    }
}

fn is_alter_numeric_lossy(error: &QueryError) -> bool {
    error.is_any(&[
        crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
        crate::error::catalog::WARN_DATA_TRUNCATED,
        crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
    ])
}

fn is_alter_temporal_lossy(error: &QueryError) -> bool {
    error.is_any(&[
        crate::error::catalog::ER_TRUNCATED_WRONG_VALUE,
        crate::error::catalog::ER_WRONG_VALUE_FOR_TYPE,
    ])
}

fn coerce_column_value_for_alter_copy_lossy(
    value: Value,
    column: &ColumnSchema,
    err: QueryError,
    warnings: &mut Vec<crate::engine::api::SqlWarning>,
) -> Result<Value, QueryError> {
    match &column.data_type {
        DataTypeKind::Char { .. } | DataTypeKind::VarChar { .. } | DataTypeKind::Text { .. }
            if err.is(crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD) =>
        {
            let charset = column.charset.as_deref().unwrap_or("utf8mb4");
            let text = value_as_lossy_string(value);
            let encoded = crate::charset::encode_string_with_replacement(charset, &text)?;
            let replacement = crate::charset::decode_bytes(charset, &encoded)?;
            push_alter_copy_warning(
                warnings,
                crate::error::catalog::ER_TRUNCATED_WRONG_VALUE_FOR_FIELD,
                format!("Incorrect string value for column '{}'", column.name),
            );
            Ok(Value::String(replacement))
        }
        DataTypeKind::Char { len } | DataTypeKind::VarChar { len }
            if err.is(crate::error::catalog::ER_DATA_TOO_LONG) =>
        {
            let Some(max_len) = *len else { return Err(err) };
            let mut text = value_as_lossy_string(value);
            if matches!(column.data_type, DataTypeKind::Char { .. }) {
                text = crate::util::pad_space::pad_space_trim_end_str(&text).to_string();
            }
            let truncated = text.chars().take(max_len).collect::<String>();
            let out = Value::String(truncated);
            validate_column_charset_value(column, &out)?;
            push_alter_copy_warning(
                warnings,
                crate::error::catalog::WARN_DATA_TRUNCATED,
                format!("data truncated for column '{}'", column.name),
            );
            Ok(out)
        }
        DataTypeKind::Text { max_len } if err.is(crate::error::catalog::ER_DATA_TOO_LONG) => {
            let text = truncate_utf8_bytes(&value_as_lossy_string(value), *max_len as usize);
            let out = Value::String(text);
            validate_column_charset_value(column, &out)?;
            push_alter_copy_warning(
                warnings,
                crate::error::catalog::WARN_DATA_TRUNCATED,
                format!("data truncated for column '{}'", column.name),
            );
            Ok(out)
        }
        DataTypeKind::Binary { len } if err.is(crate::error::catalog::ER_DATA_TOO_LONG) => {
            let Some(max_len) = *len else { return Err(err) };
            let mut bytes = value_as_lossy_bytes(value);
            bytes.truncate(max_len);
            bytes.resize(max_len, 0);
            push_alter_copy_warning(
                warnings,
                crate::error::catalog::WARN_DATA_TRUNCATED,
                format!("data truncated for column '{}'", column.name),
            );
            Ok(Value::Bytes(bytes))
        }
        DataTypeKind::VarBinary { len } if err.is(crate::error::catalog::ER_DATA_TOO_LONG) => {
            let Some(max_len) = *len else { return Err(err) };
            let mut bytes = value_as_lossy_bytes(value);
            bytes.truncate(max_len);
            push_alter_copy_warning(
                warnings,
                crate::error::catalog::WARN_DATA_TRUNCATED,
                format!("data truncated for column '{}'", column.name),
            );
            Ok(Value::Bytes(bytes))
        }
        DataTypeKind::Blob { max_len } if err.is(crate::error::catalog::ER_DATA_TOO_LONG) => {
            let mut bytes = value_as_lossy_bytes(value);
            bytes.truncate(*max_len as usize);
            push_alter_copy_warning(
                warnings,
                crate::error::catalog::WARN_DATA_TRUNCATED,
                format!("data truncated for column '{}'", column.name),
            );
            Ok(Value::Bytes(bytes))
        }
        DataTypeKind::TinyInt
        | DataTypeKind::SmallInt
        | DataTypeKind::MediumInt
        | DataTypeKind::Int
        | DataTypeKind::BigInt
            if is_alter_numeric_lossy(&err) =>
        {
            // S8 — 1264 means the value is numeric but out of range: clamp
            // it directly. 1366/1265 mean a string that isn't (fully)
            // numeric; MySQL non-strict parses the leading numeric prefix
            // ('abc' → 0, '12x' → 12) and warns, rather than failing the
            // whole ALTER. Previously only 1264 was handled, so a
            // non-numeric string aborted the copy.
            let numeric = if err.is(crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE) {
                value
            } else {
                let parsed = crate::util::decimal::parse_lenient_f64(&value_as_lossy_string(value));
                Value::Int(parsed.value.round() as i64)
            };
            let coerced = clamp_integer_for_column(numeric, &column.data_type, column.unsigned)?;
            push_alter_copy_warning(
                warnings,
                err.spec().expect("coercion errors are catalog conditions"),
                alter_copy_numeric_warning(&err, &column.name),
            );
            Ok(coerced)
        }
        DataTypeKind::Float { .. } | DataTypeKind::Double { .. }
            if is_alter_numeric_lossy(&err) =>
        {
            let parsed = crate::util::decimal::parse_lenient_f64(&value_as_lossy_string(value));
            push_alter_copy_warning(
                warnings,
                err.spec().expect("coercion errors are catalog conditions"),
                alter_copy_numeric_warning(&err, &column.name),
            );
            Ok(Value::Double(parsed.value))
        }
        DataTypeKind::Decimal { precision, scale } if is_alter_numeric_lossy(&err) => {
            let coerced = if err.is(crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE) {
                // Numeric but out of range → clamp to the column's max.
                clamp_decimal_for_column(value, *precision, *scale)?
            } else {
                // Non-numeric string → its lenient numeric prefix formatted at
                // the column scale. This value fits by construction for the
                // common small-prefix case; the row codec re-validates on write.
                let parsed = crate::util::decimal::parse_lenient_f64(&value_as_lossy_string(value));
                Value::Decimal(format!("{:.*}", scale.unwrap_or(0) as usize, parsed.value))
            };
            push_alter_copy_warning(
                warnings,
                err.spec().expect("coercion errors are catalog conditions"),
                alter_copy_numeric_warning(&err, &column.name),
            );
            Ok(coerced)
        }
        DataTypeKind::Date if is_alter_temporal_lossy(&err) => {
            push_alter_copy_warning(
                warnings,
                crate::error::catalog::ER_TRUNCATED_WRONG_VALUE,
                format!("incorrect date value for column '{}'", column.name),
            );
            Ok(Value::Date(crate::types::ZERO_DATE))
        }
        DataTypeKind::Time { .. } if is_alter_temporal_lossy(&err) => {
            push_alter_copy_warning(
                warnings,
                crate::error::catalog::ER_TRUNCATED_WRONG_VALUE,
                format!("incorrect time value for column '{}'", column.name),
            );
            Ok(Value::Time(0))
        }
        DataTypeKind::DateTime { .. } if is_alter_temporal_lossy(&err) => {
            push_alter_copy_warning(
                warnings,
                crate::error::catalog::ER_TRUNCATED_WRONG_VALUE,
                format!("incorrect datetime value for column '{}'", column.name),
            );
            Ok(Value::DateTime(crate::types::ZERO_DATETIME))
        }
        DataTypeKind::Timestamp { .. } if is_alter_temporal_lossy(&err) => {
            push_alter_copy_warning(
                warnings,
                crate::error::catalog::ER_TRUNCATED_WRONG_VALUE,
                format!("incorrect datetime value for column '{}'", column.name),
            );
            Ok(Value::Timestamp(crate::types::ZERO_DATETIME))
        }
        DataTypeKind::Year if err.is(crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE) => {
            let coerced = clamp_year_for_column(value)?;
            push_alter_copy_warning(
                warnings,
                crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                format!("out of range value for column '{}'", column.name),
            );
            Ok(coerced)
        }
        DataTypeKind::Enum { values } if err.is(crate::error::catalog::WARN_DATA_TRUNCATED) => {
            let coerced = coerce_enum_lossy(value, values);
            push_alter_copy_warning(
                warnings,
                crate::error::catalog::WARN_DATA_TRUNCATED,
                format!("data truncated for column '{}'", column.name),
            );
            Ok(coerced)
        }
        DataTypeKind::Set { values } if err.is(crate::error::catalog::WARN_DATA_TRUNCATED) => {
            let coerced = coerce_set_lossy(value, values);
            push_alter_copy_warning(
                warnings,
                crate::error::catalog::WARN_DATA_TRUNCATED,
                format!("data truncated for column '{}'", column.name),
            );
            Ok(coerced)
        }
        _ => Err(err),
    }
}

fn value_as_lossy_string(value: Value) -> String {
    match value {
        Value::String(v) | Value::Decimal(v) => v,
        Value::Bytes(v) => String::from_utf8_lossy(&v).to_string(),
        other => value_to_string(&other),
    }
}

fn value_as_lossy_bytes(value: Value) -> Vec<u8> {
    match value {
        Value::Bytes(v) => v,
        Value::String(v) | Value::Decimal(v) => v.into_bytes(),
        other => value_to_string(&other).into_bytes(),
    }
}

fn truncate_utf8_bytes(text: &str, max_len: usize) -> String {
    if text.len() <= max_len {
        return text.to_string();
    }
    let mut end = max_len;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_string()
}

fn clamp_integer_for_column(
    value: Value,
    data_type: &DataTypeKind,
    unsigned: bool,
) -> Result<Value, QueryError> {
    let (min, max) = match (data_type, unsigned) {
        (DataTypeKind::TinyInt, false) => (-128_i128, 127_i128),
        (DataTypeKind::SmallInt, false) => (-32768_i128, 32767_i128),
        (DataTypeKind::MediumInt, false) => (-8_388_608_i128, 8_388_607_i128),
        (DataTypeKind::Int, false) => (-2_147_483_648_i128, 2_147_483_647_i128),
        (DataTypeKind::BigInt, false) => (i64::MIN as i128, i64::MAX as i128),
        (DataTypeKind::TinyInt, true) => (0_i128, 255_i128),
        (DataTypeKind::SmallInt, true) => (0_i128, 65_535_i128),
        (DataTypeKind::MediumInt, true) => (0_i128, 16_777_215_i128),
        (DataTypeKind::Int, true) => (0_i128, 4_294_967_295_i128),
        (DataTypeKind::BigInt, true) => (0_i128, u64::MAX as i128),
        _ => {
            return Err(error(
                crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                "out of range value for column",
            ))
        }
    };
    let raw = match value {
        Value::UInt(v) => v as i128,
        other => to_i64(other).unwrap_or(if unsigned { 0 } else { max as i64 }) as i128,
    };
    let clamped = raw.clamp(min, max);
    if unsigned {
        Ok(Value::UInt(clamped as u64))
    } else {
        Ok(Value::Int(clamped as i64))
    }
}

fn clamp_decimal_for_column(
    value: Value,
    precision: Option<u8>,
    scale: Option<u8>,
) -> Result<Value, QueryError> {
    let Some(precision) = precision else {
        return Ok(Value::Decimal("0".to_string()));
    };
    let scale = scale.unwrap_or(0);
    if precision > 38 {
        return Err(error(
            crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
            "out of range value for DECIMAL column",
        ));
    }
    let raw = value_as_lossy_string(value);
    let negative = raw.trim_start().starts_with('-');
    let max_magnitude = 10_u128
        .checked_pow(precision as u32)
        .and_then(|v| v.checked_sub(1))
        .ok_or_else(|| {
            error(
                crate::error::catalog::ER_WARN_DATA_OUT_OF_RANGE,
                "out of range value for DECIMAL column",
            )
        })?;
    decimal_value_from_magnitude(negative, max_magnitude, scale)
}

fn decimal_value_from_magnitude(
    negative: bool,
    magnitude: u128,
    scale: u8,
) -> Result<Value, QueryError> {
    if magnitude <= i64::MAX as u128 {
        let mag = magnitude as i64;
        return Ok(Value::DecimalCompact(
            if negative { -mag } else { mag },
            scale,
        ));
    }
    let scale_len = scale as usize;
    let digits = magnitude.to_string();
    let body = if scale_len == 0 {
        digits
    } else if digits.len() <= scale_len {
        format!("0.{digits:0>scale_len$}")
    } else {
        let point = digits.len() - scale_len;
        format!("{}.{}", &digits[..point], &digits[point..])
    };
    Ok(Value::Decimal(if negative && magnitude != 0 {
        format!("-{body}")
    } else {
        body
    }))
}

fn clamp_year_for_column(value: Value) -> Result<Value, QueryError> {
    let raw = to_i64(value).unwrap_or(0);
    if raw <= 0 {
        return Ok(Value::Year(0));
    }
    let normalized = match raw {
        1..=69 => raw + 2000,
        70..=99 => raw + 1900,
        _ => raw,
    };
    if !(1901..=2155).contains(&normalized) {
        return Ok(Value::Year(0));
    }
    Ok(Value::Year((normalized - 1900) as u8))
}

fn coerce_enum_lossy(value: Value, allowed: &[String]) -> Value {
    match value {
        // 0 is the special empty-index sentinel — legal, stores ''.
        Value::Int(0) | Value::UInt(0) => Value::Bytes(Vec::new()),
        Value::Int(v) if v >= 1 && (v as usize) <= allowed.len() => {
            Value::String(allowed[v as usize - 1].clone())
        }
        Value::UInt(v) if v >= 1 && (v as usize) <= allowed.len() => {
            Value::String(allowed[v as usize - 1].clone())
        }
        // Negative / out-of-range numeric ordinal → non-strict fallback
        // still ends at '' but stays consistent with the INSERT path
        // that would have reported 1265.
        Value::Int(_) | Value::UInt(_) => Value::Bytes(Vec::new()),
        Value::Bytes(bytes) if bytes.is_empty() => Value::Bytes(bytes),
        other => {
            let raw = value_as_lossy_string(other);
            if raw.is_empty() {
                return Value::Bytes(Vec::new());
            }
            allowed
                .iter()
                .find(|v| v.eq_ignore_ascii_case(&raw))
                .cloned()
                .map(Value::String)
                .unwrap_or_else(|| Value::Bytes(Vec::new()))
        }
    }
}

fn coerce_set_lossy(value: Value, allowed: &[String]) -> Value {
    // Numeric input: interpret as bitmask (MySQL 8.0 semantics).
    match &value {
        Value::UInt(mask) => {
            return Value::String(crate::storage::core::set_bitmask_to_csv(*mask, allowed));
        }
        Value::Int(mask) if *mask >= 0 => {
            return Value::String(crate::storage::core::set_bitmask_to_csv(
                *mask as u64,
                allowed,
            ));
        }
        _ => {}
    }
    let raw = value_as_lossy_string(value);
    let mut out: Vec<String> = Vec::new();
    for part in raw.split(',') {
        let trimmed = part.trim_end_matches(' ');
        if trimmed.is_empty() {
            continue;
        }
        if let Some(matched) = allowed.iter().find(|v| v.eq_ignore_ascii_case(trimmed)) {
            if !out.iter().any(|existing: &String| existing == matched) {
                out.push(matched.clone());
            }
        }
        // Non-strict lossy: unknown tokens silently dropped (differs
        // from constraints' strict 1265). Warning already surfaced by
        // the caller.
    }
    // Preserve MySQL's canonical declaration order regardless of input
    // order — earlier callers relied on this for round-trip.
    out.sort_by_key(|m| allowed.iter().position(|v| v == m).unwrap_or(usize::MAX));
    Value::String(out.join(","))
}

/// MySQL 8.0.23+ guard: every table needs at least one visible
/// (non-INVISIBLE, non-Dropped) column. Shared by the single-action
/// MODIFY/CHANGE candidates, ALTER COLUMN … SET INVISIBLE, and the
/// multi-action validator.
fn ensure_table_keeps_visible_column(
    table: &crate::storage::TableSchema,
) -> Result<(), QueryError> {
    if table
        .columns
        .iter()
        .any(|c| c.state != ColumnState::Dropped && !c.invisible)
    {
        return Ok(());
    }
    Err(error(
        crate::error::catalog::ER_TABLE_MUST_HAVE_A_VISIBLE_COLUMN,
        "A table must have at least one visible column.",
    ))
}

/// Common publication-ready schema validation; rename propagation is performed
/// by CHANGE before entering, and physical copying remains with the caller.
fn finish_column_change_candidate(
    candidate: &mut TableSchema,
    copy_required: bool,
    auto_increment: bool,
) -> Result<(), QueryError> {
    if copy_required {
        candidate.publish_new_row_layout();
    }
    validate_and_prepare_generated_columns(candidate)?;
    if auto_increment {
        candidate.build_column_index();
        validate_auto_increment_schema(candidate)?;
    }
    candidate.build_column_index();
    candidate.bump_schema_version();
    ensure_table_keeps_visible_column(candidate)?;
    Ok(())
}

fn preserve_primary_key_not_null(table: &TableSchema, pos: usize, new_def: &mut ColumnSchema) {
    // ColumnDef does not distinguish implicit nullability from explicit NULL.
    // The primary key remains authoritative until DROP PRIMARY KEY removes it.
    if table
        .primary_key
        .iter()
        .any(|name| name.eq_ignore_ascii_case(&table.columns[pos].name))
    {
        new_def.nullable = false;
    }
}

fn apply_column_def_preserving_identity(
    slot: &mut crate::storage::ColumnSchema,
    new_def: crate::storage::ColumnSchema,
) {
    let column_id = slot.column_id;
    let not_null_from_scn = slot.not_null_from_scn;
    let state = slot.state;
    let physical_pos = slot.physical_pos;
    let logical_pos = slot.logical_pos;
    let read_backfill_default = crate::engine::executor::insert::column_read_backfill_default(slot);
    *slot = new_def;
    slot.read_backfill_default = Some(read_backfill_default);
    slot.column_id = column_id;
    slot.not_null_from_scn = not_null_from_scn;
    slot.state = state;
    slot.physical_pos = physical_pos;
    slot.logical_pos = logical_pos;
}

impl Database {
    /// `true` when this engine should run ADD INDEX backfills on a
    /// background thread. Initial value comes from `MYDB_DDL_ASYNC=1`;
    /// callers can toggle at runtime via [`set_async_ddl`].
    pub fn async_ddl_enabled(&self) -> bool {
        self.async_ddl.load(std::sync::atomic::Ordering::Relaxed)
    }

    #[cfg(test)]
    /// Per-engine toggle for [`async_ddl_enabled`]. Letting tests flip
    /// this directly avoids the env-var-pollution problem that an
    /// `MYDB_DDL_ASYNC=1` set/unset dance would create under parallel
    /// `cargo test`.
    pub fn set_async_ddl(&self, enabled: bool) {
        self.async_ddl
            .store(enabled, std::sync::atomic::Ordering::Relaxed);
    }

    /// Test-only: append an `IndexState::Building` index to `table` in the
    /// catalog and persist it, reproducing the on-disk shape a crash leaves
    /// after ADD INDEX Phase 1 (catalog published the entry as `Building`
    /// but the backfill never reached the Public flip). The next open's
    /// `migrate_catalog` must scrub the tombstone.
    #[cfg(test)]
    pub(crate) fn inject_building_index_for_test(
        &self,
        database: &str,
        table: &str,
        index: &str,
        columns: Vec<String>,
    ) -> Result<(), QueryError> {
        let table_id;
        {
            let mut catalog = self
                .catalog
                .write()
                .map_err(|_| error(crate::error::catalog::ER_GET_ERRNO, "catalog lock poisoned"))?;
            let key = table_key(database, table);
            let schema = catalog.tables.get(&key).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_NO_SUCH_TABLE,
                    format!("unknown table {database}.{table}"),
                )
            })?;
            let mut updated = (**schema).clone();
            updated.indexes.push(IndexSchema {
                readable_from_scn: 0,
                index_id: 0,
                name: index.to_string(),
                columns,
                column_prefix_lengths: Vec::new(),
                column_directions: Vec::new(),
                key_expressions: Vec::new(),
                unique: false,
                primary: false,
                included_columns: Vec::new(),
                state: IndexState::Building,
                index_type: "BTREE".to_string(),
                visible: true,
                comment: String::new(),
                fulltext: None,
            });
            table_id = updated.id;
            catalog.tables.insert(key, Arc::new(updated));
        }
        self.note_catalog_table(table_id);
        self.commit_catalog_txn()
    }

    #[cfg(test)]
    pub(crate) fn install_ddl_backfill_test_hook(&self) -> Arc<DdlBackfillTestHook> {
        let hook = Arc::new(DdlBackfillTestHook::new());
        let mut guard = self
            .ddl_backfill_test_hook
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        *guard = Some(Arc::clone(&hook));
        hook
    }

    #[cfg(test)]
    fn pause_ddl_backfill_for_test(&self) {
        let hook = self
            .ddl_backfill_test_hook
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(hook) = hook {
            hook.pause();
        }
    }

    pub fn add_indexes_with_async_options(
        self: &Arc<Self>,
        table_name: &str,
        mut indexes: Vec<IndexSchema>,
        options: IndexBuildOptions,
    ) -> Result<(), QueryError> {
        if indexes.len() == 1 {
            return self.add_index_with_async_options(
                table_name,
                indexes.pop().expect("one index"),
                options,
            );
        }
        self.add_indexes_offline_bulk_with_options(table_name, indexes, options)
    }

    pub fn add_index_with_async_options(
        self: &Arc<Self>,
        table_name: &str,
        index: IndexSchema,
        options: IndexBuildOptions,
    ) -> Result<(), QueryError> {
        // Adding a PRIMARY KEY changes more than a derived access path: its
        // columns become the table identity and must remain NOT NULL/unique
        // across the validation/build/publication interval. The online
        // Building path exposes the candidate index to concurrent DML after
        // the pre-scan, which would require a second table-wide validation and
        // retains an incrementally rewritten B-tree work set for the whole
        // build. Use the existing table-X-lock direct bulk builder instead.
        // This compatibility storage entry point retains the incremental
        // Building -> Public opt-in for its remaining callers. Standalone
        // secondary BTREE SQL instead uses add_indexes_for_sql: it bulk-loads
        // a private target while capturing DML, independent of this switch.
        // PRIMARY and non-opt-in callers here keep the offline bulk path.
        if index.primary || !self.async_ddl_enabled() {
            return self.add_index_offline_bulk_with_options(table_name, index, options);
        }
        // MySQL DDL completion is synchronous from the issuing connection's
        // perspective: online describes concurrent table access, not an early
        // OK packet. Build through the online Building -> Public phases, but
        // wait here and propagate the final error to the caller.
        let (key, building_table, index_name) = self.add_index_phase1(table_name, index)?;
        self.add_index_phase23(&key, &building_table, &index_name, options)
    }
}

impl Database {
    /// Apply a single partition DDL action to an existing table.
    /// Dispatched from `dispatch.rs` after the regular column/index
    /// actions of the same ALTER TABLE statement.
    pub fn alter_partition(
        &self,
        table_name: &str,
        action: crate::parser_partition::PartitionAction,
    ) -> Result<(), QueryError> {
        self.alter_partition_with_mdl(table_name, action, None)
    }

    pub(crate) fn alter_partition_with_mdl(
        &self,
        table_name: &str,
        action: crate::parser_partition::PartitionAction,
        cutover: Option<(u64, Duration, bool)>,
    ) -> Result<(), QueryError> {
        use crate::parser_partition::PartitionAction as A;
        // SU serializes DDL, SNW excludes writes while permitting readers.
        // Different durations keep both tickets: these modes are not a
        // total strength ordering in the MDL manager.
        let _build_mdl = if self.partition_action_shared_build_supported(&action) {
            if let Some((owner, timeout, allow_dml)) = cutover {
                if !allow_dml {
                    self.acquire_alter_execution_mdl_duration(
                        owner,
                        table_name,
                        MdlLockType::SharedNoWrite,
                        timeout,
                        MdlDuration::Transaction,
                    )?;
                }
                Some(PartitionBuildMdlGuard { db: self, owner })
            } else {
                None
            }
        } else {
            None
        };
        match action {
            A::DropPartitions(names) => self.drop_partitions(table_name, &names),
            A::TruncatePartitions(targets) => {
                self.rewrite_partition_storage_ids(table_name, &targets, false, true, cutover)
            }
            A::AddPartitions(items) => self.add_partitions(table_name, items, cutover),
            A::AddPartitionCount(n) => self.add_partition_count(table_name, n, cutover),
            A::AnalyzePartitions(targets) => {
                let (table, storage_ids) =
                    self.partition_maintenance_storage_ids(table_name, &targets)?;
                self.analyze_partition_storage_ids(&table, &storage_ids)
            }
            A::RebuildPartitions(targets) => {
                self.rewrite_partition_storage_ids(table_name, &targets, false, false, cutover)
            }
            A::OptimizePartitions(targets) => {
                // OPTIMIZE is a physical REBUILD followed by a target-only
                // ANALYZE, matching MySQL's recreate+analyze semantics
                // without scanning unrelated leaves.
                self.rewrite_partition_storage_ids(table_name, &targets, true, false, cutover)
            }
            A::RepairPartitions(targets) => {
                // Reconstruct derived indexes in private generations. A bad
                // heap or invalid routing fails without damaging public indexes.
                self.rewrite_partition_storage_ids(table_name, &targets, true, false, cutover)
            }
            A::CheckPartitions(targets) => {
                // CHECK PARTITION = integrity pass: scan only the target
                // partition heap files. Decoding/visibility failures surface
                // as errors; clean scan = OK.
                let (table, storage_ids) =
                    self.partition_maintenance_storage_ids(table_name, &targets)?;
                self.check_partition_storage_ids(&table, &storage_ids)
            }
            A::CoalescePartitions(n) => self.coalesce_partitions(table_name, n, cutover),
            A::RemovePartitioning => self.remove_partitioning(table_name, cutover),
            A::ReorganizePartitions {
                old_partitions,
                new_partitions,
            } => self.reorganize_partitions(table_name, &old_partitions, new_partitions, cutover),
            A::ExchangePartition {
                partition,
                table,
                with_validation,
            } => self.exchange_partition(table_name, &partition, &table, with_validation),
            A::PartitionBy(def) => self.repartition_table(table_name, def, cutover),
        }
    }

    fn partition_maintenance_storage_ids(
        &self,
        table_name: &str,
        targets: &crate::parser_partition::PartitionTargets,
    ) -> Result<(std::sync::Arc<TableSchema>, Vec<u64>), QueryError> {
        let (database, table_only) = split_table_name(DEFAULT_DATABASE, table_name);
        let key = table_key(&database, &table_only);
        let table = self
            .catalog_read()?
            .tables
            .get(&key)
            .cloned()
            .ok_or_else(|| ddl_table_not_found(&table_name))?;
        let part = table.partitioning.as_ref().ok_or_else(|| {
            error(
                crate::error::catalog::ER_PARTITION_MGMT_ON_NONPARTITIONED,
                "Partition management on a not partitioned table is not possible",
            )
        })?;
        let storage_ids = match targets {
            crate::parser_partition::PartitionTargets::All => part.storage_ids(),
            crate::parser_partition::PartitionTargets::Names(names) => {
                let mut out = Vec::with_capacity(names.len());
                for name in names {
                    let storage_ids = part.storage_ids_for_name(name).ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_DROP_PARTITION_NON_EXISTENT,
                            format!("partition '{name}' does not exist"),
                        )
                    })?;
                    for storage_id in storage_ids {
                        if !out.contains(&storage_id) {
                            out.push(storage_id);
                        }
                    }
                }
                out
            }
        };
        Ok((table, storage_ids))
    }

    /// Rewrite selected physical leaves into fresh storage generations and
    /// atomically publish the replacement layout. This compacts heap/LOB and
    /// bulk-rebuilds every local derived index; unselected leaves are neither
    /// opened nor have their statistics replaced.
    fn rewrite_partition_storage_ids(
        &self,
        table_name: &str,
        targets: &crate::parser_partition::PartitionTargets,
        analyze_after: bool,
        truncate: bool,
        cutover: Option<(u64, Duration, bool)>,
    ) -> Result<(), QueryError> {
        let (table, source_sids) = self.partition_maintenance_storage_ids(table_name, targets)?;
        if !cutover.is_some_and(|(_, _, online)| online) {
            self.check_ddl_table_lock(table.id)?;
        }
        let (database, name) = split_table_name(DEFAULT_DATABASE, table_name);
        let key = table_key(&database, &name);

        let build_sids = {
            let mut catalog = self.catalog_write()?;
            (0..source_sids.len())
                .map(|_| {
                    let storage_id = catalog.next_table_id;
                    catalog.next_table_id = catalog.next_table_id.saturating_add(1);
                    storage_id
                })
                .collect::<Vec<_>>()
        };
        self.ensure_table_id_highwater(self.current_next_table_id()?)?;
        let replacements = source_sids
            .iter()
            .copied()
            .zip(build_sids.iter().copied())
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut candidate = table.as_ref().clone();
        let partitioning = candidate.partitioning.as_mut().expect("validated above");
        for partition in &mut partitioning.partitions {
            if partition.subpartitions.is_empty() {
                if let Some(new_id) = replacements.get(&partition.storage_id) {
                    partition.storage_id = *new_id;
                }
            } else {
                for subpartition in &mut partition.subpartitions {
                    if let Some(new_id) = replacements.get(&subpartition.storage_id) {
                        subpartition.storage_id = *new_id;
                    }
                }
            }
        }
        candidate.build_column_index();
        self.execute_partition_reorg_streaming(
            &key,
            table_name,
            &table,
            if truncate {
                "truncate_partition"
            } else {
                "rebuild_partition"
            },
            source_sids,
            build_sids.clone(),
            candidate,
            cutover,
        )?;

        if analyze_after {
            let current = self
                .catalog_read()?
                .tables
                .get(&key)
                .cloned()
                .ok_or_else(|| ddl_table_not_found(&table_name))?;
            self.analyze_partition_storage_ids(&current, &build_sids)?;
        }
        Ok(())
    }

    fn check_partition_storage_ids(
        &self,
        table: &TableSchema,
        storage_ids: &[u64],
    ) -> Result<(), QueryError> {
        let txn = self.begin_read_only_txn()?;
        let _read_guard = PartitionReadTxnGuard {
            db: self,
            txn_id: txn.txn_id,
        };
        let mut checked = Vec::with_capacity(storage_ids.len());
        for storage_id in storage_ids {
            let page_count = self.with_heap_read(*storage_id, |heap| heap.page_count())?;
            let scratch = if table.indexes.is_empty() {
                0
            } else {
                crate::engine::executor::mem_budget::query_mem_limit_bytes()
                    .saturating_div(2)
                    .min(64 * 1024 * 1024)
                    .max(table.indexes.len() * 1024 * 1024)
            };
            let _scratch = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::PartitionDdl,
                scratch,
            )?;
            let mut expected_indexes = table
                .indexes
                .iter()
                .map(|_| {
                    IndexEntryExternalSorter::with_exact_run_bytes(scratch / table.indexes.len())
                })
                .collect::<Vec<_>>();
            let mut visible_row_count = 0u64;
            for page in 0..page_count {
                crate::engine::cancel::check_cancelled()?;
                for mut visible in
                    self.scan_rows_storage_range(table, *storage_id, txn, page, page + 1)?
                {
                    visible_row_count += 1;
                    Self::pad_full_row_to_schema(&mut visible.values, table);
                    let routed = table.route_row_to_partition(&visible.values)?;
                    if routed.storage_id != *storage_id {
                        return Err(error(crate::error::catalog::ER_GET_ERRNO,
                            format!("partition integrity error: row {:?} is stored in leaf {} but routes to {}",
                                visible.row_id, storage_id, routed.storage_id)));
                    }
                    for (index, sorter) in table.indexes.iter().zip(&mut expected_indexes) {
                        for key in table.index_keys(index, &visible.values)? {
                            sorter.push(
                                self.data_dir(),
                                IndexEntry {
                                    key,
                                    row_id: visible.row_id,
                                },
                            )?;
                        }
                    }
                }
            }
            for (index, sorter) in table.indexes.iter().zip(expected_indexes) {
                let mut expected: Box<dyn Iterator<Item = Result<IndexEntry, QueryError>>> =
                    match sorter.finish(self.data_dir())? {
                        SortedIndexEntries::Memory(rows) => Box::new(rows.into_iter().map(Ok)),
                        SortedIndexEntries::Spilled(runs) => Box::new(runs.into_merger()?),
                    };
                let mismatch = || {
                    error(
                        crate::error::catalog::ER_GET_ERRNO,
                        format!(
                            "partition integrity error: index '{}' in leaf {} differs from heap",
                            index.name, storage_id
                        ),
                    )
                };
                let mut actual = IndexRangeCursor::new(
                    self.index_handle(*storage_id, &index.name)?,
                    false,
                    None,
                    None,
                )?;
                actual.visit_entries(|entries| {
                    crate::engine::cancel::check_cancelled()?;
                    for (key, row_id) in entries {
                        let entry = expected.next().transpose()?.ok_or_else(&mismatch)?;
                        if &entry.key != *key || entry.row_id != *row_id {
                            return Err(mismatch());
                        }
                    }
                    Ok(())
                })?;
                if expected.next().transpose()?.is_some() {
                    return Err(mismatch());
                }
            }
            checked.push((*storage_id, visible_row_count, page_count));
        }
        let key = table_key(&table.database, &table.name);
        let mut catalog = self.catalog_write()?;
        let stored = catalog.tables.get_mut(&key).ok_or_else(|| {
            error(
                crate::error::catalog::ER_NO_SUCH_TABLE,
                format!("Table '{}' doesn't exist", table.qualified_name()),
            )
        })?;
        let stored = Arc::make_mut(stored);
        for (storage_id, row_count, page_count) in checked {
            let stats = stored
                .stats
                .partition_stats
                .entry(storage_id)
                .or_insert_with(|| crate::storage::PartitionStats {
                    row_count,
                    heap_page_count: page_count,
                    columns: std::collections::BTreeMap::new(),
                    indexes: std::collections::BTreeMap::new(),
                    modified_rows_since_analyze: 0,
                    analyzed_at_unix_ms: None,
                    check_ok: None,
                });
            stats.row_count = row_count;
            stats.heap_page_count = page_count;
            stats.check_ok = Some(true);
        }
        stored.bump_schema_version();
        drop(catalog);
        self.note_catalog_table(table.id);
        self.commit_catalog_txn()?;
        Ok(())
    }

    // Startup repair uses the same bounded bulk builders, without publishing
    // new generations while the catalog is being opened. SQL REPAIR instead
    // builds private replacement leaves and publishes atomically.
    fn rebuild_indexes_for_partition_storage_ids(
        &self,
        table: &TableSchema,
        storage_ids: &[u64],
    ) -> Result<(), QueryError> {
        self.check_ddl_table_lock(table.id)?;
        let txn = self.begin_read_only_txn()?;
        let _read_guard = PartitionReadTxnGuard {
            db: self,
            txn_id: txn.txn_id,
        };
        self.build_partition_copy_indexes(
            table,
            storage_ids,
            txn,
            crate::engine::cancel::check_cancelled,
        )?;
        self.evict_partition_vector_graphs(table.id, storage_ids);
        for index in table.indexes.iter().filter(|index| index.is_vector()) {
            for sid in storage_ids {
                self.remove_partition_vector_files(table, *sid, &index.name);
                if hnsw_build_on_ddl_enabled() {
                    self.vector_graph_build_on_ddl_one(table, index, *sid)?;
                }
            }
        }
        Ok(())
    }

    /// `ALTER TABLE t DROP PARTITION p1, p2`. Removes the named
    /// partitions and their data; partition storage files (heap +
    /// per-partition indexes) are deleted. Only supported for RANGE
    /// and LIST partition tables — MySQL rejects DROP PARTITION on
    /// HASH/KEY tables.
    fn drop_partitions(&self, table_name: &str, names: &[String]) -> Result<(), QueryError> {
        let qualified = table_name.to_string();
        let (database, name) = split_table_name(DEFAULT_DATABASE, &qualified);
        let key = table_key(&database, &name);
        let table = self
            .catalog_read()?
            .tables
            .get(&key)
            .cloned()
            .ok_or_else(|| ddl_table_not_found(&qualified))?;
        let part = table.partitioning.as_ref().ok_or_else(|| {
            error(
                crate::error::catalog::ER_PARTITION_MGMT_ON_NONPARTITIONED,
                "DROP PARTITION can only be used on a partitioned table",
            )
        })?;
        if !part.method.is_range() && !part.method.is_list() {
            return Err(error(
                crate::error::catalog::ER_ONLY_ON_RANGE_LIST_PARTITION,
                "DROP PARTITION is not allowed on HASH/KEY partitioned tables",
            ));
        }
        // S15 — serialize against LOCK TABLES / concurrent DDL like every
        // other partition-mutating operation. DROP PARTITION unlinks files a
        // concurrent reader may be using, so this guard is load-bearing.
        self.check_ddl_table_lock(table.id)?;
        // Build the to-drop set with case-insensitive name matching.
        // `drop_parent_names` identifies the top-level partition catalog
        // entries to remove; `drop_leaf_sids` are the physical storage ids to
        // unlink. For a SUBPARTITIONed table the heap/index/LOB files live on
        // the leaf subpartition ids, not the parent placeholder
        // `matched.storage_id` — unlinking the placeholder would leak every
        // leaf file (and let a future CREATE reuse the leaked id).
        let mut drop_parent_names = std::collections::BTreeSet::new();
        let mut drop_leaf_sids = Vec::new();
        for n in names {
            let matched = part
                .partitions
                .iter()
                .find(|p| p.name.eq_ignore_ascii_case(n))
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_DROP_PARTITION_NON_EXISTENT,
                        format!("partition '{n}' does not exist"),
                    )
                })?;
            drop_parent_names.insert(matched.name.to_ascii_lowercase());
            for sid in part.storage_ids_for_name(n).unwrap_or_default() {
                if !drop_leaf_sids.contains(&sid) {
                    drop_leaf_sids.push(sid);
                }
            }
        }
        if drop_parent_names.len() == part.partitions.len() {
            return Err(error(
                crate::error::catalog::ER_DROP_LAST_PARTITION,
                "Cannot remove all partitions; use DROP TABLE instead",
            ));
        }
        // A durable intent makes post-publication cleanup restartable.
        // Recovery commits the drop iff none of the source ids remain live.
        let mut journal = PartitionReorgJournal {
            job_id: self.allocate_nontransaction_id(),
            table_id: table.id,
            database: database.clone(),
            table_name: name.clone(),
            op: "drop_partition".to_string(),
            index_names: table
                .indexes
                .iter()
                .map(|index| index.name.clone())
                .collect(),
            generation: table.active_generation.max(1),
            index_ids: table
                .indexes
                .iter()
                .map(|i| (i.name.clone(), i.index_id))
                .collect(),
            source_sids: drop_leaf_sids.clone(),
            build_sids: Vec::new(),
            phase: PartitionDdlPhase::Publishing,
            cancel_requested: false,
            copied_rows: 0,
            total_rows: Some(0),
            replay_watermark: None,
            error_message: None,
        };
        let journal_path = self.write_partition_reorg_journal(&journal)?;
        if partition_reorg_crash_at("before_swap") {
            return Ok(());
        }
        {
            let mut catalog = self.catalog_write()?;
            let Some(table_arc) = catalog.tables.get_mut(&key) else {
                return Err(ddl_table_not_found(&qualified));
            };
            let table_mut = Arc::make_mut(table_arc);
            let part_mut = table_mut.partitioning.as_mut().unwrap();
            part_mut
                .partitions
                .retain(|p| !drop_parent_names.contains(&p.name.to_ascii_lowercase()));
            part_mut.runtime = std::sync::OnceLock::new();
            part_mut.metadata_runtime = std::sync::OnceLock::new();
            for storage_id in &drop_leaf_sids {
                table_mut.stats.partition_stats.remove(storage_id);
            }
            let remaining_ids = part_mut.storage_ids();
            if remaining_ids
                .iter()
                .all(|storage_id| table_mut.stats.partition_stats.contains_key(storage_id))
            {
                table_mut.stats.row_count = remaining_ids
                    .iter()
                    .map(|storage_id| table_mut.stats.partition_stats[storage_id].row_count)
                    .sum();
                table_mut.stats.heap_page_count = remaining_ids
                    .iter()
                    .map(|storage_id| table_mut.stats.partition_stats[storage_id].heap_page_count)
                    .sum();
            } else {
                // A partial leaf-stat map cannot be merged into a trustworthy
                // table statistic after the topology changes.
                table_mut.stats.analyzed_at_unix_ms = None;
                table_mut.stats.modified_rows_since_analyze = table_mut
                    .stats
                    .modified_rows_since_analyze
                    .saturating_add(1);
            }
            table_mut.bump_schema_version();
        }
        // Commit point — the drop is durable from here.
        self.note_catalog_table(table.id);
        self.commit_ddl_catalog_txn().map_err(|err| {
            self.quarantine_mvcc(format!(
                "partition DROP catalog publication failed; restart recovery required: {}",
                err.message()
            ));
            err
        })?;
        journal.phase = PartitionDdlPhase::Swapped;
        self.update_partition_reorg_journal(&journal_path, &mut journal)?;
        if partition_reorg_crash_at("after_swap") {
            return Ok(());
        }
        journal.phase = PartitionDdlPhase::Cleanup;
        self.update_partition_reorg_journal(&journal_path, &mut journal)?;
        if self.partition_cleanup_background.load(Ordering::Acquire) {
            return Ok(());
        }
        self.evict_partition_vector_graphs(table.id, &drop_leaf_sids);
        for sid in &drop_leaf_sids {
            self.remove_heap_cache_entry(*sid)?;
            for index in &table.indexes {
                self.remove_index_cache_entry(*sid, &index.name)?;
            }
            self.remove_partition_storage_files(
                &database,
                &name,
                table.id,
                *sid,
                &journal.index_names,
                &journal,
            )?;
        }
        self.remove_partition_reorg_journal(&journal_path)?;
        Ok(())
    }

    fn add_partitions(
        &self,
        table_name: &str,
        items: Vec<crate::parser_partition::PartitionItemDef>,
        cutover: Option<(u64, Duration, bool)>,
    ) -> Result<(), QueryError> {
        if items.is_empty() {
            return Err(error(
                crate::error::catalog::ER_PARSE_ERROR,
                "ADD PARTITION requires definitions",
            ));
        }
        let (database, name) = split_table_name(DEFAULT_DATABASE, table_name);
        let key = table_key(&database, &name);
        let table = self
            .catalog_read()?
            .tables
            .get(&key)
            .cloned()
            .ok_or_else(|| ddl_table_not_found(&table_name))?;
        let part = table.partitioning.as_ref().ok_or_else(|| {
            error(
                crate::error::catalog::ER_PARTITION_MGMT_ON_NONPARTITIONED,
                "ADD PARTITION can only be used on a partitioned table",
            )
        })?;
        if part.method.is_hash_like() {
            return Err(error(crate::error::catalog::ER_ONLY_ON_RANGE_LIST_PARTITION,
                "ADD PARTITION is not allowed on HASH/KEY partitioned tables; use COALESCE PARTITION instead",
            ));
        }
        // S15 — serialize against LOCK TABLES / concurrent DDL, matching the
        // other partition-mutating operations.
        if !cutover.is_some_and(|(_, _, online)| online) {
            self.check_ddl_table_lock(table.id)?;
        }
        let synth_def = partition_def_from_info(part, None, items)?;

        // Allocate storage ids for the new partitions atomically.
        let storage_id_count = partition_count_from_def(Some(&synth_def))?;
        let new_sids: Vec<u64> = {
            let mut catalog = self.catalog_write()?;
            let mut sids = Vec::with_capacity(storage_id_count);
            for _ in 0..storage_id_count {
                let sid = catalog.next_table_id;
                catalog.next_table_id += 1;
                sids.push(sid);
            }
            sids
        };
        self.ensure_table_id_highwater(self.current_next_table_id()?)?;
        // Convert parser items into storage Partitions using existing
        // helper, then append to the table's partition list.
        let info = build_partition_info_from_def(&synth_def, &table, &new_sids)?;
        let new_partitions = info.partitions;
        // RANGE: MAXVALUE must stay the last bound — reject ADD after it with
        // the specific 1481 before the general layout check below.
        if part.method.is_range()
            && part.partitions.iter().any(|p| {
                matches!(&p.bounds, crate::storage::PartitionBounds::RangeLessThan(v)
                    if v.iter().any(|l| l.is_max_value()))
            })
        {
            return Err(error(
                crate::error::catalog::ER_PARTITION_MAXVALUE_ERROR,
                "MAXVALUE partition already exists; cannot ADD PARTITION after it",
            ));
        }
        // Validate the *post-append* layout the same way `repartition_table`
        // does. `add_partitions` otherwise never calls
        // `validate_mysql80_partition_compat`, so a non-monotonic RANGE bound
        // (1493) or a duplicate LIST constant (1495) would be silently
        // accepted and later mis-route rows (`route_range_single` assumes
        // ascending bounds; duplicate LIST constants make partition ownership
        // ambiguous).
        let mut candidate = table.as_ref().clone();
        let part_c = candidate.partitioning.as_mut().unwrap();
        part_c.partitions.extend(new_partitions);
        part_c.runtime = std::sync::OnceLock::new();
        part_c.metadata_runtime = std::sync::OnceLock::new();
        candidate.build_column_index();
        validate_mysql80_partition_compat(&candidate, false)?;
        self.publish_added_partitions(&key, table_name, &table, candidate, new_sids, cutover)
    }

    /// `ALTER TABLE t ADD PARTITION PARTITIONS N` for HASH/KEY
    /// partitioning. MySQL grows the partition count by N and
    /// redistributes rows according to the new hash modulo.
    fn add_partition_count(
        &self,
        table_name: &str,
        n: u32,
        cutover: Option<(u64, Duration, bool)>,
    ) -> Result<(), QueryError> {
        if n == 0 {
            return Err(error(
                crate::error::catalog::ER_NO_PARTS_ERROR,
                "Number of partitions must be greater than zero",
            ));
        }
        let (database, name) = split_table_name(DEFAULT_DATABASE, table_name);
        let key = table_key(&database, &name);
        let table = self
            .catalog_read()?
            .tables
            .get(&key)
            .cloned()
            .ok_or_else(|| ddl_table_not_found(&table_name))?;
        let part = table.partitioning.as_ref().ok_or_else(|| {
            error(
                crate::error::catalog::ER_PARTITION_MGMT_ON_NONPARTITIONED,
                "ADD PARTITION can only be used on a partitioned table",
            )
        })?;
        if !part.method.is_hash_like() {
            return Err(error(
                crate::error::catalog::ER_ONLY_ON_RANGE_LIST_PARTITION,
                "ADD PARTITION PARTITIONS is only valid for HASH/KEY partitioning",
            ));
        }
        let new_count = part
            .partitions
            .len()
            .checked_add(n as usize)
            .ok_or_else(|| {
                error(
                    crate::error::catalog::ER_TOO_MANY_PARTITIONS_ERROR,
                    "too many partitions",
                )
            })?;
        let def = partition_def_from_info(part, Some(new_count as u32), Vec::new())?;
        self.repartition_table(table_name, def, cutover)
    }

    /// Repartition a table using a fresh `PARTITION BY ...`
    /// declaration. This is the shared implementation for
    /// `ALTER TABLE ... PARTITION BY ...` and HASH/KEY partition-count
    /// growth. It currently performs an offline table-level rewrite:
    /// drain visible rows, publish the new layout, recreate empty heap
    /// files, then reinsert through the normal partition router.
    fn repartition_table(
        &self,
        table_name: &str,
        def: crate::parser_partition::PartitionDef,
        cutover: Option<(u64, Duration, bool)>,
    ) -> Result<(), QueryError> {
        let (database, name) = split_table_name(DEFAULT_DATABASE, table_name);
        let key = table_key(&database, &name);
        let table = self
            .catalog_read()?
            .tables
            .get(&key)
            .cloned()
            .ok_or_else(|| ddl_table_not_found(&table_name))?;
        if !cutover.is_some_and(|(_, _, online)| online) {
            self.check_ddl_table_lock(table.id)?;
        }

        let old_storage_ids = table.storage_ids();

        let new_count = partition_count_from_def(Some(&def))?;
        let new_sids: Vec<u64> = {
            let mut catalog = self.catalog_write()?;
            (0..new_count)
                .map(|_| {
                    let sid = catalog.next_table_id;
                    catalog.next_table_id += 1;
                    sid
                })
                .collect()
        };
        self.ensure_table_id_highwater(self.current_next_table_id()?)?;

        let mut candidate = table.as_ref().clone();
        let info = build_partition_info_from_def(&def, &candidate, &new_sids)?;
        candidate.partitioning = Some(info);
        candidate.build_column_index();
        validate_mysql80_partition_compat(&candidate, false)?;

        // Crash-safe streaming build-then-swap (§2.1): fill fresh shadow
        // partitions, then publish with a single commit_catalog_txn commit
        // point, then drop the source files. Peak memory is O(batch).
        if partition_reorg_streaming_enabled() {
            return self.execute_partition_reorg_streaming(
                &key,
                table_name,
                &table,
                "repartition",
                old_storage_ids,
                new_sids,
                candidate,
                cutover,
            );
        }

        // ---- legacy full-memory path (MYDB_PARTITION_REORG_STREAMING=0,
        // differential oracle). Drains every row into RAM, swaps the
        // catalog, deletes old files, then re-inserts — not crash-safe. ----
        let old_paths = old_storage_ids
            .iter()
            .map(|sid| {
                let index_paths = table
                    .indexes
                    .iter()
                    .map(|index| {
                        (
                            index.name.clone(),
                            self.index_path_for_table_schema(&table, *sid, &index.name),
                        )
                    })
                    .collect::<Vec<_>>();
                (*sid, self.table_path(*sid), index_paths)
            })
            .collect::<Vec<_>>();

        let mut rows: Vec<Vec<crate::types::Value>> = Vec::new();
        for sid in &old_storage_ids {
            rows.extend(self.partition_rows_for_repartition(*sid)?);
        }

        {
            let mut catalog = self.catalog_write()?;
            let table_arc = catalog
                .tables
                .get_mut(&key)
                .ok_or_else(|| ddl_table_not_found(&table_name))?;
            let table_mut = Arc::make_mut(table_arc);
            table_mut.partitioning = candidate.partitioning.clone();
            table_mut.bump_schema_version();
        }

        for (sid, heap_path, index_paths) in &old_paths {
            self.remove_heap_cache_entry(*sid)?;
            let _ = self.delete_logged_file(txn_id_for_ddl(), heap_path.clone());
            for (index_name, path) in index_paths {
                self.remove_index_cache_entry(*sid, index_name)?;
                let _ = self.delete_logged_file(txn_id_for_ddl(), path.clone());
            }
        }
        for sid in &new_sids {
            self.create_logged_file(txn_id_for_ddl(), self.table_path(*sid), Vec::new())?;
            self.remove_heap_cache_entry(*sid)?;
        }
        self.note_catalog_table(table.id);
        self.commit_ddl_catalog_txn()?;

        if !rows.is_empty() {
            let txn = self.begin_txn()?;
            let qualified = table.qualified_name();
            self.insert_rows_bulk(txn, &qualified, rows, true)?;
            self.commit_txn(txn.txn_id)?;
        }
        Ok(())
    }

    /// `ALTER TABLE t COALESCE PARTITION N` — HASH/KEY only. Drop the
    /// trailing N partitions and re-distribute their rows across the
    /// remaining partitions.
    ///
    /// Algorithm:
    ///   1. Read every visible row from *all* partitions into memory.
    ///   2. Empty every partition's heap (TRUNCATE-style file recreate).
    ///   3. Drop the trailing N partitions from the catalog and from
    ///      disk; the remaining `total-N` partitions keep their
    ///      original storage_ids.
    ///   4. Re-route each in-memory row through the new partition
    ///      function and re-insert at its new partition's storage_id.
    ///
    /// Concurrent DML must be quiesced — we acquire the table-level
    /// X lock for the duration. Crash safety is best-effort: a partial
    /// failure between step 3 and step 4 may leave some rows in
    /// memory only; we rely on WAL redo from step 4's row writes to
    /// recover.
    fn coalesce_partitions(
        &self,
        table_name: &str,
        n: u32,
        cutover: Option<(u64, Duration, bool)>,
    ) -> Result<(), QueryError> {
        let (database, name) = split_table_name(DEFAULT_DATABASE, table_name);
        let key = table_key(&database, &name);
        let table = self
            .catalog_read()?
            .tables
            .get(&key)
            .cloned()
            .ok_or_else(|| ddl_table_not_found(&table_name))?;
        let part = table.partitioning.as_ref().ok_or_else(|| {
            error(
                crate::error::catalog::ER_PARTITION_MGMT_ON_NONPARTITIONED,
                "COALESCE PARTITION can only be used on a partitioned table",
            )
        })?;
        if !part.method.is_hash_like() {
            return Err(error(
                crate::error::catalog::ER_ONLY_ON_RANGE_LIST_PARTITION,
                "COALESCE PARTITION is only valid for HASH/KEY partitioning",
            ));
        }
        let total = part.partitions.len();
        if n == 0 {
            return Err(error(
                crate::error::catalog::ER_NO_PARTS_ERROR,
                "Number of partitions must be greater than zero",
            ));
        }
        if n as usize >= total {
            return Err(error(
                crate::error::catalog::ER_DROP_LAST_PARTITION,
                "cannot coalesce all partitions",
            ));
        }
        // M2.2: DDL X lock so no concurrent DML races with the
        // reshuffle.
        if !cutover.is_some_and(|(_, _, online)| online) {
            self.check_ddl_table_lock(table.id)?;
        }

        // Source = every current partition (a hash-modulo change can move
        // any row); the survivors are rebuilt into fresh shadow ids.
        let drop_from = total - n as usize;
        // Logical parents in a subpartitioned layout do not contain rows.
        let source_sids = part.storage_ids();

        // Build a candidate layout: the first `drop_from` partitions kept
        // (names preserved) but bound to fresh storage ids.
        let build_sids: Vec<u64> = {
            let mut catalog = self.catalog_write()?;
            (0..drop_from)
                .map(|_| {
                    let sid = catalog.next_table_id;
                    catalog.next_table_id += 1;
                    sid
                })
                .collect()
        };
        self.ensure_table_id_highwater(self.current_next_table_id()?)?;
        let survivor_partitions: Vec<crate::storage::partitioning::Partition> = part.partitions
            [..drop_from]
            .iter()
            .zip(build_sids.iter())
            .map(|(p, sid)| crate::storage::partitioning::Partition {
                name: p.name.clone(),
                storage_id: *sid,
                bounds: crate::storage::partitioning::PartitionBounds::Hash,
                comment: p.comment.clone(),
                options: p.options.clone(),
                subpartitions: Vec::new(),
            })
            .collect();
        let mut candidate = table.as_ref().clone();
        {
            let part_mut = candidate.partitioning.as_mut().unwrap();
            part_mut.partitions = survivor_partitions;
        }
        candidate.build_column_index();

        if partition_reorg_streaming_enabled() {
            return self.execute_partition_reorg_streaming(
                &key,
                table_name,
                &table,
                "coalesce",
                source_sids,
                build_sids,
                candidate,
                cutover,
            );
        }

        // ---- legacy full-memory path (differential oracle). ----
        // Capture source file paths *before* the swap, while their ids
        // still resolve through the catalog.
        let source_paths: Vec<(u64, PathBuf, Vec<(String, PathBuf)>)> = source_sids
            .iter()
            .map(|sid| {
                let index_paths = table
                    .indexes
                    .iter()
                    .map(|index| (index.name.clone(), self.index_path(*sid, &index.name)))
                    .collect::<Vec<_>>();
                (*sid, self.table_path(*sid), index_paths)
            })
            .collect();
        let mut all_rows: Vec<Vec<crate::types::Value>> = Vec::new();
        for sid in &source_sids {
            all_rows.extend(self.partition_rows_for_repartition(*sid)?);
        }
        {
            let mut catalog = self.catalog_write()?;
            let table_arc = catalog
                .tables
                .get_mut(&key)
                .ok_or_else(|| ddl_table_not_found(&table_name))?;
            let table_mut = Arc::make_mut(table_arc);
            table_mut.partitioning = candidate.partitioning.clone();
            table_mut.bump_schema_version();
        }
        for (sid, heap_path, index_paths) in &source_paths {
            self.remove_heap_cache_entry(*sid)?;
            let _ = self.delete_logged_file(txn_id_for_ddl(), heap_path.clone());
            for (index_name, path) in index_paths {
                self.remove_index_cache_entry(*sid, index_name)?;
                let _ = self.delete_logged_file(txn_id_for_ddl(), path.clone());
            }
        }
        for sid in &build_sids {
            self.remove_heap_cache_entry(*sid)?;
            self.create_logged_file(txn_id_for_ddl(), self.table_path(*sid), Vec::new())?;
        }
        self.note_catalog_table(table.id);
        self.commit_ddl_catalog_txn()?;

        if !all_rows.is_empty() {
            let txn = self.begin_txn()?;
            let qualified = table.qualified_name();
            self.insert_rows_bulk(txn, &qualified, all_rows, true)?;
            self.commit_txn(txn.txn_id)?;
        }
        Ok(())
    }

    /// `ALTER TABLE t REMOVE PARTITIONING` — collapse all partitions
    /// back into a single heap. Reads every row from every partition,
    /// then allocates a new storage_id (== `table.id` was previously
    /// unused for partitioned tables, so we reuse it), writes them
    /// there, drops every partition file, and clears the partitioning
    /// field on the schema.
    fn remove_partitioning(
        &self,
        table_name: &str,
        cutover: Option<(u64, Duration, bool)>,
    ) -> Result<(), QueryError> {
        let (database, name) = split_table_name(DEFAULT_DATABASE, table_name);
        let key = table_key(&database, &name);
        let table = self
            .catalog_read()?
            .tables
            .get(&key)
            .cloned()
            .ok_or_else(|| ddl_table_not_found(&table_name))?;
        let part = table.partitioning.as_ref().ok_or_else(|| {
            error(
                crate::error::catalog::ER_PARTITION_MGMT_ON_NONPARTITIONED,
                "REMOVE PARTITIONING can only be used on a partitioned table",
            )
        })?;
        if !cutover.is_some_and(|(_, _, online)| online) {
            self.check_ddl_table_lock(table.id)?;
        }
        // Only physical leaves contain rows; parent ids are metadata-only.
        let source_sids = part.storage_ids();

        // Candidate = the same table, unpartitioned. Its single heap lives
        // at the logical id `table.id` (unused while partitioned), which is
        // the sole shadow storage id we build + fill.
        let mut candidate = table.as_ref().clone();
        candidate.partitioning = None;
        candidate.build_column_index();
        let build_sids = vec![table.id];

        if partition_reorg_streaming_enabled() {
            return self.execute_partition_reorg_streaming(
                &key,
                table_name,
                &table,
                "remove_partitioning",
                source_sids,
                build_sids,
                candidate,
                cutover,
            );
        }

        // ---- legacy full-memory path (differential oracle). ----
        let mut all_rows: Vec<Vec<crate::types::Value>> = Vec::new();
        for sid in &source_sids {
            all_rows.extend(self.partition_rows_for_repartition(*sid)?);
        }
        for sid in &source_sids {
            self.remove_heap_cache_entry(*sid)?;
            let _ = self.delete_logged_file(txn_id_for_ddl(), self.table_path(*sid));
            for index in &table.indexes {
                self.remove_index_cache_entry(*sid, &index.name)?;
                let _ =
                    self.delete_logged_file(txn_id_for_ddl(), self.index_path(*sid, &index.name));
            }
        }
        {
            let mut catalog = self.catalog_write()?;
            let table_arc = catalog
                .tables
                .get_mut(&key)
                .ok_or_else(|| ddl_table_not_found(&table_name))?;
            let table_mut = Arc::make_mut(table_arc);
            table_mut.partitioning = None;
            table_mut.bump_schema_version();
        }
        self.create_logged_file(txn_id_for_ddl(), self.table_path(table.id), Vec::new())?;
        self.remove_heap_cache_entry(table.id)?;
        self.note_catalog_table(table.id);
        self.commit_ddl_catalog_txn()?;
        if !all_rows.is_empty() {
            let txn = self.begin_txn()?;
            let qualified = table.qualified_name();
            self.insert_rows_bulk(txn, &qualified, all_rows, true)?;
            self.commit_txn(txn.txn_id)?;
        }
        Ok(())
    }

    /// `ALTER TABLE t REORGANIZE PARTITION p1, p2 INTO (
    ///     PARTITION q1 VALUES ..., PARTITION q2 VALUES ...
    /// )` — RANGE/LIST only.
    ///
    /// Reads every row from the listed partitions, drops them, creates
    /// new partitions with the supplied bounds at fresh storage_ids,
    /// then re-inserts each row routed through the new partition set.
    fn reorganize_partitions(
        &self,
        table_name: &str,
        old_partitions: &[String],
        new_partitions: Vec<crate::parser_partition::PartitionItemDef>,
        cutover: Option<(u64, Duration, bool)>,
    ) -> Result<(), QueryError> {
        if old_partitions.is_empty() {
            return Err(error(
                crate::error::catalog::ER_DROP_PARTITION_NON_EXISTENT,
                "REORGANIZE PARTITION requires at least one source partition",
            ));
        }
        let (database, name) = split_table_name(DEFAULT_DATABASE, table_name);
        let key = table_key(&database, &name);
        let table = self
            .catalog_read()?
            .tables
            .get(&key)
            .cloned()
            .ok_or_else(|| ddl_table_not_found(&table_name))?;
        let part = table.partitioning.as_ref().ok_or_else(|| {
            error(
                crate::error::catalog::ER_PARTITION_MGMT_ON_NONPARTITIONED,
                "REORGANIZE PARTITION can only be used on a partitioned table",
            )
        })?;
        if !part.method.is_range() && !part.method.is_list() {
            return Err(error(
                crate::error::catalog::ER_ONLY_ON_RANGE_LIST_PARTITION,
                "REORGANIZE PARTITION is only valid for RANGE/LIST partitioning",
            ));
        }
        if !cutover.is_some_and(|(_, _, online)| online) {
            self.check_ddl_table_lock(table.id)?;
        }

        // Resolve old partition names to indices + storage_ids. They
        // must be contiguous in the declared order for RANGE so the
        // result is again a non-overlapping ascending sequence.
        let mut old_indices: Vec<usize> = Vec::with_capacity(old_partitions.len());
        for n in old_partitions {
            let idx = part
                .partitions
                .iter()
                .position(|p| p.name.eq_ignore_ascii_case(n))
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_DROP_PARTITION_NON_EXISTENT,
                        format!("partition '{n}' does not exist"),
                    )
                })?;
            if old_indices.contains(&idx) {
                return Err(error(
                    crate::error::catalog::ER_SAME_NAME_PARTITION,
                    format!("duplicate source partition '{n}'"),
                ));
            }
            old_indices.push(idx);
        }
        let mut sorted_old = old_indices.clone();
        sorted_old.sort_unstable();
        if part.method.is_range() {
            for window in sorted_old.windows(2) {
                if window[1] != window[0] + 1 {
                    return Err(error(
                        crate::error::catalog::ER_CONSECUTIVE_REORG_PARTITIONS,
                        "REORGANIZE PARTITION partitions must be contiguous for RANGE",
                    ));
                }
            }
        }
        let drop_sids: Vec<u64> = old_indices
            .iter()
            .flat_map(|idx| part.storage_ids_for_partition_index(*idx))
            .collect();
        // Build the new partition specs.
        let synth_def = crate::parser_partition::PartitionDef {
            method: match part.method {
                crate::storage::partitioning::PartitionMethod::Range => {
                    crate::parser_partition::PartitionMethod::Range
                }
                crate::storage::partitioning::PartitionMethod::RangeColumns => {
                    crate::parser_partition::PartitionMethod::RangeColumns
                }
                crate::storage::partitioning::PartitionMethod::List => {
                    crate::parser_partition::PartitionMethod::List
                }
                crate::storage::partitioning::PartitionMethod::ListColumns => {
                    crate::parser_partition::PartitionMethod::ListColumns
                }
                _ => unreachable!("range/list guarded above"),
            },
            expr_sql: (!part.expr_sql.is_empty()).then(|| part.expr_sql.clone()),
            columns: part.columns.clone(),
            key_algorithm: None,
            partition_count: None,
            partitions: new_partitions,
            subpartition_by: part.subpartitioning.as_ref().map(|sub| {
                crate::parser_partition::SubpartitionByDef {
                    method: match sub.method {
                        crate::storage::partitioning::PartitionMethod::Hash => {
                            crate::parser_partition::PartitionMethod::Hash
                        }
                        crate::storage::partitioning::PartitionMethod::LinearHash => {
                            crate::parser_partition::PartitionMethod::LinearHash
                        }
                        crate::storage::partitioning::PartitionMethod::Key => {
                            crate::parser_partition::PartitionMethod::Key
                        }
                        crate::storage::partitioning::PartitionMethod::LinearKey => {
                            crate::parser_partition::PartitionMethod::LinearKey
                        }
                        _ => unreachable!("validated HASH/KEY subpartitioning"),
                    },
                    expr_sql: (!sub.expr_sql.is_empty()).then(|| sub.expr_sql.clone()),
                    columns: sub.columns.clone(),
                    key_algorithm: sub.key_algorithm,
                    count: sub.count,
                }
            }),
        };
        let new_storage_count = partition_count_from_def(Some(&synth_def))?;
        let new_storage_ids: Vec<u64> = {
            let mut catalog = self.catalog_write()?;
            (0..new_storage_count)
                .map(|_| {
                    let pid = catalog.next_table_id;
                    catalog.next_table_id += 1;
                    pid
                })
                .collect()
        };
        self.ensure_table_id_highwater(self.current_next_table_id()?)?;
        let new_info = build_partition_info_from_def(&synth_def, &table, &new_storage_ids)?;
        let new_sids = new_info.storage_ids();
        let new_last = new_info.partitions.last().cloned();

        // Build the candidate layout in memory: drop the reorganized
        // partitions, splice the new ones in at the first old position
        // (preserves ascending RANGE ordering).
        let mut candidate = table.as_ref().clone();
        {
            let part_mut = candidate.partitioning.as_mut().unwrap();
            let old_index_set: std::collections::HashSet<usize> =
                old_indices.iter().copied().collect();
            part_mut.partitions = std::mem::take(&mut part_mut.partitions)
                .into_iter()
                .enumerate()
                .filter_map(|(idx, partition)| (!old_index_set.contains(&idx)).then_some(partition))
                .collect();
            let insert_at = sorted_old
                .first()
                .copied()
                .unwrap_or(part_mut.partitions.len());
            for (offset, new_part) in new_info.partitions.into_iter().enumerate() {
                let pos = (insert_at + offset).min(part_mut.partitions.len());
                part_mut.partitions.insert(pos, new_part);
            }
        }
        candidate.build_column_index();

        // S2 — validate the *resulting* layout before draining any rows.
        // `add_partitions`/`repartition_table` already do this; REORGANIZE
        // used to skip it, so a spliced-in bound that overlaps or is
        // non-monotonic (e.g. reorganizing `p0(<10)` into `(a<5, b<200)`
        // ahead of a retained `p1(<100)`) was silently accepted and then
        // mis-routed reads. `validate_range_bound_is_increasing` inside
        // this call rejects the non-monotonic sequence with 1493.
        validate_mysql80_partition_compat(&candidate, false)?;
        if part.method.is_range() {
            use crate::storage::partitioning::PartitionBounds;
            let last = *sorted_old.last().expect("nonempty source partitions");
            let PartitionBounds::RangeLessThan(old_bound) = &part.partitions[last].bounds else {
                unreachable!("validated range partition")
            };
            let Some(crate::storage::partitioning::Partition {
                bounds: PartitionBounds::RangeLessThan(new_bound),
                ..
            }) = new_last.as_ref()
            else {
                return Err(error(
                    crate::error::catalog::ER_REORG_OUTSIDE_RANGE,
                    "REORGANIZE requires a nonempty replacement range",
                ));
            };
            let order = compare_partition_bounds(
                new_bound,
                old_bound,
                &partition_bound_collations(&candidate),
            );
            if order == std::cmp::Ordering::Less
                || (last + 1 < part.partitions.len() && order != std::cmp::Ordering::Equal)
            {
                return Err(error(
                    crate::error::catalog::ER_REORG_OUTSIDE_RANGE,
                    "REORGANIZE must preserve the covered range; only the final range may expand",
                ));
            }
        }

        // Only the reorganized partitions are drained + rebuilt; retained
        // partitions keep their storage ids and files untouched.
        if partition_reorg_streaming_enabled() {
            return self.execute_partition_reorg_streaming(
                &key,
                table_name,
                &table,
                "reorganize",
                drop_sids,
                new_sids,
                candidate,
                cutover,
            );
        }

        // ---- legacy full-memory path (differential oracle). ----
        let mut rows: Vec<Vec<crate::types::Value>> = Vec::new();
        for sid in &drop_sids {
            rows.extend(self.partition_rows_for_repartition(*sid)?);
        }
        {
            let mut catalog = self.catalog_write()?;
            let table_arc = catalog
                .tables
                .get_mut(&key)
                .ok_or_else(|| ddl_table_not_found(&table_name))?;
            let table_mut = Arc::make_mut(table_arc);
            table_mut.partitioning = candidate.partitioning.clone();
            table_mut.bump_schema_version();
        }
        for sid in &drop_sids {
            self.remove_heap_cache_entry(*sid)?;
            self.forget_zone_map(*sid)?;
            let _ = self.delete_logged_file(txn_id_for_ddl(), self.table_path(*sid));
            let _ = self.delete_logged_file(txn_id_for_ddl(), self.zone_map_file_path(*sid));
            for index in &table.indexes {
                self.remove_index_cache_entry(*sid, &index.name)?;
                let _ =
                    self.delete_logged_file(txn_id_for_ddl(), self.index_path(*sid, &index.name));
            }
        }
        for sid in &new_sids {
            self.create_logged_file(txn_id_for_ddl(), self.table_path(*sid), Vec::new())?;
            self.remove_heap_cache_entry(*sid)?;
        }
        self.note_catalog_table(table.id);
        self.commit_ddl_catalog_txn()?;
        if !rows.is_empty() {
            let txn = self.begin_txn()?;
            let qualified = table.qualified_name();
            self.insert_rows_bulk(txn, &qualified, rows, true)?;
            self.commit_txn(txn.txn_id)?;
        }
        Ok(())
    }

    /// `ALTER TABLE p t EXCHANGE PARTITION p WITH TABLE u
    /// [WITH|WITHOUT VALIDATION]` — swap one partition's data with an
    /// unpartitioned sibling table. WITH VALIDATION verifies every row
    /// in `u` would actually route to partition `p`; WITHOUT trusts the
    /// caller (matches MySQL's semantics).
    ///
    /// Implementation: swap the `storage_id` between the partition
    /// entry and the unpartitioned table's `id`/`object_id`. The heap
    /// + per-partition index files keep their bytes intact; only the
    /// catalog binding changes.
    fn exchange_partition(
        &self,
        table_name: &str,
        partition_name: &str,
        other_table_name: &str,
        with_validation: bool,
    ) -> Result<(), QueryError> {
        let (database, name) = split_table_name(DEFAULT_DATABASE, table_name);
        let key = table_key(&database, &name);
        let other_key = {
            let (odb, oname) = split_table_name(&database, other_table_name);
            table_key(&odb, &oname)
        };
        let (table, other_table) = {
            let catalog = self.catalog_read()?;
            let table = catalog
                .tables
                .get(&key)
                .cloned()
                .ok_or_else(|| ddl_table_not_found(&table_name))?;
            let other = catalog
                .tables
                .get(&other_key)
                .cloned()
                .ok_or_else(|| ddl_table_not_found(&other_table_name))?;
            (table, other)
        };
        let part = table.partitioning.as_ref().ok_or_else(|| {
            error(
                crate::error::catalog::ER_PARTITION_MGMT_ON_NONPARTITIONED,
                "EXCHANGE PARTITION requires a partitioned table on the left side",
            )
        })?;
        if other_table.is_partitioned() {
            return Err(error(
                crate::error::catalog::ER_PARTITION_EXCHANGE_TEMP_TABLE,
                "EXCHANGE PARTITION cannot target a partitioned table",
            ));
        }
        // MySQL 8.0 forbids EXCHANGE PARTITION when either table participates
        // in a foreign key — as child (outbound `foreign_keys`) or parent
        // (inbound `fk_referenced_by`). A raw file swap bypasses FK integrity,
        // leaving dangling child rows / parentless rows and desyncing the FK
        // runtime index from the swapped data. Checked before the schema-shape
        // checks so an FK table fails fast with the specific
        // ER_PARTITION_EXCHANGE_FOREIGN_KEY (1740).
        let has_fk = |t: &TableSchema| !t.foreign_keys.is_empty() || !t.fk_referenced_by.is_empty();
        if has_fk(table.as_ref()) || has_fk(other_table.as_ref()) {
            return Err(error(
                crate::error::catalog::ER_PARTITION_EXCHANGE_FOREIGN_KEY,
                "EXCHANGE PARTITION is not allowed with tables that have foreign key constraints",
            ));
        }
        if table.columns.len() != other_table.columns.len() {
            return Err(error(
                crate::error::catalog::ER_TABLES_DIFFERENT_METADATA,
                "EXCHANGE PARTITION tables must have identical column count",
            ));
        }
        // P2-D1 phase 2: column-by-column compatibility check. MySQL
        // 8.0 surfaces ER_TABLES_DIFFERENT_METADATA (1736) when the
        // tables disagree on column name, data type, nullability, or
        // unsigned flag at the same ordinal position — exchanging
        // metadata-incompatible bytes would corrupt the row codec.
        for (idx, (left, right)) in table
            .columns
            .iter()
            .zip(other_table.columns.iter())
            .enumerate()
        {
            if left.name.to_ascii_lowercase() != right.name.to_ascii_lowercase() {
                return Err(error(
                    crate::error::catalog::ER_TABLES_DIFFERENT_METADATA,
                    format!(
                        "EXCHANGE PARTITION: column #{} name mismatch ('{}' vs '{}')",
                        idx + 1,
                        left.name,
                        right.name
                    ),
                ));
            }
            if left.data_type != right.data_type {
                return Err(error(
                    crate::error::catalog::ER_TABLES_DIFFERENT_METADATA,
                    format!(
                        "EXCHANGE PARTITION: column '{}' data type mismatch ({:?} vs {:?})",
                        left.name, left.data_type, right.data_type
                    ),
                ));
            }
            if left.charset.as_deref().unwrap_or(&table.charset)
                != right.charset.as_deref().unwrap_or(&other_table.charset)
                || left.collation.as_deref().unwrap_or(&table.collation)
                    != right.collation.as_deref().unwrap_or(&other_table.collation)
                || left.generated_expr != right.generated_expr
                || left.generated_stored != right.generated_stored
            {
                return Err(error(
                    crate::error::catalog::ER_TABLES_DIFFERENT_METADATA,
                    format!("EXCHANGE PARTITION: column '{}' charset, collation or generated expression differs", left.name),
                ));
            }
            if left.nullable != right.nullable || left.unsigned != right.unsigned {
                return Err(error(
                    crate::error::catalog::ER_TABLES_DIFFERENT_METADATA,
                    format!(
                        "EXCHANGE PARTITION: column '{}' nullability / unsigned attribute differs",
                        left.name
                    ),
                ));
            }
        }
        // EXCHANGE moves rows in both directions without evaluating CHECKs.
        // Names and declaration order do not affect enforcement, while the
        // expression and ENFORCED flag must agree even WITHOUT VALIDATION.
        let check_signature = |schema: &TableSchema| {
            let mut checks = schema
                .check_constraints
                .iter()
                .map(|check| (check.expr.clone(), check.enforced))
                .collect::<Vec<_>>();
            checks.sort_unstable();
            checks
        };
        if check_signature(&table) != check_signature(&other_table) {
            return Err(error(
                crate::error::catalog::ER_TABLES_DIFFERENT_METADATA,
                "EXCHANGE PARTITION: tables have different CHECK constraint definitions",
            ));
        }
        // Primary key shape must match — exchanging a heap whose PK
        // columns disagree with the partition's would silently break
        // the partitioning function on subsequent writes.
        let left_pk: Vec<String> = table
            .primary_key
            .iter()
            .map(|s| s.to_ascii_lowercase())
            .collect();
        let right_pk: Vec<String> = other_table
            .primary_key
            .iter()
            .map(|s| s.to_ascii_lowercase())
            .collect();
        if left_pk != right_pk {
            return Err(error(
                crate::error::catalog::ER_TABLES_DIFFERENT_METADATA,
                "EXCHANGE PARTITION: primary key columns differ between tables",
            ));
        }
        // Secondary index sets must match. EXCHANGE swaps the physical
        // storage (heap + index files) between the partition and the
        // standalone table; the post-swap cache invalidation and index-file
        // rename below pair entries *by index name*. A mismatched index set
        // would leave a stale cache handle for an unmatched index on the
        // swapped storage_id and rename `.idx` files to misplaced / orphan
        // targets. MySQL 8.0 rejects this with ER_TABLES_DIFFERENT_METADATA
        // (1736) — the tables must be index-identical.
        let index_signature = |t: &TableSchema| {
            let mut sig: Vec<_> = t
                .iter_public_indexes()
                .map(|idx| {
                    (
                        idx.name.to_ascii_lowercase(),
                        idx.unique,
                        idx.primary,
                        idx.index_type.to_ascii_uppercase(),
                        idx.columns
                            .iter()
                            .map(|c| c.to_ascii_lowercase())
                            .collect::<Vec<_>>(),
                        (0..idx.columns.len())
                            .map(|i| {
                                (
                                    idx.column_prefix_lengths.get(i).copied().flatten(),
                                    matches!(
                                        idx.column_directions.get(i),
                                        Some(crate::storage::IndexColumnDirection::Desc)
                                    ),
                                )
                            })
                            .collect::<Vec<_>>(),
                        idx.included_columns
                            .iter()
                            .map(|c| c.to_ascii_lowercase())
                            .collect::<Vec<_>>(),
                        // Expressions and FT configuration determine physical key encoding.
                        serde_json::to_string(&idx.key_expressions)
                            .expect("serializable index expressions"),
                        serde_json::to_string(&idx.fulltext)
                            .expect("serializable fulltext metadata"),
                    )
                })
                .collect();
            sig.sort();
            sig
        };
        if index_signature(table.as_ref()) != index_signature(other_table.as_ref()) {
            return Err(error(
                crate::error::catalog::ER_TABLES_DIFFERENT_METADATA,
                "EXCHANGE PARTITION: tables have different secondary index definitions",
            ));
        }
        // One standalone heap can exchange with exactly one physical leaf.
        // A logical parent of subpartitions is therefore rejected.
        let (part_idx, subpart_idx, partition_storage_id) = {
            let mut resolved = None;
            for (parent_idx, parent) in part.partitions.iter().enumerate() {
                if parent.name.eq_ignore_ascii_case(partition_name) {
                    if !parent.subpartitions.is_empty() {
                        return Err(error(
                            crate::error::catalog::ER_PARTITION_INSTEAD_OF_SUBPARTITION,
                            "Subpartitioned table, use subpartition instead of partition",
                        ));
                    }
                    resolved = Some((parent_idx, None, parent.storage_id));
                    break;
                }
                if let Some((leaf_idx, leaf)) = parent
                    .subpartitions
                    .iter()
                    .enumerate()
                    .find(|(_, leaf)| leaf.name.eq_ignore_ascii_case(partition_name))
                {
                    resolved = Some((parent_idx, Some(leaf_idx), leaf.storage_id));
                    break;
                }
            }
            resolved.ok_or_else(|| {
                error(
                    crate::error::catalog::ER_DROP_PARTITION_NON_EXISTENT,
                    format!("partition '{partition_name}' does not exist"),
                )
            })?
        };
        let outgoing_partition_stats = table
            .stats
            .partition_stats
            .get(&partition_storage_id)
            .cloned();
        let incoming_partition_stats = crate::storage::PartitionStats {
            row_count: other_table.stats.row_count,
            heap_page_count: other_table.stats.heap_page_count,
            columns: other_table.stats.columns.clone(),
            indexes: other_table.stats.indexes.clone(),
            modified_rows_since_analyze: other_table.stats.modified_rows_since_analyze,
            analyzed_at_unix_ms: other_table.stats.analyzed_at_unix_ms,
            check_ok: None,
        };
        let lock_txn = self.begin_read_only_txn()?;
        let _exchange_locks = OnlineIndexTxnGuard {
            db: self,
            txn_id: lock_txn.txn_id,
        };
        let mut lock_ids = vec![table.id, partition_storage_id, other_table.id];
        lock_ids.sort_unstable();
        lock_ids.dedup();
        for id in lock_ids {
            self.acquire_table_exclusive(lock_txn.txn_id, id)?;
        }

        // WITH VALIDATION: each row currently in the unpartitioned
        // table must route to this partition under the partitioning
        // function. Reject otherwise (matches MySQL 1737).
        if with_validation {
            let txn = self.begin_read_only_txn()?;
            let _read_guard = PartitionReadTxnGuard {
                db: self,
                txn_id: txn.txn_id,
            };
            let columns: Vec<_> = (0..other_table.columns.len()).collect();
            self.scan_rows_columns_streaming(&other_key, txn, &columns, |row| {
                let routed = table.route_row_to_partition(row)?;
                if routed.index != part_idx || routed.storage_id != partition_storage_id {
                    return Err(error(
                        crate::error::catalog::ER_ROW_DOES_NOT_MATCH_PARTITION,
                        "EXCHANGE PARTITION: found row not matching the partition's bounds",
                    ));
                }
                Ok(())
            })?;
        }

        // Flush dirty pages for both heaps before the rename, so the
        // exchanged files contain every committed write. Discarding
        // a cached heap with dirty pages would lose those writes.
        let _ = self.with_heap_exclusive(partition_storage_id, |heap| heap.flush_dirty_pages());
        let _ = self.with_heap_exclusive(other_table.id, |heap| heap.flush_dirty_pages());
        // The following rename must operate on complete files, not dirty
        // cache state. Ordinary commits may defer B-tree writeback.
        for index in &table.indexes {
            self.with_index_exclusive(partition_storage_id, &index.name, |idx| idx.flush())?;
            self.with_index_exclusive(other_table.id, &index.name, |idx| idx.flush())?;
        }
        // Flush *all* dirty buffer-pool pages (crucially the overflow/LOB
        // pages the standalone table's INSERTs just wrote) to disk before the
        // rename. `remove_overflow_cache_entry` discards without flushing, so
        // without this the un-written LOB pages would be lost and the swapped
        // `.lob` file would read back a corrupt/incomplete chain.
        self.flush_dirty_data()?;
        // Heap-cache entries are keyed by storage_id; the next access
        // must reopen via the new path that the rename installs.
        self.remove_heap_cache_entry(partition_storage_id)?;
        self.remove_heap_cache_entry(other_table.id)?;
        // Overflow handles are keyed by storage_id too; drop them so the next
        // access reopens via the post-rename `.lob` path.
        self.remove_overflow_cache_entry(partition_storage_id)?;
        self.remove_overflow_cache_entry(other_table.id)?;
        for index in &table.indexes {
            self.remove_index_cache_entry(partition_storage_id, &index.name)?;
            self.remove_index_cache_entry(other_table.id, &index.name)?;
        }
        // The on-disk file name embeds the *owner* table name, not the
        // storage id alone — `partition_storage_id` is owned by
        // `part_tbl`, so its file is `part_tbl__<sid>__data.heap`.
        // After the swap below, `partition_storage_id` is owned by the
        // unpartitioned table (`other`), so the on-disk file has to be
        // renamed to the unpartitioned name. Same for the unpartitioned
        // table's old id, which now belongs to `part_tbl`'s p0.
        let part_path_old = self.table_path(partition_storage_id);
        let other_path_old = self.table_path(other_table.id);
        // Overflow (LOB) sidecars must swap alongside the heap: a swapped heap
        // row's externalized columns hold `LobRef`s into `overflow_path(sid)`,
        // so leaving the `.lob` behind makes those columns unreadable (missing
        // file / wrong data). Resolved with the same before/after-swap timing
        // as the heap paths. AUTO_INCREMENT is logical metadata and therefore
        // does not participate in the physical rename plan.
        let part_lob_old = self.overflow_path(partition_storage_id);
        let other_lob_old = self.overflow_path(other_table.id);
        // Publish all dirty summary pages before the path changes. Evict
        // their old file handles as well: otherwise a later buffer flush can
        // reopen the pre-exchange path and recreate a stale sidecar.
        self.flush_for_rename_zone_map(partition_storage_id)?;
        self.flush_for_rename_zone_map(other_table.id)?;
        let part_zm_old = self.zone_map_file_path(partition_storage_id);
        let other_zm_old = self.zone_map_file_path(other_table.id);
        // Capture index paths up front too, indexed by name.
        let part_index_paths: Vec<(String, PathBuf)> = table
            .indexes
            .iter()
            .map(|i| {
                (
                    i.name.clone(),
                    self.index_path(partition_storage_id, &i.name),
                )
            })
            .collect();
        let other_index_paths: Vec<(String, PathBuf)> = other_table
            .indexes
            .iter()
            .map(|i| (i.name.clone(), self.index_path(other_table.id, &i.name)))
            .collect();
        // Swap storage_ids in the catalog. The partition entry takes
        // the unpartitioned table's id, and the unpartitioned table's
        // (id, object_id) become the partition's old id.
        {
            let mut catalog = self.catalog_write()?;
            let table_arc = catalog.tables.get_mut(&key).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_NO_SUCH_TABLE,
                    format!("table '{table_name}'"),
                )
            })?;
            let table_mut = Arc::make_mut(table_arc);
            let part_mut = table_mut.partitioning.as_mut().unwrap();
            if let Some(sub_idx) = subpart_idx {
                part_mut.partitions[part_idx].subpartitions[sub_idx].storage_id = other_table.id;
            } else {
                part_mut.partitions[part_idx].storage_id = other_table.id;
            }
            // The schema Arc clone may carry initialized derived maps from
            // the pre-swap generation. Never publish them with new storage
            // ids: explicit PARTITION selection and storage-id ownership
            // checks would otherwise keep routing to the exchanged-away leaf.
            part_mut.runtime = std::sync::OnceLock::new();
            part_mut.metadata_runtime = std::sync::OnceLock::new();
            table_mut
                .stats
                .partition_stats
                .remove(&partition_storage_id);
            table_mut
                .stats
                .partition_stats
                .insert(other_table.id, incoming_partition_stats);
            let current_ids = part_mut.storage_ids();
            if current_ids
                .iter()
                .all(|storage_id| table_mut.stats.partition_stats.contains_key(storage_id))
            {
                table_mut.stats.row_count = current_ids
                    .iter()
                    .map(|storage_id| table_mut.stats.partition_stats[storage_id].row_count)
                    .sum();
                table_mut.stats.heap_page_count = current_ids
                    .iter()
                    .map(|storage_id| table_mut.stats.partition_stats[storage_id].heap_page_count)
                    .sum();
            }
            table_mut.bump_schema_version();
            let other_arc = catalog.tables.get_mut(&other_key).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_NO_SUCH_TABLE,
                    format!("table '{other_table_name}'"),
                )
            })?;
            let other_mut = Arc::make_mut(other_arc);
            other_mut.id = partition_storage_id;
            other_mut.object_id = partition_storage_id;
            other_mut.stats.partition_stats.clear();
            if let Some(stats) = outgoing_partition_stats {
                other_mut.stats.row_count = stats.row_count;
                other_mut.stats.heap_page_count = stats.heap_page_count;
                other_mut.stats.columns = stats.columns;
                other_mut.stats.indexes = stats.indexes;
                other_mut.stats.modified_rows_since_analyze = stats.modified_rows_since_analyze;
                other_mut.stats.analyzed_at_unix_ms = stats.analyzed_at_unix_ms;
            } else {
                other_mut.stats.analyzed_at_unix_ms = None;
                other_mut.stats.modified_rows_since_analyze = other_mut
                    .stats
                    .modified_rows_since_analyze
                    .saturating_add(1);
            }
            other_mut.bump_schema_version();
        }
        // EXCHANGE swaps *content*, not just storage_ids. The bytes
        // that used to live in `partition_storage_id` (the old p0
        // heap) now have to be visible as the new staging table, and
        // vice versa. New paths are resolved *after* the swap so file
        // ownership reflects the new binding:
        //
        //   - the new staging row's storage_id is `partition_storage_id`,
        //     so its file is `staging__<partition_storage_id>__data.heap`.
        //   - the new p0 partition's storage_id is `other_table.id`,
        //     so its file is `part_tbl__<other_table.id>__data.heap`.
        //
        // A two-step rename via `.exchange-tmp` names keeps either move
        // from colliding with a still-occupied destination.
        let part_path_new = self.table_path(other_table.id);
        let other_path_new = self.table_path(partition_storage_id);
        let part_path_tmp = part_path_old.with_extension("heap.exchange-tmp");
        // Post-swap LOB destinations (owner names now reflect the swap).
        let part_lob_new = self.overflow_path(other_table.id);
        let other_lob_new = self.overflow_path(partition_storage_id);
        let part_lob_tmp = part_lob_old.with_extension("lob.exchange-tmp");
        let part_zm_new = self.zone_map_file_path(other_table.id);
        let other_zm_new = self.zone_map_file_path(partition_storage_id);
        let part_zm_tmp = part_zm_old.with_extension("zm.exchange-tmp");
        // Build the ordered post-commit rename plan (relative paths) and
        // journal it so a crash between the durable catalog swap and the
        // last rename can be completed at startup (§2.1 EXCHANGE
        // hardening). Replaying with "skip when source is absent" makes
        // any partial state idempotent.
        let mut renames: Vec<(String, String)> = vec![
            (
                self.relative_path(&part_path_old),
                self.relative_path(&part_path_tmp),
            ),
            (
                self.relative_path(&other_path_old),
                self.relative_path(&part_path_new),
            ),
            (
                self.relative_path(&part_path_tmp),
                self.relative_path(&other_path_new),
            ),
            // LOB overflow sidecars — same three-step tmp swap as the heap.
            (
                self.relative_path(&part_lob_old),
                self.relative_path(&part_lob_tmp),
            ),
            (
                self.relative_path(&other_lob_old),
                self.relative_path(&part_lob_new),
            ),
            (
                self.relative_path(&part_lob_tmp),
                self.relative_path(&other_lob_new),
            ),
            // Zone Map is derived but must move with the exact heap image;
            // its manifest is keyed by storage id, which remains aligned
            // after this same three-step content swap.
            (
                self.relative_path(&part_zm_old),
                self.relative_path(&part_zm_tmp),
            ),
            (
                self.relative_path(&other_zm_old),
                self.relative_path(&part_zm_new),
            ),
            (
                self.relative_path(&part_zm_tmp),
                self.relative_path(&other_zm_new),
            ),
        ];
        for (name, old) in &part_index_paths {
            let tmp = old.with_extension("idx.exchange-tmp");
            let new = self.index_path(partition_storage_id, name);
            renames.push((self.relative_path(old), self.relative_path(&tmp)));
            renames.push((self.relative_path(&tmp), self.relative_path(&new)));
        }
        for (name, old) in &other_index_paths {
            let new = self.index_path(other_table.id, name);
            renames.push((self.relative_path(old), self.relative_path(&new)));
        }
        // Both logical tables retain monotone allocation after acquiring
        // rows (and possibly unused reservations) from the other side.
        let auto_increment_floor = if other_table.auto_increment_column_index().is_some() {
            self.auto_increment_state(table.id, 0)?
                .max_value
                .load(Ordering::Acquire)
                .max(
                    self.auto_increment_state(other_table.id, 0)?
                        .max_value
                        .load(Ordering::Acquire),
                )
        } else {
            0
        };
        let exchange_journal = PartitionExchangeJournal {
            table_id: table.id,
            part_idx,
            subpart_idx,
            swapped_sid: other_table.id,
            auto_increment: if other_table.auto_increment_column_index().is_some() {
                Some(self.prepare_exchange_auto_increment(
                    other_table.id,
                    partition_storage_id,
                    auto_increment_floor,
                )?)
            } else {
                None
            },
            partition_auto_increment: if table.auto_increment_column_index().is_some() {
                Some(self.prepare_exchange_auto_increment(
                    table.id,
                    table.id,
                    auto_increment_floor,
                )?)
            } else {
                None
            },
            renames,
        };
        let journal_path = self.write_partition_exchange_journal(&exchange_journal)?;

        // Durable commit point: persist the storage-id swap. After this
        // returns, recovery treats the swap as committed and will finish
        // any half-applied renames.
        self.note_catalog_table(table.id);
        self.note_catalog_table(other_table.id);
        self.note_catalog_table(partition_storage_id);
        self.commit_ddl_catalog_txn()?;
        if partition_reorg_crash_point().as_deref() == Some("exchange_after_catalog") {
            return Ok(());
        }
        if let Some((table_id, version, max_value)) = exchange_journal.partition_auto_increment {
            self.apply_exchange_auto_increment(table_id, version, max_value)?;
        }
        if let Some((table_id, version, max_value)) = exchange_journal.auto_increment {
            self.apply_exchange_auto_increment(table_id, version, max_value)?;
            self.auto_increment.forget_table(other_table.id);
            self.delete_auto_increment_metadata(other_table.id)?;
        }

        // Test-only crash window: swap committed, renames not yet applied.
        if partition_reorg_crash_point().as_deref() == Some("exchange_after_swap") {
            return Ok(());
        }

        // Catalog publication can reopen a summary handle. Drain those
        // publishers once more and hold the exclusive gate through the
        // renames and final handle retirement; no lookup may reopen an old
        // owner path in the middle of this physical swap.
        let _zone_swap = self.zone_map_backup_capture_guard()?;
        self.flush_for_rename_zone_map(partition_storage_id)?;
        self.flush_for_rename_zone_map(other_table.id)?;
        for (from, to) in &exchange_journal.renames {
            let from_abs = self.data_dir.join(from);
            let to_abs = self.data_dir.join(to);
            if from_abs.exists() {
                if let Some(parent) = to_abs.parent() {
                    fs::create_dir_all(parent).map_err(io_err)?;
                }
                fs::rename(&from_abs, &to_abs).map_err(io_err)?;
            }
        }
        // Catalog mirroring can reopen handles between the first eviction and
        // the rename. Drop them again so no reader retains an unlinked inode,
        // then bind both exchanged heaps to their new owner codecs.
        self.remove_heap_cache_entry(partition_storage_id)?;
        self.remove_heap_cache_entry(other_table.id)?;
        self.remove_overflow_cache_entry(partition_storage_id)?;
        self.remove_overflow_cache_entry(other_table.id)?;
        for index in &table.indexes {
            self.remove_index_cache_entry(partition_storage_id, &index.name)?;
            self.remove_index_cache_entry(other_table.id, &index.name)?;
        }
        self.set_cached_heap_codec(other_table.id, table.row_codec.clone())?;
        self.set_cached_heap_codec(partition_storage_id, other_table.row_codec.clone())?;
        // Cached ZoneMap paths include the pre-swap owner table name. Force
        // the next lookup to reopen the sidecar at its post-swap path.
        self.forget_zone_map(partition_storage_id)?;
        self.forget_zone_map(other_table.id)?;
        self.remove_partition_reorg_journal(&journal_path)?;
        Ok(())
    }

    /// Read every visible row from a single partition's heap. Used by
    /// the COALESCE / REORGANIZE / REMOVE PARTITIONING repartition
    /// helpers. Each row is padded to the latest schema before being
    /// returned so the re-insert path sees a full row.
    fn partition_rows_for_repartition(
        &self,
        storage_id: u64,
    ) -> Result<Vec<Vec<crate::types::Value>>, QueryError> {
        // We use a fresh statement-level snapshot so only committed
        // rows enter the new partition layout. Uncommitted writes by
        // concurrent transactions would have been blocked already by
        // the DDL X lock check the caller performed.
        let txn = self.begin_read_only_txn()?;
        let _read_guard = PartitionReadTxnGuard {
            db: self,
            txn_id: txn.txn_id,
        };
        let rows = self.with_heap_mut(storage_id, |heap| heap.scan_mvcc())?;
        let mut out = Vec::new();
        let table = self.table_by_id(storage_id);
        for row in rows {
            if let Some(mut visible) = self.visible_row(row, txn)? {
                if let Some(t) = table.as_ref() {
                    Self::pad_full_row_to_schema(&mut visible.values, t);
                }
                out.push(visible.values);
            }
        }
        Ok(out)
    }
}

/// Number of physical partition heaps a `PARTITION BY` clause needs.
/// HASH/KEY without explicit partitions default to `PARTITIONS 1` per
/// MySQL. Returns 0 for `None` (an unpartitioned table).
pub(crate) fn partition_count_from_def(
    def: Option<&crate::parser_partition::PartitionDef>,
) -> Result<usize, QueryError> {
    use crate::parser_partition::PartitionMethod as PM;
    let Some(def) = def else { return Ok(0) };
    if def.partition_count == Some(0) {
        return Err(error(
            crate::error::catalog::ER_NO_PARTS_ERROR,
            "Number of partitions must be greater than zero",
        ));
    }
    if !def.partitions.is_empty()
        && def
            .partition_count
            .is_some_and(|count| count as usize != def.partitions.len())
    {
        return Err(error(
            crate::error::catalog::ER_PARTITION_WRONG_NO_PART_ERROR,
            "Number of partitions does not match the explicit partition definitions",
        ));
    }
    let parent_count = if !def.partitions.is_empty() {
        def.partitions.len()
    } else if matches!(
        def.method,
        PM::Range | PM::RangeColumns | PM::List | PM::ListColumns
    ) {
        return Err(error(
            crate::error::catalog::ER_PARTITIONS_MUST_BE_DEFINED_ERROR,
            "RANGE / LIST partitioning requires explicit partition definitions",
        ));
    } else {
        def.partition_count.unwrap_or(1) as usize
    };
    if let Some(sub) = def.subpartition_by.as_ref() {
        if !matches!(
            def.method,
            PM::Range | PM::RangeColumns | PM::List | PM::ListColumns
        ) {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "SUBPARTITION BY is supported only for RANGE/LIST partitioning",
            ));
        }
        if !matches!(
            sub.method,
            PM::Hash | PM::LinearHash | PM::Key | PM::LinearKey
        ) {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "SUBPARTITION BY supports HASH/KEY only",
            ));
        }
        let explicit_sub_count = def.partitions.iter().try_fold(0usize, |count, item| {
            count.checked_add(item.subpartitions.len()).ok_or_else(|| {
                error(
                    crate::error::catalog::ER_TOO_MANY_PARTITIONS_ERROR,
                    "too many partitions",
                )
            })
        })?;
        let sub_count = if explicit_sub_count > 0 {
            for item in &def.partitions {
                if item.subpartitions.is_empty() {
                    return Err(error(
                        crate::error::catalog::ER_TOO_MANY_PARTITIONS_ERROR,
                        "all partitions must define explicit subpartitions when any partition does",
                    ));
                }
                if let Some(declared) = sub.count {
                    if item.subpartitions.len() != declared as usize {
                        return Err(error(
                            crate::error::catalog::ER_TOO_MANY_PARTITIONS_ERROR,
                            "explicit subpartition count does not match SUBPARTITIONS",
                        ));
                    }
                }
            }
            explicit_sub_count
        } else {
            parent_count
                .checked_mul(sub.count.unwrap_or(1) as usize)
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_TOO_MANY_PARTITIONS_ERROR,
                        "too many partitions",
                    )
                })?
        };
        if sub_count == 0 {
            return Err(error(
                crate::error::catalog::ER_TOO_MANY_PARTITIONS_ERROR,
                "SUBPARTITIONS count must be greater than zero",
            ));
        }
        if sub_count > MYSQL80_MAX_PARTITIONS {
            return Err(error(
                crate::error::catalog::ER_TOO_MANY_PARTITIONS_ERROR,
                format!(
                    "Too many partitions ({}); MySQL 8.0 limit is {}",
                    sub_count, MYSQL80_MAX_PARTITIONS
                ),
            ));
        }
        // Logical RANGE/LIST parents do not own a heap. Allocate catalog ids
        // only for physical leaves so no DDL/recovery path can accidentally
        // treat an empty parent placeholder as table data.
        return Ok(sub_count);
    }
    if parent_count > MYSQL80_MAX_PARTITIONS {
        return Err(error(
            crate::error::catalog::ER_TOO_MANY_PARTITIONS_ERROR,
            format!(
                "Too many partitions ({}); MySQL 8.0 limit is {}",
                parent_count, MYSQL80_MAX_PARTITIONS
            ),
        ));
    }
    Ok(parent_count)
}

/// Convert the parser's [`crate::parser_partition::PartitionDef`] into
/// the persisted [`crate::storage::partitioning::PartitionInfo`].
/// `partition_storage_ids` must have exactly the number of entries
/// returned by [`partition_count_from_def`] for the same def.
fn validate_partition_options(
    def: &crate::parser_partition::PartitionOptionsDef,
) -> Result<(), QueryError> {
    let unsupported = [
        ("DATA DIRECTORY", def.data_directory.is_some()),
        ("INDEX DIRECTORY", def.index_directory.is_some()),
        ("TABLESPACE", def.tablespace.is_some()),
        ("MAX_ROWS", def.max_rows.is_some()),
        ("MIN_ROWS", def.min_rows.is_some()),
        ("NODEGROUP", def.nodegroup.is_some()),
    ];
    if let Some((option, _)) = unsupported.into_iter().find(|(_, present)| *present) {
        return Err(error(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            format!("partition option {option} is not supported by mydb"),
        ));
    }
    if let Some(engine) = def.engine.as_deref() {
        if !engine.eq_ignore_ascii_case("mydb") && !engine.eq_ignore_ascii_case("innodb") {
            return Err(error(
                crate::error::catalog::ER_UNKNOWN_STORAGE_ENGINE,
                format!("Unknown storage engine '{engine}'"),
            ));
        }
    }
    Ok(())
}

/// Valid ENGINE aliases do not change the only physical engine, so they are
/// validated but not persisted. Every other option is rejected above.
fn map_partition_options(
    _def: &crate::parser_partition::PartitionOptionsDef,
) -> crate::storage::partitioning::PartitionPhysicalOptions {
    crate::storage::partitioning::PartitionPhysicalOptions::default()
}

pub(crate) fn build_partition_info_from_def(
    def: &crate::parser_partition::PartitionDef,
    table: &TableSchema,
    partition_storage_ids: &[u64],
) -> Result<crate::storage::partitioning::PartitionInfo, QueryError> {
    use crate::parser_partition::{
        PartitionBoundsDef, PartitionLiteralDef, PartitionMethod as PMethod,
    };
    use crate::storage::partitioning::{
        Partition as StoragePartition, PartitionBounds as StorageBounds, PartitionInfo,
        PartitionLiteral as StorageLiteral, PartitionMethod as SMethod, Subpartition,
        SubpartitioningInfo,
    };

    // KEY() selects the PRIMARY KEY, or the first all-NOT-NULL UNIQUE key
    // when no primary key exists. Normalize once so routing, pruning and the
    // persisted descriptor all use the same explicit ordered column list.
    let default_key_columns = || -> Result<Vec<String>, QueryError> {
        if let Some(primary) = table.indexes.iter().find(|index| index.primary) {
            return Ok(primary.columns.clone());
        }
        if let Some(unique) = table.indexes.iter().find(|index| {
            index.unique
                && index.columns.iter().all(|name| {
                    table
                        .columns
                        .iter()
                        .any(|column| column.name.eq_ignore_ascii_case(name) && !column.nullable)
                })
        }) {
            return Ok(unique.columns.clone());
        }
        Err(error(
            crate::error::catalog::ER_FIELD_NOT_FOUND_PART_ERROR,
            "PARTITION BY KEY() requires a PRIMARY KEY or an all-NOT-NULL UNIQUE key",
        ))
    };
    let mut normalized_def = def.clone();
    if matches!(normalized_def.method, PMethod::Key | PMethod::LinearKey)
        && normalized_def.columns.is_empty()
    {
        normalized_def.columns = default_key_columns()?;
    }
    if let Some(sub) = normalized_def.subpartition_by.as_mut() {
        if matches!(sub.method, PMethod::Key | PMethod::LinearKey) && sub.columns.is_empty() {
            sub.columns = default_key_columns()?;
        }
    }
    let def = &normalized_def;

    // MySQL 8.0 caps each partition/subpartition COMMENT at 1024 characters
    // (ER_TOO_LONG_TABLE_PARTITION_COMMENT). Validating here — the single
    // build point — covers CREATE TABLE ... PARTITION BY and every ALTER
    // partition-maintenance path (ADD / REORGANIZE / subpartition rebuild).
    {
        use crate::storage::schema::{check_comment_len, CommentTarget};
        for part in &def.partitions {
            validate_partition_options(&part.options)?;
            check_comment_len(CommentTarget::Partition, &part.name, &part.comment)?;
            for sub in &part.subpartitions {
                validate_partition_options(&sub.options)?;
                check_comment_len(CommentTarget::Partition, &sub.name, &sub.comment)?;
            }
        }
    }

    let method = match def.method {
        PMethod::Range => SMethod::Range,
        PMethod::RangeColumns => SMethod::RangeColumns,
        PMethod::List => SMethod::List,
        PMethod::ListColumns => SMethod::ListColumns,
        PMethod::Hash => SMethod::Hash,
        PMethod::LinearHash => SMethod::LinearHash,
        PMethod::Key => SMethod::Key,
        PMethod::LinearKey => SMethod::LinearKey,
    };

    // Validate columns exist for COLUMNS / KEY variants.
    if method.uses_columns() {
        if def.columns.is_empty() {
            return Err(error(
                crate::error::catalog::ER_FIELD_NOT_FOUND_PART_ERROR,
                "partitioning method requires at least one column",
            ));
        }
        for col in &def.columns {
            let lower = col.to_ascii_lowercase();
            if !table.column_name_map.contains_key(&lower) {
                return Err(error(
                    crate::error::catalog::ER_FIELD_NOT_FOUND_PART_ERROR,
                    format!("partition column '{}' does not exist in the table", col),
                ));
            }
        }
    }

    // For RANGE/LIST/HASH, parse the expression text (validating syntax).
    if let Some(expr_sql) = def.expr_sql.as_deref() {
        crate::parser::parse_expr_fragment(expr_sql).map_err(|e| {
            error(
                crate::error::catalog::ER_WRONG_EXPR_IN_PARTITION_FUNC_ERROR,
                format!("invalid partition expression: {}", e.message),
            )
        })?;
    }

    let sub_method = def.subpartition_by.as_ref().map(|sub| match sub.method {
        PMethod::Hash => SMethod::Hash,
        PMethod::LinearHash => SMethod::LinearHash,
        PMethod::Key => SMethod::Key,
        PMethod::LinearKey => SMethod::LinearKey,
        _ => SMethod::Hash,
    });
    if def.subpartition_by.is_some() {
        if !method.is_range() && !method.is_list() {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "SUBPARTITION BY is supported only for RANGE/LIST partitioning",
            ));
        }
    }
    let subpartitioning = match (def.subpartition_by.as_ref(), sub_method) {
        (Some(sub), Some(sub_method)) => {
            if !sub_method.is_hash_like() {
                return Err(error(
                    crate::error::catalog::ER_NOT_SUPPORTED_YET,
                    "SUBPARTITION BY supports HASH/KEY only",
                ));
            }
            if sub_method.uses_columns() {
                if sub.columns.is_empty() {
                    return Err(error(
                        crate::error::catalog::ER_FIELD_NOT_FOUND_PART_ERROR,
                        "subpartitioning method requires at least one column",
                    ));
                }
                for col in &sub.columns {
                    let lower = col.to_ascii_lowercase();
                    if !table.column_name_map.contains_key(&lower) {
                        return Err(error(
                            crate::error::catalog::ER_FIELD_NOT_FOUND_PART_ERROR,
                            format!("subpartition column '{}' does not exist in the table", col),
                        ));
                    }
                }
            }
            if let Some(expr_sql) = sub.expr_sql.as_deref() {
                crate::parser::parse_expr_fragment(expr_sql).map_err(|e| {
                    error(
                        crate::error::catalog::ER_WRONG_EXPR_IN_PARTITION_FUNC_ERROR,
                        format!("invalid subpartition expression: {}", e.message),
                    )
                })?;
            }
            Some(SubpartitioningInfo {
                method: sub_method,
                expr_sql: sub.expr_sql.clone().unwrap_or_default(),
                columns: sub.columns.clone(),
                key_algorithm: if matches!(sub.method, PMethod::Key | PMethod::LinearKey) {
                    Some(sub.key_algorithm.unwrap_or(2))
                } else {
                    None
                },
                count: sub.count,
            })
        }
        _ => None,
    };
    let explicit_subpartition_total: usize =
        def.partitions.iter().map(|p| p.subpartitions.len()).sum();
    let has_explicit_subpartitions = explicit_subpartition_total > 0;
    if has_explicit_subpartitions {
        if subpartitioning.is_none() {
            return Err(error(
                crate::error::catalog::ER_NOT_SUPPORTED_YET,
                "explicit SUBPARTITION definitions require SUBPARTITION BY",
            ));
        }
        for item in &def.partitions {
            if item.subpartitions.is_empty() {
                return Err(error(
                    crate::error::catalog::ER_TOO_MANY_PARTITIONS_ERROR,
                    "all partitions must define explicit subpartitions when any partition does",
                ));
            }
            if let Some(declared) = def.subpartition_by.as_ref().and_then(|sub| sub.count) {
                if item.subpartitions.len() != declared as usize {
                    return Err(error(
                        crate::error::catalog::ER_TOO_MANY_PARTITIONS_ERROR,
                        "explicit subpartition count does not match SUBPARTITIONS",
                    ));
                }
            }
        }
    }
    let subpartitions_per_partition = subpartitioning
        .as_ref()
        .and_then(|sub| sub.count)
        .map(|count| count as usize)
        .unwrap_or_else(|| {
            if has_explicit_subpartitions {
                def.partitions
                    .first()
                    .map(|p| p.subpartitions.len())
                    .unwrap_or(0)
            } else {
                usize::from(subpartitioning.is_some())
            }
        });
    if subpartitioning.is_some() && subpartitions_per_partition == 0 {
        return Err(error(
            crate::error::catalog::ER_TOO_MANY_PARTITIONS_ERROR,
            "SUBPARTITIONS count must be greater than zero",
        ));
    }
    let parent_count = if subpartitioning.is_some() {
        if def.partitions.is_empty() {
            return Err(error(
                crate::error::catalog::ER_PARTITIONS_MUST_BE_DEFINED_ERROR,
                "RANGE / LIST partitioning requires explicit partition definitions",
            ));
        }
        def.partitions.len()
    } else {
        partition_storage_ids.len()
    };
    let expected_storage_ids = if subpartitioning.is_some() {
        if has_explicit_subpartitions {
            explicit_subpartition_total
        } else {
            parent_count
                .checked_mul(subpartitions_per_partition)
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_TOO_MANY_PARTITIONS_ERROR,
                        "too many partitions",
                    )
                })?
        }
    } else {
        parent_count
    };
    if partition_storage_ids.len() != expected_storage_ids {
        return Err(error(
            crate::error::catalog::ER_SUBPARTITION_ERROR,
            "internal: partition storage id count mismatch",
        ));
    }
    let logical_parent_storage_ids;
    let (parent_storage_ids, subpartition_storage_ids) = if subpartitioning.is_some() {
        // storage_id=0 is a non-owning logical-parent sentinel in catalog v42.
        // Real object ids start at 1 and every physical API uses storage_ids(),
        // which returns only the subpartition leaves for this shape.
        logical_parent_storage_ids = vec![0; parent_count];
        (logical_parent_storage_ids.as_slice(), partition_storage_ids)
    } else {
        (partition_storage_ids, &[][..])
    };

    // Build per-partition entries.
    let mut partitions: Vec<StoragePartition> = Vec::new();
    let mut seen_max = false;
    let mut next_sub_storage = 0usize;
    let convert_lit =
        |position: usize, l: &PartitionLiteralDef| -> Result<StorageLiteral, QueryError> {
            let value = match l {
                PartitionLiteralDef::MaxValue => return Ok(StorageLiteral::MaxValue),
                PartitionLiteralDef::Value(v) => v.clone(),
                PartitionLiteralDef::Expression(sql) => {
                    let expr = crate::parser::parse_expr_fragment(sql).map_err(|e| {
                        error(
                            crate::error::catalog::ER_WRONG_EXPR_IN_PARTITION_FUNC_ERROR,
                            e.message,
                        )
                    })?;
                    let mut columns = Vec::new();
                    walk_partition_expr_functions(&expr, &mut columns)?;
                    if !columns.is_empty() {
                        return Err(error(
                            crate::error::catalog::ER_PARTITION_FUNCTION_IS_NOT_ALLOWED,
                            "Partition bounds must be constant expressions",
                        ));
                    }
                    let value = crate::engine::expr::eval_const(&expr)?;
                    if !method.uses_columns() && !matches!(value, Value::Int(_) | Value::UInt(_)) {
                        return Err(error(
                            crate::error::catalog::ER_VALUES_IS_NOT_INT_TYPE_ERROR,
                            "Partition bound expression must return an integer",
                        ));
                    }
                    value
                }
            };
            let value = if matches!(method, SMethod::RangeColumns | SMethod::ListColumns) {
                let name = def.columns.get(position).ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_PARTITION_WRONG_VALUES_ERROR,
                        "Too many partition bound values",
                    )
                })?;
                let column = table
                    .columns
                    .iter()
                    .find(|c| c.name.eq_ignore_ascii_case(name))
                    .ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_BAD_FIELD_ERROR,
                            format!("Unknown partition column '{name}'"),
                        )
                    })?;
                // Normalize once at DDL time, so routing, lookup keys, validation
                // and persisted bounds all share the column's storage shape.
                coerce_partition_bound(value, column)?
            } else {
                value
            };
            Ok(StorageLiteral::Value(value))
        };
    if def.partitions.is_empty() {
        // HASH/KEY without explicit partitions — synthesise p0..p{n-1}.
        if !matches!(
            method,
            SMethod::Hash | SMethod::LinearHash | SMethod::Key | SMethod::LinearKey
        ) {
            return Err(error(
                crate::error::catalog::ER_PARTITIONS_MUST_BE_DEFINED_ERROR,
                "RANGE / LIST partitioning requires explicit partition definitions",
            ));
        }
        for (i, sid) in parent_storage_ids.iter().enumerate() {
            // Synthesised HASH/KEY partitions may still carry per-partition
            // options when the count came from an explicit list (rare); map
            // them when a matching def item exists.
            let options = def
                .partitions
                .get(i)
                .map(|p| map_partition_options(&p.options))
                .unwrap_or_default();
            partitions.push(StoragePartition {
                name: format!("p{}", i),
                storage_id: *sid,
                bounds: StorageBounds::Hash,
                comment: String::new(),
                options,
                subpartitions: Vec::new(),
            });
        }
    } else {
        if parent_storage_ids.len() != def.partitions.len() {
            return Err(error(
                crate::error::catalog::ER_SUBPARTITION_ERROR,
                "internal: partition storage id count mismatch",
            ));
        }
        // Validate uniqueness of partition and subpartition names.
        let mut seen = std::collections::BTreeSet::new();
        for item in &def.partitions {
            if !seen.insert(item.name.to_ascii_lowercase()) {
                return Err(error(
                    crate::error::catalog::ER_SAME_NAME_PARTITION,
                    format!("duplicate partition name '{}'", item.name),
                ));
            }
            for sub in &item.subpartitions {
                if !seen.insert(sub.name.to_ascii_lowercase()) {
                    return Err(error(
                        crate::error::catalog::ER_SAME_NAME_PARTITION,
                        format!("duplicate partition/subpartition name '{}'", sub.name),
                    ));
                }
            }
        }
        for (item, sid) in def.partitions.iter().zip(parent_storage_ids.iter()) {
            let bounds = match &item.bounds {
                PartitionBoundsDef::RangeLessThan(lits) => {
                    if !method.is_range() {
                        return Err(error(
                            crate::error::catalog::ER_PARTITION_WRONG_VALUES_ERROR,
                            "VALUES LESS THAN is only valid for RANGE partitioning",
                        ));
                    }
                    let has_max = lits
                        .iter()
                        .any(|l| matches!(l, PartitionLiteralDef::MaxValue));
                    if seen_max {
                        return Err(error(
                            crate::error::catalog::ER_PARTITION_MAXVALUE_ERROR,
                            "MAXVALUE can only appear in the last partition",
                        ));
                    }
                    if has_max {
                        seen_max = true;
                    }
                    StorageBounds::RangeLessThan(
                        lits.iter()
                            .enumerate()
                            .map(|(i, l)| convert_lit(i, l))
                            .collect::<Result<Vec<_>, _>>()?,
                    )
                }
                PartitionBoundsDef::ListIn(groups) => {
                    if !method.is_list() {
                        return Err(error(
                            crate::error::catalog::ER_PARTITION_WRONG_VALUES_ERROR,
                            "VALUES IN is only valid for LIST partitioning",
                        ));
                    }
                    StorageBounds::ListIn(
                        groups
                            .iter()
                            .map(|g| {
                                g.iter()
                                    .enumerate()
                                    .map(|(i, l)| convert_lit(i, l))
                                    .collect::<Result<Vec<_>, _>>()
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    )
                }
                PartitionBoundsDef::Hash => StorageBounds::Hash,
            };
            let explicit_subpartitions = item.subpartitions.as_slice();
            let leaf_count = if has_explicit_subpartitions {
                explicit_subpartitions.len()
            } else {
                subpartitions_per_partition
            };
            let mut subpartitions = Vec::with_capacity(leaf_count);
            for sub_idx in 0..leaf_count {
                let storage_id = subpartition_storage_ids
                    .get(next_sub_storage)
                    .copied()
                    .ok_or_else(|| {
                        error(
                            crate::error::catalog::ER_SUBPARTITION_ERROR,
                            "internal: subpartition storage id count mismatch",
                        )
                    })?;
                next_sub_storage += 1;
                let (name, comment, options) =
                    if let Some(explicit) = explicit_subpartitions.get(sub_idx) {
                        (
                            explicit.name.clone(),
                            explicit.comment.clone(),
                            map_partition_options(&explicit.options),
                        )
                    } else {
                        (
                            format!("{}sp{}", item.name, sub_idx),
                            String::new(),
                            Default::default(),
                        )
                    };
                subpartitions.push(Subpartition {
                    name,
                    storage_id,
                    comment,
                    options,
                });
            }
            partitions.push(StoragePartition {
                name: item.name.clone(),
                storage_id: *sid,
                bounds,
                comment: item.comment.clone(),
                options: map_partition_options(&item.options),
                subpartitions,
            });
        }
    }

    // The complete, coerced layout is validated by
    // validate_mysql80_partition_compat before catalog publication.

    Ok(PartitionInfo {
        method,
        expr_sql: def.expr_sql.clone().unwrap_or_default(),
        columns: def.columns.clone(),
        key_algorithm: if matches!(def.method, PMethod::Key | PMethod::LinearKey) {
            Some(def.key_algorithm.unwrap_or(2))
        } else {
            None
        },
        partitions,
        subpartitioning,
        expr_ast: None,
        expr_program: None,
        sub_expr_ast: None,
        sub_expr_program: None,
        runtime: std::sync::OnceLock::new(),
        metadata_runtime: std::sync::OnceLock::new(),
    })
}

const MYSQL80_MAX_PARTITIONS: usize = 8192;

fn validate_mysql80_partition_compat(
    table: &TableSchema,
    temporary: bool,
) -> Result<(), QueryError> {
    let Some(part) = table.partitioning.as_ref() else {
        return Ok(());
    };
    if temporary {
        return Err(error(
            crate::error::catalog::ER_PARTITION_NO_TEMPORARY,
            "Partitioned TEMPORARY tables are not supported",
        ));
    }
    validate_partition_layout_mysql80(table)?;
    validate_partition_unique_keys_mysql80(table)?;
    validate_partition_expr_type_mysql80(table)?;
    let physical_partition_count = part.storage_ids().len();
    if physical_partition_count > MYSQL80_MAX_PARTITIONS {
        return Err(error(
            crate::error::catalog::ER_TOO_MANY_PARTITIONS_ERROR,
            format!(
                "Too many partitions ({}); MySQL 8.0 limit is {}",
                physical_partition_count, MYSQL80_MAX_PARTITIONS
            ),
        ));
    }
    Ok(())
}

fn partition_def_from_info(
    part: &crate::storage::partitioning::PartitionInfo,
    partition_count: Option<u32>,
    partitions: Vec<crate::parser_partition::PartitionItemDef>,
) -> Result<crate::parser_partition::PartitionDef, QueryError> {
    use crate::parser_partition::PartitionMethod as PM;
    use crate::storage::partitioning::PartitionMethod as SM;
    let method = match part.method {
        SM::Range => PM::Range,
        SM::RangeColumns => PM::RangeColumns,
        SM::List => PM::List,
        SM::ListColumns => PM::ListColumns,
        SM::Hash => PM::Hash,
        SM::LinearHash => PM::LinearHash,
        SM::Key => PM::Key,
        SM::LinearKey => PM::LinearKey,
    };
    let subpartition_by = part.subpartitioning.as_ref().map(|sub| {
        let method = match sub.method {
            SM::Hash => PM::Hash,
            SM::LinearHash => PM::LinearHash,
            SM::Key => PM::Key,
            SM::LinearKey => PM::LinearKey,
            SM::Range => PM::Range,
            SM::RangeColumns => PM::RangeColumns,
            SM::List => PM::List,
            SM::ListColumns => PM::ListColumns,
        };
        crate::parser_partition::SubpartitionByDef {
            method,
            expr_sql: (!sub.expr_sql.is_empty()).then(|| sub.expr_sql.clone()),
            columns: sub.columns.clone(),
            key_algorithm: sub.key_algorithm,
            count: sub.count,
        }
    });
    Ok(crate::parser_partition::PartitionDef {
        method,
        expr_sql: (!part.expr_sql.is_empty()).then(|| part.expr_sql.clone()),
        columns: part.columns.clone(),
        key_algorithm: part.key_algorithm,
        partition_count,
        partitions,
        subpartition_by,
    })
}

/// The closed set of functions MySQL permits inside a (sub)partitioning
/// expression. Every one of them returns an integer, which is why an
/// expression built only from these functions (plus integer columns and
/// integer arithmetic) satisfies the "partition function must yield an
/// integer" rule. Anything else → `1564
/// ER_PARTITION_FUNCTION_IS_NOT_ALLOWED`.
fn is_allowed_partition_function(name: &str) -> bool {
    const ALLOWED: &[&str] = &[
        "ABS",
        "CEILING",
        "CEIL",
        "FLOOR",
        "DIV",
        "MOD",
        "DATEDIFF",
        "DAY",
        "DAYOFMONTH",
        "DAYOFWEEK",
        "DAYOFYEAR",
        "EXTRACT",
        "HOUR",
        "MICROSECOND",
        "MINUTE",
        "MONTH",
        "QUARTER",
        "SECOND",
        "TIME_TO_SEC",
        "TO_DAYS",
        "TO_SECONDS",
        "UNIX_TIMESTAMP",
        "WEEKDAY",
        "YEAR",
        "YEARWEEK",
    ];
    ALLOWED.iter().any(|f| f.eq_ignore_ascii_case(name))
}

/// Verdict for one column's type when used as a partitioning field.
enum PartFieldVerdict {
    /// Type is acceptable for this partitioning role.
    Allowed,
    /// BLOB/TEXT family → `1486 ER_BLOB_FIELD_IN_PART_FUNC_ERROR`.
    Blob,
    /// Otherwise disallowed → `1659
    /// ER_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD`.
    NotAllowed,
}

/// Field-type rule for *expression-based* partitioning (plain
/// RANGE/LIST/HASH/LINEAR HASH). Every column referenced by the
/// partition expression must be an integer or a temporal type (the
/// latter only make sense wrapped in a whitelisted date function —
/// a bare temporal column still fails the integer-result check below).
fn expr_partition_field_verdict(kind: &crate::storage::DataTypeKind) -> PartFieldVerdict {
    use crate::storage::DataTypeKind as K;
    match kind {
        K::TinyInt
        | K::SmallInt
        | K::MediumInt
        | K::Int
        | K::BigInt
        | K::Bool
        | K::Year
        | K::Date
        | K::DateTime { .. }
        | K::Timestamp { .. }
        | K::Time { .. } => PartFieldVerdict::Allowed,
        K::Text { .. } | K::Blob { .. } => PartFieldVerdict::Blob,
        _ => PartFieldVerdict::NotAllowed,
    }
}

/// Field-type rule for `RANGE COLUMNS` / `LIST COLUMNS`: MySQL 8.0 allows
/// integer types, DATE/DATETIME, BIT and CHAR/VARCHAR/BINARY/VARBINARY;
/// FLOAT/DOUBLE/DECIMAL and the rest are rejected. (D5: added BIT.)
fn columns_partition_field_verdict(kind: &crate::storage::DataTypeKind) -> PartFieldVerdict {
    use crate::storage::DataTypeKind as K;
    match kind {
        K::TinyInt
        | K::SmallInt
        | K::MediumInt
        | K::Int
        | K::BigInt
        | K::Bool
        | K::Bit { .. }
        | K::Date
        | K::DateTime { .. }
        | K::Char { .. }
        | K::VarChar { .. }
        | K::Binary { .. }
        | K::VarBinary { .. } => PartFieldVerdict::Allowed,
        K::Text { .. } | K::Blob { .. } => PartFieldVerdict::Blob,
        _ => PartFieldVerdict::NotAllowed,
    }
}

/// Field-type rule for `KEY` / `LINEAR KEY`: MySQL accepts any column
/// type except TEXT/BLOB (and the internally-blob JSON/GEOMETRY, plus
/// mydb's VECTOR).
fn key_partition_field_verdict(kind: &crate::storage::DataTypeKind) -> PartFieldVerdict {
    use crate::storage::DataTypeKind as K;
    match kind {
        K::Text { .. } | K::Blob { .. } => PartFieldVerdict::Blob,
        K::Json | K::Geometry { .. } | K::Vector { .. } => PartFieldVerdict::NotAllowed,
        _ => PartFieldVerdict::Allowed,
    }
}

/// Look up a column's declared type by (case-insensitive) name.
fn partition_column_kind<'a>(
    table: &'a TableSchema,
    name: &str,
) -> Option<&'a crate::storage::DataTypeKind> {
    let idx = *table.column_name_map.get(&name.to_ascii_lowercase())?;
    table.columns.get(idx).map(|c| &c.data_type)
}

/// Whether a partition expression is statically integer-valued. Bare
/// integer columns, whitelisted functions, and MySQL-legal integer arithmetic
/// qualify; a bare temporal/string column, `/`, or any bit operation does not.
fn partition_expr_is_int_valued(expr: &crate::parser::Expr, table: &TableSchema) -> bool {
    use crate::parser::{BinaryOp, Expr, Literal, UnaryOp};
    match expr {
        Expr::Literal(Literal::Int(_) | Literal::UInt(_) | Literal::Bool(_)) => true,
        Expr::Literal(Literal::HexBytes(b) | Literal::BitBytes(b)) => b.len() <= 8,
        Expr::Literal(_) => false,
        Expr::BoundValue(v) => matches!(
            v,
            crate::types::Value::Int(_)
                | crate::types::Value::UInt(_)
                | crate::types::Value::Year(_)
        ),
        Expr::Identifier(name) => partition_column_kind(table, name)
            .map(datatype_is_integer)
            .unwrap_or(false),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .and_then(|name| partition_column_kind(table, name))
            .map(datatype_is_integer)
            .unwrap_or(false),
        // Every whitelisted partition function returns an integer.
        Expr::Function {
            name,
            args,
            distinct,
        } if name.eq_ignore_ascii_case("DIV") => {
            !*distinct
                && args.len() == 2
                && args
                    .iter()
                    .all(|arg| partition_expr_is_int_valued(arg, table))
        }
        Expr::Function { name, .. } => is_allowed_partition_function(name),
        Expr::Unary { op, expr } => match op {
            UnaryOp::Neg => partition_expr_is_int_valued(expr, table),
            UnaryOp::Not => true,
            UnaryOp::BitNot => false,
        },
        Expr::Binary { left, op, right } => match op {
            BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::IntDiv | BinaryOp::Mod => {
                partition_expr_is_int_valued(left, table)
                    && partition_expr_is_int_valued(right, table)
            }
            _ => false,
        },
        _ => false,
    }
}

fn datatype_is_integer(kind: &crate::storage::DataTypeKind) -> bool {
    use crate::storage::DataTypeKind as K;
    matches!(
        kind,
        K::TinyInt | K::SmallInt | K::MediumInt | K::Int | K::BigInt | K::Bool | K::Year
    )
}

/// Walk an expression-based partition function AST rejecting anything
/// MySQL forbids: non-whitelisted functions, subqueries, user variables,
/// window functions, and other non-deterministic / disallowed
/// constructs (all `1564`). Referenced columns are collected into
/// `cols_out` for the follow-up field-type check.
fn walk_partition_expr_functions(
    expr: &crate::parser::Expr,
    cols_out: &mut Vec<String>,
) -> Result<(), QueryError> {
    use crate::parser::Expr;
    let not_allowed = || {
        error(
            crate::error::catalog::ER_PARTITION_FUNCTION_IS_NOT_ALLOWED,
            "This partition function is not allowed",
        )
    };
    match expr {
        Expr::Identifier(name) => cols_out.push(name.clone()),
        Expr::CompoundIdentifier(parts) => {
            if let Some(last) = parts.last() {
                cols_out.push(last.clone());
            }
        }
        Expr::Literal(_) | Expr::BoundValue(_) => {}
        Expr::Unary { expr, .. } => walk_partition_expr_functions(expr, cols_out)?,
        Expr::Binary { left, right, .. } => {
            walk_partition_expr_functions(left, cols_out)?;
            walk_partition_expr_functions(right, cols_out)?;
        }
        Expr::Function {
            name,
            args,
            distinct,
        } => {
            if *distinct || args.is_empty() || !is_allowed_partition_function(name) {
                return Err(not_allowed());
            }
            for arg in args {
                walk_partition_expr_functions(arg, cols_out)?;
            }
        }
        // Everything else (subquery, EXISTS, IN, CASE, BETWEEN, LIKE,
        // window functions, user variables, placeholders, tuples, …) is
        // non-deterministic or otherwise not a legal partition function.
        _ => return Err(not_allowed()),
    }
    Ok(())
}

/// Validate one partitioning dimension's column / expression types
/// against the MySQL 8.0 rules. `expr_ast` is the compiled partition
/// expression (present for RANGE/LIST/HASH/LINEAR HASH), `columns` the
/// declared column list (present for COLUMNS / KEY variants).
fn validate_partition_dimension_types(
    table: &TableSchema,
    method: crate::storage::partitioning::PartitionMethod,
    expr_ast: Option<&crate::parser::Expr>,
    columns: &[String],
) -> Result<(), QueryError> {
    use crate::storage::partitioning::PartitionMethod as PM;

    let field_error = |verdict: PartFieldVerdict, col: &str| -> Option<QueryError> {
        match verdict {
            PartFieldVerdict::Allowed => None,
            PartFieldVerdict::Blob => Some(error(
                crate::error::catalog::ER_WRONG_EXPR_IN_PARTITION_FUNC_ERROR,
                format!(
                    "A BLOB field '{}' is not allowed in partition function",
                    col
                ),
            )),
            PartFieldVerdict::NotAllowed => Some(error(
                crate::error::catalog::ER_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD,
                format!(
                    "Field '{}' is of a not allowed type for this type of partitioning",
                    col
                ),
            )),
        }
    };

    match method {
        PM::Range | PM::List | PM::Hash | PM::LinearHash => {
            let Some(expr) = expr_ast else {
                // No compiled expression to analyse (defensive; the parse
                // step already validated syntax). Skip rather than panic.
                return Ok(());
            };
            // 1) Function whitelist + collect referenced columns.
            let mut cols = Vec::new();
            walk_partition_expr_functions(expr, &mut cols)?;
            // 2) Every referenced column's type must be acceptable.
            for col in &cols {
                let kind = partition_column_kind(table, col).ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_BAD_FIELD_ERROR,
                        format!("Unknown column '{col}' in 'partition function'"),
                    )
                })?;
                if let Some(err) = field_error(expr_partition_field_verdict(kind), col) {
                    return Err(err);
                }
            }
            // 3) The expression as a whole must be integer-valued.
            if !partition_expr_is_int_valued(expr, table) {
                // A bare non-integer column reads best as a field-type
                // error naming the column; anything else is a value/result
                // type error.
                if let crate::parser::Expr::Identifier(name) = expr {
                    return Err(error(
                        crate::error::catalog::ER_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD,
                        format!(
                            "Field '{}' is of a not allowed type for this type of partitioning",
                            name
                        ),
                    ));
                }
                if let crate::parser::Expr::CompoundIdentifier(parts) = expr {
                    let name = parts.last().cloned().unwrap_or_default();
                    return Err(error(
                        crate::error::catalog::ER_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD,
                        format!(
                            "Field '{}' is of a not allowed type for this type of partitioning",
                            name
                        ),
                    ));
                }
                return Err(error(crate::error::catalog::ER_VALUES_IS_NOT_INT_TYPE_ERROR,
                    "VALUES value must have type INT: the partition expression does not evaluate to an integer",
                ));
            }
        }
        PM::RangeColumns | PM::ListColumns => {
            for col in columns {
                if let Some(kind) = partition_column_kind(table, col) {
                    if let Some(err) = field_error(columns_partition_field_verdict(kind), col) {
                        return Err(err);
                    }
                }
            }
        }
        PM::Key | PM::LinearKey => {
            for col in columns {
                if let Some(kind) = partition_column_kind(table, col) {
                    if let Some(err) = field_error(key_partition_field_verdict(kind), col) {
                        return Err(err);
                    }
                }
            }
        }
    }
    Ok(())
}

/// MySQL 8.0 partition column / expression type validation (§2.2 of the
/// partition compat plan). Single entry point covering the top-level
/// partitioning dimension, the optional subpartitioning dimension, and —
/// for plain (non-COLUMNS) RANGE/LIST — the requirement that every
/// `VALUES` literal be an integer (`1697`).
fn validate_partition_expr_type_mysql80(table: &TableSchema) -> Result<(), QueryError> {
    use crate::storage::partitioning::{PartitionBounds, PartitionLiteral, PartitionMethod as PM};
    let Some(part) = table.partitioning.as_ref() else {
        return Ok(());
    };

    // Top-level dimension.
    validate_partition_dimension_types(table, part.method, part.expr_ast.as_ref(), &part.columns)?;

    // Subpartitioning dimension (HASH/KEY only).
    if let Some(sub) = part.subpartitioning.as_ref() {
        validate_partition_dimension_types(
            table,
            sub.method,
            part.sub_expr_ast.as_ref(),
            &sub.columns,
        )?;
    }

    // Value-layer integer check: plain RANGE / LIST over an integer
    // expression require integer VALUES literals. COLUMNS variants compare
    // against typed columns and are exempt; NULL is skipped (RANGE NULL is
    // rejected elsewhere, LIST NULL is a legal singleton partition).
    if matches!(part.method, PM::Range | PM::List) {
        let check_literal = |lit: &PartitionLiteral, pname: &str| -> Result<(), QueryError> {
            if let PartitionLiteral::Value(v) = lit {
                if !partition_value_is_integer(v) {
                    return Err(error(
                        crate::error::catalog::ER_VALUES_IS_NOT_INT_TYPE_ERROR,
                        format!("VALUES value for partition '{}' must have type INT", pname),
                    ));
                }
            }
            Ok(())
        };
        for partition in &part.partitions {
            match &partition.bounds {
                PartitionBounds::RangeLessThan(lits) => {
                    for lit in lits {
                        check_literal(lit, &partition.name)?;
                    }
                }
                PartitionBounds::ListIn(groups) => {
                    for tuple in groups {
                        for lit in tuple {
                            check_literal(lit, &partition.name)?;
                        }
                    }
                }
                PartitionBounds::Hash => {}
            }
        }
    }
    Ok(())
}

/// Whether a `VALUES` literal is integer-typed (or NULL, which is handled
/// separately). Accepts integer variants and scale-0 decimals; rejects
/// strings, floats, and temporal values.
fn partition_value_is_integer(v: &crate::types::Value) -> bool {
    use crate::types::Value;
    match v {
        Value::Null => true, // handled by caller (RANGE rejects, LIST allows)
        Value::Int(_) | Value::UInt(_) | Value::Year(_) => true,
        Value::Decimal(s) => !s.contains('.'),
        Value::DecimalCompact(_, scale) => *scale == 0,
        _ => false,
    }
}

fn validate_partition_layout_mysql80(table: &TableSchema) -> Result<(), QueryError> {
    use crate::storage::partitioning::{PartitionBounds, PartitionLiteral, PartitionMethod};
    let part = table
        .partitioning
        .as_ref()
        .expect("called only for partitioned tables");
    if part.partitions.is_empty() {
        return Err(error(
            crate::error::catalog::ER_WRONG_VALUE,
            "table has no partitions defined",
        ));
    }
    let mut names = std::collections::BTreeSet::new();
    let collations = partition_bound_collations(table);
    let mut list_values = std::collections::BTreeSet::new();
    let mut previous_range: Option<Vec<PartitionLiteral>> = None;
    for partition in &part.partitions {
        if partition.name.is_empty() || partition.name.len() > 64 {
            return Err(error(
                crate::error::catalog::ER_TOO_LONG_IDENT,
                format!(
                    "identifier name '{}' is invalid for a partition",
                    partition.name
                ),
            ));
        }
        if !names.insert(partition.name.to_ascii_lowercase()) {
            return Err(error(
                crate::error::catalog::ER_SAME_NAME_PARTITION,
                format!("duplicate partition name '{}'", partition.name),
            ));
        }
        for subpartition in &partition.subpartitions {
            if subpartition.name.is_empty() || subpartition.name.len() > 64 {
                return Err(error(
                    crate::error::catalog::ER_TOO_LONG_IDENT,
                    format!(
                        "identifier name '{}' is invalid for a subpartition",
                        subpartition.name
                    ),
                ));
            }
            if !names.insert(subpartition.name.to_ascii_lowercase()) {
                return Err(error(
                    crate::error::catalog::ER_SAME_NAME_PARTITION,
                    format!("duplicate partition name '{}'", subpartition.name),
                ));
            }
        }
        match (&part.method, &partition.bounds) {
            (PartitionMethod::Range, PartitionBounds::RangeLessThan(bound)) => {
                validate_range_bound_arity(bound, 1)?;
                validate_no_null_range_bound(bound)?;
                validate_range_bound_is_increasing(previous_range.as_ref(), bound, &collations)?;
                previous_range = Some(bound.clone());
            }
            (PartitionMethod::RangeColumns, PartitionBounds::RangeLessThan(bound)) => {
                validate_range_bound_arity(bound, part.columns.len())?;
                validate_no_null_range_bound(bound)?;
                validate_range_bound_is_increasing(previous_range.as_ref(), bound, &collations)?;
                previous_range = Some(bound.clone());
            }
            (PartitionMethod::List, PartitionBounds::ListIn(groups)) => {
                validate_list_groups(groups, 1, &collations, &mut list_values)?;
            }
            (PartitionMethod::ListColumns, PartitionBounds::ListIn(groups)) => {
                validate_list_groups(groups, part.columns.len(), &collations, &mut list_values)?;
            }
            (method, PartitionBounds::Hash) if method.is_hash_like() => {}
            _ => {
                return Err(error(
                    crate::error::catalog::ER_PARTITION_WRONG_VALUES_ERROR,
                    "partition bound type does not match partition method",
                ))
            }
        }
    }
    Ok(())
}

fn validate_range_bound_arity(
    bound: &[crate::storage::partitioning::PartitionLiteral],
    expected: usize,
) -> Result<(), QueryError> {
    if bound.len() != expected {
        return Err(error(
            crate::error::catalog::ER_PARTITION_WRONG_VALUES_ERROR,
            format!(
                "partition range bound has {} values, expected {}",
                bound.len(),
                expected
            ),
        ));
    }
    Ok(())
}

fn validate_no_null_range_bound(
    bound: &[crate::storage::partitioning::PartitionLiteral],
) -> Result<(), QueryError> {
    for lit in bound {
        if matches!(
            lit,
            crate::storage::partitioning::PartitionLiteral::Value(crate::types::Value::Null)
        ) {
            return Err(error(
                crate::error::catalog::ER_PARTITION_CONST_DOMAIN_ERROR,
                "VALUES LESS THAN value must be non-NULL",
            ));
        }
    }
    Ok(())
}

fn validate_range_bound_is_increasing(
    previous: Option<&Vec<crate::storage::partitioning::PartitionLiteral>>,
    current: &[crate::storage::partitioning::PartitionLiteral],
    collations: &[Option<&str>],
) -> Result<(), QueryError> {
    let Some(previous) = previous else {
        return Ok(());
    };
    if compare_partition_bounds(previous, current, collations) != std::cmp::Ordering::Less {
        return Err(error(
            crate::error::catalog::ER_RANGE_NOT_INCREASING_ERROR,
            "VALUES LESS THAN values must be strictly increasing for each partition",
        ));
    }
    Ok(())
}

fn compare_partition_bounds(
    a: &[crate::storage::partitioning::PartitionLiteral],
    b: &[crate::storage::partitioning::PartitionLiteral],
    collations: &[Option<&str>],
) -> std::cmp::Ordering {
    use crate::storage::partitioning::PartitionLiteral;
    for (idx, (left, right)) in a.iter().zip(b.iter()).enumerate() {
        let ord = match (left, right) {
            (PartitionLiteral::MaxValue, PartitionLiteral::MaxValue) => std::cmp::Ordering::Equal,
            (PartitionLiteral::MaxValue, _) => std::cmp::Ordering::Greater,
            (_, PartitionLiteral::MaxValue) => std::cmp::Ordering::Less,
            (PartitionLiteral::Value(l), PartitionLiteral::Value(r)) => {
                crate::storage::partitioning::compare_value_with_collation(
                    l,
                    r,
                    collations.get(idx).copied().flatten(),
                )
            }
        };
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    a.len().cmp(&b.len())
}

fn validate_list_groups(
    groups: &[Vec<crate::storage::partitioning::PartitionLiteral>],
    expected_arity: usize,
    collations: &[Option<&str>],
    seen: &mut std::collections::BTreeSet<Vec<Vec<u8>>>,
) -> Result<(), QueryError> {
    for tuple in groups {
        if tuple.len() != expected_arity {
            return Err(error(
                crate::error::catalog::ER_PARTITION_WRONG_VALUES_ERROR,
                format!(
                    "partition list value has {} values, expected {}",
                    tuple.len(),
                    expected_arity
                ),
            ));
        }
        if tuple.iter().any(|lit| {
            matches!(
                lit,
                crate::storage::partitioning::PartitionLiteral::MaxValue
            )
        }) {
            return Err(error(
                crate::error::catalog::ER_PARTITION_WRONG_VALUES_ERROR,
                "MAXVALUE is not valid in LIST partition values",
            ));
        }
        let key = tuple
            .iter()
            .enumerate()
            .map(|(idx, lit)| partition_literal_key(lit, collations.get(idx).copied().flatten()))
            .collect::<Vec<_>>();
        if !seen.insert(key) {
            return Err(error(
                crate::error::catalog::ER_MULTIPLE_DEF_CONST_IN_LIST_PART_ERROR,
                "Multiple definition of same constant in list partitioning",
            ));
        }
    }
    Ok(())
}

fn partition_bound_collations(table: &TableSchema) -> Vec<Option<&str>> {
    table
        .partitioning
        .as_ref()
        .into_iter()
        .flat_map(|part| &part.columns)
        .map(|name| {
            table
                .column_index(name)
                .ok()
                .and_then(|idx| table.columns[idx].collation.as_deref())
        })
        .collect()
}

fn partition_literal_key(
    lit: &crate::storage::partitioning::PartitionLiteral,
    collation: Option<&str>,
) -> Vec<u8> {
    use crate::storage::partitioning::PartitionLiteral;
    use crate::types::Value;
    match lit {
        PartitionLiteral::Value(Value::Null) => vec![0],
        PartitionLiteral::Value(value) => {
            let mut key = vec![1];
            match (value, collation) {
                (Value::String(text), Some(collation)) => {
                    key.extend(crate::charset::collation_key(collation, text))
                }
                _ => key.extend(crate::storage::value_to_string(value).as_bytes()),
            }
            key
        }
        PartitionLiteral::MaxValue => vec![2],
    }
}

fn validate_partition_unique_keys_mysql80(table: &TableSchema) -> Result<(), QueryError> {
    let unique_indexes = table
        .indexes
        .iter()
        .filter(|idx| idx.unique || idx.primary)
        .collect::<Vec<_>>();
    if unique_indexes.is_empty() {
        return Ok(());
    }
    let required = partition_referenced_columns(table)?;
    if required.is_empty() {
        return Ok(());
    }
    for index in unique_indexes {
        let key_columns = unique_index_referenced_columns(index);
        // MySQL distinguishes the wording: PRIMARY KEY uses "A PRIMARY KEY
        // must include all columns …" (ER_INCONSISTENT_PARTITION_INFO wording
        // family), while a plain UNIQUE index says "A UNIQUE INDEX must …".
        let key_kind = if index.primary {
            "PRIMARY KEY"
        } else {
            "UNIQUE INDEX"
        };
        for required_col in &required {
            if !key_columns.contains(required_col) {
                return Err(error(crate::error::catalog::ER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF,
                    format!(
                        "A {} must include all columns in the table's partitioning function; missing '{}'",
                        key_kind, required_col
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn partition_referenced_columns(
    table: &TableSchema,
) -> Result<std::collections::BTreeSet<String>, QueryError> {
    let part = table
        .partitioning
        .as_ref()
        .expect("called only for partitioned tables");
    let mut out = std::collections::BTreeSet::new();
    if part.method.uses_columns() {
        out.extend(part.columns.iter().map(|c| c.to_ascii_lowercase()));
    } else {
        collect_partition_program_columns(
            table,
            part.expr_program.as_ref(),
            "partition expression has not been compiled",
            &mut out,
        )?;
    }
    if let Some(sub) = part.subpartitioning.as_ref() {
        if sub.method.uses_columns() {
            out.extend(sub.columns.iter().map(|c| c.to_ascii_lowercase()));
        } else {
            collect_partition_program_columns(
                table,
                part.sub_expr_program.as_ref(),
                "subpartition expression has not been compiled",
                &mut out,
            )?;
        }
    }
    Ok(out)
}

fn collect_partition_program_columns(
    table: &TableSchema,
    program: Option<&std::sync::Arc<crate::engine::expr_program::CompiledTableExpr>>,
    missing_message: &str,
    out: &mut std::collections::BTreeSet<String>,
) -> Result<(), QueryError> {
    let Some(program) = program else {
        return Err(error(
            crate::error::catalog::ER_WRONG_EXPR_IN_PARTITION_FUNC_ERROR,
            missing_message,
        ));
    };
    let mut indices = std::collections::HashSet::new();
    if !program.try_collect_referenced_columns(&mut indices) {
        return Err(error(crate::error::catalog::ER_WRONG_EXPR_IN_PARTITION_FUNC_ERROR,
            "partition expression is not deterministic or cannot be analyzed for UNIQUE key compatibility",
        ));
    }
    for idx in indices {
        if let Some(column) = table.columns.get(idx) {
            out.insert(column.name.to_ascii_lowercase());
        }
    }
    Ok(())
}

fn unique_index_referenced_columns(
    index: &crate::storage::IndexSchema,
) -> std::collections::BTreeSet<String> {
    let mut out = std::collections::BTreeSet::new();
    for (pos, column) in index.columns.iter().enumerate() {
        if index
            .column_prefix_lengths
            .get(pos)
            .copied()
            .flatten()
            .is_some()
        {
            continue;
        }
        match index
            .key_expressions
            .get(pos)
            .and_then(|expr| expr.as_ref())
        {
            Some(expr) => {
                out.extend(
                    expr.referenced_columns
                        .iter()
                        .map(|c| c.to_ascii_lowercase()),
                );
            }
            None => {
                out.insert(column.to_ascii_lowercase());
            }
        }
    }
    out
}

/// Plan the index adjustments required when dropping `column_name` (issue
/// #83). Plain indexes that reference the column are either:
///   * dropped entirely — single-column plain indexes, special indexes
///     (FULLTEXT / SPATIAL / HNSW vector), or plain composite indexes that
///     would be left with no remaining key columns; or
///   * rebuilt without the column — plain composite b-tree indexes.
/// A functional index whose expression depends on the column BLOCKS the
/// drop (error 1553), matching MySQL — the user must drop that index first.
/// Foreign-key backing indexes are NOT handled here — the caller rejects a
/// drop of any FK-referenced column before invoking this planner.
/// Returns `(index names to drop, rebuilt index schemas)`.
fn plan_index_cascade_for_drop_column(
    table: &TableSchema,
    column_name: &str,
) -> Result<(Vec<String>, Vec<crate::storage::IndexSchema>), QueryError> {
    use crate::storage::IndexKeyPart;
    let lower_target = column_name.to_ascii_lowercase();
    let mut to_drop = Vec::new();
    let mut to_rebuild = Vec::new();
    for index in &table.indexes {
        // A functional index that depends on the column can't be mechanically
        // rewritten — MySQL blocks the whole drop (1553).
        if index
            .key_expressions
            .iter()
            .flatten()
            .any(|expr| expr.referenced_columns.iter().any(|c| c == &lower_target))
        {
            return Err(error(
                crate::error::catalog::ER_DROP_INDEX_FK,
                "cannot drop column referenced by an index",
            ));
        }
        // Positions naming the dropped column as a plain column key part.
        let plain_positions: std::collections::HashSet<usize> = index
            .columns
            .iter()
            .enumerate()
            .filter(|(pos, c)| {
                matches!(index.key_part(*pos), Some(IndexKeyPart::Column(_)))
                    && c.eq_ignore_ascii_case(column_name)
            })
            .map(|(pos, _)| pos)
            .collect();
        let include_hit = index
            .included_columns
            .iter()
            .any(|c| c.eq_ignore_ascii_case(column_name));
        if plain_positions.is_empty() && !include_hit {
            continue;
        }
        let is_special = index.fulltext.is_some()
            || index.index_type.eq_ignore_ascii_case("SPATIAL")
            || index.index_type.eq_ignore_ascii_case("FULLTEXT")
            || index.is_vector();
        let remaining_key_columns = index.columns.len().saturating_sub(plain_positions.len());
        if is_special || remaining_key_columns == 0 {
            to_drop.push(index.name.clone());
            continue;
        }
        // Rebuild a plain composite index without the dropped column: trim
        // the matched positions from every parallel array and drop the
        // column from the covering (INCLUDE) list.
        let mut rebuilt = index.clone();
        let keep = |len: usize| (0..len).filter(|i| !plain_positions.contains(i));
        rebuilt.columns = keep(index.columns.len())
            .map(|i| index.columns[i].clone())
            .collect();
        if !index.column_prefix_lengths.is_empty() {
            rebuilt.column_prefix_lengths = keep(index.column_prefix_lengths.len())
                .map(|i| index.column_prefix_lengths[i])
                .collect();
        }
        if !index.column_directions.is_empty() {
            rebuilt.column_directions = keep(index.column_directions.len())
                .map(|i| index.column_directions[i])
                .collect();
        }
        if !index.key_expressions.is_empty() {
            rebuilt.key_expressions = keep(index.key_expressions.len())
                .map(|i| index.key_expressions[i].clone())
                .collect();
        }
        rebuilt
            .included_columns
            .retain(|c| !c.eq_ignore_ascii_case(column_name));
        rebuilt.state = crate::storage::schema::IndexState::Public;
        to_rebuild.push(rebuilt);
    }
    Ok((to_drop, to_rebuild))
}

fn move_column_in_candidate(
    table: &mut TableSchema,
    column_name: &str,
    position: &crate::parser::ColumnPosition,
) -> Result<(), QueryError> {
    let target_idx = table.column_index(column_name)?;
    let mut order = table
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| column.state != ColumnState::Dropped)
        .map(|(idx, column)| (idx, column.logical_pos))
        .collect::<Vec<_>>();
    order.sort_by_key(|(idx, logical)| (*logical, *idx));
    let mut indices = order.into_iter().map(|(idx, _)| idx).collect::<Vec<_>>();
    indices.retain(|idx| *idx != target_idx);
    let insert_at = match position {
        crate::parser::ColumnPosition::First => 0,
        crate::parser::ColumnPosition::After(after) => {
            if after.eq_ignore_ascii_case(column_name) {
                return Err(error(
                    crate::error::catalog::ER_PARSE_ERROR,
                    "column cannot be positioned after itself",
                ));
            }
            let after_idx = table.column_index(after)?;
            indices
                .iter()
                .position(|idx| *idx == after_idx)
                .map(|pos| pos + 1)
                .ok_or_else(|| {
                    error(
                        crate::error::catalog::ER_BAD_FIELD_ERROR,
                        format!("unknown column '{after}'"),
                    )
                })?
        }
    };
    indices.insert(insert_at.min(indices.len()), target_idx);
    for (logical, idx) in indices.into_iter().enumerate() {
        table.columns[idx].logical_pos = logical as u32;
    }
    Ok(())
}

#[inline]
fn check_table_ddl_writable(database: &str) -> Result<(), QueryError> {
    if table_ddl_schema_is_read_only(database) {
        return Err(error(
            crate::error::catalog::ER_DBACCESS_DENIED_ERROR,
            format!("access denied for database '{database}'"),
        ));
    }
    Ok(())
}
