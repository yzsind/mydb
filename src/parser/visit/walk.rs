//! One structural traversal for immutable inspection and AST rewriting.
//! Node ownership differs; child order and scope events are shared.
macro_rules! define_walk {
    ($module:ident, $visitor_trait:ident, [$($mutable:tt)*], $iter:ident, $one:ident) => {
        mod $module {
            use super::*;
        pub(crate) fn walk_statement<V: $visitor_trait + ?Sized>(
            visitor: &mut V,
            statement: & $($mutable)* Statement,
        ) {
            visitor.enter_statement(statement);
            match statement {
                Statement::CreateView { query, .. } | Statement::AlterView { query, .. } => {
                    walk_query(visitor, query)
                }
                Statement::CreateTable(table) => walk_create_table(visitor, table),
                Statement::AlterTable(table) => walk_alter_table(visitor, table),
                Statement::CreateTableAsSelect { table, query, .. } => {
                    walk_create_table(visitor, table);
                    walk_query(visitor, query);
                }
                Statement::Insert(insert) => walk_insert(visitor, insert),
                Statement::Select(select) => walk_select(visitor, select),
                Statement::SetOperation(set) => walk_set_operation(visitor, set),
                Statement::WithCte(with) => {
                    for cte in & $($mutable)* with.ctes {
                        visitor.enter_cte(cte);
                        walk_query(visitor, & $($mutable)* cte.query);
                        visitor.exit_cte(cte);
                    }
                    walk_statement(visitor, & $($mutable)* with.body);
                }
                Statement::Explain { target, .. } => walk_statement(visitor, target),
                Statement::SelectIntoVars { inner, .. } | Statement::SelectIntoFile { inner, .. } => {
                    walk_query(visitor, inner)
                }
                Statement::Update(update) => walk_update(visitor, update),
                Statement::Delete(delete) => walk_delete(visitor, delete),
                Statement::CreateRoutine(def) => walk_routine_body(visitor, & $($mutable)* def.body),
                Statement::CreateTrigger(def) => walk_routine_body(visitor, & $($mutable)* def.body),
                Statement::Call(call) => walk_exprs(visitor, & $($mutable)* call.args),
                Statement::GetDiagnostics { condition, .. } => {
                    if let Some(condition) = condition {
                        walk_expr(visitor, condition);
                    }
                }
                Statement::Set(assignments) => {
                    for assignment in assignments {
                        walk_expr(visitor, & $($mutable)* assignment.value);
                    }
                }
                Statement::LoadData(load) => {
                    for (_, expr) in & $($mutable)* load.set_clauses {
                        walk_expr(visitor, expr);
                    }
                }
                Statement::Do(exprs) => walk_exprs(visitor, exprs),
                Statement::Prepare { source, .. } => walk_expr(visitor, source),
                Statement::DeclareSqlCursor { query, .. } => walk_query(visitor, query),
                Statement::ShowSequences { filter, .. }
                | Statement::ShowDatabases { filter }
                | Statement::ShowTables { filter, .. }
                | Statement::ShowTableStatus { filter, .. }
                | Statement::ShowColumns { filter, .. }
                | Statement::ShowIndex { filter, .. }
                | Statement::ShowRoutineStatus { filter, .. }
                | Statement::ShowTriggers { filter, .. }
                | Statement::ShowEvents { filter, .. }
                | Statement::ShowCharset { filter }
                | Statement::ShowCollation { filter }
                | Statement::ShowVariables { filter, .. }
                | Statement::ShowStatus { filter, .. }
                | Statement::ShowOpenTables { filter, .. } => walk_show_filter(visitor, filter),
                Statement::Begin { .. }
                | Statement::Commit { .. }
                | Statement::Rollback { .. }
                | Statement::Savepoint(_)
                | Statement::RollbackToSavepoint(_)
                | Statement::ReleaseSavepoint(_)
                | Statement::ExecutePrepared { .. }
                | Statement::DeallocatePrepared { .. }
                | Statement::SetAutocommit(_)
                | Statement::SetTransactionIsolation { .. }
                | Statement::LockTables(_)
                | Statement::UnlockTables
                | Statement::CreateTableLike { .. }
                | Statement::CreateDatabase { .. }
                | Statement::AlterDatabase { .. }
                | Statement::DropDatabase { .. }
                | Statement::DropTable { .. }
                | Statement::CreateSequence { .. }
                | Statement::AlterSequence { .. }
                | Statement::DropSequence { .. }
                | Statement::ShowCreateSequence { .. }
                | Statement::TruncateTable { .. }
                | Statement::CreateIndex(_)
                | Statement::DropIndex { .. }
                | Statement::AnalyzeTable { .. }
                | Statement::CheckTable { .. }
                | Statement::RepairTable { .. }
                | Statement::OptimizeTable { .. }
                | Statement::ChecksumTable { .. }
                | Statement::CreateTablespace { .. }
                | Statement::DropTablespace { .. }
                | Statement::AlterTablespace { .. }
                | Statement::DropView { .. }
                | Statement::ExplainForConnection { .. }
                | Statement::RenameTables { .. }
                | Statement::Describe { .. }
                | Statement::UseDatabase(_)
                | Statement::ShowCreateDatabase { .. }
                | Statement::ShowCreateTable { .. }
                | Statement::AlterRoutine { .. }
                | Statement::DropRoutine { .. }
                | Statement::ShowCreateRoutine { .. }
                | Statement::DropTrigger { .. }
                | Statement::ShowCreateTrigger { .. }
                | Statement::CreateEvent(_)
                | Statement::AlterEvent(_)
                | Statement::DropEvent { .. }
                | Statement::ShowCreateEvent { .. }
                | Statement::ShowDiagnostics { .. }
                | Statement::ShowCountDiagnostics { .. }
                | Statement::ShowEngines
                | Statement::ShowEngineMydbStatus
                | Statement::ShowMydbMemory
                | Statement::ShowMydbMemorySessions
                | Statement::ShowEngine { .. }
                | Statement::ShowPlugins
                | Statement::ShowProcesslist { .. }
                | Statement::ShowGrants
                | Statement::ShowGrantsFor { .. }
                | Statement::ShowCreateUser { .. }
                | Statement::Kill { .. }
                | Statement::CancelDdlJob(_)
                | Statement::BackupAdmin { .. }
                | Statement::Shutdown
                | Statement::Restart
                | Statement::AlterSystemCheckpoint
                | Statement::AlterSystemFlushDataCache
                | Statement::AlterSystemFlushSqlCache
                | Statement::ShowPrivileges
                | Statement::CreateUser(_)
                | Statement::AlterUser(_)
                | Statement::AlterUsers(_)
                | Statement::DropUser { .. }
                | Statement::RenameUser { .. }
                | Statement::CreateRole { .. }
                | Statement::DropRole { .. }
                | Statement::Grant(_)
                | Statement::Revoke(_)
                | Statement::SetRole(_)
                | Statement::SetDefaultRole(_)
                | Statement::SetPassword { .. }
                | Statement::SetNames { .. }
                | Statement::SetCharacterSet { .. }
                | Statement::ResetPersist { .. }
                | Statement::SetResourceGroup { .. }
                | Statement::CreateResourceGroup { .. }
                | Statement::DropResourceGroup { .. }
                | Statement::AlterResourceGroup { .. }
                | Statement::SetTransactionCharacteristics { .. }
                | Statement::FlushPrivileges
                | Statement::FlushUserResources
                | Statement::FlushHosts
                | Statement::FlushStatus
                | Statement::FlushTables { .. }
                | Statement::FlushQueryCache
                | Statement::FlushLogs { .. }
                | Statement::InstallComponent { .. }
                | Statement::UninstallComponent { .. }
                | Statement::ReplicationAdmin { .. }
                | Statement::MydbReplicationAdmin { .. }
                | Statement::BinlogAdmin { .. }
                | Statement::Noop { .. }
                | Statement::Unsupported { .. }
                | Statement::MultiStatement { .. }
                | Statement::XaStart { .. }
                | Statement::XaEnd { .. }
                | Statement::XaPrepare { .. }
                | Statement::XaCommit { .. }
                | Statement::XaRollback { .. }
                | Statement::XaRecover { .. }
                | Statement::FetchSqlCursor { .. }
                | Statement::CloseSqlCursor { .. } => {}
            }
            visitor.exit_statement(statement);
        }

        pub(crate) fn walk_select<V: $visitor_trait + ?Sized>(visitor: &mut V, select: & $($mutable)* Select) {
            visitor.enter_select(select);
            if !visitor.walk_select_children(select) { visitor.exit_select(select); return; }
            walk_projection(visitor, & $($mutable)* select.projection);

            for table in & $($mutable)* select.from {
                walk_table_ref(visitor, table);
            }
            if let Some(derived) = & $($mutable)* select.derived {
                walk_subquery(visitor, derived);
            }
            if let Some(values) = & $($mutable)* select.values {
                for row in & $($mutable)* values.rows {
                    walk_exprs(visitor, row);
                }
            }
            if let Some(json_table) = & $($mutable)* select.json_table {
                walk_expr(visitor, & $($mutable)* json_table.expr);
                walk_json_table_columns(visitor, & $($mutable)* json_table.columns);
            }
            for join in & $($mutable)* select.joins {
                walk_join(visitor, join);
            }
            if let Some(selection) = & $($mutable)* select.selection {
                walk_expr(visitor, selection);
            }
            visitor.enter_select_alias_scope(&select.projection);
            walk_exprs(visitor, & $($mutable)* select.group_by);
            if let Some(having) = & $($mutable)* select.having {
                walk_expr(visitor, having);
            }
            visitor.exit_select_alias_scope();
            for window in & $($mutable)* select.named_windows {
                walk_exprs(visitor, & $($mutable)* window.partition_by);
                walk_order_by(visitor, & $($mutable)* window.order_by);
                if let Some(frame) = & $($mutable)* window.frame {
                    walk_window_frame(visitor, frame);
                }
            }
            visitor.enter_select_order_scope(&select.projection);
            walk_order_by(visitor, & $($mutable)* select.order_by);
            visitor.exit_select_alias_scope();
            if matches!(select.limit, Some(LimitValue::OffsetCommaPlaceholder(_))) {
                walk_optional_limit(visitor, & $($mutable)* select.offset);
                walk_optional_limit(visitor, & $($mutable)* select.limit);
            } else {
                walk_optional_limit(visitor, & $($mutable)* select.limit);
                walk_optional_limit(visitor, & $($mutable)* select.offset);
            }
            visitor.exit_select(select);
        }

        pub(crate) fn walk_expr<V: $visitor_trait + ?Sized>(visitor: &mut V, expr: & $($mutable)* Expr) {
            crate::util::stack::ensure_stack(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::ParserTransient,
                || walk_expr_checked(visitor, expr),
            )
        }

        fn walk_expr_checked<V: $visitor_trait + ?Sized>(visitor: &mut V, expr: & $($mutable)* Expr) {
            if !visitor.continue_expr_walk() { return; }
            visitor.enter_expr(expr);
            if !visitor.walk_expr_children(expr) { visitor.exit_expr(expr); return; }
            match expr {
                Expr::Unary { expr, .. } | Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
                    walk_expr(visitor, expr);
                }
                Expr::Binary { left, right, .. } => {
                    walk_expr(visitor, left);
                    walk_expr(visitor, right);
                }
                Expr::InList { expr, list, .. } => {
                    walk_expr(visitor, expr);
                    walk_exprs(visitor, list);
                }
                Expr::InSubquery { expr, subquery, .. } => {
                    walk_expr(visitor, expr);
                    walk_select(visitor, subquery);
                }
                Expr::QuantifiedComparison { left, right, .. } => {
                    walk_expr(visitor, left);
                    match right {
                        QuantifiedComparisonRight::Subquery(select) => walk_select(visitor, select),
                        QuantifiedComparisonRight::List(exprs) => walk_exprs(visitor, exprs),
                    }
                }
                Expr::Subquery(select)
                | Expr::Exists {
                    subquery: select, ..
                } => {
                    walk_select(visitor, select);
                }
                Expr::Between {
                    expr, low, high, ..
                } => {
                    walk_expr(visitor, expr);
                    walk_expr(visitor, low);
                    walk_expr(visitor, high);
                }
                Expr::Like { expr, pattern, .. } => {
                    walk_expr(visitor, expr);
                    walk_expr(visitor, pattern);
                }
                Expr::Case {
                    operand,
                    conditions,
                    results,
                    else_result,
                } => {
                    if let Some(operand) = operand {
                        walk_expr(visitor, operand);
                    }
                    if visitor.case_results_interleaved() {
                        for (condition, result) in conditions.$iter().zip(results.$iter()) {
                            walk_expr(visitor, condition);
                            walk_expr(visitor, result);
                        }
                        // Keep malformed ASTs traversable instead of dropping unmatched
                        // elements created by a future converter change.
                        if conditions.len() > results.len() {
                            walk_exprs(visitor, & $($mutable)* conditions[results.len()..]);
                        } else if results.len() > conditions.len() {
                            walk_exprs(visitor, & $($mutable)* results[conditions.len()..]);
                        }
                    } else {
                        walk_exprs(visitor, conditions);
                        walk_exprs(visitor, results);
                    }
                    if let Some(else_result) = else_result {
                        walk_expr(visitor, else_result);
                    }
                }
                Expr::Function { args, .. } | Expr::Tuple(args) => walk_exprs(visitor, args),
                Expr::GroupKeyRef { expr, .. } => walk_expr(visitor, expr),
                Expr::WindowFunction {args, spec, ..} => {
let crate::parser::WindowSpec { partition_by, order_by, frame, .. } = & $($mutable)* **spec;

                    walk_exprs(visitor, args);
                    walk_exprs(visitor, partition_by);
                    walk_order_by(visitor, order_by);
                    if let Some(frame) = frame {
                        walk_window_frame(visitor, frame);
                    }
                }
                Expr::Wildcard
                | Expr::Identifier(_)
                | Expr::CompoundIdentifier(_)
                | Expr::UserVariable(_)
                | Expr::SystemVariable { .. }
                | Expr::Literal(_)
                | Expr::BoundValue(_)
                | Expr::Placeholder(_)
                | Expr::Default(_) | Expr::DefaultQualified(_)
                | Expr::Grouping(_) => {}
            }
            visitor.exit_expr(expr);
        }

        pub(crate) fn walk_insert<V: $visitor_trait + ?Sized>(visitor: &mut V, insert: & $($mutable)* Insert) {
            match & $($mutable)* insert.source {
                InsertSource::Values(rows) => {
                    for row in rows {
                        walk_exprs(visitor, row);
                    }
                }
                InsertSource::Query(query) => walk_query(visitor, query),
            }
            if let Some(predicate) = & $($mutable)* insert.view_check_predicate {
                walk_expr(visitor, predicate);
            }
            if let Some(assignments) = & $($mutable)* insert.on_duplicate_key_update {
                for assignment in assignments {
                    walk_expr(visitor, & $($mutable)* assignment.value);
                }
            }
            walk_returning(visitor, & $($mutable)* insert.returning);
        }

        pub(crate) fn walk_update<V: $visitor_trait + ?Sized>(visitor: &mut V, update: & $($mutable)* Update) {
            for join in & $($mutable)* update.joins {
                walk_join(visitor, join);
            }
            for assignment in & $($mutable)* update.assignments {
                walk_expr(visitor, & $($mutable)* assignment.value);
            }
            if let Some(selection) = & $($mutable)* update.selection {
                walk_expr(visitor, selection);
            }
            if let Some(predicate) = & $($mutable)* update.view_check_predicate {
                walk_expr(visitor, predicate);
            }
            walk_order_by(visitor, & $($mutable)* update.order_by);
            walk_optional_limit(visitor, & $($mutable)* update.limit);
            walk_returning(visitor, & $($mutable)* update.returning);
        }

        pub(crate) fn walk_delete<V: $visitor_trait + ?Sized>(visitor: &mut V, delete: & $($mutable)* Delete) {
            for join in & $($mutable)* delete.joins {
                walk_join(visitor, join);
            }
            if let Some(selection) = & $($mutable)* delete.selection {
                walk_expr(visitor, selection);
            }
            walk_order_by(visitor, & $($mutable)* delete.order_by);
            walk_optional_limit(visitor, & $($mutable)* delete.limit);
            walk_returning(visitor, & $($mutable)* delete.returning);
        }

        pub(crate) fn walk_join<V: $visitor_trait + ?Sized>(visitor: &mut V, join: & $($mutable)* Join) {
            walk_table_ref(visitor, & $($mutable)* join.table);
            if let Some(derived) = & $($mutable)* join.derived {
                walk_subquery(visitor, derived);
            }
            if let Some(condition) = & $($mutable)* join.condition {
                walk_expr(visitor, condition);
            }
        }

        pub(crate) fn walk_table_ref<V: $visitor_trait + ?Sized>(visitor: &mut V, table: & $($mutable)* TableRef) {
            if let Some(system_time) = & $($mutable)* table.system_time {
                walk_expr(visitor, & $($mutable)* system_time.expr);
            }
            if let Some(series) = & $($mutable)* table.generate_series {
                walk_expr(visitor, & $($mutable)* series.start);
                walk_expr(visitor, & $($mutable)* series.stop);
                if let Some(step) = & $($mutable)* series.step {
                    walk_expr(visitor, step);
                }
            }
        }

        pub(crate) fn walk_set_operation<V: $visitor_trait + ?Sized>(
            visitor: &mut V, set: & $($mutable)* SetOperation,
        ) {
            walk_query(visitor, & $($mutable)* set.left);
            walk_query(visitor, & $($mutable)* set.right);
            walk_order_by(visitor, & $($mutable)* set.order_by);
            if matches!(set.limit, Some(LimitValue::OffsetCommaPlaceholder(_))) {
                walk_optional_limit(visitor, & $($mutable)* set.offset);
                walk_optional_limit(visitor, & $($mutable)* set.limit);
            } else {
                walk_optional_limit(visitor, & $($mutable)* set.limit);
                walk_optional_limit(visitor, & $($mutable)* set.offset);
            }
        }

        pub(crate) fn walk_subquery<V: $visitor_trait + ?Sized>(visitor: &mut V, subquery: & $($mutable)* SubQuery) {
            walk_query(visitor, subquery);
        }

        pub(crate) fn walk_query<V: $visitor_trait + ?Sized>(visitor: &mut V, query: & $($mutable)* QueryExpr) {
            crate::util::stack::ensure_stack(
                crate::engine::executor::mem_budget::ControlledMemoryCategory::ParserTransient,
                || walk_query_checked(visitor, query),
            )
        }

        fn walk_query_checked<V: $visitor_trait + ?Sized>(visitor: &mut V, query: & $($mutable)* QueryExpr) {
            visitor.enter_query(query);
            if !visitor.walk_query_children(query) { visitor.exit_query(query); return; }
            match query {
                QueryExpr::Select(select) => walk_select(visitor, select),
                QueryExpr::SetOp(set) => walk_set_operation(visitor, set),
                QueryExpr::WithCte(with) => {
                    for cte in & $($mutable)* with.ctes {
                        visitor.enter_cte(cte);
                        walk_query(visitor, & $($mutable)* cte.query);
                        visitor.exit_cte(cte);
                    }
                    walk_query(visitor, & $($mutable)* with.body);
                }
            }
            visitor.exit_query(query);
        }

        pub(crate) fn walk_create_table<V: $visitor_trait + ?Sized>(visitor: &mut V, table: & $($mutable)* CreateTable) {
            for column in & $($mutable)* table.columns {
                walk_column(visitor, column);
            }
            for check in & $($mutable)* table.checks {
                walk_expr(visitor, & $($mutable)* check.expr);
            }
        }

        pub(crate) fn walk_alter_table<V: $visitor_trait + ?Sized>(visitor: &mut V, table: & $($mutable)* AlterTable) {
            for action in & $($mutable)* table.actions {
                match action {
                    AlterAction::AddColumn(column) | AlterAction::ModifyColumn(column) => {
                        walk_column(visitor, column);
                    }
                    AlterAction::ChangeColumn { new_def, .. } => walk_column(visitor, new_def),
                    AlterAction::AddCheck(check) => walk_expr(visitor, & $($mutable)* check.expr),
                    AlterAction::SetColumnDefault {
                        default: Some(expr),
                        ..
                    } => walk_expr(visitor, expr),
                    AlterAction::Rebuild
                    | AlterAction::AddIndex(_)
                    | AlterAction::AddForeignKey(_)
                    | AlterAction::MoveColumn { .. }
                    | AlterAction::SetColumnComment { .. }
                    | AlterAction::SetColumnDefault { default: None, .. }
                    | AlterAction::SetTableOptions { .. }
                    | AlterAction::SetStorageProperties { .. }
                    | AlterAction::DropColumn(_)
                    | AlterAction::DropIndex(_)
                    | AlterAction::DropPrimaryKey
                    | AlterAction::DropCheck { .. }
                    | AlterAction::DropForeignKey { .. }
                    | AlterAction::AlterIndexVisibility { .. }
                    | AlterAction::AlterCheck { .. }
                    | AlterAction::RenameColumn { .. }
                    | AlterAction::RenameIndex { .. }
                    | AlterAction::RenameTable { .. }
                    | AlterAction::SetAutoIncrement(_)
                    | AlterAction::Noop { .. }
                    | AlterAction::SetColumnVisibility { .. }
                    | AlterAction::ConvertToCharset { .. } => {}
                }
            }
        }

        pub(crate) fn walk_column<V: $visitor_trait + ?Sized>(visitor: &mut V, column: & $($mutable)* ColumnDef) {
            if let Some(default) = & $($mutable)* column.default {
                walk_expr(visitor, default);
            }
            if let Some(on_update) = & $($mutable)* column.on_update {
                walk_expr(visitor, on_update);
            }
            for check in & $($mutable)* column.checks {
                walk_expr(visitor, & $($mutable)* check.expr);
            }
            if let Some(generated) = & $($mutable)* column.generated {
                walk_expr(visitor, & $($mutable)* generated.expr);
            }
        }

        pub(crate) fn walk_routine_body<V: $visitor_trait + ?Sized>(visitor: &mut V, body: & $($mutable)* RoutineBody) {
            walk_routine_statements(visitor, & $($mutable)* body.statements);
        }

        pub(crate) fn walk_routine_statements<V: $visitor_trait + ?Sized>(
            visitor: &mut V,
            statements: & $($mutable)* [RoutineStmt],
        ) {
            for statement in statements {
                match statement {
                    RoutineStmt::Block { statements, .. } | RoutineStmt::Loop { statements, .. } => {
                        walk_routine_statements(visitor, statements);
                    }
                    RoutineStmt::DeclareVar { default, .. } => {
                        if let Some(default) = default {
                            walk_expr(visitor, default);
                        }
                    }
                    RoutineStmt::DeclareHandler { statement, .. } => {
                        walk_routine_statements(visitor, std::slice::$one(statement));
                    }
                    RoutineStmt::Set { assignments } => {
                        for (_, expr) in assignments {
                            walk_expr(visitor, expr);
                        }
                    }
                    RoutineStmt::If {
                        branches,
                        else_branch,
                    } => {
                        for (condition, statements) in branches {
                            walk_expr(visitor, condition);
                            walk_routine_statements(visitor, statements);
                        }
                        walk_routine_statements(visitor, else_branch);
                    }
                    RoutineStmt::Case {
                        operand,
                        branches,
                        else_branch,
                    } => {
                        if let Some(operand) = operand {
                            walk_expr(visitor, operand);
                        }
                        for (condition, statements) in branches {
                            walk_expr(visitor, condition);
                            walk_routine_statements(visitor, statements);
                        }
                        walk_routine_statements(visitor, else_branch);
                    }
                    RoutineStmt::While {
                        condition,
                        statements,
                        ..
                    } => {
                        walk_expr(visitor, condition);
                        walk_routine_statements(visitor, statements);
                    }
                    RoutineStmt::Repeat {
                        statements, until, ..
                    } => {
                        walk_routine_statements(visitor, statements);
                        walk_expr(visitor, until);
                    }
                    RoutineStmt::Return(expr) => walk_expr(visitor, expr),
                    RoutineStmt::Signal {
                        message_text,
                        mysql_errno,
                        ..
                    }
                    | RoutineStmt::Resignal {
                        message_text,
                        mysql_errno,
                        ..
                    } => {
                        if let Some(expr) = message_text {
                            walk_expr(visitor, expr);
                        }
                        if let Some(expr) = mysql_errno {
                            walk_expr(visitor, expr);
                        }
                    }
                    RoutineStmt::Call(call) => walk_exprs(visitor, & $($mutable)* call.args),
                    RoutineStmt::Prepare { source, .. } => walk_expr(visitor, source),
                    RoutineStmt::GetDiagnostics { condition, .. } => {
                        if let Some(condition) = condition {
                            walk_expr(visitor, condition);
                        }
                    }
                    RoutineStmt::DeclareCondition { .. }
                    | RoutineStmt::DeclareCursor { .. }
                    | RoutineStmt::SelectInto { .. }
                    | RoutineStmt::Leave(_)
                    | RoutineStmt::Iterate(_)
                    | RoutineStmt::Open(_)
                    | RoutineStmt::Fetch { .. }
                    | RoutineStmt::Close(_)
                    | RoutineStmt::Execute { .. }
                    | RoutineStmt::Deallocate { .. }
                    | RoutineStmt::Sql(_) => {}
                }
            }
        }

        pub(crate) fn walk_projection<V: $visitor_trait + ?Sized>(visitor: &mut V, projection: & $($mutable)* [SelectItem]) {
            for item in projection {
                if let SelectItem::Expr { expr, .. } = item {
                    walk_expr(visitor, expr);
                }
            }
        }

        pub(crate) fn walk_returning<V: $visitor_trait + ?Sized>(
            visitor: &mut V,
            returning: & $($mutable)* Option<ReturningClause>,
        ) {
            if let Some(returning) = returning {
                walk_projection(visitor, & $($mutable)* returning.projection);
            }
        }

        pub(crate) fn walk_json_table_columns<V: $visitor_trait + ?Sized>(
            visitor: &mut V,
            columns: & $($mutable)* [JsonTableColumnDef],
        ) {
            for column in columns {
                match column {
                    JsonTableColumnDef::Named {
                        on_empty, on_error, ..
                    } => {
                        walk_json_response(visitor, on_empty);
                        walk_json_response(visitor, on_error);
                    }
                    JsonTableColumnDef::Nested { columns, .. } => {
                        walk_json_table_columns(visitor, columns);
                    }
                    JsonTableColumnDef::Ordinality { .. } => {}
                }
            }
        }

        pub(crate) fn walk_json_response<V: $visitor_trait + ?Sized>(
            visitor: &mut V,
            response: & $($mutable)* JsonTableOnResponse,
        ) {
            if let JsonTableOnResponse::Default(expr) = response {
                walk_expr(visitor, expr);
            }
        }

        pub(crate) fn walk_show_filter<V: $visitor_trait + ?Sized>(
            visitor: &mut V,
            filter: & $($mutable)* Option<ShowFilter>,
        ) {
            if let Some(ShowFilter::Where(expr)) = filter {
                walk_expr(visitor, expr);
            }
        }

        pub(crate) fn walk_exprs<V: $visitor_trait + ?Sized>(visitor: &mut V, exprs: & $($mutable)* [Expr]) {
            for expr in exprs {
                if !visitor.continue_expr_walk() { break; }
                walk_expr(visitor, expr);
            }
        }

        pub(crate) fn walk_order_by<V: $visitor_trait + ?Sized>(visitor: &mut V, order_by: & $($mutable)* [OrderBy]) {
            for item in order_by {
                if visitor.walk_order_expr(item) {
                    walk_expr(visitor, & $($mutable)* item.expr);
                }
            }
        }

        pub(crate) fn walk_optional_limit<V: $visitor_trait + ?Sized>(
            visitor: &mut V,
            limit: & $($mutable)* Option<LimitValue>,
        ) {
            if let Some(limit) = limit {
                visitor.enter_limit(limit);
            }
        }

        pub(crate) fn walk_window_frame<V: $visitor_trait + ?Sized>(visitor: &mut V, frame: & $($mutable)* WindowFrame) {
            visitor.enter_window_frame(frame);
            if !visitor.walk_window_frame_children(frame) { return; }
            walk_window_bound(visitor, & $($mutable)* frame.start);
            walk_window_bound(visitor, & $($mutable)* frame.end);
        }

        pub(crate) fn walk_window_bound<V: $visitor_trait + ?Sized>(
            visitor: &mut V,
            bound: & $($mutable)* WindowFrameBound,
        ) {
            match bound {
                WindowFrameBound::Preceding(expr) | WindowFrameBound::Following(expr) => {
                    walk_expr(visitor, expr);
                }
                WindowFrameBound::UnboundedPreceding
                | WindowFrameBound::UnboundedFollowing
                | WindowFrameBound::CurrentRow => {}
            }
        }
        }
    };
}
pub(super) use define_walk;
