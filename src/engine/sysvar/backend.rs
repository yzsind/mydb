//! The `SysVar` entry: a definition + read/write closures. Plus a small
//! family of helper constructors for the common backend shapes.

use std::sync::Arc;

use crate::types::QueryError;

use super::ctx::{ReadCtx, WriteCtx};
use super::meta::{Access, DataType, Scope, SysVarDef};
use super::value::SysVarValue;

/// Read closure signature. Returns the current value for the requested
/// scope. Most readers ignore the scope (single-storage vars); `Both`-scope
/// vars dispatch on `ctx.scope`.
pub type ReadFn = Arc<dyn Fn(&ReadCtx<'_>) -> SysVarValue + Send + Sync + 'static>;

/// Write closure signature. The dispatcher has already validated scope
/// and coerced the value to `def.data_type`.
pub type WriteFn =
    Arc<dyn Fn(&mut WriteCtx<'_>, SysVarValue) -> Result<(), QueryError> + Send + Sync + 'static>;

/// One registered system variable.
pub struct SysVar {
    pub def: SysVarDef,
    pub read: ReadFn,
    pub write: Option<WriteFn>,
    pub compiled_default: SysVarValue,
    pub default_provider: Option<Arc<dyn Fn() -> SysVarValue + Send + Sync>>,
    pub inherit: Option<WriteFn>,
    pub validate:
        Option<Arc<dyn Fn(&ReadCtx<'_>, &SysVarValue) -> Result<(), QueryError> + Send + Sync>>,
}

impl std::fmt::Debug for SysVar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SysVar")
            .field("def", &self.def)
            .field("read", &"<fn>")
            .field("write", &self.write.as_ref().map(|_| "<fn>"))
            .finish()
    }
}

impl SysVar {
    /// Construct a `SysVar` with explicit read/write closures.
    pub fn new<R, W>(def: SysVarDef, read: R, write: Option<W>) -> Self
    where
        R: Fn(&ReadCtx<'_>) -> SysVarValue + Send + Sync + 'static,
        W: Fn(&mut WriteCtx<'_>, SysVarValue) -> Result<(), QueryError> + Send + Sync + 'static,
    {
        let compiled_default = read(&ReadCtx::new(None, None, Scope::Global));
        SysVar {
            compiled_default,
            inherit: None,
            validate: None,
            default_provider: None,
            def,
            read: Arc::new(read),
            write: write.map(|w| -> WriteFn { Arc::new(w) }),
        }
    }

    /// Read-only variable backed by a constant — `version`, `license`,
    /// `protocol_version`, etc.
    pub fn const_read_only(def: SysVarDef, value: SysVarValue) -> Self {
        let v = value;
        let def = SysVarDef {
            access: Access::ReadOnly,
            ..def
        };
        let val = v.clone();
        let read: ReadFn = Arc::new(move |_ctx| val.clone());
        SysVar {
            compiled_default: read(&ReadCtx::new(None, None, Scope::Global)),
            inherit: None,
            validate: None,
            default_provider: None,
            def,
            read,
            write: None,
        }
    }

    /// Read-only variable backed by a runtime probe (e.g. `hostname`,
    /// `system_time_zone`, `port` — values computed once at startup).
    pub fn runtime_read_only<F>(def: SysVarDef, probe: F) -> Self
    where
        F: Fn() -> SysVarValue + Send + Sync + 'static,
    {
        let def = SysVarDef {
            access: Access::ReadOnly,
            ..def
        };
        let read: ReadFn = Arc::new(move |_ctx| probe());
        SysVar {
            compiled_default: read(&ReadCtx::new(None, None, Scope::Global)),
            inherit: None,
            validate: None,
            default_provider: None,
            def,
            read,
            write: None,
        }
    }

    /// Settable variable with read + write closures. The data type
    /// (already on the def) drives validation; backends receive the
    /// fully-coerced value.
    pub fn settable<R, W>(def: SysVarDef, read: R, write: W) -> Self
    where
        R: Fn(&ReadCtx<'_>) -> SysVarValue + Send + Sync + 'static,
        W: Fn(&mut WriteCtx<'_>, SysVarValue) -> Result<(), QueryError> + Send + Sync + 'static,
    {
        let compiled_default = read(&ReadCtx::new(None, None, Scope::Global));
        SysVar {
            compiled_default,
            inherit: None,
            validate: None,
            default_provider: None,
            def,
            read: Arc::new(read),
            write: Some(Arc::new(write)),
        }
    }

    /// A session field plus an independent global default. Session reads and
    /// writes keep their direct field path; only GLOBAL changes and connection
    /// initialization touch the shared default. Validate GLOBAL writes on an
    /// isolated context, so normalization or failure cannot alter the caller.
    pub fn session_backed<R, W>(def: SysVarDef, read: R, write: W) -> Self
    where
        R: Fn(&ReadCtx<'_>) -> SysVarValue + Send + Sync + 'static,
        W: Fn(&mut WriteCtx<'_>, SysVarValue) -> Result<(), QueryError> + Send + Sync + 'static,
    {
        assert_eq!(def.scope, Scope::Both);
        let default = read(&ReadCtx::new(None, None, Scope::Session));
        let global = Arc::new(std::sync::RwLock::new(None::<SysVarValue>));
        let read = Arc::new(read);
        let write: WriteFn = Arc::new(write);
        let mut var = Self::settable(
            def,
            {
                let global = global.clone();
                let default = default.clone();
                let read = read.clone();
                move |ctx| {
                    if ctx.scope == Scope::Session && ctx.session.is_some() {
                        read(ctx)
                    } else {
                        global.read().unwrap().as_ref().unwrap_or(&default).clone()
                    }
                }
            },
            {
                let global = global.clone();
                let write = write.clone();
                move |ctx, value| {
                    if ctx.scope == Scope::Session {
                        return write(ctx, value);
                    }
                    let mut guard = global.write().unwrap();
                    let mut scratch =
                        crate::engine::api::SessionContext::without_sysvar_inheritance();
                    let mut isolated = WriteCtx::new(&mut scratch, ctx.db, Scope::Session);
                    if let Some(current) = guard.as_ref() {
                        write(&mut isolated, current.clone())?;
                    }
                    write(&mut isolated, value)?;
                    *guard = Some(read(&ReadCtx::new(Some(&scratch), ctx.db, Scope::Session)));
                    Ok(())
                }
            },
        );
        // Numeric/boolean session fields are fully validated by DataType.
        // String/enum backends additionally resolve charsets, optimizer flags,
        // storage engines, etc. Validate those on an isolated context only.
        if matches!(
            var.def.data_type,
            DataType::String { .. } | DataType::StrictString { .. } | DataType::Enum { .. }
        ) {
            let validate_write = write.clone();
            var.validate = Some(Arc::new(move |ctx, value| {
                let mut scratch = crate::engine::api::SessionContext::without_sysvar_inheritance();
                let mut isolated = WriteCtx::new(&mut scratch, ctx.db, Scope::Session);
                crate::engine::api::with_statement_warning_sink(Default::default(), || {
                    validate_write(&mut isolated, value.clone())
                })
            }));
        }
        var.inherit = Some(Arc::new(move |ctx, _| {
            if let Some(value) = global.read().unwrap().as_ref() {
                write(ctx, value.clone())?;
            }
            Ok(())
        }));
        var
    }

    /// Pure backend validation, performed before any SET assignment is applied.
    pub fn with_validation<F>(mut self, validate: F) -> Self
    where
        F: Fn(&ReadCtx<'_>, &SysVarValue) -> Result<(), QueryError> + Send + Sync + 'static,
    {
        self.validate = Some(Arc::new(validate));
        self
    }

    /// Resolve a default with a runtime location (e.g. a log under data_dir),
    /// without reading any mutable variable value or startup override.
    pub fn with_default_provider<F>(mut self, provider: F) -> Self
    where
        F: Fn() -> SysVarValue + Send + Sync + 'static,
    {
        self.default_provider = Some(Arc::new(provider));
        self
    }

    pub fn with_default(mut self, value: SysVarValue) -> Self {
        self.compiled_default = value;
        self
    }

    pub fn data_type(&self) -> &DataType {
        &self.def.data_type
    }

    pub fn scope(&self) -> Scope {
        self.def.scope
    }

    pub fn name(&self) -> &'static str {
        self.def.name
    }
}
