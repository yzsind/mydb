//! Cost-based optimizer coefficients (`cbo_*`). Delegates to the existing
//! `engine::cbo` module so the AtomicF64 cells stay the single source of
//! truth.

use crate::engine::sysvar::backend::SysVar;
use crate::engine::sysvar::ctx::WriteCtx;
use crate::engine::sysvar::errors::wrong_value_for_var;
use crate::engine::sysvar::meta::{DataType, Scope, SysVarDef};
use crate::engine::sysvar::registry::Registry;
use crate::engine::sysvar::value::SysVarValue;
use crate::types::QueryError;

pub(crate) fn register(reg: &mut Registry) {
    reg.register(SysVar::const_read_only(
        SysVarDef::new(
            "cbo_model",
            Scope::Global,
            DataType::Enum { values: &["V2"] },
        )
        .with_doc("Read-only cost-based optimizer implementation identifier."),
        SysVarValue::String(crate::engine::cbo::cbo_model().to_string()),
    ));

    for (name, value) in crate::engine::cbo::variables_snapshot() {
        let canon: &'static str = name;
        let _ = value; // default is reflected through cbo::variable_value at read time
        reg.register(SysVar::settable(
            SysVarDef::new(
                canon,
                Scope::Global,
                DataType::Double {
                    min: 0.0,
                    max: f64::MAX,
                },
            )
            .with_doc("Cost-based optimizer coefficient."),
            move |_ctx| {
                crate::engine::cbo::variable_value(canon)
                    .map(|s| SysVarValue::String(s))
                    .unwrap_or(SysVarValue::String("0".to_string()))
            },
            move |_ctx: &mut WriteCtx<'_>, v: SysVarValue| -> Result<(), QueryError> {
                let n = v
                    .as_f64()
                    .ok_or_else(|| wrong_value_for_var(canon, &v.to_display_string()))?;
                crate::engine::cbo::set_variable(canon, n)
                    .map_err(|_| wrong_value_for_var(canon, &v.to_display_string()))
            },
        ));
    }
}
