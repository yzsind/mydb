//! MySQL 8.0.46 Performance Schema sizing/configuration variables.
//!
//! The current collector uses the fixed 8.0.46 defaults. These variables are
//! startup/read-only, matching MySQL's runtime access policy; the generated
//! fixture is the single source for names and advertised values.

use super::super::backend::SysVar;
use super::super::meta::{DataType, Scope, SysVarDef};
use super::super::registry::Registry;
use super::super::value::SysVarValue;

pub fn register(reg: &mut Registry) {
    for &(name, value) in
        crate::engine::performance_schema_mysql80_generated::SYSTEM_VARIABLE_DEFAULTS
    {
        if name == "performance_schema" {
            continue;
        }
        if name == "performance_schema_show_processlist" {
            reg.register(SysVar::runtime_read_only(
                SysVarDef::new(name, Scope::Global, DataType::Bool),
                move || {
                    let default = i64::from(value.eq_ignore_ascii_case("ON"));
                    SysVarValue::Bool(crate::engine::perf_setup::configured_i64(name, default) != 0)
                },
            ));
            continue;
        }
        let parsed = value
            .parse::<i64>()
            .expect("generated Performance Schema size default is numeric");
        let unsigned = name.ends_with("_classes")
            || matches!(
                name,
                "performance_schema_max_digest_sample_age"
                    | "performance_schema_max_file_handles"
                    | "performance_schema_max_statement_stack"
            );
        let data_type = if unsigned {
            DataType::UInt {
                min: 0,
                max: i64::MAX as u64,
            }
        } else {
            DataType::Int {
                min: -1,
                max: i64::MAX,
            }
        };
        reg.register(SysVar::runtime_read_only(
            SysVarDef::new(name, Scope::Global, data_type),
            move || {
                let value = crate::engine::perf_setup::configured_i64(name, parsed);
                if unsigned {
                    SysVarValue::UInt(u64::try_from(value).unwrap_or(parsed as u64))
                } else {
                    SysVarValue::Int(value)
                }
            },
        ));
    }
}
