//! MySQL 8.0.46 spatial-reference-system registry.
//!
//! The seed is a black-box export of
//! `information_schema.ST_SPATIAL_REFERENCE_SYSTEMS` from an isolated
//! MySQL 8.0.46 instance. It contains metadata only (EPSG WKT definitions),
//! not MySQL source or test code.

use std::io::Read;
use std::sync::OnceLock;

use flate2::read::GzDecoder;

use crate::engine::executor::mem_budget::{ControlledMemoryCategory, ControlledMemoryCharge};
use crate::types::QueryError;

const MYSQL80_SRS_SEED: &[u8] =
    include_bytes!("../../../assets/mysql80/spatial_reference_systems.tsv.gz");
const MYSQL80_SRS_COUNT: usize = 5_152;
// The gzip expands to 4,669,205 bytes. Reserve conservatively for the decoded
// text, owned strings, Vec capacity and parsed registry objects. This charge
// lives with the OnceLock registry until process exit.
const SRS_REGISTRY_RESERVATION_BYTES: usize = 12 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SrsKind {
    Cartesian,
    Geographic,
    Projected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AxisOrder {
    Xy,
    LatLong,
    LongLat,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct AngularUnit {
    pub(crate) radians_per_unit: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct LinearUnit {
    pub(crate) metres_per_unit: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct UnitDefinition {
    pub(crate) name: &'static str,
    pub(crate) unit_type: &'static str,
    pub(crate) conversion_factor: f64,
    pub(crate) description: Option<&'static str>,
}

const MYSQL80_UNITS: &[UnitDefinition] = &[
    UnitDefinition {
        name: "British chain (Benoit 1895 A)",
        unit_type: "linear",
        conversion_factor: 20.1167824,
        description: None,
    },
    UnitDefinition {
        name: "British chain (Benoit 1895 B)",
        unit_type: "linear",
        conversion_factor: 20.116782494375872,
        description: None,
    },
    UnitDefinition {
        name: "British chain (Sears 1922 truncated)",
        unit_type: "linear",
        conversion_factor: 20.116756,
        description: None,
    },
    UnitDefinition {
        name: "British chain (Sears 1922)",
        unit_type: "linear",
        conversion_factor: 20.116765121552632,
        description: None,
    },
    UnitDefinition {
        name: "British foot (1865)",
        unit_type: "linear",
        conversion_factor: 0.30480083333333335,
        description: None,
    },
    UnitDefinition {
        name: "British foot (1936)",
        unit_type: "linear",
        conversion_factor: 0.3048007491,
        description: None,
    },
    UnitDefinition {
        name: "British foot (Benoit 1895 A)",
        unit_type: "linear",
        conversion_factor: 0.3047997333333333,
        description: None,
    },
    UnitDefinition {
        name: "British foot (Benoit 1895 B)",
        unit_type: "linear",
        conversion_factor: 0.30479973476327077,
        description: None,
    },
    UnitDefinition {
        name: "British foot (Sears 1922 truncated)",
        unit_type: "linear",
        conversion_factor: 0.30479933333333337,
        description: None,
    },
    UnitDefinition {
        name: "British foot (Sears 1922)",
        unit_type: "linear",
        conversion_factor: 0.3047994715386762,
        description: None,
    },
    UnitDefinition {
        name: "British link (Benoit 1895 A)",
        unit_type: "linear",
        conversion_factor: 0.201167824,
        description: None,
    },
    UnitDefinition {
        name: "British link (Benoit 1895 B)",
        unit_type: "linear",
        conversion_factor: 0.2011678249437587,
        description: None,
    },
    UnitDefinition {
        name: "British link (Sears 1922 truncated)",
        unit_type: "linear",
        conversion_factor: 0.20116756,
        description: None,
    },
    UnitDefinition {
        name: "British link (Sears 1922)",
        unit_type: "linear",
        conversion_factor: 0.2011676512155263,
        description: None,
    },
    UnitDefinition {
        name: "British yard (Benoit 1895 A)",
        unit_type: "linear",
        conversion_factor: 0.9143992,
        description: None,
    },
    UnitDefinition {
        name: "British yard (Benoit 1895 B)",
        unit_type: "linear",
        conversion_factor: 0.9143992042898124,
        description: None,
    },
    UnitDefinition {
        name: "British yard (Sears 1922 truncated)",
        unit_type: "linear",
        conversion_factor: 0.914398,
        description: None,
    },
    UnitDefinition {
        name: "British yard (Sears 1922)",
        unit_type: "linear",
        conversion_factor: 0.9143984146160288,
        description: None,
    },
    UnitDefinition {
        name: "centimetre",
        unit_type: "linear",
        conversion_factor: 0.01,
        description: None,
    },
    UnitDefinition {
        name: "chain",
        unit_type: "linear",
        conversion_factor: 20.1168,
        description: None,
    },
    UnitDefinition {
        name: "Clarke's chain",
        unit_type: "linear",
        conversion_factor: 20.1166195164,
        description: None,
    },
    UnitDefinition {
        name: "Clarke's foot",
        unit_type: "linear",
        conversion_factor: 0.3047972654,
        description: None,
    },
    UnitDefinition {
        name: "Clarke's link",
        unit_type: "linear",
        conversion_factor: 0.201166195164,
        description: None,
    },
    UnitDefinition {
        name: "Clarke's yard",
        unit_type: "linear",
        conversion_factor: 0.9143917962,
        description: None,
    },
    UnitDefinition {
        name: "fathom",
        unit_type: "linear",
        conversion_factor: 1.8288,
        description: None,
    },
    UnitDefinition {
        name: "foot",
        unit_type: "linear",
        conversion_factor: 0.3048,
        description: None,
    },
    UnitDefinition {
        name: "German legal metre",
        unit_type: "linear",
        conversion_factor: 1.0000135965,
        description: None,
    },
    UnitDefinition {
        name: "Gold Coast foot",
        unit_type: "linear",
        conversion_factor: 0.3047997101815088,
        description: None,
    },
    UnitDefinition {
        name: "Indian foot",
        unit_type: "linear",
        conversion_factor: 0.30479951024814694,
        description: None,
    },
    UnitDefinition {
        name: "Indian foot (1937)",
        unit_type: "linear",
        conversion_factor: 0.30479841,
        description: None,
    },
    UnitDefinition {
        name: "Indian foot (1962)",
        unit_type: "linear",
        conversion_factor: 0.3047996,
        description: None,
    },
    UnitDefinition {
        name: "Indian foot (1975)",
        unit_type: "linear",
        conversion_factor: 0.3047995,
        description: None,
    },
    UnitDefinition {
        name: "Indian yard",
        unit_type: "linear",
        conversion_factor: 0.9143985307444408,
        description: None,
    },
    UnitDefinition {
        name: "Indian yard (1937)",
        unit_type: "linear",
        conversion_factor: 0.91439523,
        description: None,
    },
    UnitDefinition {
        name: "Indian yard (1962)",
        unit_type: "linear",
        conversion_factor: 0.9143988,
        description: None,
    },
    UnitDefinition {
        name: "Indian yard (1975)",
        unit_type: "linear",
        conversion_factor: 0.9143985,
        description: None,
    },
    UnitDefinition {
        name: "kilometre",
        unit_type: "linear",
        conversion_factor: 1000.0,
        description: None,
    },
    UnitDefinition {
        name: "link",
        unit_type: "linear",
        conversion_factor: 0.201168,
        description: None,
    },
    UnitDefinition {
        name: "metre",
        unit_type: "linear",
        conversion_factor: 1.0,
        description: None,
    },
    UnitDefinition {
        name: "millimetre",
        unit_type: "linear",
        conversion_factor: 0.001,
        description: None,
    },
    UnitDefinition {
        name: "nautical mile",
        unit_type: "linear",
        conversion_factor: 1852.0,
        description: None,
    },
    UnitDefinition {
        name: "Statute mile",
        unit_type: "linear",
        conversion_factor: 1609.344,
        description: None,
    },
    UnitDefinition {
        name: "US survey chain",
        unit_type: "linear",
        conversion_factor: 20.11684023368047,
        description: None,
    },
    UnitDefinition {
        name: "US survey foot",
        unit_type: "linear",
        conversion_factor: 0.30480060960121924,
        description: None,
    },
    UnitDefinition {
        name: "US survey link",
        unit_type: "linear",
        conversion_factor: 0.2011684023368047,
        description: None,
    },
    UnitDefinition {
        name: "US survey mile",
        unit_type: "linear",
        conversion_factor: 1609.3472186944375,
        description: None,
    },
    UnitDefinition {
        name: "yard",
        unit_type: "linear",
        conversion_factor: 0.9144,
        description: None,
    },
];

pub(crate) fn units() -> &'static [UnitDefinition] {
    MYSQL80_UNITS
}

pub(crate) fn find_unit(name: &str) -> Option<&'static UnitDefinition> {
    MYSQL80_UNITS
        .iter()
        .find(|unit| unit.name.eq_ignore_ascii_case(name))
}

#[derive(Debug, Clone)]
pub(crate) struct SrsDefinition {
    pub(crate) id: u32,
    pub(crate) name: String,
    pub(crate) organization: Option<String>,
    pub(crate) organization_coordsys_id: Option<u32>,
    pub(crate) definition: String,
    pub(crate) description: Option<String>,
    pub(crate) kind: SrsKind,
    pub(crate) axis_order: AxisOrder,
    pub(crate) angular_unit: Option<AngularUnit>,
    pub(crate) linear_unit: Option<LinearUnit>,
}

struct SrsRegistry {
    definitions: Vec<SrsDefinition>,
    _charge: ControlledMemoryCharge,
}

static REGISTRY: OnceLock<Result<SrsRegistry, String>> = OnceLock::new();

pub(crate) fn all() -> Result<&'static [SrsDefinition], QueryError> {
    registry().map(|registry| registry.definitions.as_slice())
}

pub(crate) fn find(id: u32) -> Result<Option<&'static SrsDefinition>, QueryError> {
    let definitions = all()?;
    Ok(definitions
        .binary_search_by_key(&id, |definition| definition.id)
        .ok()
        .map(|index| &definitions[index]))
}

pub(crate) fn require(id: u32, function: &str) -> Result<&'static SrsDefinition, QueryError> {
    find(id)?.ok_or_else(|| {
        crate::engine::geo::geo_err(
            crate::error::catalog::ER_SRS_NOT_FOUND,
            format!("There is no spatial reference system with SRID {id} for function {function}."),
        )
    })
}

fn registry() -> Result<&'static SrsRegistry, QueryError> {
    match REGISTRY.get_or_init(load_registry) {
        Ok(registry) => Ok(registry),
        Err(message) => Err(crate::storage::error(
            crate::error::catalog::ER_OUTOFMEMORY,
            message.clone(),
        )),
    }
}

fn load_registry() -> Result<SrsRegistry, String> {
    let charge = ControlledMemoryCharge::try_new(
        ControlledMemoryCategory::DerivedCache,
        SRS_REGISTRY_RESERVATION_BYTES,
    )
    .map_err(|error| error.message().to_string())?;

    let mut decoded = String::with_capacity(4_669_205);
    GzDecoder::new(MYSQL80_SRS_SEED)
        .read_to_string(&mut decoded)
        .map_err(|error| format!("failed to decode MySQL 8.0.46 SRS seed: {error}"))?;
    let mut definitions = Vec::with_capacity(MYSQL80_SRS_COUNT);
    for (line_number, line) in decoded.lines().enumerate() {
        let mut fields = line.split('\t');
        let id = fields
            .next()
            .and_then(|value| value.parse::<u32>().ok())
            .ok_or_else(|| format!("invalid SRS id at seed line {}", line_number + 1))?;
        let name = fields.next().unwrap_or_default().to_string();
        let organization = nonempty(fields.next().unwrap_or_default());
        let organization_coordsys_id = fields
            .next()
            .filter(|value| !value.is_empty())
            .map(str::parse::<u32>)
            .transpose()
            .map_err(|_| {
                format!(
                    "invalid organization coordinate-system id at seed line {}",
                    line_number + 1
                )
            })?;
        let definition = fields.next().unwrap_or_default().to_string();
        let description = nonempty(fields.next().unwrap_or_default());
        if fields.next().is_some() {
            return Err(format!(
                "too many SRS fields at seed line {}",
                line_number + 1
            ));
        }
        let kind = if id == 0 {
            SrsKind::Cartesian
        } else if definition.starts_with("GEOGCS[") {
            SrsKind::Geographic
        } else {
            SrsKind::Projected
        };
        let axis_order = infer_axis_order(kind, &definition);
        definitions.push(SrsDefinition {
            id,
            name,
            organization,
            organization_coordsys_id,
            angular_unit: angular_unit(&definition),
            linear_unit: linear_unit(kind, &definition),
            definition,
            description,
            kind,
            axis_order,
        });
    }
    if definitions.len() != MYSQL80_SRS_COUNT {
        return Err(format!(
            "MySQL 8.0.46 SRS seed contains {} rows, expected {MYSQL80_SRS_COUNT}",
            definitions.len()
        ));
    }
    definitions.sort_unstable_by_key(|definition| definition.id);
    Ok(SrsRegistry {
        definitions,
        _charge: charge,
    })
}

fn nonempty(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_string())
}

fn infer_axis_order(kind: SrsKind, definition: &str) -> AxisOrder {
    if kind != SrsKind::Geographic {
        return AxisOrder::Xy;
    }
    let upper = definition.to_ascii_uppercase();
    let lat = upper.find("AXIS[\"LAT");
    let lon = upper.find("AXIS[\"LON");
    match (lat, lon) {
        (Some(lat), Some(lon)) if lat < lon => AxisOrder::LatLong,
        (Some(_), Some(_)) => AxisOrder::LongLat,
        _ => AxisOrder::LatLong,
    }
}

fn angular_unit(definition: &str) -> Option<AngularUnit> {
    unit_factor_after(definition, "UNIT[\"degree\",")
        .map(|radians_per_unit| AngularUnit { radians_per_unit })
}

fn linear_unit(kind: SrsKind, definition: &str) -> Option<LinearUnit> {
    if kind != SrsKind::Projected {
        return None;
    }
    definition
        .rfind("UNIT[")
        .and_then(|start| definition[start..].split_once(','))
        .and_then(|(_, tail)| {
            tail.split([',', ']'])
                .next()
                .and_then(|value| value.parse::<f64>().ok())
        })
        .map(|metres_per_unit| LinearUnit { metres_per_unit })
}

fn unit_factor_after(definition: &str, marker: &str) -> Option<f64> {
    let start = definition.find(marker)? + marker.len();
    definition[start..].split([',', ']']).next()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mysql8046_seed_has_expected_cardinality_and_landmarks() {
        let definitions = all().unwrap();
        assert_eq!(definitions.len(), 5_152);
        assert_eq!(find(0).unwrap().unwrap().kind, SrsKind::Cartesian);
        let wgs84 = find(4326).unwrap().unwrap();
        assert_eq!(wgs84.name, "WGS 84");
        assert_eq!(wgs84.kind, SrsKind::Geographic);
        assert_eq!(wgs84.axis_order, AxisOrder::LatLong);
        assert_eq!(find(3857).unwrap().unwrap().kind, SrsKind::Projected);
    }
}
