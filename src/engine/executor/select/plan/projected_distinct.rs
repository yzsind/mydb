//! External fallback for materialized projected DISTINCT. Saved canonical
//! keys prevent projection expressions from being evaluated again on spill.
use super::*;
use crate::engine::executor::mem_budget::{self, ScopedCharge};
use crate::engine::executor::row_spool::{RowSpoolBatches, RowSpoolBuilder};
use crate::engine::executor::vec::{
    self,
    batch::Batch,
    schema::{BatchField, BatchSchema},
    stream::{BatchOperator, BatchProperties},
};

fn spool_key(builder: &mut RowSpoolBuilder, key: Vec<u8>, ordinal: u64) -> Result<(), QueryError> {
    builder.append(&[Value::Bytes(key), Value::UInt(ordinal)])
}

fn retain_ordinals(
    rows: &mut Vec<PlanRow>,
    mut next_ordinal: impl FnMut() -> Result<Option<u64>, QueryError>,
) -> Result<(), QueryError> {
    let mut next = next_ordinal()?;
    let mut ordinal = 0u64;
    let mut failure = None;
    rows.retain(|_| {
        let current = ordinal;
        ordinal += 1;
        if failure.is_some() || next != Some(current) {
            return false;
        }
        match next_ordinal() {
            Ok(value) if value.is_none_or(|value| value > current) => next = value,
            Ok(_) => {
                failure = Some(
                    crate::error::catalog::ER_UNKNOWN_ERROR
                        .error("invalid projected DISTINCT ordinal order"),
                )
            }
            Err(error) => failure = Some(error),
        }
        true
    });
    if let Some(error) = failure {
        return Err(error);
    }
    if next.is_some() {
        return Err(crate::error::catalog::ER_UNKNOWN_ERROR
            .error("projected DISTINCT ordinal out of bounds"));
    }
    Ok(())
}

pub(super) fn dedup(
    rows: &mut Vec<PlanRow>,
    projection: &[SelectItem],
    collations: &[Option<u16>],
    data_dir: &std::path::Path,
) -> Result<(), QueryError> {
    let mut seen: hashbrown::HashMap<Vec<u8>, u64, ahash::RandomState> =
        hashbrown::HashMap::with_hasher(ahash::RandomState::new());
    let mut charge = ScopedCharge::new(0);
    let mut spool = None;
    for (ordinal, row) in rows.iter().enumerate() {
        crate::engine::cancel::check_cancelled()?;
        let ordinal = u64::try_from(ordinal)
            .map_err(|_| mem_budget::allocation_error("DISTINCT ordinal overflow"))?;
        let mut key = Vec::new();
        for (value, collation) in projection
            .iter()
            .flat_map(|item| project_item(row, item))
            .zip(collations)
        {
            vec::key_encode::encode_with_collation(&value?, *collation, &mut key);
        }
        if let Some(spool) = spool.as_mut() {
            spool_key(spool, key, ordinal)?;
            continue;
        }
        if seen.contains_key(&key) {
            continue;
        }
        let bytes = key.capacity().saturating_add(64);
        if charge.add(bytes).is_ok() {
            seen.try_reserve(1)
                .map_err(|_| mem_budget::allocation_error("projected DISTINCT key map"))?;
            seen.insert(key, ordinal);
            continue;
        }
        let mut writer = RowSpoolBuilder::new(data_dir, 2)?;
        for (key, ordinal) in seen.drain() {
            spool_key(&mut writer, key, ordinal)?;
        }
        drop(std::mem::take(&mut seen));
        charge = charge.sibling();
        spool_key(&mut writer, key, ordinal)?;
        spool = Some(writer);
    }
    let Some(spool) = spool else {
        charge.add_force(seen.len().saturating_mul(std::mem::size_of::<u64>()))?;
        let mut ordinals =
            mem_budget::try_vec_with_capacity(seen.len(), "projected DISTINCT ordinals")?;
        ordinals.extend(seen.values().copied());
        drop(seen);
        ordinals.sort_unstable();
        let mut ordinals = ordinals.into_iter();
        return retain_ordinals(rows, || Ok(ordinals.next()));
    };
    let schema = Arc::new(BatchSchema::new(vec![
        BatchField {
            table: String::new(),
            alias: None,
            name: "canonical_key".into(),
            column_type: ColumnType::Blob,
            source: Default::default(),
        },
        BatchField {
            table: String::new(),
            alias: None,
            name: "ordinal".into(),
            column_type: ColumnType::LongLong,
            source: Default::default(),
        },
    ]));
    let spool = spool.finish()?;
    let mut input = RowSpoolBatches::new(&spool, schema.clone())?;
    let distinct =
        vec::distinct::run_distinct_stream_unordered(&mut input, &[0], &[0, 1], data_dir)?;
    drop(input);
    drop(spool);
    let mut sorted = vec::sort::ExternalSortBatchOperator::new(
        None,
        distinct,
        schema,
        BatchProperties::UNORDERED_SINGLE,
        vec![vec::sort::SortKeySource::Column(1)],
        vec![crate::engine::executor::sort::SortField {
            ascending: true,
            nulls_first: true,
            collation_id: None,
        }],
        data_dir,
    );
    let mut batch: Option<Batch> = None;
    let mut position = 0;
    retain_ordinals(rows, || loop {
        if let Some(current) = batch.as_ref().filter(|batch| position < batch.num_rows()) {
            let value = current.columns()[1].value_at(position);
            position += 1;
            return match value {
                Value::UInt(value) => Ok(Some(value)),
                _ => Err(crate::error::catalog::ER_UNKNOWN_ERROR
                    .error("invalid projected DISTINCT ordinal")),
            };
        }
        batch = sorted.next_batch()?;
        position = 0;
        if batch.is_none() {
            return Ok(None);
        }
    })
}
