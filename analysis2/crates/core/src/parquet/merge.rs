//! Ordered tree-merge of ResidentStore shards.

use crate::entity::ResidentStore;
use crate::parquet::LoadOptions;
use crate::Analysis2Error;

pub fn merge_shards_ordered(
    mut shards: Vec<Result<ResidentStore, Analysis2Error>>,
    options: &LoadOptions,
) -> Result<ResidentStore, Analysis2Error> {
    match shards.len() {
        0 => Ok(ResidentStore::with_options(
            options.metadata_anchors,
            &options.evm_chains,
        )),
        1 => shards.pop().expect("one shard is present"),
        _ => {
            let right = shards.split_off(shards.len() / 2);
            let (left, right) = rayon::join(
                || merge_shards_ordered(shards, options),
                || merge_shards_ordered(right, options),
            );
            let mut left = left?;
            left.merge_shard(right?)?;
            Ok(left)
        }
    }
}
