//! Ephemeral group-local BaseEquivalent routing for shared-token contexts.

use super::{simhash_band_value, AtomSketch, ANCHOR_COUNT, BANDS, JOINT_BAND_FAMILIES};
use std::collections::BTreeMap;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Kind {
    Joint,
    TemplateAnchor,
    ContentAnchor,
}
struct Block {
    kind: Kind,
    members: Vec<u32>,
}

pub(crate) struct LocalRoutingPlan {
    blocks: Vec<Block>,
    atom_blocks: Vec<Vec<u32>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LocalRoutingTile {
    block_id: u32,
    left_begin: u32,
    left_end: u32,
    right_begin: u32,
    right_end: u32,
}

pub(crate) struct LocalRoutingTiles<'a> {
    plan: &'a LocalRoutingPlan,
    tile_members: usize,
    block_id: usize,
    left_begin: usize,
    right_begin: usize,
}

impl LocalRoutingPlan {
    pub(crate) fn build(sketches: &[AtomSketch]) -> Self {
        let mut mapped = BTreeMap::<(Kind, u64), Vec<u32>>::new();
        for (i, s) in sketches.iter().enumerate() {
            if !s.has_content_terms {
                continue;
            }
            let i = i as u32;
            if s.has_template_terms {
                for family in 0..JOINT_BAND_FAMILIES {
                    let tb = family / BANDS;
                    let cb = family % BANDS;
                    let bucket = (u16::from(simhash_band_value(s.template_simhash, tb)) << 8)
                        | u16::from(simhash_band_value(s.content_simhash, cb));
                    mapped
                        .entry((Kind::Joint, ((family as u64) << 16) | u64::from(bucket)))
                        .or_default()
                        .push(i);
                }
                for &a in s.template_anchors.iter().take(ANCHOR_COUNT) {
                    mapped
                        .entry((Kind::TemplateAnchor, u64::from(a)))
                        .or_default()
                        .push(i);
                }
            }
            for &a in s.content_anchors.iter().take(ANCHOR_COUNT) {
                mapped
                    .entry((Kind::ContentAnchor, u64::from(a)))
                    .or_default()
                    .push(i);
            }
        }
        let blocks = mapped
            .into_iter()
            .map(|((kind, _), mut members)| {
                members.sort_unstable();
                members.dedup();
                Block { kind, members }
            })
            .collect::<Vec<_>>();
        let mut atom_blocks = vec![Vec::new(); sketches.len()];
        for (id, block) in blocks.iter().enumerate() {
            for &atom in &block.members {
                atom_blocks[atom as usize].push(id as u32);
            }
        }
        Self {
            blocks,
            atom_blocks,
        }
    }

    pub(crate) fn tiles(&self, tile_members: usize) -> LocalRoutingTiles<'_> {
        LocalRoutingTiles {
            plan: self,
            tile_members: tile_members.max(1),
            block_id: 0,
            left_begin: 0,
            right_begin: 0,
        }
    }

    pub(crate) fn visit_tile(
        &self,
        sketches: &[AtomSketch],
        tile: &LocalRoutingTile,
        mut visit: impl FnMut(u32, u32) -> bool,
    ) -> bool {
        let Some(block) = self.blocks.get(tile.block_id as usize) else {
            return false;
        };
        for left_index in tile.left_begin as usize..tile.left_end as usize {
            let right_begin = (tile.right_begin as usize).max(left_index.saturating_add(1));
            for right_index in right_begin..tile.right_end as usize {
                let left = block.members[left_index];
                let right = block.members[right_index];
                if owner(&self.blocks, &self.atom_blocks, sketches, left, right)
                    == Some(tile.block_id)
                    && !visit(left, right)
                {
                    return false;
                }
            }
        }
        true
    }
}

impl Iterator for LocalRoutingTiles<'_> {
    type Item = LocalRoutingTile;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let block = self.plan.blocks.get(self.block_id)?;
            if self.left_begin >= block.members.len() {
                self.block_id += 1;
                self.left_begin = 0;
                self.right_begin = 0;
                continue;
            }
            if self.right_begin >= block.members.len() {
                self.left_begin = self.left_begin.saturating_add(self.tile_members);
                self.right_begin = self.left_begin;
                continue;
            }
            let tile = LocalRoutingTile {
                block_id: self.block_id as u32,
                left_begin: self.left_begin as u32,
                left_end: self
                    .left_begin
                    .saturating_add(self.tile_members)
                    .min(block.members.len()) as u32,
                right_begin: self.right_begin as u32,
                right_end: self
                    .right_begin
                    .saturating_add(self.tile_members)
                    .min(block.members.len()) as u32,
            };
            self.right_begin = self.right_begin.saturating_add(self.tile_members);
            return Some(tile);
        }
    }
}

pub fn for_each_local_base_equivalent_pair(
    sketches: &[AtomSketch],
    mut visit: impl FnMut(u32, u32),
) {
    let _ = for_each_local_base_equivalent_pair_while(sketches, |left, right| {
        visit(left, right);
        true
    });
}

pub fn for_each_local_base_equivalent_pair_while(
    sketches: &[AtomSketch],
    mut visit: impl FnMut(u32, u32) -> bool,
) -> bool {
    let plan = LocalRoutingPlan::build(sketches);
    for (block_id, block) in plan.blocks.iter().enumerate() {
        for i in 0..block.members.len() {
            for &right in &block.members[i + 1..] {
                let left = block.members[i];
                if owner(&plan.blocks, &plan.atom_blocks, sketches, left, right)
                    == Some(block_id as u32)
                    && !visit(left, right)
                {
                    return false;
                }
            }
        }
    }
    true
}
fn owner(
    blocks: &[Block],
    memberships: &[Vec<u32>],
    sketches: &[AtomSketch],
    left: u32,
    right: u32,
) -> Option<u32> {
    let a = &memberships[left as usize];
    let b = &memberships[right as usize];
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                let id = a[i];
                if gate(blocks[id as usize].kind, sketches, left, right) {
                    return Some(id);
                }
                i += 1;
                j += 1
            }
        }
    }
    None
}
fn gate(kind: Kind, s: &[AtomSketch], a: u32, b: u32) -> bool {
    match kind {
        Kind::Joint => true,
        Kind::TemplateAnchor => dimension_recalls(&s[a as usize], &s[b as usize], false),
        Kind::ContentAnchor => dimension_recalls(&s[a as usize], &s[b as usize], true),
    }
}
fn dimension_recalls(a: &AtomSketch, b: &AtomSketch, template: bool) -> bool {
    let (aa, bb, ha, hb) = if template {
        (
            &a.template_anchors,
            &b.template_anchors,
            a.template_simhash,
            b.template_simhash,
        )
    } else {
        (
            &a.content_anchors,
            &b.content_anchors,
            a.content_simhash,
            b.content_simhash,
        )
    };
    if intersects(aa, bb) {
        return true;
    }
    (0..BANDS).any(|i| simhash_band_value(ha, i) == simhash_band_value(hb, i))
}
fn intersects(a: &[u32], b: &[u32]) -> bool {
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => return true,
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hot_sketch() -> AtomSketch {
        AtomSketch {
            template_simhash: 0,
            content_simhash: 0,
            template_anchors: Vec::new(),
            content_anchors: vec![7],
            has_template_terms: false,
            has_content_terms: true,
        }
    }

    #[test]
    fn tiled_local_routing_matches_serial_owner_pairs() {
        let sketches = vec![hot_sketch(); 33];
        let mut serial = Vec::new();
        for_each_local_base_equivalent_pair(&sketches, |left, right| {
            serial.push((left, right));
        });
        serial.sort_unstable();

        let plan = LocalRoutingPlan::build(&sketches);
        let tiles = plan.tiles(8).collect::<Vec<_>>();
        let mut tiled = Vec::new();
        for tile in &tiles {
            assert!(plan.visit_tile(&sketches, tile, |left, right| {
                tiled.push((left, right));
                true
            }));
        }
        tiled.sort_unstable();

        assert!(tiles.len() > 4);
        assert_eq!(tiled, serial);
    }
}
