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
    for (id, b) in blocks.iter().enumerate() {
        for &a in &b.members {
            atom_blocks[a as usize].push(id as u32)
        }
    }
    for (block_id, block) in blocks.iter().enumerate() {
        for i in 0..block.members.len() {
            for &right in &block.members[i + 1..] {
                let left = block.members[i];
                if owner(&blocks, &atom_blocks, sketches, left, right) == Some(block_id as u32)
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
