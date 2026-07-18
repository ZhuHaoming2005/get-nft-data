#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CandidateBounds {
    pub minimum_length: usize,
    pub maximum_length: usize,
    pub minimum_multiset_overlap: usize,
}

impl CandidateBounds {
    pub fn for_lengths(left: usize, right: usize) -> Self {
        Self::for_lengths_and_prefix(left, right, 4)
    }

    pub fn for_lengths_and_prefix(left: usize, right: usize, common_prefix: usize) -> Self {
        let minimum = left.min(right);
        let (numerator, denominator) = match common_prefix.min(4) {
            0 => (37_u128, 20_u128),
            1 => (11, 6),
            2 => (29, 16),
            3 => (25, 14),
            _ => (7, 4),
        };
        let minimum_multiset_overlap = numerator
            .saturating_mul(left as u128)
            .saturating_mul(right as u128)
            .div_ceil(denominator.saturating_mul((left + right) as u128));
        Self {
            minimum_length: left.saturating_mul(3).div_ceil(4),
            maximum_length: left.saturating_mul(4) / 3,
            minimum_multiset_overlap: usize::try_from(minimum_multiset_overlap)
                .unwrap_or(usize::MAX)
                .min(minimum),
        }
    }

    pub fn can_pair_lengths(left: usize, right: usize) -> bool {
        let minimum = left.min(right);
        let maximum = left.max(right);
        minimum.saturating_mul(4) >= maximum.saturating_mul(3)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn threshold_95_length_bound_is_three_quarters() {
        assert!(CandidateBounds::can_pair_lengths(75, 100));
        assert!(!CandidateBounds::can_pair_lengths(74, 100));
        assert_eq!(
            CandidateBounds::for_lengths(8, 8).minimum_multiset_overlap,
            7
        );
        assert_eq!(
            CandidateBounds::for_lengths_and_prefix(8, 8, 0).minimum_multiset_overlap,
            8
        );
    }
}
