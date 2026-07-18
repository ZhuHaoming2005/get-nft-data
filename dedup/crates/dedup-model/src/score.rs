use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Q16_16(u32);

impl Q16_16 {
    pub const SCALE: u64 = 1 << 16;

    pub fn from_unit(value: f64) -> Option<Self> {
        if !value.is_finite() || !(0.0..=1.0).contains(&value) {
            return None;
        }
        let scaled = (value * Self::SCALE as f64).round();
        u32::try_from(scaled as u64).ok().map(Self)
    }

    pub const fn raw(self) -> u32 {
        self.0
    }

    pub fn as_f64(self) -> f64 {
        f64::from(self.0) / Self::SCALE as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_out_of_range_scores() {
        assert!(Q16_16::from_unit(-0.1).is_none());
        assert!(Q16_16::from_unit(1.1).is_none());
        assert!(Q16_16::from_unit(f64::NAN).is_none());
    }
}
