//! A whole number with bounds that the type carries.
//!
//! The config parser builds each bounded value, so a value outside its
//! bounds never exists past the parse. A refused value names its bounds.

use std::num::NonZeroU32;

use serde::{de::Error as _, Deserialize, Deserializer, Serialize, Serializer};

/// A whole number from `MIN` to `MAX`, both included. Only
/// [`TryFrom<u32>`] and [`BoundedU32::from_constant`] build one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BoundedU32<const MIN: u32, const MAX: u32>(u32);

/// A number outside the bounds of its [`BoundedU32`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("{value} is outside {min}..={max}")]
pub struct OutOfBounds {
    pub value: u32,
    pub min: u32,
    pub max: u32,
}

impl<const MIN: u32, const MAX: u32> BoundedU32<MIN, MAX> {
    /// The smallest value, `MIN`.
    pub const SMALLEST: Self = Self::from_constant(MIN);
    /// The largest value, `MAX`.
    pub const LARGEST: Self = Self::from_constant(MAX);

    /// A constant value. Use it only in a `const` item: a value outside the
    /// bounds then fails the build. Called at run time, it panics instead.
    pub const fn from_constant(value: u32) -> Self {
        assert!(
            MIN <= value && value <= MAX,
            "a bounded constant is outside its bounds"
        );
        Self(value)
    }

    /// The number.
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl<const MIN: u32, const MAX: u32> TryFrom<u32> for BoundedU32<MIN, MAX> {
    type Error = OutOfBounds;

    fn try_from(value: u32) -> Result<Self, OutOfBounds> {
        match (MIN..=MAX).contains(&value) {
            true => Ok(Self(value)),
            false => Err(OutOfBounds {
                value,
                min: MIN,
                max: MAX,
            }),
        }
    }
}

impl<const MIN: u32, const MAX: u32> From<BoundedU32<MIN, MAX>> for u32 {
    fn from(bounded: BoundedU32<MIN, MAX>) -> Self {
        bounded.0
    }
}

/// A count whose smallest value is 1 is never 0.
impl<const MAX: u32> From<BoundedU32<1, MAX>> for NonZeroU32 {
    fn from(bounded: BoundedU32<1, MAX>) -> Self {
        // `bounded.0 >= 1`, so the subtraction cannot wrap.
        NonZeroU32::MIN.saturating_add(bounded.0 - 1)
    }
}

impl<const MIN: u32, const MAX: u32> Serialize for BoundedU32<MIN, MAX> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u32(self.0)
    }
}

impl<'de, const MIN: u32, const MAX: u32> Deserialize<'de> for BoundedU32<MIN, MAX> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u32::deserialize(deserializer)?;
        Self::try_from(value).map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type OneToThree = BoundedU32<1, 3>;

    #[test]
    fn a_value_inside_the_bounds_is_kept() {
        for value in 1..=3 {
            let bounded = OneToThree::try_from(value).expect("inside the bounds");
            assert_eq!(bounded.get(), value);
        }
    }

    #[test]
    fn a_value_outside_the_bounds_is_refused_with_them() {
        for value in [0, 4] {
            assert_eq!(
                OneToThree::try_from(value),
                Err(OutOfBounds {
                    value,
                    min: 1,
                    max: 3
                }),
            );
        }
    }

    #[test]
    fn the_parser_refuses_a_value_outside_the_bounds() {
        let error = serde_json::from_str::<OneToThree>("4").expect_err("4 is outside the bounds");
        assert!(error.to_string().contains("4 is outside 1..=3"), "{error}");
        assert!(serde_json::from_str::<OneToThree>("-1").is_err());
        assert_eq!(
            serde_json::from_str::<OneToThree>("2").expect("2 is inside the bounds"),
            OneToThree::from_constant(2),
        );
    }

    #[test]
    fn a_count_from_one_converts_to_a_non_zero_number() {
        assert_eq!(NonZeroU32::from(OneToThree::SMALLEST).get(), 1);
        assert_eq!(NonZeroU32::from(OneToThree::LARGEST).get(), 3);
    }
}
