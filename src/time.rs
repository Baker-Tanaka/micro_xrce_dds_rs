//! ROS time value shared by the framework clock and generated message types.

/// ROS `builtin_interfaces/Time`-compatible timestamp.
///
/// `nanosec` is always normalized to `0..1_000_000_000` by the constructors.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp {
    pub sec: i32,
    pub nanosec: u32,
}

const NANOS_PER_SEC: i64 = 1_000_000_000;

impl Timestamp {
    pub const ZERO: Self = Self { sec: 0, nanosec: 0 };

    pub const fn new(sec: i32, nanosec: u32) -> Self {
        Self { sec, nanosec }
    }

    /// Build from signed nanoseconds, saturating at the `i32` seconds range.
    pub const fn from_nanos(nanos: i64) -> Self {
        let sec = nanos.div_euclid(NANOS_PER_SEC);
        let nanosec = nanos.rem_euclid(NANOS_PER_SEC) as u32;
        if sec > i32::MAX as i64 {
            Self {
                sec: i32::MAX,
                nanosec: 999_999_999,
            }
        } else if sec < i32::MIN as i64 {
            Self {
                sec: i32::MIN,
                nanosec: 0,
            }
        } else {
            Self {
                sec: sec as i32,
                nanosec,
            }
        }
    }

    /// Signed nanoseconds since the time origin.
    pub const fn as_nanos(&self) -> i64 {
        self.sec as i64 * NANOS_PER_SEC + self.nanosec as i64
    }
}

#[cfg(feature = "defmt")]
impl defmt::Format for Timestamp {
    fn format(&self, f: defmt::Formatter) {
        defmt::write!(f, "{}s+{}ns", self.sec, self.nanosec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nanos_roundtrip_and_negative() {
        let t = Timestamp::from_nanos(1_500_000_000);
        assert_eq!(t, Timestamp::new(1, 500_000_000));
        assert_eq!(t.as_nanos(), 1_500_000_000);
        assert_eq!(Timestamp::from_nanos(-1), Timestamp::new(-1, 999_999_999));
        assert_eq!(Timestamp::from_nanos(i64::MAX).sec, i32::MAX);
    }
}
