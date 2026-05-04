//! `std_msgs` message types.

use crate::{cdr::CdrWriter, cdr_reader::CdrReader, error::Error, message::Message};

const STRING_INLINE_MAX: usize = 64;

// ── std_msgs/Float32 ─────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Float32(pub f32);

impl Message for Float32 {
    const TYPE_NAME: &'static str = "std_msgs::msg::dds_::Float32_";
    const MAX_SERIALIZED_SIZE: usize = 4;

    fn serialize(&self, w: &mut CdrWriter) {
        w.f32_val(self.0);
    }

    fn deserialize(r: &mut CdrReader<'_>) -> Result<Self, Error> {
        Ok(Float32(r.f32_val()?))
    }
}

// ── std_msgs/Int32 ───────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Int32(pub i32);

impl Message for Int32 {
    const TYPE_NAME: &'static str = "std_msgs::msg::dds_::Int32_";
    const MAX_SERIALIZED_SIZE: usize = 4;

    fn serialize(&self, w: &mut CdrWriter) {
        w.i32_val(self.0);
    }

    fn deserialize(r: &mut CdrReader<'_>) -> Result<Self, Error> {
        Ok(Int32(r.i32_val()?))
    }
}

// ── std_msgs/String ──────────────────────────────────────────────────────────

/// Borrowed-form String for **publishing only** (`'a` ties to a slice on the
/// publisher's stack). For subscribing use [`StringOwned`].
#[derive(Clone, Copy, Debug)]
pub struct String<'a>(pub &'a str);

impl<'a> Message for String<'a> {
    const TYPE_NAME: &'static str = "std_msgs::msg::dds_::String_";
    const MAX_SERIALIZED_SIZE: usize = 4 + STRING_INLINE_MAX + 1;

    fn serialize(&self, w: &mut CdrWriter) {
        w.string(self.0);
    }

    fn deserialize(_r: &mut CdrReader<'_>) -> Result<Self, Error> {
        // Borrowed String can't be returned without lifetime gymnastics; use
        // `StringOwned` for subscribe.
        Err(Error::Deserialization)
    }
}

/// Owned form of `std_msgs/String` for **subscribing**, backed by a fixed-size
/// `heapless::String<N>` so it can sit in a [`crate::Subscription`] inbox.
#[derive(Clone, Debug)]
pub struct StringOwned<const N: usize = STRING_INLINE_MAX> {
    pub data: heapless::String<N>,
}

impl<const N: usize> Default for StringOwned<N> {
    fn default() -> Self {
        Self {
            data: heapless::String::new(),
        }
    }
}

impl<const N: usize> Message for StringOwned<N> {
    const TYPE_NAME: &'static str = "std_msgs::msg::dds_::String_";
    const MAX_SERIALIZED_SIZE: usize = 4 + N + 1;

    fn serialize(&self, w: &mut CdrWriter) {
        w.string(self.data.as_str());
    }

    fn deserialize(r: &mut CdrReader<'_>) -> Result<Self, Error> {
        let bytes = r.string_bytes()?;
        let s = core::str::from_utf8(bytes).map_err(|_| Error::Deserialization)?;
        let mut data: heapless::String<N> = heapless::String::new();
        data.push_str(s).map_err(|_| Error::Deserialization)?;
        Ok(StringOwned { data })
    }
}

// ── shared helpers ───────────────────────────────────────────────────────────

/// Write a `builtin_interfaces/Time { sec: i32, nanosec: u32 }` field.
pub fn write_time(w: &mut CdrWriter, sec: i32, nanosec: u32) {
    w.i32_val(sec);
    w.u32_val(nanosec);
}

/// Write a `std_msgs/Header { stamp, frame_id }` with zero timestamp and an
/// empty frame_id. Convenient default for embedded sensors that have no
/// synced clock.
pub fn write_header_zero(w: &mut CdrWriter) {
    write_time(w, 0, 0);
    w.string("");
}
