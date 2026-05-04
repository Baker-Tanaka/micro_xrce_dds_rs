use crate::cdr::CdrWriter;

// All serializers in this module emit ONLY the CDR field bytes — the 4-byte
// CDR encapsulation header (`0x00 0x01 0x00 0x00` for CDR_LE) is prepended by
// the agent / Fast-DDS when forming the DDS SerializedPayload. Writing it here
// produces a doubled header on the wire and breaks deserialization.

// ── std_msgs/Float32 ─────────────────────────────────────────────────────────

pub const FLOAT32_TYPE: &str = "std_msgs::msg::dds_::Float32_";

/// CDR-serialize a std_msgs/Float32 message body (no encap header).
/// Buffer must be ≥ 4 bytes. Returns the filled slice.
pub fn serialize_float32<'a>(buf: &'a mut [u8], data: f32) -> &'a [u8] {
    let mut w = CdrWriter::new(buf);
    w.f32_val(data);
    w.finish()
}

// ── std_msgs/String ──────────────────────────────────────────────────────────

pub const STRING_TYPE: &str = "std_msgs::msg::dds_::String_";

/// CDR-serialize a std_msgs/String message body (no encap header).
/// Buffer must be ≥ 4 + data.len() + 1 bytes (length + bytes + null).
pub fn serialize_string<'a>(buf: &'a mut [u8], data: &str) -> &'a [u8] {
    let mut w = CdrWriter::new(buf);
    w.string(data);
    w.finish()
}

// ── builtin_interfaces/Time (used by message headers) ───────────────────────

/// Write a builtin_interfaces/Time (sec: u32, nanosec: u32) into `w`.
pub fn write_time(w: &mut CdrWriter, sec: u32, nanosec: u32) {
    w.u32_val(sec);
    w.u32_val(nanosec);
}

// ── std_msgs/Header (stamp + frame_id string) ────────────────────────────────

/// Write a std_msgs/Header with zero timestamp and an empty frame_id.
pub fn write_header_zero(w: &mut CdrWriter) {
    write_time(w, 0, 0); // stamp
    w.string("");          // frame_id = ""
}
