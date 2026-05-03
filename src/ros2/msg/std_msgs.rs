use crate::cdr::CdrWriter;

// ── std_msgs/Float32 ─────────────────────────────────────────────────────────

pub const FLOAT32_TYPE: &str = "std_msgs::msg::dds_::Float32_";

/// CDR-serialize a std_msgs/Float32 message.
/// Buffer must be ≥ 8 bytes. Returns the filled slice.
pub fn serialize_float32(buf: &mut [u8], data: f32) -> &[u8] {
    let mut w = CdrWriter::new(buf);
    w.header();   // 4 bytes
    w.f32_val(data); // 4 bytes
    w.finish()
}

// ── std_msgs/String ──────────────────────────────────────────────────────────

pub const STRING_TYPE: &str = "std_msgs::msg::dds_::String_";

/// CDR-serialize a std_msgs/String message.
/// Buffer must be ≥ 4 + 4 + data.len() + 1 bytes (header + len + bytes + null).
pub fn serialize_string(buf: &mut [u8], data: &str) -> &[u8] {
    let mut w = CdrWriter::new(buf);
    w.header();
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
