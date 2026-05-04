use crate::cdr::CdrWriter;
use super::std_msgs::write_header_zero;

// All serializers emit only CDR field bytes — Fast-DDS adds the 4-byte
// encapsulation header. See `std_msgs.rs` for the rationale.

// ── sensor_msgs/Range ─────────────────────────────────────────────────────────

pub const RANGE_TYPE: &str = "sensor_msgs::msg::dds_::Range_";

/// radiation_type constants (sensor_msgs/Range.msg)
pub const RANGE_ULTRASOUND: u8 = 0;
pub const RANGE_INFRARED: u8 = 1;

/// CDR-serialize a sensor_msgs/Range message body with zero-timestamp header.
pub fn serialize_range<'a>(
    buf: &'a mut [u8],
    radiation_type: u8,
    field_of_view: f32,
    min_range: f32,
    max_range: f32,
    range: f32,
    variance: f32,
) -> &'a [u8] {
    let mut w = CdrWriter::new(buf);
    write_header_zero(&mut w);
    w.u8_val(radiation_type);
    // Next field (f32) needs 4-byte alignment; u8 leaves pos%4 == 1 → 3 pad bytes.
    w.f32_val(field_of_view);
    w.f32_val(min_range);
    w.f32_val(max_range);
    w.f32_val(range);
    w.f32_val(variance);
    w.finish()
}

// ── sensor_msgs/Imu ──────────────────────────────────────────────────────────

pub const IMU_TYPE: &str = "sensor_msgs::msg::dds_::Imu_";

/// CDR-serialize a sensor_msgs/Imu message (zero-timestamp header).
///
/// CDR layout (≈ 320 bytes):
///   header (16 bytes)
///   orientation [f64; 4]               (7-byte pad to 8-align after header) + 32 bytes
///   orientation_covariance [f64; 9]    72 bytes
///   angular_velocity [f64; 3]          24 bytes
///   angular_velocity_covariance [f64;9] 72 bytes
///   linear_acceleration [f64; 3]       24 bytes
///   linear_acceleration_covariance [f64;9] 72 bytes
#[allow(clippy::too_many_arguments)]
pub fn serialize_imu<'a>(
    buf: &'a mut [u8],
    orientation: &[f64; 4],
    orientation_covariance: &[f64; 9],
    angular_velocity: &[f64; 3],
    angular_velocity_covariance: &[f64; 9],
    linear_acceleration: &[f64; 3],
    linear_acceleration_covariance: &[f64; 9],
) -> &'a [u8] {
    let mut w = CdrWriter::new(buf);
    write_header_zero(&mut w);
    // pos so far = 8 (stamp) + 5 (empty string) = 13
    // f64 needs 8-byte alignment → align(8) inserts 3 bytes of padding → pos = 16
    w.f64_array(orientation);
    w.f64_array(orientation_covariance);
    w.f64_slice(angular_velocity);
    w.f64_array(angular_velocity_covariance);
    w.f64_slice(linear_acceleration);
    w.f64_array(linear_acceleration_covariance);
    w.finish()
}
