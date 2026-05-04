//! `sensor_msgs` message types.

use super::std_msgs::write_header_zero;
use crate::{cdr::CdrWriter, cdr_reader::CdrReader, error::Error, message::Message};

// ── sensor_msgs/Range ────────────────────────────────────────────────────────

pub const RANGE_ULTRASOUND: u8 = 0;
pub const RANGE_INFRARED: u8 = 1;

/// `sensor_msgs/Range`. Header (stamp + frame_id) is implicitly zero/empty
/// — typical for embedded sensors without a synced clock.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Range {
    pub radiation_type: u8,
    pub field_of_view: f32,
    pub min_range: f32,
    pub max_range: f32,
    pub range: f32,
    pub variance: f32,
}

impl Message for Range {
    const TYPE_NAME: &'static str = "sensor_msgs::msg::dds_::Range_";
    // Header (stamp 8 + empty string 5) + radiation_type (1) + 3 pad + 5×f32 (20) ≈ 37
    const MAX_SERIALIZED_SIZE: usize = 48;

    fn serialize(&self, w: &mut CdrWriter) {
        write_header_zero(w);
        w.u8_val(self.radiation_type);
        // Next field (f32) needs 4-align; 3 pad bytes auto-inserted by f32_val.
        w.f32_val(self.field_of_view);
        w.f32_val(self.min_range);
        w.f32_val(self.max_range);
        w.f32_val(self.range);
        w.f32_val(self.variance);
    }

    fn deserialize(r: &mut CdrReader<'_>) -> Result<Self, Error> {
        // header
        let _sec = r.i32_val()?;
        let _ns = r.u32_val()?;
        let _frame = r.string_bytes()?;
        let radiation_type = r.u8_val()?;
        let field_of_view = r.f32_val()?;
        let min_range = r.f32_val()?;
        let max_range = r.f32_val()?;
        let range = r.f32_val()?;
        let variance = r.f32_val()?;
        Ok(Range {
            radiation_type,
            field_of_view,
            min_range,
            max_range,
            range,
            variance,
        })
    }
}

// ── sensor_msgs/Imu ──────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Imu {
    pub orientation: [f64; 4],
    pub orientation_covariance: [f64; 9],
    pub angular_velocity: [f64; 3],
    pub angular_velocity_covariance: [f64; 9],
    pub linear_acceleration: [f64; 3],
    pub linear_acceleration_covariance: [f64; 9],
}

impl Default for Imu {
    fn default() -> Self {
        Self {
            orientation: [0.0, 0.0, 0.0, 1.0],
            orientation_covariance: [0.0; 9],
            angular_velocity: [0.0; 3],
            angular_velocity_covariance: [0.0; 9],
            linear_acceleration: [0.0; 3],
            linear_acceleration_covariance: [0.0; 9],
        }
    }
}

impl Message for Imu {
    const TYPE_NAME: &'static str = "sensor_msgs::msg::dds_::Imu_";
    // Header (~16) + 8-align pad + 4 + 9 + 3 + 9 + 3 + 9 = 37 f64s = 296 bytes ≈ 320.
    const MAX_SERIALIZED_SIZE: usize = 320;

    fn serialize(&self, w: &mut CdrWriter) {
        write_header_zero(w);
        w.f64_array(&self.orientation);
        w.f64_array(&self.orientation_covariance);
        w.f64_slice(&self.angular_velocity);
        w.f64_array(&self.angular_velocity_covariance);
        w.f64_slice(&self.linear_acceleration);
        w.f64_array(&self.linear_acceleration_covariance);
    }

    fn deserialize(r: &mut CdrReader<'_>) -> Result<Self, Error> {
        let _sec = r.i32_val()?;
        let _ns = r.u32_val()?;
        let _frame = r.string_bytes()?;
        let orientation = r.f64_array::<4>()?;
        let orientation_covariance = r.f64_array::<9>()?;
        let angular_velocity = r.f64_array::<3>()?;
        let angular_velocity_covariance = r.f64_array::<9>()?;
        let linear_acceleration = r.f64_array::<3>()?;
        let linear_acceleration_covariance = r.f64_array::<9>()?;
        Ok(Imu {
            orientation,
            orientation_covariance,
            angular_velocity,
            angular_velocity_covariance,
            linear_acceleration,
            linear_acceleration_covariance,
        })
    }
}
