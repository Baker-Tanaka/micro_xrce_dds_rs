//! `geometry_msgs` message types — currently just `Vector3` and `Twist` for
//! the canonical `/cmd_vel` use case.

use crate::{cdr::CdrWriter, cdr_reader::CdrReader, error::Error, message::Message};

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Vector3 {
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

impl Vector3 {
    fn serialize_inline(&self, w: &mut CdrWriter) {
        w.f64_val(self.x);
        w.f64_val(self.y);
        w.f64_val(self.z);
    }

    fn deserialize_inline(r: &mut CdrReader<'_>) -> Result<Self, Error> {
        Ok(Self {
            x: r.f64_val()?,
            y: r.f64_val()?,
            z: r.f64_val()?,
        })
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Twist {
    pub linear: Vector3,
    pub angular: Vector3,
}

impl Message for Twist {
    const TYPE_NAME: &'static str = "geometry_msgs::msg::dds_::Twist_";
    // 6 × f64 = 48 bytes (8-aligned).
    const MAX_SERIALIZED_SIZE: usize = 48;

    fn serialize(&self, w: &mut CdrWriter) {
        self.linear.serialize_inline(w);
        self.angular.serialize_inline(w);
    }

    fn deserialize(r: &mut CdrReader<'_>) -> Result<Self, Error> {
        let linear = Vector3::deserialize_inline(r)?;
        let angular = Vector3::deserialize_inline(r)?;
        Ok(Twist { linear, angular })
    }
}
