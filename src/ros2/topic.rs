/// Metadata describing a ROS2 topic as seen by the DDS / XRCE-DDS layer.
pub struct TopicDescriptor {
    /// DDS topic name.
    /// Convention: ROS2 topic `/foo/bar` → `"rt/foo/bar"` (non-keyed).
    pub dds_name: &'static str,
    /// DDS type name, e.g. `"std_msgs::msg::dds_::Float32_"`.
    pub type_name: &'static str,
    /// RIHS01 type hash (SHA-256-based).
    /// `None` until build.rs automatic computation is implemented.
    /// When available, agents can use this for strict type matching.
    pub type_hash: Option<[u8; 32]>,
}

impl TopicDescriptor {
    pub const fn new(dds_name: &'static str, type_name: &'static str) -> Self {
        Self { dds_name, type_name, type_hash: None }
    }

    pub const fn with_hash(mut self, hash: [u8; 32]) -> Self {
        self.type_hash = Some(hash);
        self
    }
}
