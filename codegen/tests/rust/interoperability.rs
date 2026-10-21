use micro_xrce_dds_rs::{cdr::CdrWriter, cdr_reader::CdrReader, Message, Timestamp};
use std::path::Path;

fn header(text: &str) -> std_msgs::msg::Header {
    let mut value = std_msgs::msg::Header::default();
    value.set_stamp(Timestamp { sec: -123, nanosec: 987654321 });
    value.frame_id.push_str(text).unwrap();
    value
}

fn vector(base: f64) -> geometry_msgs::msg::Vector3 {
    geometry_msgs::msg::Vector3 { x: base + 0.125, y: base - 0.25, z: -base - 0.5 }
}

fn quaternion() -> geometry_msgs::msg::Quaternion {
    geometry_msgs::msg::Quaternion { x: 0.125, y: -0.25, z: 0.5, w: -0.875 }
}

fn twist() -> geometry_msgs::msg::Twist {
    geometry_msgs::msg::Twist { linear: vector(1.0), angular: vector(-2.0) }
}

fn exercise<T: Message + PartialEq + core::fmt::Debug>(root: &Path, name: &str, expected: T) {
    let official = std::fs::read(root.join(format!("{name}.ros"))).unwrap();
    let mut reader = CdrReader::from_body(&official);
    assert_eq!(T::deserialize(&mut reader).unwrap(), expected, "ROS -> Rust {name}");
    assert_eq!(reader.remaining(), 0, "unconsumed official bytes: {name}");
    let mut buffer = vec![0; T::MAX_SERIALIZED_SIZE];
    let mut writer = CdrWriter::new(&mut buffer);
    expected.serialize(&mut writer);
    let rust = writer.finish_checked().unwrap();
    let canonical = std::fs::read(root.join(format!("{name}.canonical"))).unwrap();
    assert_eq!(rust, canonical, "Rust/official CDR body mismatch: {name}");
    std::fs::write(root.join(format!("{name}.rust")), rust).unwrap();
}

#[test]
fn official_jazzy_cdr_in_both_directions() {
    let root = std::env::var_os("MICROXRCE_INTEROP_FIXTURES").expect("run interoperability.py");
    let root = Path::new(&root);
    exercise(root, "twist", twist());
    for length in [0, 1, 2, 3, 4, 7, 8, 15, 31, 32] {
        let text = "x".repeat(length);
        let header = header(&text);
        exercise(root, &format!("header_{length}"), header.clone());
        let imu = sensor_msgs::msg::Imu {
            header: header.clone(),
            orientation: quaternion(),
            orientation_covariance: core::array::from_fn(|i| i as f64 + 0.125),
            angular_velocity: vector(3.0),
            angular_velocity_covariance: core::array::from_fn(|i| -(i as f64) - 0.25),
            linear_acceleration: vector(-4.0),
            linear_acceleration_covariance: core::array::from_fn(|i| i as f64 * 0.5),
        };
        exercise(root, &format!("imu_{length}"), imu);
        let range = sensor_msgs::msg::Range {
            header: header.clone(),
            radiation_type: sensor_msgs::msg::Range::INFRARED,
            field_of_view: 0.125, min_range: -0.25, max_range: 16.5, range: 2.75, variance: 0.0625,
        };
        exercise(root, &format!("range_{length}"), range);
        // Unequal consecutive string lengths force distinct alignment residues.
        let child: heapless::String<32> = "y".repeat(32 - length).as_str().try_into().unwrap();
        let odom = nav_msgs::msg::Odometry {
            header: header.clone(), child_frame_id: child.clone(),
            pose: geometry_msgs::msg::PoseWithCovariance {
                pose: geometry_msgs::msg::Pose {
                    position: geometry_msgs::msg::Point { x: -1.125, y: 2.25, z: -3.5 },
                    orientation: quaternion(),
                },
                covariance: core::array::from_fn(|i| i as f64 * 0.125 - 2.0),
            },
            twist: geometry_msgs::msg::TwistWithCovariance {
                twist: twist(),
                covariance: core::array::from_fn(|i| 4.0 - i as f64 * 0.25),
            },
        };
        exercise(root, &format!("odometry_{length}"), odom);
        let mut tf = tf2_msgs::msg::TFMessage::default();
        tf.transforms.push(geometry_msgs::msg::TransformStamped {
            header, child_frame_id: child,
            transform: geometry_msgs::msg::Transform {
                translation: vector(5.0), rotation: quaternion(),
            },
        }).unwrap();
        exercise(root, &format!("tf_{length}"), tf);
    }
    exercise(root, "tf_empty", tf2_msgs::msg::TFMessage::default());
}
