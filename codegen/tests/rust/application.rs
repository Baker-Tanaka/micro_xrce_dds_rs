use micro_xrce_dds_rs::{cdr::CdrWriter, cdr_reader::CdrReader, Error, Message, Timestamp};
use sensor_msgs::msg::{Imu, JointState, Range};
use std_msgs::msg::{Float32, Header};

fn check_roundtrip<T: Message + core::fmt::Debug + PartialEq>(value: &T) {
    for offset in 0..8 {
        let mut buffer = vec![0; T::MAX_SERIALIZED_SIZE + offset];
        let mut writer = CdrWriter::new(&mut buffer);
        for _ in 0..offset {
            writer.u8_val(0);
        }
        value.serialize(&mut writer);
        let bytes = writer.finish_checked().unwrap();
        assert!(bytes.len() - offset <= T::MAX_SERIALIZED_SIZE);
        let mut reader = CdrReader::from_body(bytes);
        for _ in 0..offset {
            reader.u8_val().unwrap();
        }
        assert_eq!(T::deserialize(&mut reader).as_ref(), Ok(value));
        assert_eq!(reader.remaining(), 0);
        for end in offset..bytes.len() {
            let mut reader = CdrReader::from_body(&bytes[..end]);
            for _ in 0..offset {
                reader.u8_val().unwrap();
            }
            assert!(T::deserialize(&mut reader).is_err());
        }
    }
}

#[test]
fn all_application_messages_and_maximum_containers() {
    let mut header = Header::default();
    header.set_stamp(Timestamp { sec: -5, nanosec: 123456789 });
    assert_eq!(header.stamp.sec, -5);
    header.frame_id.push_str("0123456789abcdef0123456789abcdef").unwrap();
    assert!(header.frame_id.push('x').is_err());
    check_roundtrip(&header);
    check_roundtrip(&builtin_interfaces::msg::Duration::default());
    check_roundtrip(&Float32 { data: 1.5 });
    let imu = Imu { header: header.clone(), ..Default::default() };
    check_roundtrip(&imu);
    let range = Range { header: header.clone(), radiation_type: Range::INFRARED, ..Default::default() };
    check_roundtrip(&range);
    let mut joint = JointState { header: header.clone(), ..Default::default() };
    joint.name.push(header.frame_id.clone()).unwrap();
    joint.position.push(1.0).unwrap();
    joint.velocity.push(-2.0).unwrap();
    joint.effort.push(3.0).unwrap();
    assert!(joint.position.push(0.0).is_err());
    check_roundtrip(&joint);
    let odom = nav_msgs::msg::Odometry {
        header: header.clone(),
        child_frame_id: header.frame_id.clone(),
        ..Default::default()
    };
    check_roundtrip(&odom);
    let transform = geometry_msgs::msg::TransformStamped {
        header: header.clone(), child_frame_id: header.frame_id.clone(), ..Default::default()
    };
    let mut transforms = tf2_msgs::msg::TFMessage::default();
    transforms.transforms.push(transform).unwrap();
    check_roundtrip(&transforms);
    assert_eq!(Header::TYPE_NAME, "std_msgs::msg::dds_::Header_");
    assert_eq!(Range::ULTRASOUND, 0);
}

#[test]
fn float32_wire_golden() {
    let mut buffer = [0; 4];
    let mut writer = CdrWriter::new(&mut buffer);
    Float32 { data: 1.5 }.serialize(&mut writer);
    assert_eq!(writer.finish_checked().unwrap(), &[0, 0, 0xc0, 0x3f]);
}

#[test]
fn invalid_strings_and_oversized_sequences_are_rejected() {
    for (len, bytes) in [
        (0, &[][..]),                       // CDR strings must contain a terminator.
        (2, &[b'a', b'x'][..]),             // Not null-terminated.
        (2, &[0xff, 0][..]),                // Invalid UTF-8.
        (34, &[0; 34][..]),                 // Beyond local capacity.
        (u32::MAX, &[][..]),                // Must fail before iteration/allocation.
        (4, &[b'a', 0][..]),                // Truncated.
    ] {
        let mut buffer = [0; 64];
        let mut writer = CdrWriter::new(&mut buffer);
        writer.i32_val(0);
        writer.u32_val(0);
        writer.u32_val(len);
        writer.bytes_raw(bytes);
        let bytes = writer.finish_checked().unwrap();
        assert_eq!(Header::deserialize(&mut CdrReader::from_body(bytes)), Err(Error::Deserialization));
    }
    let mut buffer = [0; 64];
    let mut writer = CdrWriter::new(&mut buffer);
    Header::default().serialize(&mut writer);
    writer.u32_val(2); // JointState.name has local cap 1.
    let bytes = writer.finish_checked().unwrap();
    assert_eq!(
        JointState::deserialize(&mut CdrReader::from_body(bytes)),
        Err(Error::Deserialization),
    );
}
