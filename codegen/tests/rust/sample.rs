use micro_xrce_dds_rs::{cdr::CdrWriter, cdr_reader::CdrReader, Error, Message};
use sample_msgs::msg::{Empty, Nested, Sample};

#[test]
fn ros_defaults_constants_and_large_array_traits() {
    let value = Sample::default();
    assert!(value.enabled);
    assert_eq!(value.signed_byte, -8);
    assert_eq!(value.small, -123);
    assert_eq!(value.count, 456);
    assert_eq!(value.large, -123456789);
    assert_eq!(value.unsigned_large, 123456789);
    assert_eq!(value.ratio, 1.5);
    assert_eq!(value.special, f64::INFINITY);
    assert_eq!(value.label.as_str(), "hello");
    assert_eq!(value.fixed, [1, 2, 3]);
    assert_eq!(value.names[0].as_str(), "left");
    assert_eq!(value.names[1].as_str(), "right");
    assert_eq!(&*value.samples, &[1, 2]);
    assert_eq!(&*value.values, &[-1]);
    assert_eq!(value.labels[0].as_str(), "hi");
    assert!(Sample::ACTIVE);
    assert_eq!(Sample::QUOTE, "a\"b");
    assert_eq!(Sample::LOW, -128);
    assert_eq!(Sample::HIGH, 65535);
    assert_eq!(Sample::SCALE, 1.25);
    assert!(Sample::NOT_A_NUMBER.is_nan());
    assert_eq!(value.r#type, u32::MAX);
    assert_eq!(value.utf8.as_str(), "éé");
    assert_eq!(value.utf8.len(), 4);
    assert_eq!(value.covariance, [0.0; 36]);
    assert_eq!(value.clone(), value);
    assert!(!format!("{value:?}").is_empty());
}

#[test]
fn nested_message_arrays_and_bounded_sequences() {
    let mut value = Nested::default();
    value.bounded.push(Sample::default()).unwrap();
    value.bounded.push(Sample::default()).unwrap();
    assert!(value.bounded.push(Sample::default()).is_err());
    let mut buffer = vec![0; Nested::MAX_SERIALIZED_SIZE];
    let mut writer = CdrWriter::new(&mut buffer);
    value.serialize(&mut writer);
    let bytes = writer.finish_checked().unwrap();
    assert_eq!(Nested::deserialize(&mut CdrReader::from_body(bytes)), Ok(value));
}

#[test]
fn rosidl_empty_message_placeholder() {
    let mut buffer = [0; Empty::MAX_SERIALIZED_SIZE];
    let mut writer = CdrWriter::new(&mut buffer);
    Empty::default().serialize(&mut writer);
    let bytes = writer.finish_checked().unwrap();
    assert_eq!(bytes, &[0]);
    assert_eq!(Empty::deserialize(&mut CdrReader::from_body(bytes)), Ok(Empty::default()));
}

#[test]
fn fixture_cdr_roundtrip_and_short_buffer() {
    let mut value = Sample::default();
    // A declared sequence bound of 3 is retained despite the unbounded cap of 1.
    value.samples.push(3).unwrap();
    assert!(value.samples.push(4).is_err());
    assert!(value.values.push(0).is_err());
    assert!(value.label.push_str("longer").is_err());
    let mut buffer = vec![0; Sample::MAX_SERIALIZED_SIZE];
    let mut writer = CdrWriter::new(&mut buffer);
    value.serialize(&mut writer);
    let bytes = writer.finish_checked().unwrap();
    // bool + int8 + little-endian int16 + uint16.
    assert_eq!(&bytes[..6], &[1, 248, 133, 255, 200, 1]);
    assert_eq!(Sample::deserialize(&mut CdrReader::from_body(bytes)), Ok(value));
    for end in 0..bytes.len() {
        assert_eq!(
            Sample::deserialize(&mut CdrReader::from_body(&bytes[..end])),
            Err(Error::Deserialization),
            "truncation at {end}",
        );
    }
    let mut reader = CdrReader::from_body(&[2]);
    assert_eq!(Sample::deserialize(&mut reader), Err(Error::Deserialization));
    let mut small = [0; 8];
    let mut writer = CdrWriter::new(&mut small);
    Sample::default().serialize(&mut writer);
    assert_eq!(writer.finish_checked(), Err(Error::BufferTooSmall));
}
