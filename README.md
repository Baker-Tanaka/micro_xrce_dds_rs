# micro_xrce_dds_rs

Minimal Micro XRCE-DDS client for `no_std` embedded targets.

This crate implements a small XRCE-DDS session layer suitable for use with a micro-ROS Agent over TCP. It is designed for `Embassy`-style async embedded Rust environments and is used by the `tenshi-no-hana` project to publish ROS2-compatible messages from RP2040 firmware.

## Features

- `#![no_std]`
- async XRCE-DDS session over a framed TCP stream
- CREATE_CLIENT / STATUS_AGENT handshake
- DDS entity creation: Participant, Topic, Publisher, DataWriter
- BEST_EFFORT `WRITE_DATA` publishing
- CDR serialization helpers for ROS2 message types
- optional `defmt` formatting for `XrceError`

## Cargo

```toml
[dependencies]
micro-xrce-dds-rs = { path = "../external/micro_xrce_dds_rs" }

[features]
default = ["defmt"]
```

## Usage

This crate exposes a small async session API around a transport implementing `embedded_io_async::Read + embedded_io_async::Write`.

### Basic flow

1. Open a TCP connection to the micro-ROS Agent.
2. Create an `XrceSession` with `XrceSession::connect()`.
3. Create DDS entities in order:
   - `create_participant()`
   - `create_topic()`
   - `create_publisher()`
   - `create_datawriter()`
4. Publish CDR-serialized payloads with `write_data()`.

## ROS2 message serialization

The crate provides CDR helpers for common ROS2 message types in `ros2::msg::std_msgs`:

- `std_msgs::STRING_TYPE`
- `std_msgs::FLOAT32_TYPE`
- `std_msgs::serialize_string()`
- `std_msgs::serialize_float32()`

Note: the serializers emit only the CDR field payload. Do not prepend a CDR encapsulation header (`0x00 0x01 0x00 0x00`), because the micro-ROS Agent / Fast-DDS stack will add it internally.

## Notes

- `write_data()` is BEST_EFFORT and does not wait for a status reply.
- Entity creation methods wait for `STATUS` responses by request ID.
- `XrceError` can be formatted with `defmt` when the `defmt` feature is enabled.

