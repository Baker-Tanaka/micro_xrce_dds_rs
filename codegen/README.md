# Bounded Rust ROS messages

This generator follows the generator/runtime split described in
[ros2_rust message generation](https://github.com/ros2-rust/ros2_rust/blob/main/docs/message-generation.md).
The official `rosidl_adapter` converts `.msg` to IDL and the official
`rosidl_parser` provides the IDL AST. There is no independent `.msg` parser,
C ABI representation, C RMW dependency, or ROS dependency in firmware builds.
Runtime support comes from `micro_xrce_dds_rs`, not `rosidl_runtime_rs`.

## Regenerate this repository

With ROS Jazzy development tools and `rustfmt` installed:

```sh
bash external/micro_xrce_dds_rs/codegen/regenerate.sh
# CI/drift check (no crate files are changed):
bash external/micro_xrce_dds_rs/codegen/regenerate.sh --check
```

The script sources `/opt/ros/jazzy/setup.bash`, uses `/usr/bin/python3`, and
replaces `generated/<package>/{Cargo.toml,src/lib.rs}` deterministically.
Each source contains a SHA-256 provenance fingerprint of official adapter
IDL and the configured capacity policy. `--check` compares fully rendered,
formatted source and manifests, failing if inputs/output have drifted.
Check those files into the parent repository. The generator itself lives in
the backend submodule: its changes must be committed in that repository and
the parent gitlink updated separately to persist them across clones.

Outside this repository, source ROS and put `codegen/` on `PYTHONPATH`, then:

```sh
python3 -m rosidl_generator_microxrcedds.cli \
  --messages /path/to/msgs --output /path/to/generated \
  --runtime /path/to/micro_xrce_dds_rs \
  --string-capacity 32 --sequence-capacity 1
```

Only **unbounded** strings/sequences use these defaults. Explicit ROS bounds
are retained, not silently clamped. String capacities count UTF-8 **bytes**,
excluding the wire terminator. Defaults exceeding local capacities fail
generation. Capacity overrides change the Rust API and require regeneration.

## ament / colcon

The ament package registers `rosidl_generate_idl_interfaces`. Vendored message
packages call `rosidl_generate_interfaces`, so official adapter IDL is passed
to this extension. From the repository root, use an isolated generation build:

```sh
source /opt/ros/jazzy/setup.bash
colcon --log-base external/micro_xrce_dds_rs/codegen/log build \
  --base-paths external/micro_xrce_dds_rs/codegen msgs \
  --build-base external/micro_xrce_dds_rs/codegen/build \
  --install-base external/micro_xrce_dds_rs/codegen/install \
  --cmake-args -DPython3_EXECUTABLE=/usr/bin/python3 \
    -DPYTHON_EXECUTABLE=/usr/bin/python3 \
    -DMICROXRCE_GENERATED_ROOT="$PWD/generated" \
    -DMICROXRCE_RUNTIME_PATH="$PWD/external/micro_xrce_dds_rs"
```

If colcon detects the vendored package names in the Jazzy underlay, it may ask
for `--allow-overriding builtin_interfaces std_msgs geometry_msgs nav_msgs
sensor_msgs tf2_msgs`. These intentionally generate only the vendored subset.
This build does not generate/link C type support. `rustfmt` must be on PATH.

## Generated API and wire contract

Each package is an ordinary `#![no_std]` Cargo crate with `pkg::msg::Name`
structs deriving `Clone`, `Debug`, and `PartialEq`; manual `Default` handles
large arrays and ROS field defaults. Constants are associated constants.
Container fields are `heapless::String<N>` / `heapless::Vec<T, N>`. Nested
types use sibling Cargo path dependencies. `Header::set_stamp` accepts the
shared `micro_xrce_dds_rs::Timestamp`.

`Message::serialize` writes CDR little-endian **body** bytes without a DDS
encapsulation header. Call `CdrWriter::finish_checked()` to detect a short
output buffer; legacy unchecked `finish()` cannot detect truncation.
`Message::deserialize` consumes an already-positioned `CdrReader`
(`from_body` for body-only buffers; `new` for encapsulated buffers).
String lengths, terminators, UTF-8, sequence capacities, boolean encodings,
and truncated input are checked. `MAX_SERIALIZED_SIZE` is a deliberately
conservative bound that remains safe at arbitrary nested starting offsets;
it is not the exact size of the default instance.

Supported: ROS numeric primitives, booleans, strings, fixed arrays, bounded
and unbounded sequences, message references, constants and field defaults.
Unsupported types (including wide strings/characters, long double, and
non-message namespaces), `.srv`, and `.action` fail explicitly. Services and
actions are outside the current application generator scope.

## Validation

```sh
source /opt/ros/jazzy/setup.bash
PYTHONPATH=external/micro_xrce_dds_rs/codegen:$PYTHONPATH \
  /usr/bin/python3 -m unittest discover \
  -s external/micro_xrce_dds_rs/codegen/tests -v
for package in nav_msgs sensor_msgs tf2_msgs; do
  cargo check --manifest-path "generated/$package/Cargo.toml" \
    --target thumbv6m-none-eabi
done
```

Tests use official adapter/parser fixtures, generate a standalone host Cargo
test workspace, exercise wire bytes/failures and defaults, and verify that
the checked-in application output matches regeneration. Scratch work stays
inside `codegen/tests/.work/` and is removed when tests finish.

### Optional official Jazzy interoperability regression

This explicitly invoked test requires installed Jazzy `rclpy` and native
message type support; ordinary host tests do not import those bindings:

```sh
source /opt/ros/jazzy/setup.bash
/usr/bin/python3 external/micro_xrce_dds_rs/codegen/tests/interoperability.py
```

It compares generated Rust bodies byte-for-byte with official `rclpy`
serialization (stripping the four-byte little-endian XCDR1 encapsulation and
normalizing only unspecified CDR alignment padding using the **official**
message slot schema; installed Fast-CDR emits nonzero padding),
decodes official bytes in Rust, and decodes Rust bytes with `rclpy`, asserting
all populated fields independently in both languages. It covers Twist,
Header, Imu, Range (including Jazzy variance), Odometry, and TFMessage,
nonzero/negative stamps, covariance arrays, empty/nonempty TF sequences,
and variable string lengths 0/1/2/3/4/7/8/15/31/32 to exercise f64 alignment
after nested variable-length data. It does not require a ROS daemon or network.
The generated Cargo host harness uses `cargo test --target
x86_64-unknown-linux-gnu --test interoperability`; scratch files stay under
`tests/.interop-work/` and are removed afterward.
