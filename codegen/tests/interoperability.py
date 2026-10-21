"""Explicit integration test: installed Jazzy rclpy <-> generated Rust CDR.

Not named test_*.py: ordinary host/generator tests never import native ROS
bindings. Source /opt/ros/jazzy/setup.bash before invoking this script.
"""

import os
from pathlib import Path
import shutil
import subprocess


HERE = Path(__file__).resolve().parent
BACKEND = HERE.parents[1]
REPO = BACKEND.parents[1]


def main():
    try:
        from rclpy.serialization import deserialize_message, serialize_message
        from geometry_msgs.msg import Quaternion, TransformStamped, Twist, Vector3
        from nav_msgs.msg import Odometry
        from sensor_msgs.msg import Imu, Range
        from std_msgs.msg import Header
        from tf2_msgs.msg import TFMessage
        from rosidl_parser import definition as d
    except (ImportError, OSError) as exc:
        raise SystemExit(
            "Jazzy native serialization unavailable. Source /opt/ros/jazzy/setup.bash "
            f"and use /usr/bin/python3. Exact import failure: {exc}"
        ) from exc

    def header(text):
        value = Header()
        value.stamp.sec = -123
        value.stamp.nanosec = 987654321
        value.frame_id = text
        return value

    def vector(base):
        return Vector3(x=base + 0.125, y=base - 0.25, z=-base - 0.5)

    def quaternion():
        return Quaternion(x=0.125, y=-0.25, z=0.5, w=-0.875)

    def twist():
        return Twist(linear=vector(1.0), angular=vector(-2.0))

    messages = {"twist": twist(), "tf_empty": TFMessage()}
    for length in (0, 1, 2, 3, 4, 7, 8, 15, 31, 32):
        stamp = header("x" * length)
        messages[f"header_{length}"] = stamp
        imu = Imu(header=stamp, orientation=quaternion())
        imu.orientation_covariance = [i + 0.125 for i in range(9)]
        imu.angular_velocity = vector(3.0)
        imu.angular_velocity_covariance = [-i - 0.25 for i in range(9)]
        imu.linear_acceleration = vector(-4.0)
        imu.linear_acceleration_covariance = [i * 0.5 for i in range(9)]
        messages[f"imu_{length}"] = imu
        messages[f"range_{length}"] = Range(
            header=stamp, radiation_type=Range.INFRARED,
            field_of_view=0.125, min_range=-0.25, max_range=16.5, range=2.75, variance=0.0625,
        )
        child = "y" * (32 - length)
        odom = Odometry(header=stamp, child_frame_id=child)
        odom.pose.pose.position.x = -1.125
        odom.pose.pose.position.y = 2.25
        odom.pose.pose.position.z = -3.5
        odom.pose.pose.orientation = quaternion()
        odom.pose.covariance = [i * 0.125 - 2.0 for i in range(36)]
        odom.twist.twist = twist()
        odom.twist.covariance = [4.0 - i * 0.25 for i in range(36)]
        messages[f"odometry_{length}"] = odom
        transform = TransformStamped(header=stamp, child_frame_id=child)
        transform.transform.translation = vector(5.0)
        transform.transform.rotation = quaternion()
        messages[f"tf_{length}"] = TFMessage(transforms=[transform])

    work = HERE / ".interop-work"
    if work.exists():
        raise SystemExit(f"Refusing to overwrite existing test workspace: {work}")
    work.mkdir()
    try:
        def canonical_body(message, body):
            # Fast-CDR leaves padding bytes unspecified (observed nonzero after
            # Header.frame_id). Walk the OFFICIAL message slot schema, not the
            # Rust generator, to zero only alignment gaps for byte comparison.
            result = bytearray(body)
            position = 0

            def align(size):
                nonlocal position
                end = (position + size - 1) // size * size
                result[position:end] = b"\0" * (end - position)
                position = end

            def visit(type_, value):
                nonlocal position
                if isinstance(type_, d.BasicType):
                    sizes = {
                        "boolean": 1, "octet": 1, "char": 1, "int8": 1, "uint8": 1,
                        "int16": 2, "uint16": 2, "int32": 4, "uint32": 4,
                        "float": 4, "int64": 8, "uint64": 8, "double": 8,
                    }
                    size = sizes[type_.typename]
                    align(size)
                    position += size
                elif isinstance(type_, (d.BoundedString, d.UnboundedString)):
                    align(4)
                    length = int.from_bytes(body[position:position + 4], "little")
                    assert length == len(value.encode("utf-8")) + 1
                    position += 4 + length
                elif isinstance(type_, d.Array):
                    assert len(value) == type_.size
                    for item in value:
                        visit(type_.value_type, item)
                elif isinstance(type_, (d.BoundedSequence, d.UnboundedSequence)):
                    align(4)
                    length = int.from_bytes(body[position:position + 4], "little")
                    assert length == len(value)
                    position += 4
                    for item in value:
                        visit(type_.value_type, item)
                elif isinstance(type_, d.NamespacedType):
                    for slot, member_type in zip(value.__slots__, value.SLOT_TYPES):
                        visit(member_type, getattr(value, slot))
                else:
                    raise AssertionError(f"Unhandled official slot type {type_!r}")

            visit(d.NamespacedType(["unused", "msg"], "Root"), message)
            assert position == len(body), (type(message).__name__, position, len(body))
            return bytes(result)

        # Official serializers own the 4-byte CDR encapsulation; backend Message
        # implementations intentionally write only the body.
        for name, value in messages.items():
            wire = serialize_message(value)
            if wire[:4] != b"\0\1\0\0":
                raise AssertionError(f"{name}: expected little-endian XCDR1, got {wire[:4]!r}")
            (work / f"{name}.ros").write_bytes(wire[4:])
            (work / f"{name}.canonical").write_bytes(canonical_body(value, wire[4:]))
        (work / "tests").mkdir()
        shutil.copyfile(HERE / "rust/interoperability.rs", work / "tests/interoperability.rs")
        dependencies = "\n".join(
            f'{package} = {{ path = "{REPO / "generated" / package}" }}'
            for package in ("std_msgs", "geometry_msgs", "sensor_msgs", "nav_msgs", "tf2_msgs")
        )
        (work / "Cargo.toml").write_text(
            '[package]\nname = "jazzy_interoperability"\nversion = "0.0.0"\nedition = "2021"\n'
            '[workspace]\n[dependencies]\nheapless = "0.8"\n'
            f'micro-xrce-dds-rs = {{ path = "{BACKEND}", default-features = false }}\n'
            + dependencies + "\n"
        )
        env = dict(os.environ, MICROXRCE_INTEROP_FIXTURES=str(work))
        env.setdefault("CARGO_TARGET_DIR", str(REPO / "target/codegen-tests"))
        subprocess.run(
            [
                "cargo", "test", "--quiet", "--manifest-path", str(work / "Cargo.toml"),
                "--target", "x86_64-unknown-linux-gnu", "--test", "interoperability",
            ],
            check=True, env=env,
        )
        for name, expected in messages.items():
            body = (work / f"{name}.rust").read_bytes()
            actual = deserialize_message(b"\0\1\0\0" + body, type(expected))
            if actual != expected:
                raise AssertionError(f"Rust -> Jazzy decoded field mismatch: {name}")
        print(
            f"PASS: {len(messages)} official Jazzy CDR vectors; Rust byte equality "
            "(unspecified alignment padding normalized), "
            "ROS -> Rust field equality and Rust -> ROS field equality"
        )
    finally:
        shutil.rmtree(work)


if __name__ == "__main__":
    main()
