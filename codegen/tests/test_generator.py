import os
from pathlib import Path
import shutil
import subprocess
import unittest

from rosidl_adapter import convert_to_idl
from rosidl_parser import definition as d

from rosidl_generator_microxrcedds import GenerationError, Generator, generate_package
from rosidl_generator_microxrcedds.cli import regenerate


HERE = Path(__file__).resolve().parent
BACKEND = HERE.parents[1]
REPO = BACKEND.parents[1]


class GenerationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.work = HERE / ".work"
        if cls.work.exists():
            shutil.rmtree(cls.work)
        cls.work.mkdir()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.work)

    def test_invalid_capacities_and_unsupported_types(self):
        for capacity in (0, -1, 2**32, True, 1.5):
            for option in ("string_capacity", "sequence_capacity"):
                with self.subTest(capacity=capacity, option=option):
                    with self.assertRaises(GenerationError):
                        Generator("sample_msgs", **{option: capacity})
        generator = Generator("sample_msgs")
        for type_ in (
            d.UnboundedWString(), d.BasicType("wchar"), d.BasicType("long double"),
            d.NamespacedType(["sample_msgs", "srv"], "Request"),
        ):
            with self.subTest(type_=type_):
                with self.assertRaises(GenerationError):
                    generator.rust_type(type_)
        with self.assertRaises(GenerationError):
            generator.default(d.UnboundedString(), "x" * 33)
        with self.assertRaises(GenerationError):
            generator.default(d.UnboundedSequence(d.BasicType("int32")), [1, 2])
        self.assertEqual(generator.rust_type(d.BoundedString(64)), "heapless::String<64>")
        self.assertEqual(
            generator.rust_type(d.BoundedSequence(d.BasicType("int32"), 10)),
            "heapless::Vec<i32, 10>",
        )
        self.assertEqual(
            generator.rust_type(d.NamespacedType(["type", "msg"], "Other")),
            "r#type::msg::Other",
        )
        with self.assertRaises(GenerationError):
            generator.default(d.BoundedString(3), "éé")

    def test_reject_services_actions_and_missing_dependencies(self):
        for kind in ("srv", "action"):
            source = self.work / kind / "invalid_msgs"
            (source / kind).mkdir(parents=True)
            (source / "package.xml").write_text("<package><name>invalid_msgs</name></package>")
            (source / kind / "Sample.srv").write_text("")
            with self.assertRaisesRegex(GenerationError, "Services/actions"):
                regenerate(source.parent, self.work / f"out_{kind}", BACKEND)
        source = self.work / "missing" / "invalid_msgs"
        (source / "msg").mkdir(parents=True)
        (source / "package.xml").write_text("<package><name>invalid_msgs</name></package>")
        (source / "msg/Missing.msg").write_text("other_msgs/Other missing\n")
        with self.assertRaisesRegex(GenerationError, "missing message packages"):
            regenerate(source.parent, self.work / "out_missing", BACKEND)
        self.assertFalse((self.work / "out_missing/.rosidl_adapter").exists())

    def test_fixture_defaults_and_rust_wire_behavior(self):
        fixture = HERE / "fixtures/sample_msgs"
        idls = [
            convert_to_idl(
                fixture, "sample_msgs", path.relative_to(fixture), self.work / "idl"
            )
            for path in sorted(fixture.glob("msg/*.msg"))
        ]
        output = self.work / "sample_msgs"
        generate_package("sample_msgs", idls, output, BACKEND)
        self.assertIn("[f64; 36]", (output / "src/lib.rs").read_text())
        (output / "tests").mkdir()
        shutil.copyfile(HERE / "rust/sample.rs", output / "tests/sample.rs")
        harness = self.work / "application_tests"
        (harness / "tests").mkdir(parents=True)
        shutil.copyfile(HERE / "rust/application.rs", harness / "tests/application.rs")
        dependencies = "\n".join(
            f'{package} = {{ path = "{REPO / "generated" / package}" }}'
            for package in (
                "builtin_interfaces", "std_msgs", "geometry_msgs", "nav_msgs",
                "sensor_msgs", "tf2_msgs",
            )
        )
        (harness / "Cargo.toml").write_text(
            '[package]\nname = "application_tests"\nversion = "0.0.0"\nedition = "2021"\n'
            f'[dependencies]\nmicro-xrce-dds-rs = {{ path = "{BACKEND}", default-features = false }}\n'
            + dependencies + "\n"
        )
        (self.work / "Cargo.toml").write_text(
            '[workspace]\nmembers = ["sample_msgs", "application_tests"]\nresolver = "2"\n'
        )
        env = dict(os.environ)
        # Share compiled artifacts with other host validations, not the firmware.
        env.setdefault("CARGO_TARGET_DIR", str(REPO / "target/codegen-tests"))
        subprocess.run(
            [
                "cargo", "test", "--quiet", "--manifest-path", str(self.work / "Cargo.toml"),
                "--target", "x86_64-unknown-linux-gnu",
            ],
            check=True, env=env,
        )

    def test_repository_output_is_reproducible(self):
        output = self.work / "regenerated"
        packages = regenerate(REPO / "msgs", output, BACKEND)
        for package in packages:
            expected = REPO / "generated" / package
            self.assertEqual(
                (output / package / "src/lib.rs").read_bytes(),
                (expected / "src/lib.rs").read_bytes(),
                package,
            )
            # Relative runtime paths intentionally vary with output location.
            normalized = (output / package / "Cargo.toml").read_text()
            runtime_path = os.path.relpath(BACKEND, output / package)
            expected_path = os.path.relpath(BACKEND, expected)
            self.assertEqual(
                normalized.replace(runtime_path, expected_path),
                (expected / "Cargo.toml").read_text(),
            )
        regenerate(REPO / "msgs", output, BACKEND, check=True)
        (output / packages[0] / "src/lib.rs").write_text("// stale\n")
        with self.assertRaisesRegex(GenerationError, "stale"):
            regenerate(REPO / "msgs", output, BACKEND, check=True)
        self.assertEqual((output / packages[0] / "src/lib.rs").read_text(), "// stale\n")


if __name__ == "__main__":
    unittest.main()
