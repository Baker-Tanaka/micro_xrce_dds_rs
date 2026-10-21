"""CLI and ament entry point. ROS dependencies are generation-time only."""

import argparse
from pathlib import Path
import shutil
import xml.etree.ElementTree as ET

from . import GenerationError, generate_package


def regenerate(messages, output, runtime, string_capacity=32, sequence_capacity=1, check=False):
    from rosidl_adapter import convert_to_idl

    messages, output = Path(messages).resolve(), Path(output).resolve()
    metadata = sorted(messages.glob("*/package.xml"))
    if not metadata:
        raise GenerationError(f"No message packages found under {messages}")
    staging = output / ".rosidl_adapter"
    if staging.exists():
        raise GenerationError(f"Remove stale generation directory first: {staging}")
    packages = {}
    try:
        for metadata_file in metadata:
            package_dir = metadata_file.parent
            package = ET.parse(metadata_file).getroot().findtext("name")
            if package != package_dir.name:
                raise GenerationError(f"Package name/directory mismatch: {metadata_file}")
            unsupported = list(package_dir.glob("srv/*")) + list(package_dir.glob("action/*"))
            if unsupported:
                raise GenerationError(f"Services/actions are not supported: {unsupported[0]}")
            interfaces = sorted(package_dir.glob("msg/*.msg"))
            if not interfaces:
                raise GenerationError(f"No .msg interfaces in {package_dir}")
            idls = [
                convert_to_idl(
                    package_dir, package, interface.relative_to(package_dir), staging / package
                )
                for interface in interfaces
            ]
            packages[package] = idls
        # Render everything before replacing any checked-in source.
        from . import Generator
        for package, idls in packages.items():
            generator = Generator(package, string_capacity, sequence_capacity)
            generator.render(idls)
            missing = generator.dependencies - packages.keys()
            if missing:
                raise GenerationError(f"{package}: missing message packages: {sorted(missing)}")
        for package, idls in packages.items():
            generate_package(
                package, idls, output / package, runtime, string_capacity, sequence_capacity, check
            )
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return sorted(packages)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--messages", type=Path, help="Repository msgs/ directory (.msg -> IDL -> Rust)")
    parser.add_argument("--package", help="ament package name (already-adapted IDL mode)")
    parser.add_argument("--idl", type=Path, action="append", default=[])
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--runtime", type=Path, required=True, help="micro_xrce_dds_rs backend directory")
    parser.add_argument("--string-capacity", type=int, default=32)
    parser.add_argument("--sequence-capacity", type=int, default=1)
    parser.add_argument("--check", action="store_true", help="Fail on stale/missing output without changing crate files")
    args = parser.parse_args(argv)
    try:
        if args.messages and not (args.package or args.idl):
            regenerate(
                args.messages, args.output, args.runtime,
                args.string_capacity, args.sequence_capacity, args.check,
            )
        elif args.package and args.idl and not args.messages:
            generate_package(
                args.package, args.idl, args.output, args.runtime,
                args.string_capacity, args.sequence_capacity, args.check,
            )
        else:
            parser.error("Use either --messages, or --package with one or more --idl")
    except (GenerationError, ValueError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
