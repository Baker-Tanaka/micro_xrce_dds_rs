"""Official rosidl IDL AST -> standalone, bounded, no_std Rust messages."""

import ast
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess

from rosidl_parser import definition as d
from rosidl_parser.parser import parse_idl_file


class GenerationError(ValueError):
    pass


# Rust type, CDR method, size/alignment.
BASIC = {
    "boolean": ("bool", "bool", 1),
    "octet": ("u8", "u8", 1),
    "char": ("u8", "u8", 1),
    "int8": ("i8", "i8", 1),
    "uint8": ("u8", "u8", 1),
    "int16": ("i16", "i16", 2),
    "uint16": ("u16", "u16", 2),
    "int32": ("i32", "i32", 4),
    "uint32": ("u32", "u32", 4),
    "int64": ("i64", "i64", 8),
    "uint64": ("u64", "u64", 8),
    "float": ("f32", "f32", 4),
    "double": ("f64", "f64", 8),
}
KEYWORDS = set(
    "as async await break const continue dyn else enum extern false fn for if impl "
    "in let loop match mod move mut pub ref return static struct trait true type "
    "unsafe use where while abstract become box do final macro override priv typeof "
    "unsized virtual yield try gen".split()
)


def ident(name):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", name) or name in {
        "self", "Self", "super", "crate", "_",
    }:
        raise GenerationError(f"Unsupported Rust identifier: {name!r}")
    return f"r#{name}" if name in KEYWORDS else name


def rust_string(value):
    # JSON and Rust differ for non-ASCII escapes, so emit UTF-8 literally.
    result = '"'
    for char in value:
        result += {
            '"': '\\"', "\\": "\\\\", "\n": "\\n", "\r": "\\r", "\t": "\\t",
        }.get(char, f"\\u{{{ord(char):x}}}" if ord(char) < 32 else char)
    return result + '"'


def annotation_value(value):
    """Decode rosidl's tuple-valued default annotations without executing code."""
    if not isinstance(value, str):
        return value
    node = ast.parse(value, mode="eval").body

    def visit(item):
        if isinstance(item, ast.Constant):
            return item.value
        if isinstance(item, (ast.Tuple, ast.List)):
            return [visit(v) for v in item.elts]
        if isinstance(item, ast.Name) and item.id in ("inf", "nan"):
            return float(item.id)
        if isinstance(item, ast.UnaryOp) and isinstance(item.op, (ast.USub, ast.UAdd)):
            return (-1 if isinstance(item.op, ast.USub) else 1) * visit(item.operand)
        raise GenerationError(f"Unsupported default annotation: {value!r}")

    return visit(node)


class Generator:
    def __init__(self, package, string_capacity=32, sequence_capacity=1):
        ident(package)
        for name, value in (("string", string_capacity), ("sequence", sequence_capacity)):
            if isinstance(value, bool) or not isinstance(value, int) or not 0 < value < 2**32:
                raise GenerationError(f"{name} capacity must be in 1..2^32-1")
        self.package = package
        self.string_capacity = string_capacity
        self.sequence_capacity = sequence_capacity
        self.dependencies = set()

    def capacity(self, type_):
        if isinstance(type_, (d.BoundedString, d.BoundedSequence)):
            capacity = type_.maximum_size
        elif isinstance(type_, d.UnboundedString):
            capacity = self.string_capacity
        elif isinstance(type_, d.UnboundedSequence):
            capacity = self.sequence_capacity
        else:
            raise GenerationError(f"Not a bounded container: {type_!r}")
        if not 0 < capacity < 2**32:
            raise GenerationError(f"Unsupported container capacity: {capacity}")
        return capacity

    def rust_type(self, type_):
        if isinstance(type_, d.BasicType):
            if type_.typename not in BASIC:
                raise GenerationError(f"Unsupported IDL primitive: {type_.typename}")
            return BASIC[type_.typename][0]
        if isinstance(type_, (d.BoundedString, d.UnboundedString)):
            return f"heapless::String<{self.capacity(type_)}>"
        if isinstance(type_, d.Array):
            return f"[{self.rust_type(type_.value_type)}; {type_.size}]"
        if isinstance(type_, (d.BoundedSequence, d.UnboundedSequence)):
            return f"heapless::Vec<{self.rust_type(type_.value_type)}, {self.capacity(type_)}>"
        if isinstance(type_, d.NamespacedType):
            if len(type_.namespaces) != 2 or type_.namespaces[1] != "msg":
                raise GenerationError(f"Only message references are supported: {type_}")
            package = type_.namespaces[0]
            if package == self.package:
                package = "crate"
            else:
                self.dependencies.add(package)
                package = ident(package)
            return f"{package}::msg::{ident(type_.name)}"
        raise GenerationError(f"Unsupported IDL type: {type(type_).__name__}")

    def max_size(self, type_):
        # Each scalar includes worst-case padding from ANY starting offset.
        # Thus sums/products remain safe for nested messages and sequences.
        if isinstance(type_, d.BasicType):
            size = BASIC[type_.typename][2]
            return str(2 * size - 1)
        if isinstance(type_, (d.BoundedString, d.UnboundedString)):
            return str(7 + self.capacity(type_) + 1)
        if isinstance(type_, d.Array):
            return f"{type_.size} * ({self.max_size(type_.value_type)})"
        if isinstance(type_, (d.BoundedSequence, d.UnboundedSequence)):
            return f"7 + {self.capacity(type_)} * ({self.max_size(type_.value_type)})"
        return f"<{self.rust_type(type_)} as Message>::MAX_SERIALIZED_SIZE"

    def default(self, type_, value=None):
        rust_type = self.rust_type(type_)
        if isinstance(type_, d.Array):
            if value is None:
                return f"core::array::from_fn(|_| {self.default(type_.value_type)})"
            value = annotation_value(value)
            if len(value) != type_.size:
                raise GenerationError("Array default length does not match array size")
            return "[" + ", ".join(self.default(type_.value_type, v) for v in value) + "]"
        if isinstance(type_, (d.BoundedSequence, d.UnboundedSequence)):
            if value is None:
                return "heapless::Vec::new()"
            value = annotation_value(value)
            if len(value) > self.capacity(type_):
                raise GenerationError("Sequence default exceeds configured capacity")
            items = " ".join(
                f"value.push({self.default(type_.value_type, v)}).unwrap();"
                for v in value
            )
            return f"{{ let mut value = {rust_type.replace('<', '::<', 1)}::new(); {items} value }}"
        if isinstance(type_, (d.BoundedString, d.UnboundedString)):
            if value is None:
                return "heapless::String::new()"
            if len(value.encode("utf-8")) > self.capacity(type_):
                raise GenerationError("String default exceeds configured byte capacity")
            return f"heapless::String::try_from({rust_string(value)}).unwrap()"
        if value is None:
            return "Default::default()"
        if isinstance(type_, d.BasicType):
            if rust_type == "bool":
                return str(value).lower()
            if rust_type in ("f32", "f64"):
                value = float(value)
                if math.isnan(value):
                    return f"{rust_type}::NAN"
                if math.isinf(value):
                    return f"{rust_type}::{'NEG_INFINITY' if value < 0 else 'INFINITY'}"
                return f"{repr(value)}{rust_type}"
            return f"{value}{rust_type}"
        raise GenerationError(f"Unsupported default for {rust_type}")

    def encode(self, type_, expression):
        if isinstance(type_, d.BasicType):
            return f"w.{BASIC[type_.typename][1]}_val(*{expression});"
        if isinstance(type_, (d.BoundedString, d.UnboundedString)):
            return f"w.string({expression});"
        if isinstance(type_, d.Array):
            return f"for item in {expression} {{ {self.encode(type_.value_type, 'item')} }}"
        if isinstance(type_, (d.BoundedSequence, d.UnboundedSequence)):
            return (
                f"w.u32_val(({expression}).len() as u32); "
                f"for item in {expression} {{ {self.encode(type_.value_type, 'item')} }}"
            )
        return f"Message::serialize({expression}, w);"

    def decode(self, type_):
        rust_type = self.rust_type(type_)
        if isinstance(type_, d.BasicType):
            if type_.typename == "boolean":
                return (
                    "match r.u8_val()? { 0 => false, 1 => true, "
                    "_ => return Err(Error::Deserialization) }"
                )
            return f"r.{BASIC[type_.typename][1]}_val()?"
        if isinstance(type_, (d.BoundedString, d.UnboundedString)):
            cap = self.capacity(type_)
            return (
                "{ let len = r.u32_val()? as usize; "
                f"if len == 0 || len - 1 > {cap} {{ return Err(Error::Deserialization); }} "
                f"let mut bytes = heapless::Vec::<u8, {cap}>::new(); "
                "for _ in 0..len - 1 { bytes.push(r.u8_val()?).map_err(|_| Error::Deserialization)?; } "
                "if r.u8_val()? != 0 { return Err(Error::Deserialization); } "
                "heapless::String::try_from(core::str::from_utf8(&bytes).map_err(|_| Error::Deserialization)?)"
                ".map_err(|_| Error::Deserialization)? }"
            )
        if isinstance(type_, d.Array):
            return (
                f"{{ let mut value: {rust_type} = core::array::from_fn(|_| Default::default()); "
                f"for item in &mut value {{ *item = {self.decode(type_.value_type)}; }} value }}"
            )
        if isinstance(type_, (d.BoundedSequence, d.UnboundedSequence)):
            return (
                "{ let len = r.u32_val()? as usize; "
                f"if len > {self.capacity(type_)} {{ return Err(Error::Deserialization); }} "
                f"let mut value = {rust_type.replace('<', '::<', 1)}::new(); "
                f"for _ in 0..len {{ value.push({self.decode(type_.value_type)})"
                ".map_err(|_| Error::Deserialization)?; } value }"
            )
        return f"<{rust_type} as Message>::deserialize(r)?"

    def message(self, message):
        structure = message.structure
        name = ident(structure.namespaced_type.name)
        if structure.namespaced_type.namespaces != [self.package, "msg"]:
            raise GenerationError("Only pkg::msg::Name IDL structures are supported")
        members = structure.members
        fields = "\n".join(
            f"pub {ident(m.name)}: {self.rust_type(m.type)}," for m in members
        )
        defaults = "\n".join(
            f"{ident(m.name)}: {self.default(m.type, m.get_annotation_value('default')['value'] if m.has_annotation('default') else None)},"
            for m in members
        )
        constants = []
        for constant in message.constants:
            if isinstance(constant.type, (d.BoundedString, d.UnboundedString)):
                type_, value = "&'static str", rust_string(constant.value)
            else:
                type_ = self.rust_type(constant.type)
                value = self.default(constant.type, constant.value)
            constants.append(f"pub const {ident(constant.name)}: {type_} = {value};")
        special = ""
        if self.package == "std_msgs" and name == "Header":
            special = """
pub fn set_stamp(&mut self, stamp: micro_xrce_dds_rs::Timestamp) {
    self.stamp.sec = stamp.sec;
    self.stamp.nanosec = stamp.nanosec;
}
"""
        inherent = (
            f"impl {name} {{ {' '.join(constants)} {special} }}"
            if constants or special else ""
        )
        encoded = "\n".join(self.encode(m.type, f"&self.{ident(m.name)}") for m in members)
        decoded = "\n".join(
            f"{ident(m.name)}: {self.decode(m.type)}," for m in members
        )
        size = " + ".join(f"({self.max_size(m.type)})" for m in members) or "0"
        return f"""
#[derive(Clone, Debug, PartialEq)]
pub struct {name} {{
{fields}
}}
impl Default for {name} {{
fn default() -> Self {{ Self {{
{defaults}
}} }}
}}
{inherent}
impl Message for {name} {{
const TYPE_NAME: &'static str = "{self.package}::msg::dds_::{name}_";
const MAX_SERIALIZED_SIZE: usize = {size};
fn serialize(&self, w: &mut CdrWriter) {{
{encoded}
}}
fn deserialize(r: &mut CdrReader<'_>) -> Result<Self, Error> {{
Ok(Self {{
{decoded}
}})
}}
}}
"""

    def render(self, idl_files):
        messages = []
        provenance = hashlib.sha256(
            f"microxrcedds-v1;strings={self.string_capacity};sequences={self.sequence_capacity}\0".encode()
        )
        for path in sorted(Path(p).resolve() for p in idl_files):
            provenance.update(path.name.encode() + b"\0" + path.read_bytes() + b"\0")
            if path.parent.name != "msg":
                raise GenerationError(f"Services/actions are not supported: {path}")
            content = parse_idl_file(d.IdlLocator(path.parent, path.name)).content
            if content.get_elements_of_type(d.Service) or content.get_elements_of_type(d.Action):
                raise GenerationError(f"Services/actions are not supported: {path}")
            parsed = content.get_elements_of_type(d.Message)
            if not parsed:
                raise GenerationError(f"No message in IDL: {path}")
            messages.extend(parsed)
        names = [m.structure.namespaced_type.name for m in messages]
        if len(names) != len(set(names)):
            raise GenerationError("Duplicate message name")
        rendered = "\n".join(self.message(m) for m in messages)
        return (
            "// Generated by rosidl_generator_microxrcedds; DO NOT EDIT.\n"
            f"// ROS IDL + capacity policy SHA-256: {provenance.hexdigest()}\n"
            "#![no_std]\n\npub mod msg {\n"
            "use micro_xrce_dds_rs::{Message, cdr::CdrWriter, cdr_reader::CdrReader, Error};\n"
            + rendered + "\n}\n"
        )


def generate_package(package, idl_files, output, runtime, string_capacity=32, sequence_capacity=1, check=False):
    generator = Generator(package, string_capacity, sequence_capacity)
    source = generator.render(idl_files)
    source = subprocess.run(
        ["rustfmt", "--edition", "2021", "--emit", "stdout"],
        input=source, text=True, stdout=subprocess.PIPE, check=True,
    ).stdout
    output = Path(output).resolve()
    backend = os.path.relpath(Path(runtime).resolve(), output)
    dependencies = "\n".join(
        f'{dep} = {{ path = "../{dep}" }}' for dep in sorted(generator.dependencies)
    )
    manifest = f"""# Generated by rosidl_generator_microxrcedds; DO NOT EDIT.
[package]
name = "{package}"
version = "0.0.0"
edition = "2021"
license = "Apache-2.0"

[dependencies]
micro-xrce-dds-rs = {{ path = {json.dumps(backend)}, default-features = false }}
heapless = {{ version = "0.8", default-features = false }}
{dependencies}
"""
    for relative, content in (("src/lib.rs", source), ("Cargo.toml", manifest)):
        path = output / relative
        if check:
            if not path.exists() or path.read_text(encoding="utf-8") != content:
                raise GenerationError(f"Generated output is stale: {path}; run regeneration")
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
    return generator.dependencies
