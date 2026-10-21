# The extension consumes official adapter-produced IDL, not .msg text.
find_package(Python3 REQUIRED COMPONENTS Interpreter)
execute_process(
  COMMAND "${Python3_EXECUTABLE}" -c
    "import pathlib, rosidl_generator_microxrcedds; print(pathlib.Path(rosidl_generator_microxrcedds.__file__).parent)"
  OUTPUT_VARIABLE _microxrce_generator_module
  OUTPUT_STRIP_TRAILING_WHITESPACE
  RESULT_VARIABLE _microxrce_import_result)
if(NOT _microxrce_import_result EQUAL 0)
  message(FATAL_ERROR "rosidl_generator_microxrcedds Python module is not importable")
endif()
if(NOT MICROXRCE_GENERATED_ROOT OR NOT MICROXRCE_RUNTIME_PATH)
  message(FATAL_ERROR
    "Set MICROXRCE_GENERATED_ROOT (sibling Cargo crates) and MICROXRCE_RUNTIME_PATH (backend source)")
endif()
if(NOT DEFINED MICROXRCE_STRING_CAPACITY)
  set(MICROXRCE_STRING_CAPACITY 32)
endif()
if(NOT DEFINED MICROXRCE_SEQUENCE_CAPACITY)
  set(MICROXRCE_SEQUENCE_CAPACITY 1)
endif()
set(_microxrce_idl_args "")
foreach(_idl ${rosidl_generate_interfaces_ABS_IDL_FILES})
  list(APPEND _microxrce_idl_args --idl "${_idl}")
endforeach()
set(_microxrce_output "${MICROXRCE_GENERATED_ROOT}/${PROJECT_NAME}")
add_custom_command(
  OUTPUT "${_microxrce_output}/Cargo.toml" "${_microxrce_output}/src/lib.rs"
  COMMAND "${Python3_EXECUTABLE}" -m rosidl_generator_microxrcedds.cli
    --package "${PROJECT_NAME}" ${_microxrce_idl_args}
    --output "${_microxrce_output}"
    --runtime "${MICROXRCE_RUNTIME_PATH}"
    --string-capacity "${MICROXRCE_STRING_CAPACITY}"
    --sequence-capacity "${MICROXRCE_SEQUENCE_CAPACITY}"
  DEPENDS ${rosidl_generate_interfaces_ABS_IDL_FILES}
    "${_microxrce_generator_module}/__init__.py"
    "${_microxrce_generator_module}/cli.py"
  VERBATIM)
add_custom_target(${rosidl_generate_interfaces_TARGET}__microxrcedds ALL
  DEPENDS "${_microxrce_output}/Cargo.toml" "${_microxrce_output}/src/lib.rs")
add_dependencies(${rosidl_generate_interfaces_TARGET}__microxrcedds
  ${rosidl_generate_interfaces_TARGET})
