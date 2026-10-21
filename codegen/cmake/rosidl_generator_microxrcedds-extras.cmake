find_package(ament_cmake_core REQUIRED)
find_package(rosidl_cmake REQUIRED)
ament_register_extension(
  "rosidl_generate_idl_interfaces"
  "rosidl_generator_microxrcedds"
  "rosidl_generator_microxrcedds_generate_interfaces.cmake")
