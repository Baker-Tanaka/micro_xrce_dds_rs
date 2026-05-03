fn main() {
    // Reserved for future RIHS01 type-hash computation.
    // When implemented, sha2 will be added to [build-dependencies] and each
    // ROS2 message type's canonical .msg definition will be hashed here and
    // injected via cargo:rustc-env=RIHS01_<TYPE>=<hex>.
    //
    // Example (not yet active):
    //   let h = rihs01_hash("float32 data\n");
    //   println!("cargo:rustc-env=RIHS01_FLOAT32={}", hex::encode(h));
}
