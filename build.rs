#[path = "src/build_metadata.rs"]
mod build_metadata;

use std::env;
use std::process::Command;

const OS_OVERRIDE: &str = "MYDB_BUILD_VERSION_COMPILE_OS";
const MACHINE_OVERRIDE: &str = "MYDB_BUILD_VERSION_COMPILE_MACHINE";

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    // Refresh the embedded date whenever source or package metadata changes.
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=Cargo.toml");
    println!("cargo:rerun-if-env-changed=TZ");
    println!("cargo:rerun-if-env-changed={OS_OVERRIDE}");
    println!("cargo:rerun-if-env-changed={MACHINE_OVERRIDE}");

    let target_os = required_env("CARGO_CFG_TARGET_OS");
    let target_arch = required_env("CARGO_CFG_TARGET_ARCH");
    if target_os == "macos"
        && target_arch == "aarch64"
        && env::var_os("CARGO_FEATURE_TOOLS").is_some()
    {
        build_vector_gpu();
    }
    let pointer_width = required_env("CARGO_CFG_TARGET_POINTER_WIDTH");

    let compile_os = build_override(OS_OVERRIDE).unwrap_or_else(|| {
        let macos_version = (target_os == "macos").then(macos_product_version).flatten();
        let compile_os =
            build_metadata::mysql_compile_os(&target_os, &pointer_width, macos_version.as_deref());
        assert!(
            target_os != "macos" || compile_os != "macos",
            "could not determine the macOS build version; set {OS_OVERRIDE} explicitly"
        );
        compile_os
    });
    let compile_machine = build_override(MACHINE_OVERRIDE)
        .unwrap_or_else(|| build_metadata::mysql_compile_machine(&target_os, &target_arch));

    println!("cargo:rustc-env=MYDB_VERSION_COMPILE_OS={compile_os}");
    println!("cargo:rustc-env=MYDB_VERSION_COMPILE_MACHINE={compile_machine}");
    println!(
        "cargo:rustc-env=MYDB_BUILD_DATE={}",
        chrono::Local::now().format("%Y%m%d")
    );
}

fn build_vector_gpu() {
    let source = "src/bin/vector_search_bench/gpu.m";
    println!("cargo:rerun-if-changed={source}");
    let out = std::path::PathBuf::from(required_env("OUT_DIR"));
    let object = out.join("vector_gpu.o");
    let archive = out.join("libvector_gpu.a");
    let status = Command::new("xcrun")
        .args([
            "clang",
            "-c",
            "-O3",
            "-fobjc-arc",
            "-arch",
            "arm64",
            source,
            "-o",
        ])
        .arg(&object)
        .status()
        .expect("failed to run clang for the Metal benchmark bridge");
    assert!(
        status.success(),
        "failed to compile the Metal benchmark bridge"
    );
    let status = Command::new("xcrun")
        .args(["ar", "crs"])
        .arg(&archive)
        .arg(&object)
        .status()
        .expect("failed to archive the Metal benchmark bridge");
    assert!(
        status.success(),
        "failed to archive the Metal benchmark bridge"
    );
    println!("cargo:rustc-link-search=native={}", out.display());
    // Only this tools binary links the bridge and Apple GPU frameworks.
    for arg in [
        "-lvector_gpu",
        "-framework",
        "Foundation",
        "-framework",
        "Metal",
        "-framework",
        "MetalPerformanceShaders",
    ] {
        println!("cargo:rustc-link-arg-bin=vector_search_bench={arg}");
    }
}

fn required_env(name: &str) -> String {
    env::var(name)
        .unwrap_or_else(|_| panic!("Cargo did not provide required build variable {name}"))
}

fn build_override(name: &str) -> Option<String> {
    let value = env::var(name).ok()?;
    let value = value.trim();
    assert!(
        !value.is_empty() && !value.contains(['\r', '\n', '\0']),
        "{name} must be a non-empty single-line value"
    );
    Some(value.to_owned())
}

fn macos_product_version() -> Option<String> {
    let output = Command::new("sw_vers")
        .arg("-productVersion")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|version| version.trim().to_owned())
        .filter(|version| !version.is_empty())
}
