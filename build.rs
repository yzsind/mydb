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
