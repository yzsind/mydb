//! MySQL-compatible names embedded in `version_compile_os` and
//! `version_compile_machine`.
//!
//! This module is shared by `build.rs` and unit tests. The server consumes the
//! generated compile-time strings rather than running these probes at startup.

pub fn mysql_compile_os(
    target_os: &str,
    pointer_width: &str,
    macos_product_version: Option<&str>,
) -> String {
    match target_os {
        "macos" => macos_product_version
            .and_then(macos_platform_name)
            .unwrap_or_else(|| "macos".to_owned()),
        "linux" => "Linux".to_owned(),
        "windows" if pointer_width == "64" => "Win64".to_owned(),
        "windows" => "Win32".to_owned(),
        "freebsd" => "FreeBSD".to_owned(),
        "netbsd" => "NetBSD".to_owned(),
        "openbsd" => "OpenBSD".to_owned(),
        "dragonfly" => "DragonFly".to_owned(),
        "solaris" | "illumos" => "SunOS".to_owned(),
        other => other.to_owned(),
    }
}

pub fn mysql_compile_machine(target_os: &str, target_arch: &str) -> String {
    match (target_os, target_arch) {
        ("macos", "aarch64") => "arm64".to_owned(),
        _ => target_arch.to_owned(),
    }
}

fn macos_platform_name(product_version: &str) -> Option<String> {
    let mut components = product_version.trim().split('.');
    let major = numeric_component(components.next()?)?;
    let minor = numeric_component(components.next()?)?;
    Some(format!("macos{major}.{minor}"))
}

fn numeric_component(component: &str) -> Option<&str> {
    (!component.is_empty() && component.bytes().all(|byte| byte.is_ascii_digit()))
        .then_some(component)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mysql_os_names_match_supported_platforms() {
        assert_eq!(mysql_compile_os("macos", "64", Some("15.7.1")), "macos15.7");
        assert_eq!(mysql_compile_os("linux", "64", None), "Linux");
        assert_eq!(mysql_compile_os("windows", "64", None), "Win64");
        assert_eq!(mysql_compile_os("windows", "32", None), "Win32");
    }

    #[test]
    fn malformed_macos_version_has_stable_fallback() {
        assert_eq!(mysql_compile_os("macos", "64", None), "macos");
        assert_eq!(
            mysql_compile_os("macos", "64", Some("not-a-version")),
            "macos"
        );
    }

    #[test]
    fn apple_silicon_uses_mysql_machine_name_only_on_macos() {
        assert_eq!(mysql_compile_machine("macos", "aarch64"), "arm64");
        assert_eq!(mysql_compile_machine("linux", "aarch64"), "aarch64");
        assert_eq!(mysql_compile_machine("macos", "x86_64"), "x86_64");
    }
}
