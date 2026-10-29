//! Release-format fixtures are embedded, including in archived test binaries.
pub(crate) fn check(name: &str, actual: &[u8], expected: &[u8]) {
    if let Some(directory) = std::env::var_os("MYDB_FORMAT_GOLDEN_OUTPUT") {
        let directory = std::path::PathBuf::from(directory);
        assert!(directory.starts_with("/tmp/mydb"));
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join(format!("{name}.bin")), actual).unwrap();
    } else {
        assert_eq!(
            actual, expected,
            "persistent format fixture changed: {name}"
        );
    }
}

macro_rules! golden {
    ($name:literal, $actual:expr) => {
        $crate::storage::format_golden::check(
            $name,
            $actual,
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/src/storage/fixtures/release-v1/",
                $name,
                ".bin"
            )),
        )
    };
}
pub(crate) use golden;
