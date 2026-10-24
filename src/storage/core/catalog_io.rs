fn sanitize_name(name: &str) -> String {
    crate::storage::naming::sanitize_fs_name(name)
}
