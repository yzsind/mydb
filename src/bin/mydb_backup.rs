fn main() {
    if let Err(err) = mydb::backup::cli_main(std::env::args().skip(1).collect()) {
        eprintln!("mydb-backup: {} ({})", err.message(), err.sql_state());
        if let Some(detail) = err.diagnostic_detail() {
            eprintln!("  {detail}");
        }
        std::process::exit(1);
    }
}
