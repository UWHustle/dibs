use std::env;

fn main() {
    println!("cargo:rerun-if-changed=src/sqlite/sqlite3.c");

    cc::Build::new()
        .file("src/systems/sqlite/sqlite3.c")
        .flag("-DSQLITE_THREADSAFE=2")
        .flag("-DSQLITE_DEFAULT_MEMSTATUS=0")
        .opt_level(3)
        .compile("sqlite");

    env::set_var("SQLITE3_LIB_DIR", env::var("OUT_DIR").unwrap());
}
