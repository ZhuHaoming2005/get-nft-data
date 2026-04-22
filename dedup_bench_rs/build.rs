fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    if cfg!(target_os = "windows") {
        println!("cargo:rustc-link-lib=Rstrtmgr");
    }
}
