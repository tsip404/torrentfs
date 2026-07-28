use std::env;
use std::path::PathBuf;

fn main() {
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());

    println!("cargo:rerun-if-changed=wrapper/libtorrent_wrapper.h");
    println!("cargo:rerun-if-changed=wrapper/libtorrent_wrapper.cpp");

    let libtorrent_cflags = pkg_config::Config::new()
        .probe("libtorrent-rasterbar")
        .expect("Could not find libtorrent-rasterbar");

    let openssl_cflags = pkg_config::Config::new()
        .probe("openssl")
        .expect("Could not find openssl");

    let mut cpp_build = cc::Build::new();
    cpp_build
        .cpp(true)
        .file("wrapper/libtorrent_wrapper.cpp")
        .include("wrapper")
        .flag("-std=c++17")
        .flag("-fexceptions")
        .define("TORRENT_USE_OPENSSL", None)
        .define("TORRENT_USE_LIBCRYPTO", None)
        .define("TORRENT_SSL_PEERS", None)
        .define("TORRENT_LINKING_SHARED", None)
        .define("BOOST_ASIO_ENABLE_CANCELIO", None)
        .define("BOOST_ASIO_NO_DEPRECATED", None)
        .define("BOOST_SYSTEM_USE_UTF8", None)
        .define("TORRENT_ABI_VERSION", "2");

    for include_path in libtorrent_cflags.include_paths.iter() {
        cpp_build.include(include_path);
    }

    for include_path in openssl_cflags.include_paths.iter() {
        cpp_build.include(include_path);
    }

    cpp_build.compile("libtorrent_wrapper");

    for lib in libtorrent_cflags.libs.iter() {
        println!("cargo:rustc-link-lib={}", lib);
    }

    for lib in openssl_cflags.libs.iter() {
        println!("cargo:rustc-link-lib={}", lib);
    }

    let bindings = bindgen::Builder::default()
        .header("wrapper/libtorrent_wrapper.h")
        .generate()
        .expect("Unable to generate bindings");

    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
