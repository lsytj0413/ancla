use cling::prelude::*;

#[derive(Debug, Clone, clap::ValueEnum)]
enum Endian {
    Little,
    Big,
}

const fn is_target_little_endian() -> bool {
    // cfg!(target_endian = "little")
    u16::from_ne_bytes([1, 0]) == 1
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> ClingFinished<boltcli::App> {
    Cling::parse_and_run().await
}
