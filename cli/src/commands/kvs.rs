use anyhow::Result;
use clap::Parser;
use cling::prelude::*;

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "run_kv")]
pub struct KVCommand {
    #[clap(long)]
    pub buckets: Vec<String>,
    #[clap(long)]
    pub key: String,
}

pub fn run_kv(
    _state: State<crate::cli_env::Env>,
    args: &KVCommand,
    common_opts: &crate::opts::CommonOpts,
) -> Result<()> {
    println!("{:?}", args);

    let options = ancla::AnclaOptions::builder()
        .db_path(common_opts.db.clone())
        .build();
    let db = ancla::DB::build(options);

    let kv = ancla::DB::get_key_value(db, &args.buckets, &args.key);
    if let Some(kv) = kv {
        println!("Key: {:?}", String::from_utf8(kv.key));
        println!("Value: {:?}", String::from_utf8(kv.value));
    } else {
        println!("Key not found");
    }
    Ok(())
}
