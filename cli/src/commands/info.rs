use anyhow::Result;
use clap::Parser;
use cling::prelude::*;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_info")]
pub struct InfoCommand {}

pub fn run_info(
    _state: State<crate::cli_env::Env>,
    _args: &InfoCommand,
    common_opts: &crate::opts::CommonOpts,
) -> Result<()> {
    let options = ancla::AnclaOptions::builder()
        .db_path(common_opts.db.clone())
        .build();
    let db = ancla::DB::build(options);
    let info = ancla::DB::info(db);
    println!("Page Size: {:?}", info.page_size);
    Ok(())
}
