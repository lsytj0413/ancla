use anyhow::Result;
use clap::{Parser, Subcommand};
use cling::prelude::*;

#[derive(Run, Parser, Clone)]
#[cling(run = "init")]
pub struct App {
    #[clap(flatten)]
    pub common_opts: crate::opts::CommonOpts,

    #[clap(subcommand)]
    pub cmd: Commands,
}

fn init() -> Result<State<crate::cli_env::Env>> {
    Ok(State(crate::cli_env::Env {}))
}

#[derive(Run, Subcommand, Clone)]
pub enum Commands {
    /// List all buckets
    Buckets(crate::commands::buckets::BucketsCommand),
    /// List all pages
    Pages(crate::commands::pages::PageCommand),
    /// Show info about the database
    Info(crate::commands::info::InfoCommand),
    /// Get key value
    KV(crate::commands::kvs::KVCommand),
}
