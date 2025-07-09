// MIT License
//
// Copyright (c) 2024 Songlin Yang
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use anyhow::Result;
use clap::{Parser, Subcommand};
use cling::prelude::*;

#[derive(Run, Parser, Clone)]
#[command(author, version = crate::build_info::version(), about)]
#[cling(run = "init")]
pub struct App {
    #[clap(flatten)]
    pub common_opts: crate::opts::CommonOpts,

    #[clap(subcommand)]
    pub cmd: Commands,
}

fn init(common_opts: &crate::opts::CommonOpts) -> Result<State<crate::cli_env::Env>> {
    common_opts.validate()?;
    let options = ancla::AnclaOptions::builder()
        .db_path(common_opts.db.clone())
        .page_size(common_opts.page_size)
        .build();
    let db = ancla::DBWrapper::open(options)?;
    Ok(State(crate::cli_env::Env { db }))
}

#[derive(Run, Subcommand, Clone)]
pub enum Commands {
    /// List all buckets
    Buckets(crate::commands::buckets::BucketsCommand),
    /// Operations on pages
    #[clap(subcommand)]
    Pages(crate::commands::pages::Pages),
    /// Show info about the database
    Info(crate::commands::info::InfoCommand),

    #[clap(subcommand)]
    KV(crate::commands::kvs::Kvs),
}
