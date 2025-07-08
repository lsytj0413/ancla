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
use clap::Parser;
use cling::prelude::*;
use comfy_table::{presets, Cell, Table};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_info")]
pub struct InfoCommand {
    /// Print output in JSON format
    #[clap(long)]
    json: bool,
}

pub fn run_info(
    state: State<crate::cli_env::Env>,
    args: &InfoCommand,
    _common_opts: &crate::opts::CommonOpts,
) -> Result<()> {
    let info = state.0.db.info();

    if args.json {
        println!("{}", serde_json::to_string_pretty(&info)?);
        return Ok(());
    }

    let mut table = Table::new();
    table.load_preset(presets::NOTHING);
    table.set_header(vec![Cell::new("Page Size"), Cell::new("Max PGID")]);
    table.add_row(vec![
        Cell::new(info.page_size.to_string()),
        Cell::new(info.max_pgid.to_string()),
    ]);
    println!("{table}");

    Ok(())
}
