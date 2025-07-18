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
use comfy_table::{Cell, Table};
use serde_json;
use serde_json_path::{JsonPath, JsonPathExt};

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_info")]
pub struct InfoCommand {}

pub fn run_info(
    state: State<crate::cli_env::Env>,
    _args: &InfoCommand,
    common_opts: &crate::opts::CommonOpts,
) -> Result<()> {
    let info = state.0.db.info();

    match common_opts.output {
        crate::opts::OutputFormat::Json => {
            if let Some(json_path_str) = &common_opts.json_path {
                let json_value = serde_json::to_value(&info)?;
                let path = JsonPath::parse(json_path_str)?;

                let selected_nodes: Vec<_> = json_value.json_path(&path).into_iter().collect();

                println!("{}", serde_json::to_string_pretty(&selected_nodes)?);
            } else {
                let json = serde_json::to_string_pretty(&info)?;
                println!("{json}");
            }
        }
        crate::opts::OutputFormat::Table => {
            let mut table = Table::new();
            table.set_header(vec!["Name", "Value"]);
            table.add_row(vec![Cell::new("Page-Size"), Cell::new(info.page_size)]);
            table.add_row(vec![Cell::new("Max-PGID"), Cell::new(info.max_pgid)]);
            table.add_row(vec![Cell::new("Root-PGID"), Cell::new(info.root_pgid)]);
            table.add_row(vec![
                Cell::new("Freelist-PGID"),
                Cell::new(info.freelist_pgid),
            ]);
            table.add_row(vec![Cell::new("TXID"), Cell::new(info.txid)]);
            table.add_row(vec![Cell::new("Meta-PGID"), Cell::new(info.meta_pgid)]);
            println!("{table}");
        }
    }
    Ok(())
}
