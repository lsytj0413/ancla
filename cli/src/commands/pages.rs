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
use comfy_table::Table;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_pages")]
pub struct PageCommand {}

pub fn run_pages(
    state: State<crate::cli_env::Env>,
    _args: &PageCommand,
    _common_opts: &crate::opts::CommonOpts,
) -> Result<()> {
    let mut pages: Vec<ancla::PageInfo> = state.0.db.iter_pages().collect();
    pages.sort();
    let mut pages_table = Table::new();
    pages_table.set_content_arrangement(comfy_table::ContentArrangement::Dynamic);
    pages_table.load_preset(comfy_table::presets::NOTHING);
    pages_table.enforce_styling();
    pages_table.set_header(vec![
        "PAGE-ID",
        "TYPE",
        "OVERFLOW",
        "CAPACITY",
        "USED",
        "PARENT-PAGE-ID",
    ]);

    pages.iter().for_each(|p| {
        pages_table.add_row(vec![
            comfy_table::Cell::new(p.id),
            comfy_table::Cell::new(format!("{:?}", p.typ)),
            comfy_table::Cell::new(p.overflow),
            comfy_table::Cell::new(p.capacity),
            comfy_table::Cell::new(format!("{:?}", p.used)),
            comfy_table::Cell::new(format!("{:?}", p.parent_page_id)),
        ]);
    });
    println!("{pages_table}");
    Ok(())
}
