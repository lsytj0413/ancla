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
use std::collections::HashSet;

#[derive(Parser, Collect, Clone, Run)]
#[cling(run = "run_unreachable")]
pub struct Unreachable {}

pub fn run_unreachable(
    state: State<crate::cli_env::Env>,
    _args: &Unreachable,
    _common_opts: &crate::opts::CommonOpts,
) -> Result<()> {
    let db = &state.0.db;

    let mut unreachable_pages = HashSet::new();
    let max_pgid = db.info().max_pgid;

    let known_pages: std::collections::HashMap<u64, ancla::PageType> = db.iter_pages()
        .map(|p| (p.id, p.typ))
        .collect();

    for i in 0..max_pgid.into() {
        match known_pages.get(&i) {
            Some(page_type) => {
                match page_type {
                    ancla::PageType::Meta |
                    ancla::PageType::Freelist |
                    ancla::PageType::DataBranch |
                    ancla::PageType::DataLeaf => {
                        // These are reachable pages, do nothing
                    },
                    _ => {
                        // Other page types (e.g., Free) are considered unreachable based on the definition
                        unreachable_pages.insert(i);
                    }
                }
            },
            None => {
                // If a page ID within the 0..max_pgid range is not in known_pages, it's unreachable
                unreachable_pages.insert(i);
            }
        }
    }

    if unreachable_pages.is_empty() {
        println!("No unreachable pages found.");
    } else {
        println!("Unreachable pages: {unreachable_pages:?}");
    }

    Ok(())
}
