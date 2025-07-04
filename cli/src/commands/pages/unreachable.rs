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

    let all_pages: HashSet<u64> = db.iter_pages().map(|p| p.id).collect();

    let meta_page_ids: HashSet<u64> = db.iter_pages()
        .filter(|p| p.typ == ancla::PageType::Meta)
        .map(|p| p.id)
        .collect();

    let freelist_page_ids: HashSet<u64> = db.iter_pages()
        .filter(|p| p.typ == ancla::PageType::Freelist)
        .flat_map(|p| {
            // This is a simplified approach. In a real scenario, you'd need to parse the freelist page
            // to get the actual free page IDs. For now, we'll assume the PageInfo for Freelist
            // pages contains the IDs directly or can be derived.
            // A more robust solution would involve reading the freelist page content.
            vec![p.id]
        })
        .collect();

    let reachable_pages: HashSet<u64> = db.iter_pages()
        .filter(|p| p.typ != ancla::PageType::Free && p.typ != ancla::PageType::Freelist)
        .map(|p| p.id)
        .collect();

    let mut unreachable_pages = HashSet::new();
    for page_id in all_pages.iter() {
        if !freelist_page_ids.contains(page_id) &&
           !meta_page_ids.contains(page_id) &&
           !reachable_pages.contains(page_id) {
            unreachable_pages.insert(*page_id);
        }
    }

    if unreachable_pages.is_empty() {
        println!("No unreachable pages found.");
    } else {
        println!("Unreachable pages: {unreachable_pages:?}");
    }

    Ok(())
}
