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

#[derive(Parser, Collect, Clone, Run)]
#[cling(run = "run_unreachable")]
pub struct Unreachable {}

pub fn run_unreachable(
    state: State<crate::cli_env::Env>,
    _args: &Unreachable,
    _common_opts: &crate::opts::CommonOpts,
) -> Result<()> {
    let db = &state.0.db;

    let max_pgid = db.info().max_pgid;

    let known_pages: std::collections::HashMap<u64, ancla::PageType> = db.iter_pages()
        .map(|p| (p.id, p.typ))
        .collect();

    // Wrap known_pages in an Arc for shared, immutable access across threads
    let known_pages_arc = std::sync::Arc::new(known_pages);

    // Determine the number of threads to use
    let num_threads = std::thread::available_parallelism().map_or(1, |x| x.get());
    let total_pages = max_pgid.into();
    let chunk_size = (total_pages as f64 / num_threads as f64).ceil() as u64;

    let mut handles = vec![];
    let mut all_unreachable_pages = std::collections::HashSet::new();

    for i in 0..num_threads {
        let start = i as u64 * chunk_size;
        let end = std::cmp::min(start + chunk_size, total_pages);

        if start >= end {
            continue; // Skip empty chunks
        }

        let known_pages_clone = std::sync::Arc::clone(&known_pages_arc);

        let handle = std::thread::spawn(move || {
            let mut local_unreachable_pages = std::collections::HashSet::new();
            for page_id in start..end {
                match known_pages_clone.get(&page_id) {
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
                                local_unreachable_pages.insert(page_id);
                            }
                        }
                    },
                    None => {
                        // If a page ID within the 0..max_pgid range is not in known_pages, it's unreachable
                        local_unreachable_pages.insert(page_id);
                    }
                }
            }
            local_unreachable_pages
        });
        handles.push(handle);
    }

    // Collect results from all threads
    for handle in handles {
        let local_set = handle.join().map_err(|e| anyhow::anyhow!("Thread panicked: {:?}", e))?;
        all_unreachable_pages.extend(local_set);
    }

    if all_unreachable_pages.is_empty() {
        println!("No unreachable pages found.");
    } else {
        println!("Unreachable pages: {all_unreachable_pages:?}");
    }

    Ok(())
}
