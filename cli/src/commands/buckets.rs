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
use comfy_table::presets::NOTHING;
use comfy_table::Table;
use std::cell::RefCell;
use std::rc::Rc;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_buckets")]
pub struct BucketsCommand {}

pub fn run_buckets(
    _state: State<crate::cli_env::Env>,
    _args: &BucketsCommand,
    common_opts: &crate::opts::CommonOpts,
) -> Result<()> {
    let options = ancla::AnclaOptions::builder()
        .db_path(common_opts.db.clone())
        .build();
    let db = ancla::DB::open(options)?;
    let buckets = iter_buckets(db);
    print_buckets(&buckets);

    Ok(())
}

struct Bucket {
    name: Vec<u8>,
    page_id: u64,
    is_inline: bool,
    child_buckets: Vec<Bucket>,
}

fn iter_buckets_inner(bucket: &ancla::Bucket) -> Vec<Bucket> {
    let mut buckets: Vec<Bucket> = Vec::new();

    let child_buckets: Vec<ancla::Bucket> = bucket.iter_buckets().collect();
    for child_bucket in child_buckets {
        buckets.push(Bucket {
            name: child_bucket.name.clone(),
            page_id: child_bucket.page_id,
            is_inline: child_bucket.is_inline,
            child_buckets: iter_buckets_inner(&child_bucket),
        })
    }

    buckets
}

fn iter_buckets(db: Rc<RefCell<ancla::DB>>) -> Vec<Bucket> {
    let buckets: Vec<ancla::Bucket> = ancla::DB::iter_buckets(db).collect();
    buckets
        .iter()
        .map(|bucket| Bucket {
            name: bucket.name.clone(),
            page_id: bucket.page_id,
            is_inline: bucket.is_inline,
            child_buckets: iter_buckets_inner(bucket),
        })
        .collect()
}

fn print_buckets_inner(buckets: &[Bucket], table: &mut comfy_table::Table, level: usize) {
    for (i, bucket) in buckets.iter().enumerate() {
        let chr = if i == buckets.len() - 1 { '└' } else { '├' };

        table.add_row(vec![
            format!(
                "{chr:>level$}{}",
                String::from_utf8(bucket.name.clone()).unwrap()
            ),
            bucket.page_id.to_string(),
            bucket.is_inline.to_string(),
        ]);
        print_buckets_inner(&bucket.child_buckets, table, level + 1);
    }
}

fn print_buckets(buckets: &Vec<Bucket>) {
    let mut buckets_table = Table::new();
    buckets_table.set_content_arrangement(comfy_table::ContentArrangement::Dynamic);
    buckets_table.load_preset(NOTHING);
    buckets_table.enforce_styling();
    buckets_table.set_header(vec!["BUCKET-NAME", "PAGE-ID", "IS-INLINE"]);

    for bucket in buckets {
        buckets_table.add_row(vec![
            String::from_utf8(bucket.name.clone()).unwrap(),
            bucket.page_id.to_string(),
            bucket.is_inline.to_string(),
        ]);

        print_buckets_inner(&bucket.child_buckets, &mut buckets_table, 1);
    }
    println!("{buckets_table}");
}
