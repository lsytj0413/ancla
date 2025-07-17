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
use std::iter::Peekable;

/// Command to display a tree of all buckets in the database.
#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_buckets")]
pub struct BucketsCommand {}

pub fn run_buckets(
    state: State<crate::cli_env::Env>,
    _args: &BucketsCommand,
    _common_opts: &crate::opts::CommonOpts,
) -> Result<()> {
    let buckets = iter_buckets(state.0.db);
    print_buckets(&buckets);

    Ok(())
}

/// A local representation of a bucket, used to build a tree structure for display.
/// This struct mirrors the `ancla::Bucket` but is adapted for CLI display purposes.
struct Bucket {
    /// The unique identifier for the bucket, composed of its name and the page ID it resides on.
    id: String,
    /// The identifier of the bucket itself.
    identifier: ancla::BucketIdentifier,
    /// A flag indicating whether the bucket is stored inline within its parent's page.
    is_inline: bool,
    /// The nesting depth of the bucket.
    depth: u64,
    /// The parent bucket, if any.
    parent: Option<ancla::BucketIdentifier>,
    /// A list of child buckets nested under this one.
    child_buckets: Vec<Bucket>,
}

/// Recursively iterates through the buckets to build a hierarchical structure for display.
///
/// # Arguments
///
/// * `peek_iter` - A peekable iterator over `ancla::Bucket` items.
/// * `depth` - The current nesting depth in the bucket tree.
///
/// # Returns
///
/// A vector of `Bucket` structs representing the current level and its children.
///
/// # Panics
///
/// This function will panic if an unexpected bucket depth is encountered or if there's a database error.
fn iter_buckets_inner<T>(peek_iter: &mut Peekable<T>, depth: u64) -> Vec<Bucket>
where
    T: Iterator<Item = Result<ancla::Bucket, ancla::DatabaseError>>,
{
    let mut buckets: Vec<Bucket> = Vec::new();
    loop {
        let item = peek_iter.peek().cloned();
        match item {
            None => {
                break;
            }
            Some(Ok(bucket)) => match bucket.depth.cmp(&depth) {
                std::cmp::Ordering::Less => {
                    break;
                }
                std::cmp::Ordering::Equal => {
                    peek_iter.next();
                    buckets.push(Bucket {
                        id: bucket.id(),
                        identifier: bucket.identifier.clone(),
                        is_inline: bucket.is_inline,
                        depth: bucket.depth,
                        parent: bucket.parent,
                        child_buckets: iter_buckets_inner(peek_iter, depth + 1),
                    });
                }
                std::cmp::Ordering::Greater => {
                    panic!(
                        "unexpect depth {} at bucket {}, parent depth is {}",
                        bucket.depth,
                        String::from_utf8(bucket.identifier.name.clone()).unwrap(),
                        depth,
                    )
                }
            },
            Some(Err(e)) => panic!("unexepct err {e}"),
        }
    }

    buckets
}

/// Iterates over all buckets in the database and constructs a flat list of `Bucket` structs.
/// This function is the entry point for retrieving bucket data from the `ancla::DB`.
///
/// # Arguments
///
/// * `db` - A `ancla::DB` instance to retrieve buckets from.
///
/// # Returns
///
/// A vector of `Bucket` structs, representing all buckets in the database.
///
/// # Panics
///
/// This function will panic if a root-level bucket has a non-zero depth or if there's a database error.
fn iter_buckets(db: ancla::DB) -> Vec<Bucket> {
    let mut buckets = Vec::<Bucket>::new();
    let mut peek_iter = db.iter_buckets().peekable();

    loop {
        let item = peek_iter.next();
        if item.is_none() {
            break;
        }

        match item {
            None => break,
            Some(Ok(bucket)) => {
                if bucket.depth != 0 {
                    panic!(
                        "unexpect depth {} at bucket {}, it must be 0",
                        bucket.depth,
                        String::from_utf8(bucket.identifier.name.clone()).unwrap()
                    )
                }

                buckets.push(Bucket {
                    id: bucket.id(),
                    identifier: bucket.identifier.clone(),
                    is_inline: bucket.is_inline,
                    depth: bucket.depth,
                    parent: bucket.parent,
                    child_buckets: iter_buckets_inner(&mut peek_iter, 1),
                });
            }
            Some(Err(e)) => panic!("unexpect err {e}",),
        }
    }

    buckets
}

/// Recursively prints the bucket tree to the console using `comfy_table` for formatting.
///
/// # Arguments
///
/// * `buckets` - A slice of `Bucket` structs to print.
/// * `table` - A mutable reference to the `comfy_table::Table` to add rows to.
/// * `level` - The current nesting level, used for indentation.
fn print_buckets_inner(buckets: &[Bucket], table: &mut comfy_table::Table, level: usize) {
    for (i, bucket) in buckets.iter().enumerate() {
        let chr = if i == buckets.len() - 1 { '└' } else { '├' };

        table.add_row(vec![
            format!(
                "{chr:>level$}{}",
                String::from_utf8(bucket.identifier.name.clone()).unwrap()
            ),
            bucket.id.to_string(),
            bucket.identifier.page_id.to_string(),
            bucket.is_inline.to_string(),
            bucket.depth.to_string(),
            bucket.parent.clone().map(|p| p.id()).unwrap_or_default(),
            String::from_utf8(bucket.parent.clone().map(|p| p.name).unwrap_or_default()).unwrap(),
        ]);
        print_buckets_inner(&bucket.child_buckets, table, level + 1);
    }
}

/// Prints the top-level buckets and their hierarchical structure to the console.
/// This function initializes the table and then calls `print_buckets_inner` to populate it.
///
/// # Arguments
///
/// * `buckets` - A reference to a vector of top-level `Bucket` structs to print.
fn print_buckets(buckets: &Vec<Bucket>) {
    let mut buckets_table = Table::new();
    buckets_table.set_content_arrangement(comfy_table::ContentArrangement::Dynamic);
    buckets_table.load_preset(NOTHING);
    buckets_table.enforce_styling();
    buckets_table.set_header(vec![
        "BUCKET-NAME",
        "ID",
        "PAGE-ID",
        "IS-INLINE",
        "DEPTH",
        "PARENT-ID",
        "PARENT-NAME",
    ]);

    for bucket in buckets {
        buckets_table.add_row(vec![
            String::from_utf8(bucket.identifier.name.clone()).unwrap(),
            bucket.id.to_string(),
            bucket.identifier.page_id.to_string(),
            bucket.is_inline.to_string(),
            bucket.depth.to_string(),
            bucket.parent.clone().map(|p| p.id()).unwrap_or_default(),
            String::from_utf8(bucket.parent.clone().map(|p| p.name).unwrap_or_default()).unwrap(),
        ]);

        print_buckets_inner(&bucket.child_buckets, &mut buckets_table, 1);
    }
    println!("{buckets_table}");
}
