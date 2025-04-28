use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use clap_verbosity_flag::{LogLevel, VerbosityFilter};
use cling::prelude::*;
use comfy_table::presets::NOTHING;
use comfy_table::Table;
use std::cell::RefCell;
use std::rc::Rc;

#[derive(Run, Parser, Clone)]
#[cling(run = "init")]
pub struct App {
    #[clap(flatten)]
    pub common_opts: CommonOpts,

    #[clap(subcommand)]
    pub cmd: Commands,
}

#[derive(Run, Subcommand, Clone)]
pub enum Commands {
    /// List all buckets
    Buckets(BucketsCommand),
    /// List all pages
    Pages(PageCommand),
    /// Show info about the database
    Info(InfoCommand),
    /// Get key value
    KV(KVCommand),
}

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_buckets")]
pub struct BucketsCommand {}

pub fn run_buckets(
    _state: State<Env>,
    _args: &BucketsCommand,
    common_opts: &CommonOpts,
) -> Result<()> {
    let options = ancla::AnclaOptions::builder()
        .db_path(common_opts.db.clone())
        .build();
    let db = ancla::DB::build(options);
    let buckets = iter_buckets(db);
    print_buckets(&buckets);

    Ok(())
}

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_pages")]
pub struct PageCommand {}

pub fn run_pages(_state: State<Env>, _args: &PageCommand, common_opts: &CommonOpts) -> Result<()> {
    // This function is just a placeholder for the actual implementation
    let options = ancla::AnclaOptions::builder()
        .db_path(common_opts.db.clone())
        .build();
    let db = ancla::DB::build(options);

    let mut pages: Vec<ancla::PageInfo> = ancla::DB::iter_pages(db).collect();
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
            comfy_table::Cell::new(p.used),
            comfy_table::Cell::new(format!("{:?}", p.parent_page_id)),
        ]);
    });
    println!("{pages_table}");
    Ok(())
}

#[derive(Run, Parser, Collect, Clone, Debug)]
#[cling(run = "run_kv")]
pub struct KVCommand {
    #[clap(long)]
    pub buckets: Vec<String>,
    #[clap(long)]
    pub key: String,
}

pub fn run_kv(_state: State<Env>, args: &KVCommand, common_opts: &CommonOpts) -> Result<()> {
    println!("{:?}", args);

    let options = ancla::AnclaOptions::builder()
        .db_path(common_opts.db.clone())
        .build();
    let db = ancla::DB::build(options);

    let kv = ancla::DB::get_key_value(db, &args.buckets, &args.key);
    if let Some(kv) = kv {
        println!("Key: {:?}", String::from_utf8(kv.key));
        println!("Value: {:?}", String::from_utf8(kv.value));
    } else {
        println!("Key not found");
    }
    Ok(())
}

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_info")]
pub struct InfoCommand {}

pub fn run_info(_state: State<Env>, _args: &InfoCommand, common_opts: &CommonOpts) -> Result<()> {
    let options = ancla::AnclaOptions::builder()
        .db_path(common_opts.db.clone())
        .build();
    let db = ancla::DB::build(options);
    let info = ancla::DB::info(db);
    println!("Page Size: {:?}", info.page_size);
    Ok(())
}

#[derive(Clone, Default)]
pub(crate) struct Quiet;
impl LogLevel for Quiet {
    fn default_filter() -> VerbosityFilter {
        VerbosityFilter::Error
    }

    fn verbose_long_help() -> Option<&'static str> {
        None
    }

    fn quiet_help() -> Option<&'static str> {
        None
    }

    fn quiet_long_help() -> Option<&'static str> {
        None
    }
}

#[derive(Args, Collect, Clone, Default)]
pub struct CommonOpts {
    #[clap(flatten)]
    pub(crate) verbose: clap_verbosity_flag::Verbosity<Quiet>,

    #[arg(long)]
    pub(crate) db: String,
}

#[derive(Clone, Debug)]
pub struct Env {}

fn init() -> Result<State<Env>> {
    Ok(State(Env {}))
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum Endian {
    Little,
    Big,
}

const fn is_target_little_endian() -> bool {
    // cfg!(target_endian = "little")
    u16::from_ne_bytes([1, 0]) == 1
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

#[tokio::main(flavor = "multi_thread")]
async fn main() -> ClingFinished<App> {
    Cling::parse_and_run().await
}
