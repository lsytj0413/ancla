use clap::{Args, Parser, Subcommand};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::error::Error;
use std::rc::Rc;
use std::result::Result;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Command {
    #[arg(short, long, default_value_t = false)]
    verbose: bool,

    #[arg(short, long)]
    page_size: Option<u32>,

    #[arg(short, long)]
    endian: Option<Endian>,

    #[clap(subcommand)]
    command: SubCommand,

    db: String,
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum Endian {
    Little,
    Big,
}

#[derive(Debug, Subcommand)]
enum SubCommand {
    Buckets(BucketsArgs),
    Pages {},
}

#[derive(Debug, Args)]
struct BucketsArgs {}

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
            name: bucket.name.clone(),
            page_id: bucket.page_id,
            is_inline: bucket.is_inline,
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

fn print_buckets(buckets: &Vec<Bucket>, level: usize) {
    for bucket in buckets {
        println!(
            "{}{}, {}, {}",
            '-'.to_string().repeat(level),
            String::from_utf8(bucket.name.clone()).unwrap(),
            bucket.is_inline,
            bucket.page_id
        );
        print_buckets(&bucket.child_buckets, level + 2);
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut cli = Command::parse();

    if cli.endian.is_none() {
        if is_target_little_endian() {
            cli.endian = Some(Endian::Little);
        } else {
            cli.endian = Some(Endian::Big);
        }
    }

    println!("{:?}", cli);
    println!("{:?}", page_size::get());

    let options = ancla::AnclaOptions::builder()
        .db_path(
            // Path::new(env!("CARGO_MANIFEST_DIR"))
            //     .join("testdata")
            //     .join("test1.db")
            //     .to_str()
            //     .unwrap()
            //     .to_string(),
            cli.db,
        )
        .build();
    let mut db = ancla::DB::build(options);

    match cli.command {
        SubCommand::Buckets(_) => {
            let buckets = iter_buckets(db);
            print_buckets(&buckets, 0);
        }
        SubCommand::Pages {} => {
            db.borrow_mut().print_db();
        }
    }

    Ok(())
}
