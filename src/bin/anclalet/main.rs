use clap::{Args, Parser, Subcommand};
use comfy_table::presets::NOTHING;
use comfy_table::Table;
use std::cell::RefCell;
use std::error::Error;
use std::rc::Rc;
use std::result::Result;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None, subcommand_precedence_over_arg = true)]
struct Command {
    #[arg(short, long, default_value_t = false)]
    verbose: bool,

    #[arg(short, long)]
    page_size: Option<u32>,

    #[arg(short, long)]
    endian: Option<Endian>,

    #[clap(subcommand)]
    command: SubCommand,

    #[arg(global = true, required = false)]
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
    Info {},
    KV {
        #[arg(long)]
        buckets: Vec<String>,
        #[arg(long)]
        key: String,
    },
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

fn print_buckets_inner(buckets: &[Bucket], table: &mut comfy_table::Table, level: usize) {
    for (i, bucket) in buckets.iter().enumerate() {
        let chr = if i == buckets.len() - 1 { '└' } else { '├' };

        let bucket_name = String::from_utf8(bucket.name.clone()).unwrap();
        assert!(bucket.name.len() > 2, "{}", bucket_name);
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
    let db = ancla::DB::build(options);

    match cli.command {
        SubCommand::Buckets(_) => {
            let buckets = iter_buckets(db);
            print_buckets(&buckets);
        }
        SubCommand::Pages {} => {
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
        }
        SubCommand::Info {} => {
            let info = ancla::DB::info(db);
            println!("Page Size: {:?}", info.page_size);
        }
        // kv --key bucket0_key0 --buckets bucket0
        // 可以通过增加 --buckets 指定多个 bucket
        SubCommand::KV { buckets, key } => {
            let kv = ancla::DB::get_key_value(db, &buckets, &key);
            if let Some(kv) = kv {
                println!("Key: {:?}", String::from_utf8(kv.key));
                println!("Value: {:?}", String::from_utf8(kv.value));
            } else {
                println!("Key not found");
            }
        }
    }

    Ok(())
}
