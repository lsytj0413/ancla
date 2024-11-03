use clap::Parser;
use std::error::Error;
use std::path::Path;
use std::result::Result;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Command {
    db: String,

    #[arg(short, long, default_value_t = false)]
    verbose: bool,

    #[arg(short, long)]
    page_size: Option<u32>,

    #[arg(short, long)]
    endian: Option<Endian>,
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
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("testdata")
                .join("test.db")
                .to_str()
                .unwrap()
                .to_string(),
        )
        .build();
    let mut db = ancla::DB::build(options);
    db.print_db();
    Ok(())
}
