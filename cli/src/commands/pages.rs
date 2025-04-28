use anyhow::Result;
use clap::Parser;
use cling::prelude::*;
use comfy_table::Table;

#[derive(Run, Parser, Collect, Clone)]
#[cling(run = "run_pages")]
pub struct PageCommand {}

pub fn run_pages(
    _state: State<crate::cli_env::Env>,
    _args: &PageCommand,
    common_opts: &crate::opts::CommonOpts,
) -> Result<()> {
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
