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

use std::sync::Arc;

use ancla::query::{buckets::BucketsTableProvider, engine::QueryEngine, pages::PagesTableProvider};
use anyhow::Result;
use cling::prelude::*;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::print_batches;

/// `QueryCommand` defines the command-line interface for executing SQL queries.
/// It uses `clap` for argument parsing and `cling` for command execution.
#[derive(Run, Parser, Clone, Default, Collect)]
#[cling(run = "run")]
pub struct QueryCommand {
    /// The SQL query to execute.
    /// This field will capture the SQL string provided by the user on the command line.
    #[clap(verbatim_doc_comment)]
    sql: String,
}

/// The main execution logic for the `query` command.
/// This function is asynchronous and will be run within a Tokio runtime provided by `cling`.
/// It sets up the DataFusion engine, registers the `pages` table, executes the SQL query,
/// and prints the results.
///
/// # Arguments
///
/// * `env` - The application environment, containing the `DB` instance.
/// * `me` - The `QueryCommand` instance, holding the SQL query string.
///
/// # Returns
///
/// A `Result` indicating success or failure of the query execution.
async fn run(env: State<crate::cli_env::Env>, me: &QueryCommand) -> Result<()> {
    // Initialize the DataFusion query engine.
    let engine = QueryEngine::new();

    // Register the `pages` table with the query engine.
    // `PagesTableProvider` is responsible for providing DataFusion with access to the BoltDB page data.
    // `env.0.db.clone()` creates a new `DB` instance that shares the underlying database connection.
    engine.register_table("pages", Arc::new(PagesTableProvider::new(env.0.db.clone())))?;
    // Register the `buckets` table with the query engine.
    engine.register_table(
        "buckets",
        Arc::new(BucketsTableProvider::new(env.0.db.clone())),
    )?;

    // Execute the SQL query using the DataFusion context.
    // This returns a DataFrame, which represents the logical plan of the query.
    let df = engine.context().sql(&me.sql).await?;

    // Collect the results of the query into a vector of `RecordBatch`es.
    // This triggers the physical execution of the query.
    let results: Vec<RecordBatch> = df.collect().await?;

    // Print the collected `RecordBatch`es to the console in a human-readable format.
    print_batches(results.as_slice())?;

    Ok(())
}
