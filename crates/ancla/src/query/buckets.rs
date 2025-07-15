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

use std::any::Any;
use std::sync::Arc;

use crate::db::DB;
use async_trait::async_trait;
use datafusion::arrow::array::builder::{BooleanBuilder, StringBuilder, UInt64Builder};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;

/// A `TableProvider` for the buckets data.
///
/// This struct is responsible for providing the buckets data to the DataFusion query engine.
/// It wraps a `DB` instance and implements the `TableProvider` trait, allowing it to be
/// registered as a table in DataFusion.
///
/// The provider uses a full-batch loading approach (`MemoryExec`) because the total number
/// of buckets in a typical BoltDB file is expected to be small enough to fit comfortably
/// in memory. This simplifies the implementation compared to a streaming approach.
pub struct BucketsTableProvider {
    db: DB,
}

impl BucketsTableProvider {
    /// Creates a new `BucketsTableProvider`.
    ///
    /// # Arguments
    ///
    /// * `db` - A `DB` instance used to access the underlying BoltDB file.
    pub fn new(db: DB) -> Self {
        Self { db }
    }
}

#[async_trait]
impl TableProvider for BucketsTableProvider {
    /// Returns a reference to the `Any` trait object, allowing for dynamic type casting.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Defines and returns the schema for the "buckets" table.
    ///
    /// The schema specifies the column names, data types, and nullability, which is essential
    /// for DataFusion's query planner and type checking.
    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("page_id", DataType::UInt64, false),
            Field::new("is_inline", DataType::Boolean, false),
            Field::new("depth", DataType::UInt64, false),
            Field::new("parent_id", DataType::Utf8, true),
            Field::new("parent_name", DataType::Utf8, true),
        ]))
    }

    /// Returns the type of the table, which is `Base` for a regular data table.
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Creates an `ExecutionPlan` for scanning the bucket data.
    ///
    /// This method is called by the DataFusion query planner to generate the physical plan.
    /// It reads all buckets from the database, converts them into a single `RecordBatch`,
    /// and wraps it in a `MemoryExec` node. This approach is chosen for its simplicity,
    /// under the assumption that the number of buckets is manageably small.
    ///
    /// # Arguments
    ///
    /// * `_state`: The current session state (unused).
    /// * `projection`: An optional list of column indices to read. This is used to
    ///   optimize the scan by only creating the required columns.
    /// * `_filters`: Filter expressions (not pushed down in this implementation).
    /// * `_limit`: A row limit (not pushed down in this implementation).
    ///
    /// You can use the following SQL query to select all nodes under an expected node:
    /// ```sql
    /// WITH RECURSIVE descendants AS (
    ///   -- This is the starting point: find the direct children of the parent
    ///   SELECT id, name, page_id, is_inline, depth, parent_id, parent_name
    ///   FROM buckets
    ///   WHERE parent_id = 'bbb05/36'
    ///
    ///   UNION ALL
    ///
    ///   -- This is the recursive part: join the buckets table with the descendants we've already found
    ///   SELECT b.id, b.name, b.page_id, b.is_inline, b.depth, b.parent_id, b.parent_name
    ///   FROM buckets b
    ///   INNER JOIN descendants d ON b.parent_id = d.id
    /// )
    /// -- Finally, select all the rows we found
    /// SELECT * FROM descendants;
    /// ```
    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[datafusion::logical_expr::Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Clone the database handle to ensure thread-safe access.
        let db = self.db.clone();
        // Eagerly collect all buckets into a vector in memory.
        let buckets: Vec<_> = db.iter_buckets().map(|b| b.unwrap()).collect();

        let schema = self.schema();
        let mut id_builder = StringBuilder::new();
        let mut name_builder = StringBuilder::new();
        let mut page_id_builder = UInt64Builder::new();
        let mut is_inline_builder = BooleanBuilder::new();
        let mut depth_builder = UInt64Builder::new();
        let mut parent_id_builder = StringBuilder::new();
        let mut parent_name_builder = StringBuilder::new();

        // Iterate over the in-memory vector of buckets and populate the Arrow array builders.
        for bucket in buckets {
            id_builder.append_value(bucket.id);
            name_builder.append_value(String::from_utf8(bucket.name).unwrap_or_default());
            page_id_builder.append_value(bucket.page_id);
            is_inline_builder.append_value(bucket.is_inline);
            depth_builder.append_value(bucket.depth);
            if let Some(parent_id) = bucket.parent_id {
                parent_id_builder.append_value(parent_id);
            } else {
                parent_id_builder.append_null();
            }
            if let Some(parent_name) = bucket.parent_name {
                parent_name_builder
                    .append_value(String::from_utf8(parent_name).unwrap_or_default());
            } else {
                parent_name_builder.append_null();
            }
        }

        // Create a single `RecordBatch` containing all the bucket data.
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_builder.finish()),
                Arc::new(name_builder.finish()),
                Arc::new(page_id_builder.finish()),
                Arc::new(is_inline_builder.finish()),
                Arc::new(depth_builder.finish()),
                Arc::new(parent_id_builder.finish()),
                Arc::new(parent_name_builder.finish()),
            ],
        )?;

        // Create a `MemoryExec` node, which is an execution plan that serves data
        // from an in-memory `RecordBatch`.
        let exec = MemoryExec::try_new(&[vec![batch]], schema, projection.cloned())?;
        Ok(Arc::new(exec))
    }
}
