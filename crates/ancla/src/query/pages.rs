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

use std::{any::Any, collections::HashMap, fmt, pin::Pin, sync::Arc};

use crate::{
    db::{PageInfo, DB},
    DatabaseError,
};
use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{ArrayRef, StringBuilder, UInt64Builder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{TableProvider, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    execution::{context::SessionState, TaskContext},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionMode,
        ExecutionPlan, Partitioning, PlanProperties, SendableRecordBatchStream,
    },
    prelude::Expr,
};
use futures::Stream;

/// `PagesTableProvider` implements DataFusion's `TableProvider` trait for `ancla`'s page data.
/// It defines the schema of the `pages` table and how to create an execution plan
/// for scanning the page data.
pub struct PagesTableProvider {
    db: DB,
}

impl PagesTableProvider {
    /// Creates a new `PagesTableProvider` instance.
    ///
    /// # Arguments
    ///
    /// * `db` - A `DB` instance used to access the underlying BoltDB.
    pub fn new(db: DB) -> Self {
        Self { db }
    }
}

#[async_trait]
impl TableProvider for PagesTableProvider {
    /// Returns a reference to the `Any` trait object, allowing downcasting.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Defines the schema of the `pages` table.
    /// This schema is used by DataFusion to validate SQL queries and understand
    /// the structure of the data.
    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("typ", DataType::Utf8, false),
            Field::new("overflow", DataType::UInt64, false),
            Field::new("capacity", DataType::UInt64, false),
            Field::new("used", DataType::UInt64, false),
            Field::new("parent_page_id", DataType::UInt64, true),
        ]))
    }

    /// Returns the type of the table, which is `Base` for a fundamental data source.
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Creates an `ExecutionPlan` for scanning the page data.
    /// This method is called by DataFusion's query optimizer to build the physical plan.
    /// It receives information about projections (columns to select), filters (WHERE clauses),
    /// and limits, which can be used to optimize data retrieval.
    ///
    /// # Arguments
    ///
    /// * `_state` - The DataFusion session state (unused in this implementation).
    /// * `projection` - Optional list of column indices to project. If `None`, all columns are projected.
    /// * `_filters` - List of filter expressions (not yet pushed down to the scanner in this implementation).
    /// * `limit` - Optional limit on the number of rows to return.
    ///
    /// # Returns
    ///
    /// An `Arc` to an `ExecutionPlan` that will perform the actual data scan.
    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let schema = self.schema();
        // Determine the schema of the data after applying the projection.
        let projected_schema = if let Some(projection) = projection {
            let fields = schema.fields();
            let projected_fields: Vec<_> = projection.iter().map(|i| fields[*i].clone()).collect();
            Arc::new(Schema::new(projected_fields))
        } else {
            // If no projection is specified, use the full schema.
            schema
        };

        // Create and return a `PagesScanExec` which is the physical operator
        // responsible for reading the page data.
        Ok(Arc::new(PagesScanExec::new(
            self.db.clone(), // Clone DB to pass to the execution plan
            projected_schema,
            limit,
        )))
    }
}

/// `PagesScanExec` is a physical operator that scans `ancla`'s page data.
/// It implements DataFusion's `ExecutionPlan` trait, defining how to execute
/// the scan operation and produce `RecordBatch`es.
#[derive(Debug)]
struct PagesScanExec {
    db: DB,
    projected_schema: SchemaRef,
    limit: Option<usize>,
    properties: PlanProperties,
}

impl PagesScanExec {
    /// Creates a new `PagesScanExec` instance.
    ///
    /// # Arguments
    ///
    /// * `db` - A `DB` instance to access the underlying BoltDB.
    /// * `projected_schema` - The schema of the data that this operator will produce.
    /// * `limit` - An optional limit on the number of rows to read.
    pub fn new(db: DB, projected_schema: SchemaRef, limit: Option<usize>) -> Self {
        // Define the properties of this execution plan, which are used by DataFusion
        // for optimization and scheduling.
        let partitioning = Partitioning::UnknownPartitioning(1); // No specific partitioning
        let equivalence = EquivalenceProperties::new(projected_schema.clone());
        let properties = PlanProperties::new(equivalence, partitioning, ExecutionMode::Bounded);
        Self {
            db,
            projected_schema,
            limit,
            properties,
        }
    }
}

impl DisplayAs for PagesScanExec {
    /// Formats the `PagesScanExec` for display in query plans.
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PagesScanExec, limit={:?}, projection={:?}",
            self.limit,
            self.projected_schema
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>()
        )
    }
}

#[async_trait]
impl ExecutionPlan for PagesScanExec {
    /// Returns a reference to the `Any` trait object, allowing downcasting.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns the name of this execution plan node.
    fn name(&self) -> &str {
        "PagesScanExec"
    }

    /// Returns the properties of this execution plan node.
    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    /// Returns the schema of the `RecordBatch`es produced by this operator.
    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    /// Returns the children of this execution plan node (none for a scan operator).
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    /// Creates a new instance of this execution plan with new children (not applicable for scan).
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    /// Executes the scan operation and returns a stream of `RecordBatch`es.
    /// This is where the actual data reading from BoltDB happens.
    ///
    /// # Arguments
    ///
    /// * `_partition` - The partition index (unused as we have a single partition).
    /// * `_context` - The task context (unused in this implementation).
    ///
    /// # Returns
    ///
    /// A `SendableRecordBatchStream` which yields `RecordBatch`es containing page data.
    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        // Create a new `PagesStream` to read data from the database.
        let stream = Box::pin(PagesStream::new(
            self.db.clone(), // Clone DB for the stream
            self.projected_schema.clone(),
            self.limit,
        ));
        // Wrap the stream in a `RecordBatchStreamAdapter` to conform to DataFusion's interface.
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

/// `PagesStream` is an asynchronous stream of `RecordBatch`es for page data.
/// It reads `PageInfo` from the BoltDB and converts them into Arrow `RecordBatch`es.
struct PagesStream {
    projected_schema: SchemaRef,
    limit: Option<usize>,
    // An iterator over `PageInfo` results from the database.
    iterator: Box<dyn Iterator<Item = Result<PageInfo, DatabaseError>> + Send>,
    processed_count: usize,
}

impl PagesStream {
    /// Defines the batch size for `RecordBatch`es produced by this stream.
    const BATCH_SIZE: usize = 1024;

    /// Creates a new `PagesStream` instance.
    ///
    /// # Arguments
    ///
    /// * `db` - A `DB` instance to access the underlying BoltDB.
    /// * `projected_schema` - The schema of the `RecordBatch`es to produce.
    /// * `limit` - An optional limit on the total number of rows to return.
    fn new(db: DB, projected_schema: SchemaRef, limit: Option<usize>) -> Self {
        Self {
            projected_schema,
            limit,
            // Initialize the iterator to read pages from the database.
            iterator: Box::new(db.iter_pages()),
            processed_count: 0,
        }
    }

    /// Builds a `RecordBatch` from a vector of `PageInfo` structs.
    /// This function converts native Rust structs into Arrow's columnar format.
    ///
    /// # Arguments
    ///
    /// * `batch` - A vector of `PageInfo` structs to convert.
    ///
    /// # Returns
    ///
    /// A `RecordBatch` containing the converted data.
    fn build_record_batch(&self, batch: Vec<PageInfo>) -> Result<RecordBatch, DataFusionError> {
        let mut columns: HashMap<&str, ArrayRef> = HashMap::new();
        // Collect the names of the fields that are part of the projected schema.
        let field_names: Vec<&str> = self
            .projected_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();

        // Iterate over each field name in the projected schema and build the corresponding Arrow array.
        for field_name in field_names {
            let array: ArrayRef = match field_name {
                "id" => {
                    let mut builder = UInt64Builder::with_capacity(batch.len());
                    for p in &batch {
                        builder.append_value(p.id);
                    }
                    Arc::new(builder.finish())
                }
                "typ" => {
                    let mut builder = StringBuilder::new();
                    for p in &batch {
                        // Convert PageType enum to its debug string representation for storage as Utf8.
                        builder.append_value(format!("{:?}", p.typ));
                    }
                    Arc::new(builder.finish())
                }
                "overflow" => {
                    let mut builder = UInt64Builder::with_capacity(batch.len());
                    for p in &batch {
                        builder.append_value(p.overflow);
                    }
                    Arc::new(builder.finish())
                }
                "capacity" => {
                    let mut builder = UInt64Builder::with_capacity(batch.len());
                    for p in &batch {
                        builder.append_value(p.capacity);
                    }
                    Arc::new(builder.finish())
                }
                "used" => {
                    let mut builder = UInt64Builder::with_capacity(batch.len());
                    for p in &batch {
                        builder.append_value(p.used);
                    }
                    Arc::new(builder.finish())
                }
                "parent_page_id" => {
                    let mut builder = UInt64Builder::with_capacity(batch.len());
                    for p in &batch {
                        // Handle optional parent_page_id, appending None for null values.
                        builder.append_option(p.parent_page_id);
                    }
                    Arc::new(builder.finish())
                }
                _ => {
                    // This case should ideally not be reached if the projected schema is valid.
                    return Err(DataFusionError::Internal(format!(
                        "Unknown column {field_name}"
                    )));
                }
            };
            columns.insert(field_name, array);
        }

        // Collect the built arrays in the order defined by the projected schema.
        let arrays = self
            .projected_schema
            .fields()
            .iter()
            .map(|f| columns.get(f.name().as_str()).unwrap().clone())
            .collect();

        // Create a new RecordBatch from the projected schema and the arrays.
        RecordBatch::try_new(self.projected_schema.clone(), arrays).map_err(DataFusionError::from)
    }
}

impl Stream for PagesStream {
    // The type of items yielded by this stream, which are DataFusion results containing RecordBatch.
    type Item = DataFusionResult<RecordBatch>;

    /// Polls for the next `RecordBatch` from the stream.
    /// This method is called by DataFusion's execution engine to retrieve data.
    /// It reads `PageInfo` structs from the underlying iterator, batches them,
    /// converts them to `RecordBatch`es, and handles limits.
    ///
    /// # Arguments
    ///
    /// * `self` - A pinned mutable reference to the stream itself.
    /// * `_cx` - The task context (unused in this synchronous iteration).
    ///
    /// # Returns
    ///
    /// A `Poll` indicating whether a `RecordBatch` is ready, the stream is exhausted,
    /// or if it's pending.
    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        // Check if the total limit has been reached.
        if let Some(limit) = self.limit {
            if self.processed_count >= limit {
                return std::task::Poll::Ready(None);
            }
        }

        // Determine the batch size, respecting the overall limit.
        let batch_size = if let Some(limit) = self.limit {
            (limit - self.processed_count).min(Self::BATCH_SIZE)
        } else {
            Self::BATCH_SIZE
        };

        let mut batch = Vec::with_capacity(batch_size);
        // Read `PageInfo` structs up to the determined batch size.
        for _ in 0..batch_size {
            match self.iterator.next() {
                Some(Ok(page_info)) => {
                    batch.push(page_info);
                    self.processed_count += 1;
                    // Check limit again after adding each item, in case the limit is hit mid-batch.
                    if let Some(limit) = self.limit {
                        if self.processed_count >= limit {
                            break;
                        }
                    }
                }
                Some(Err(e)) => {
                    // If an error occurs during iteration, return it as a DataFusionError.
                    return std::task::Poll::Ready(Some(Err(DataFusionError::Execution(
                        e.to_string(),
                    ))));
                }
                None => break, // End of iterator
            }
        }

        // If no items were collected in this batch, the stream is finished.
        if batch.is_empty() {
            std::task::Poll::Ready(None)
        } else {
            // Otherwise, build a RecordBatch from the collected items and return it.
            std::task::Poll::Ready(Some(self.build_record_batch(batch)))
        }
    }
}
