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

use datafusion::{datasource::TableProvider, prelude::SessionContext};

/// `QueryEngine` wraps DataFusion's `SessionContext` to provide SQL query capabilities.
/// It manages the registration of data sources as tables and executes SQL queries against them.
pub struct QueryEngine {
    ctx: SessionContext,
}

impl QueryEngine {
    /// Creates a new `QueryEngine` instance.
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
        }
    }

    /// Registers a `TableProvider` with the `QueryEngine`.
    /// Once registered, the data provided by `provider` can be queried using SQL
    /// under the given `table_name`.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name under which the table will be accessible in SQL queries.
    /// * `provider` - An `Arc` to an object implementing `TableProvider`, which defines
    ///   how DataFusion can access and scan the data.
    pub fn register_table(
        &self,
        table_name: &'static str,
        provider: Arc<dyn TableProvider>,
    ) -> Result<(), datafusion::error::DataFusionError> {
        // DataFusion's `register_table` returns `Result<Option<Arc<dyn TableProvider>>, ...>`
        // We only care about success or failure, so we map the `Ok` variant to `()`.
        self.ctx.register_table(table_name, provider).map(|_| ())
    }

    /// Returns a reference to the underlying DataFusion `SessionContext`.
    /// This allows direct interaction with DataFusion's API for more advanced use cases.
    pub fn context(&self) -> &SessionContext {
        &self.ctx
    }
}

impl Default for QueryEngine {
    fn default() -> Self {
        Self::new()
    }
}
