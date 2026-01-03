// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Context-free source catalog for DataFusion logical planning.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use datafusion::logical_expr::TableSource;

use crate::config::{NodeMapping, RelationshipMapping};

/// A minimal catalog to resolve node labels and relationship types to logical table sources.
///
/// This trait also provides optional methods for retrieving node and relationship mappings,
/// allowing catalog implementations to serve as the source of schema metadata.
pub trait GraphSourceCatalog: Send + Sync {
    /// Get the table source for a node label.
    fn node_source(&self, label: &str) -> Option<Arc<dyn TableSource>>;

    /// Get the table source for a relationship type.
    fn relationship_source(&self, rel_type: &str) -> Option<Arc<dyn TableSource>>;

    /// Get the relationship mapping for a given relationship type.
    ///
    /// Default implementation returns `None`. Override this to provide
    /// relationship mappings from the catalog instead of config.
    fn get_relationship_mapping(&self, _rel_type: &str) -> Option<RelationshipMapping> {
        None
    }

    /// Get the node mapping for a given label.
    ///
    /// Default implementation returns `None`. Override this to provide
    /// node mappings from the catalog instead of config.
    fn get_node_mapping(&self, _label: &str) -> Option<NodeMapping> {
        None
    }
}

/// A simple in-memory catalog useful for tests and bootstrap wiring.
pub struct InMemoryCatalog {
    node_sources: HashMap<String, Arc<dyn TableSource>>,
    rel_sources: HashMap<String, Arc<dyn TableSource>>,
}

impl InMemoryCatalog {
    pub fn new() -> Self {
        Self {
            node_sources: HashMap::new(),
            rel_sources: HashMap::new(),
        }
    }

    pub fn with_node_source(
        mut self,
        label: impl Into<String>,
        source: Arc<dyn TableSource>,
    ) -> Self {
        self.node_sources.insert(label.into(), source);
        self
    }

    pub fn with_relationship_source(
        mut self,
        rel_type: impl Into<String>,
        source: Arc<dyn TableSource>,
    ) -> Self {
        self.rel_sources.insert(rel_type.into(), source);
        self
    }
}

impl Default for InMemoryCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl GraphSourceCatalog for InMemoryCatalog {
    fn node_source(&self, label: &str) -> Option<Arc<dyn TableSource>> {
        self.node_sources.get(label).cloned()
    }

    fn relationship_source(&self, rel_type: &str) -> Option<Arc<dyn TableSource>> {
        self.rel_sources.get(rel_type).cloned()
    }
}

/// A trivial logical table source with a fixed schema.
pub struct SimpleTableSource {
    schema: SchemaRef,
}

impl SimpleTableSource {
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
    pub fn empty() -> Self {
        Self {
            schema: Arc::new(Schema::empty()),
        }
    }
}

impl TableSource for SimpleTableSource {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
