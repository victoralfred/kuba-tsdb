//! Tag Resolution and Bitmap Index Module
//!
//! This module provides efficient tag-based series lookup using bitmap indexes.
//! It supports O(1) single-tag lookups and O(n) multi-tag queries via bitwise operations.
//!
//! # Components
//!
//! - **bitmap**: Low-level bitmap implementation for tracking series membership
//! - **tag_resolver**: High-level API integrating MetadataStore with BitmapIndex

pub mod bitmap;
pub mod tag_resolver;

// Re-export main types
pub use bitmap::{BitmapIndex, BitmapIndexStats, BitmapIterator, TagBitmap, TagFilter};
pub use tag_resolver::{
    MatchMode, ResolvedSeries, TagMatcher, TagResolver, TagResolverConfig, TagResolverStats,
    TagResolverStatsSnapshot,
};
