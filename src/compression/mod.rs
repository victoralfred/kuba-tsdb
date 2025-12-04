//! Compression implementations
//!
//! This module provides compression algorithms for time-series data:
//!
//! - **Kuba**: The default XOR-based compression (Facebook Gorilla algorithm)
//! - **AHPAC**: Adaptive compression that selects the best codec per chunk

pub mod ahpac;
pub mod bit_stream;
pub mod kuba;

pub use ahpac::AhpacCompressor;
pub use kuba::KubaCompressor;
