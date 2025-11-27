//! Spill file management
//!
//! Handles individual spill files including creation, writing, reading,
//! and compression/decompression.

use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use tracing::debug;

use super::config::SpillConfig;
use super::error::{SpillError, SpillResult};
use crate::types::DataPoint;

/// Spill file ID type
pub type SpillFileId = u64;

/// Spill file header magic number
const SPILL_MAGIC: u32 = 0x5350494C; // "SPIL"

/// Spill file version
const SPILL_VERSION: u16 = 1;

/// Header size in bytes
const HEADER_SIZE: usize = 24;

/// Spill file for storing overflow data
pub struct SpillFile {
    /// File ID
    id: SpillFileId,
    /// File path
    path: PathBuf,
    /// Whether compression is enabled
    compressed: bool,
    /// Number of points in file
    point_count: usize,
    /// File size in bytes
    size: usize,
}

impl SpillFile {
    /// Create a new spill file and write data
    ///
    /// # Arguments
    ///
    /// * `config` - Spill configuration
    /// * `id` - File ID
    /// * `points` - Data points to write
    pub fn create(
        config: &SpillConfig,
        id: SpillFileId,
        points: &[DataPoint],
    ) -> SpillResult<Self> {
        let path = config.file_path(id);
        let compressed = config.compression_enabled;

        // Ensure directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| SpillError::Io {
                source: e,
                context: format!("creating spill directory {:?}", parent),
            })?;
        }

        // Serialize points
        let data = Self::serialize_points(points);

        // Optionally compress
        let final_data = if compressed {
            Self::compress(&data, config.compression_level)?
        } else {
            data
        };

        // Write file
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .map_err(|e| SpillError::Io {
                source: e,
                context: format!("creating spill file {:?}", path),
            })?;

        let mut writer = BufWriter::new(file);

        // Write header
        Self::write_header(&mut writer, points.len(), compressed)?;

        // Write data
        writer.write_all(&final_data).map_err(|e| SpillError::Io {
            source: e,
            context: "writing spill data".to_string(),
        })?;

        writer.flush().map_err(|e| SpillError::Io {
            source: e,
            context: "flushing spill file".to_string(),
        })?;

        let size = HEADER_SIZE + final_data.len();

        debug!(
            "Created spill file {} with {} points ({} bytes)",
            id,
            points.len(),
            size
        );

        Ok(Self {
            id,
            path,
            compressed,
            point_count: points.len(),
            size,
        })
    }

    /// Open an existing spill file
    pub fn open(path: &Path) -> SpillResult<Self> {
        let file = File::open(path).map_err(|e| SpillError::Io {
            source: e,
            context: format!("opening spill file {:?}", path),
        })?;

        let metadata = file.metadata().map_err(|e| SpillError::Io {
            source: e,
            context: "getting file metadata".to_string(),
        })?;

        let mut reader = BufReader::new(file);

        // Read and validate header
        let (point_count, compressed) = Self::read_header(&mut reader, path)?;

        // Extract ID from filename
        let id = Self::extract_id_from_path(path)?;

        Ok(Self {
            id,
            path: path.to_path_buf(),
            compressed,
            point_count,
            size: metadata.len() as usize,
        })
    }

    /// Read all points from the spill file
    pub fn read_points(&self) -> SpillResult<Vec<DataPoint>> {
        let file = File::open(&self.path).map_err(|e| SpillError::Io {
            source: e,
            context: format!("opening spill file {:?}", self.path),
        })?;

        let mut reader = BufReader::new(file);

        // Skip header
        let mut header = [0u8; HEADER_SIZE];
        reader.read_exact(&mut header).map_err(|e| SpillError::Io {
            source: e,
            context: "reading header".to_string(),
        })?;

        // Read data
        let mut data = Vec::new();
        reader.read_to_end(&mut data).map_err(|e| SpillError::Io {
            source: e,
            context: "reading spill data".to_string(),
        })?;

        // Decompress if needed
        let raw_data = if self.compressed {
            Self::decompress(&data)?
        } else {
            data
        };

        // Deserialize points
        Self::deserialize_points(&raw_data)
    }

    /// Delete the spill file
    pub fn delete(self) -> SpillResult<()> {
        fs::remove_file(&self.path).map_err(|e| SpillError::Io {
            source: e,
            context: format!("deleting spill file {:?}", self.path),
        })?;

        debug!("Deleted spill file {}", self.id);
        Ok(())
    }

    /// Write file header
    fn write_header<W: Write>(
        writer: &mut W,
        point_count: usize,
        compressed: bool,
    ) -> SpillResult<()> {
        let mut header = [0u8; HEADER_SIZE];

        // Magic (4 bytes)
        header[0..4].copy_from_slice(&SPILL_MAGIC.to_le_bytes());
        // Version (2 bytes)
        header[4..6].copy_from_slice(&SPILL_VERSION.to_le_bytes());
        // Flags (2 bytes) - bit 0 = compressed
        let flags: u16 = if compressed { 1 } else { 0 };
        header[6..8].copy_from_slice(&flags.to_le_bytes());
        // Point count (8 bytes)
        header[8..16].copy_from_slice(&(point_count as u64).to_le_bytes());
        // Reserved (8 bytes)

        writer.write_all(&header).map_err(|e| SpillError::Io {
            source: e,
            context: "writing header".to_string(),
        })?;

        Ok(())
    }

    /// Read and validate file header
    fn read_header<R: Read>(reader: &mut R, path: &Path) -> SpillResult<(usize, bool)> {
        let mut header = [0u8; HEADER_SIZE];
        reader.read_exact(&mut header).map_err(|e| SpillError::Io {
            source: e,
            context: "reading header".to_string(),
        })?;

        // Validate magic
        let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        if magic != SPILL_MAGIC {
            return Err(SpillError::Corruption {
                path: path.to_path_buf(),
                message: format!(
                    "invalid magic: expected {:08x}, got {:08x}",
                    SPILL_MAGIC, magic
                ),
            });
        }

        // Validate version
        let version = u16::from_le_bytes([header[4], header[5]]);
        if version > SPILL_VERSION {
            return Err(SpillError::Corruption {
                path: path.to_path_buf(),
                message: format!("unsupported version: {}", version),
            });
        }

        // Read flags
        let flags = u16::from_le_bytes([header[6], header[7]]);
        let compressed = (flags & 1) != 0;

        // Read point count
        let point_count = u64::from_le_bytes([
            header[8], header[9], header[10], header[11], header[12], header[13], header[14],
            header[15],
        ]) as usize;

        Ok((point_count, compressed))
    }

    /// Serialize points to bytes
    fn serialize_points(points: &[DataPoint]) -> Vec<u8> {
        // Same format as WAL: u128 series_id + i64 timestamp + f64 value = 32 bytes per point
        let mut buf = Vec::with_capacity(points.len() * 32);

        for point in points {
            buf.extend_from_slice(&point.series_id.to_le_bytes());
            buf.extend_from_slice(&point.timestamp.to_le_bytes());
            buf.extend_from_slice(&point.value.to_le_bytes());
        }

        buf
    }

    /// Deserialize points from bytes
    fn deserialize_points(data: &[u8]) -> SpillResult<Vec<DataPoint>> {
        if !data.len().is_multiple_of(32) {
            return Err(SpillError::Corruption {
                path: PathBuf::new(),
                message: format!("invalid data length: {} not divisible by 32", data.len()),
            });
        }

        let count = data.len() / 32;
        let mut points = Vec::with_capacity(count);
        let mut offset = 0;

        for _ in 0..count {
            let series_id = u128::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
                data[offset + 8],
                data[offset + 9],
                data[offset + 10],
                data[offset + 11],
                data[offset + 12],
                data[offset + 13],
                data[offset + 14],
                data[offset + 15],
            ]);
            offset += 16;

            let timestamp = i64::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
            ]);
            offset += 8;

            let value = f64::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
            ]);
            offset += 8;

            points.push(DataPoint::new(series_id, timestamp, value));
        }

        Ok(points)
    }

    /// Compress data using LZ4
    fn compress(data: &[u8], _level: u32) -> SpillResult<Vec<u8>> {
        // Use lz4_flex for compression
        Ok(lz4_flex::compress_prepend_size(data))
    }

    /// Decompress LZ4 data
    fn decompress(data: &[u8]) -> SpillResult<Vec<u8>> {
        lz4_flex::decompress_size_prepended(data).map_err(|e| SpillError::Decompression {
            message: e.to_string(),
        })
    }

    /// Extract file ID from path
    fn extract_id_from_path(path: &Path) -> SpillResult<SpillFileId> {
        path.file_stem()
            .and_then(|s| s.to_str())
            .and_then(|name| name.strip_prefix("spill-"))
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| SpillError::Corruption {
                path: path.to_path_buf(),
                message: "invalid filename format".to_string(),
            })
    }

    /// Get file ID
    pub fn id(&self) -> SpillFileId {
        self.id
    }

    /// Get file path
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get point count
    pub fn point_count(&self) -> usize {
        self.point_count
    }

    /// Get file size
    pub fn size(&self) -> usize {
        self.size
    }

    /// Check if compressed
    pub fn is_compressed(&self) -> bool {
        self.compressed
    }
}

/// List all spill files in a directory
pub fn list_spill_files(directory: &Path) -> SpillResult<Vec<PathBuf>> {
    let mut files = Vec::new();

    if !directory.exists() {
        return Ok(files);
    }

    for entry in fs::read_dir(directory).map_err(|e| SpillError::Io {
        source: e,
        context: format!("reading directory {:?}", directory),
    })? {
        let entry = entry.map_err(|e| SpillError::Io {
            source: e,
            context: "reading directory entry".to_string(),
        })?;

        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.starts_with("spill-") && (name.ends_with(".lz4") || name.ends_with(".spill")) {
                files.push(path);
            }
        }
    }

    // Sort by file ID
    files.sort_by(|a, b| {
        let id_a = SpillFile::extract_id_from_path(a).unwrap_or(0);
        let id_b = SpillFile::extract_id_from_path(b).unwrap_or(0);
        id_a.cmp(&id_b)
    });

    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn test_config(dir: &Path) -> SpillConfig {
        SpillConfig {
            directory: dir.to_path_buf(),
            compression_enabled: true,
            compression_level: 1,
            ..Default::default()
        }
    }

    fn sample_points() -> Vec<DataPoint> {
        vec![
            DataPoint::new(1, 1000, 42.5),
            DataPoint::new(2, 1001, 43.5),
            DataPoint::new(3, 1002, 44.5),
        ]
    }

    #[test]
    fn test_serialize_deserialize() {
        let points = sample_points();
        let data = SpillFile::serialize_points(&points);
        let recovered = SpillFile::deserialize_points(&data).unwrap();

        assert_eq!(points.len(), recovered.len());
        for (orig, rec) in points.iter().zip(recovered.iter()) {
            assert_eq!(orig.series_id, rec.series_id);
            assert_eq!(orig.timestamp, rec.timestamp);
            assert!((orig.value - rec.value).abs() < f64::EPSILON);
        }
    }

    #[test]
    fn test_compress_decompress() {
        let data = vec![1u8; 1000]; // Compressible data
        let compressed = SpillFile::compress(&data, 1).unwrap();
        let decompressed = SpillFile::decompress(&compressed).unwrap();

        assert_eq!(data, decompressed);
        assert!(compressed.len() < data.len()); // Should be smaller
    }

    #[test]
    fn test_spill_file_create_read() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());
        let points = sample_points();

        // Create spill file
        let file = SpillFile::create(&config, 1, &points).unwrap();
        assert_eq!(file.point_count(), 3);
        assert!(file.is_compressed());

        // Read it back
        let recovered = file.read_points().unwrap();
        assert_eq!(recovered.len(), 3);
    }

    #[test]
    fn test_spill_file_open() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());
        let points = sample_points();

        // Create spill file
        let file = SpillFile::create(&config, 1, &points).unwrap();
        let path = file.path().to_path_buf();

        // Open it
        let opened = SpillFile::open(&path).unwrap();
        assert_eq!(opened.point_count(), 3);
    }

    #[test]
    fn test_spill_file_delete() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());
        let points = sample_points();

        let file = SpillFile::create(&config, 1, &points).unwrap();
        let path = file.path().to_path_buf();

        assert!(path.exists());
        file.delete().unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn test_list_spill_files() {
        let dir = tempdir().unwrap();
        let config = test_config(dir.path());
        let points = sample_points();

        // Create multiple files
        SpillFile::create(&config, 1, &points).unwrap();
        SpillFile::create(&config, 3, &points).unwrap();
        SpillFile::create(&config, 2, &points).unwrap();

        let files = list_spill_files(dir.path()).unwrap();
        assert_eq!(files.len(), 3);

        // Should be sorted by ID
        let ids: Vec<_> = files
            .iter()
            .map(|p| SpillFile::extract_id_from_path(p).unwrap())
            .collect();
        assert_eq!(ids, vec![1, 2, 3]);
    }

    #[test]
    fn test_uncompressed_spill() {
        let dir = tempdir().unwrap();
        let config = SpillConfig {
            directory: dir.path().to_path_buf(),
            compression_enabled: false,
            ..Default::default()
        };
        let points = sample_points();

        let file = SpillFile::create(&config, 1, &points).unwrap();
        assert!(!file.is_compressed());

        let recovered = file.read_points().unwrap();
        assert_eq!(recovered.len(), 3);
    }
}
