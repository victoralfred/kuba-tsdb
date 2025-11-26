//! Lua scripts for atomic Redis operations
//!
//! These scripts ensure atomic multi-key operations in Redis,
//! preventing race conditions in concurrent environments.
//!
//! # Scripts Provided
//!
//! - `add_chunk`: Atomically add a chunk to the time index
//! - `update_chunk_status`: Update chunk status atomically
//! - `register_series`: Create a new series with metadata
//! - `cleanup_expired`: Remove expired chunks from index
//!
//! # Example
//!
//! ```rust,no_run
//! use gorilla_tsdb::redis::LuaScripts;
//!
//! let scripts = LuaScripts::new();
//! let add_chunk_script = scripts.add_chunk();
//! ```

use parking_lot::RwLock;
use redis::Script;
use std::collections::HashMap;
use std::sync::Arc;

/// Collection of Lua scripts for atomic Redis operations
///
/// Scripts are cached after first use to avoid repeated parsing.
pub struct LuaScripts {
    /// Cache of compiled scripts by name
    cache: RwLock<HashMap<String, Arc<Script>>>,
}

impl LuaScripts {
    /// Create a new LuaScripts instance
    pub fn new() -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
        }
    }

    /// Get or create a cached script
    fn get_or_create(&self, name: &str, lua: &str) -> Arc<Script> {
        // Check cache first
        {
            let cache = self.cache.read();
            if let Some(script) = cache.get(name) {
                return Arc::clone(script);
            }
        }

        // Create and cache the script
        let script = Arc::new(Script::new(lua));
        {
            let mut cache = self.cache.write();
            cache.insert(name.to_string(), Arc::clone(&script));
        }
        script
    }

    /// Atomically add a chunk to the series time index
    ///
    /// # Keys
    /// - KEYS[1]: Series index key (ts:series:{id}:index)
    /// - KEYS[2]: Chunk metadata key (ts:chunks:{chunk_id})
    /// - KEYS[3]: Series registry key (ts:registry)
    /// - KEYS[4]: Series metadata key (ts:series:{id}:meta)
    ///
    /// # Arguments
    /// - ARGV[1]: Start timestamp (score for ZSET)
    /// - ARGV[2]: Chunk ID
    /// - ARGV[3]: Series ID (for registry)
    /// - ARGV[4]: Chunk metadata JSON
    /// - ARGV[5]: Current timestamp (for last_write)
    ///
    /// # Returns
    /// - 1 on success
    /// - 0 if chunk already exists
    pub fn add_chunk(&self) -> Arc<Script> {
        self.get_or_create(
            "add_chunk",
            r#"
            local series_index = KEYS[1]
            local chunk_key = KEYS[2]
            local registry_key = KEYS[3]
            local series_meta_key = KEYS[4]

            local start_timestamp = ARGV[1]
            local chunk_id = ARGV[2]
            local series_id = ARGV[3]
            local chunk_metadata = ARGV[4]
            local current_time = ARGV[5]

            -- Check if chunk already exists
            if redis.call('EXISTS', chunk_key) == 1 then
                return 0
            end

            -- Add chunk to series time index (sorted by start_timestamp)
            redis.call('ZADD', series_index, start_timestamp, chunk_id)

            -- Store chunk metadata
            redis.call('HSET', chunk_key, 'metadata', chunk_metadata)

            -- Register series if not exists
            redis.call('SADD', registry_key, series_id)

            -- Update series last_write timestamp
            redis.call('HSET', series_meta_key, 'last_write', current_time)

            -- Increment total_points (will be updated with actual count later)
            redis.call('HINCRBY', series_meta_key, 'total_chunks', 1)

            return 1
            "#,
        )
    }

    /// Update chunk status atomically
    ///
    /// # Keys
    /// - KEYS[1]: Chunk metadata key (ts:chunks:{chunk_id})
    ///
    /// # Arguments
    /// - ARGV[1]: New status (active|sealed|compressed|archived|deleted)
    /// - ARGV[2]: Current timestamp
    ///
    /// # Returns
    /// - 1 on success
    /// - 0 if chunk doesn't exist
    pub fn update_chunk_status(&self) -> Arc<Script> {
        self.get_or_create(
            "update_chunk_status",
            r#"
            local chunk_key = KEYS[1]

            local new_status = ARGV[1]
            local current_time = ARGV[2]

            -- Check if chunk exists
            if redis.call('EXISTS', chunk_key) == 0 then
                return 0
            end

            -- Update status and timestamp
            redis.call('HSET', chunk_key, 'status', new_status)
            redis.call('HSET', chunk_key, 'updated_at', current_time)

            return 1
            "#,
        )
    }

    /// Register a new series with metadata and secondary indexes
    ///
    /// This script atomically:
    /// 1. Checks if series already exists
    /// 2. Adds series to the main registry
    /// 3. Creates secondary indexes for metric name and tags
    /// 4. Stores series metadata
    ///
    /// # Secondary Index Schema
    /// - `ts:metric:{metric_name}:series` - SET of series_ids with this metric
    /// - `ts:tag:{key}:{value}:series` - SET of series_ids with this tag k/v pair
    ///
    /// # Keys
    /// - KEYS[1]: Series registry key (ts:registry)
    /// - KEYS[2]: Series metadata key (ts:series:{id}:meta)
    ///
    /// # Arguments
    /// - ARGV[1]: Series ID
    /// - ARGV[2]: Current timestamp (created_at)
    /// - ARGV[3]: Metric name
    /// - ARGV[4]: Tags JSON (object with string keys and values)
    /// - ARGV[5]: Retention days (optional, 0 for none)
    ///
    /// # Returns
    /// - 1 if series was created
    /// - 0 if series already exists
    pub fn register_series(&self) -> Arc<Script> {
        self.get_or_create(
            "register_series",
            r#"
            local registry_key = KEYS[1]
            local series_meta_key = KEYS[2]

            local series_id = ARGV[1]
            local created_at = ARGV[2]
            local metric_name = ARGV[3]
            local tags_json = ARGV[4]
            local retention_days = ARGV[5]

            -- Check if series already exists
            if redis.call('SISMEMBER', registry_key, series_id) == 1 then
                return 0
            end

            -- Add to main registry
            redis.call('SADD', registry_key, series_id)

            -- Add to metric secondary index for efficient metric-based queries
            redis.call('SADD', 'ts:metric:' .. metric_name .. ':series', series_id)

            -- Add to tag secondary indexes for efficient tag-based filtering
            -- Parse tags JSON and create an index entry for each tag key-value pair
            local tags = cjson.decode(tags_json)
            for key, value in pairs(tags) do
                redis.call('SADD', 'ts:tag:' .. key .. ':' .. value .. ':series', series_id)
            end

            -- Set metadata
            redis.call('HSET', series_meta_key,
                'created_at', created_at,
                'last_write', created_at,
                'metric_name', metric_name,
                'tags', tags_json,
                'retention_days', retention_days,
                'total_points', 0,
                'total_chunks', 0
            )

            return 1
            "#,
        )
    }

    /// Delete a series and all its data including secondary indexes
    ///
    /// This script atomically:
    /// 1. Retrieves series metadata (metric name and tags)
    /// 2. Removes series from secondary indexes (metric and tag indexes)
    /// 3. Deletes all chunk data associated with the series
    /// 4. Removes series from main registry
    ///
    /// # Keys
    /// - KEYS[1]: Series registry key (ts:registry)
    /// - KEYS[2]: Series index key (ts:series:{id}:index)
    /// - KEYS[3]: Series metadata key (ts:series:{id}:meta)
    ///
    /// # Arguments
    /// - ARGV[1]: Series ID
    /// - ARGV[2]: Chunk key prefix (ts:chunks:)
    ///
    /// # Returns
    /// - Number of chunks deleted
    pub fn delete_series(&self) -> Arc<Script> {
        self.get_or_create(
            "delete_series",
            r#"
            local registry_key = KEYS[1]
            local series_index = KEYS[2]
            local series_meta_key = KEYS[3]

            local series_id = ARGV[1]
            local chunk_prefix = ARGV[2]

            -- Get metadata before deletion for secondary index cleanup
            local metric_name = redis.call('HGET', series_meta_key, 'metric_name')
            local tags_json = redis.call('HGET', series_meta_key, 'tags')

            -- Remove from metric secondary index
            if metric_name then
                redis.call('SREM', 'ts:metric:' .. metric_name .. ':series', series_id)
            end

            -- Remove from tag secondary indexes
            if tags_json then
                local tags = cjson.decode(tags_json)
                for key, value in pairs(tags) do
                    redis.call('SREM', 'ts:tag:' .. key .. ':' .. value .. ':series', series_id)
                end
            end

            -- Get all chunk IDs for this series
            local chunk_ids = redis.call('ZRANGE', series_index, 0, -1)
            local deleted_count = 0

            -- Delete each chunk's metadata
            for _, chunk_id in ipairs(chunk_ids) do
                redis.call('DEL', chunk_prefix .. chunk_id)
                deleted_count = deleted_count + 1
            end

            -- Delete series index
            redis.call('DEL', series_index)

            -- Delete series metadata
            redis.call('DEL', series_meta_key)

            -- Remove from registry
            redis.call('SREM', registry_key, series_id)

            return deleted_count
            "#,
        )
    }

    /// Cleanup expired chunks from a series
    ///
    /// # Keys
    /// - KEYS[1]: Series index key (ts:series:{id}:index)
    ///
    /// # Arguments
    /// - ARGV[1]: Cutoff timestamp (remove chunks older than this)
    /// - ARGV[2]: Chunk key prefix (ts:chunks:)
    ///
    /// # Returns
    /// - Number of chunks removed
    pub fn cleanup_expired(&self) -> Arc<Script> {
        self.get_or_create(
            "cleanup_expired",
            r#"
            local series_index = KEYS[1]

            local cutoff_time = ARGV[1]
            local chunk_prefix = ARGV[2]

            -- Get chunks older than cutoff
            local old_chunks = redis.call('ZRANGEBYSCORE', series_index, '-inf', cutoff_time)
            local removed_count = 0

            -- Delete each chunk's metadata
            for _, chunk_id in ipairs(old_chunks) do
                redis.call('DEL', chunk_prefix .. chunk_id)
                removed_count = removed_count + 1
            end

            -- Remove from index
            redis.call('ZREMRANGEBYSCORE', series_index, '-inf', cutoff_time)

            return removed_count
            "#,
        )
    }

    /// Find chunks in a time range
    ///
    /// # Keys
    /// - KEYS[1]: Series index key (ts:series:{id}:index)
    ///
    /// # Arguments
    /// - ARGV[1]: Start timestamp
    /// - ARGV[2]: End timestamp
    /// - ARGV[3]: Limit (0 for no limit)
    ///
    /// # Returns
    /// - Array of chunk IDs
    pub fn find_chunks_in_range(&self) -> Arc<Script> {
        self.get_or_create(
            "find_chunks_in_range",
            r#"
            local series_index = KEYS[1]

            local start_time = ARGV[1]
            local end_time = ARGV[2]
            local limit = tonumber(ARGV[3])

            -- Query chunks in time range
            local chunks
            if limit > 0 then
                chunks = redis.call('ZRANGEBYSCORE', series_index, start_time, end_time, 'LIMIT', 0, limit)
            else
                chunks = redis.call('ZRANGEBYSCORE', series_index, start_time, end_time)
            end

            return chunks
            "#,
        )
    }

    /// Batch add points to the series buffer
    ///
    /// # Keys
    /// - KEYS[1]: Series buffer key (ts:series:{id}:buffer)
    /// - KEYS[2]: Series metadata key (ts:series:{id}:meta)
    ///
    /// # Arguments
    /// - ARGV[1]: Serialized points (JSON array)
    /// - ARGV[2]: Current timestamp
    /// - ARGV[3]: Max buffer size
    ///
    /// # Returns
    /// - Number of points added (may be less if buffer is full)
    pub fn buffer_points(&self) -> Arc<Script> {
        self.get_or_create(
            "buffer_points",
            r#"
            local buffer_key = KEYS[1]
            local series_meta_key = KEYS[2]

            local points_json = ARGV[1]
            local current_time = ARGV[2]
            local max_buffer_size = tonumber(ARGV[3])

            -- Check current buffer size
            local current_size = redis.call('LLEN', buffer_key)
            if current_size >= max_buffer_size then
                return 0
            end

            -- Add points to buffer (as single JSON entry for efficiency)
            redis.call('RPUSH', buffer_key, points_json)

            -- Update last_write
            redis.call('HSET', series_meta_key, 'last_write', current_time)

            return 1
            "#,
        )
    }

    /// Flush and get all buffered points
    ///
    /// # Keys
    /// - KEYS[1]: Series buffer key (ts:series:{id}:buffer)
    ///
    /// # Arguments
    /// None
    ///
    /// # Returns
    /// - Array of serialized point batches
    pub fn flush_buffer(&self) -> Arc<Script> {
        self.get_or_create(
            "flush_buffer",
            r#"
            local buffer_key = KEYS[1]

            -- Get all buffered points
            local points = redis.call('LRANGE', buffer_key, 0, -1)

            -- Clear buffer
            redis.call('DEL', buffer_key)

            return points
            "#,
        )
    }

    /// Update series statistics
    ///
    /// # Keys
    /// - KEYS[1]: Series metadata key (ts:series:{id}:meta)
    ///
    /// # Arguments
    /// - ARGV[1]: Points to add to total
    /// - ARGV[2]: Current timestamp
    ///
    /// # Returns
    /// - New total points count
    pub fn update_series_stats(&self) -> Arc<Script> {
        self.get_or_create(
            "update_series_stats",
            r#"
            local series_meta_key = KEYS[1]

            local points_to_add = tonumber(ARGV[1])
            local current_time = ARGV[2]

            -- Increment total points
            local new_total = redis.call('HINCRBY', series_meta_key, 'total_points', points_to_add)

            -- Update last_write
            redis.call('HSET', series_meta_key, 'last_write', current_time)

            return new_total
            "#,
        )
    }
}

impl Default for LuaScripts {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_script_caching() {
        let scripts = LuaScripts::new();

        // First call creates the script
        let script1 = scripts.add_chunk();

        // Second call returns cached version
        let script2 = scripts.add_chunk();

        // Should be the same Arc
        assert!(Arc::ptr_eq(&script1, &script2));
    }

    #[test]
    fn test_all_scripts_compile() {
        let scripts = LuaScripts::new();

        // Just verify all scripts can be created without panicking
        let _ = scripts.add_chunk();
        let _ = scripts.update_chunk_status();
        let _ = scripts.register_series();
        let _ = scripts.delete_series();
        let _ = scripts.cleanup_expired();
        let _ = scripts.find_chunks_in_range();
        let _ = scripts.buffer_points();
        let _ = scripts.flush_buffer();
        let _ = scripts.update_series_stats();
    }
}
