use std::{num::NonZeroUsize, path::Path};

use lru::LruCache;
#[cfg(feature = "rocksdb")]
use rocksdb::BlockBasedOptions;
#[cfg(feature = "rocksdb")]
use rocksdb::backup;
use serde::Serialize;
use serde::de::DeserializeOwned;
#[cfg(feature = "sled")]
use sled::Db;
use std::hash::Hash;
use tempfile::{TempDir, tempdir};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BackingStoreError {
    #[error("Can't create backing store.")]
    BackingStoreCreation(String),
    #[error("Internal error.")]
    InternalError(String),
}

/// A trait to define the interface of the disk backing store DB.
/// Used to be able to experiment with multiple types of databases.
pub trait BackingStore {
    /// Create a new backing store.
    fn new<P: AsRef<Path>>(path: P) -> Result<Self, BackingStoreError>
    where
        Self: Sized;

    /// Get a value from the backing store.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, BackingStoreError>;

    /// Store a value in the database.
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), BackingStoreError>;

    /// Check if the database has the key.
    fn contains_key(&self, key: &[u8]) -> Result<bool, BackingStoreError>;
}

/// A simple key-value store using Sqlite.
/// SQL statements generated with AI.
impl BackingStore for SqliteKvStore {
    fn new<P: AsRef<Path>>(path: P) -> Result<Self, BackingStoreError> {
        let conn = Connection::open(path)
            .map_err(|e| BackingStoreError::BackingStoreCreation(e.to_string()))?;

        // Use BLOB for both key and value
        conn.execute(
            "CREATE TABLE IF NOT EXISTS kv (
                key BLOB PRIMARY KEY,
                value BLOB NOT NULL
            )",
            [],
        )
        .map_err(|e| BackingStoreError::InternalError(e.to_string()))?;

        // Limit the size of the in-memory cache of the DB.
        conn.execute_batch(
            "
            PRAGMA cache_size = 256;
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
        ",
        )
        .map_err(|e| BackingStoreError::InternalError(e.to_string()))?;

        Ok(Self { conn })
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, BackingStoreError> {
        self.conn
            .query_row("SELECT value FROM kv WHERE key = ?1", params![key], |row| {
                row.get(0)
            })
            .optional()
            .map_err(|e| BackingStoreError::InternalError(e.to_string()))
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), BackingStoreError> {
        self.conn
            .execute(
                "INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)",
                params![key, value],
            )
            .map_err(|e| BackingStoreError::InternalError(e.to_string()))?;
        Ok(())
    }

    fn contains_key(&self, key: &[u8]) -> Result<bool, BackingStoreError> {
        let mut stmt = self
            .conn
            .prepare("SELECT 1 FROM kv WHERE key = ?1")
            .map_err(|e| BackingStoreError::InternalError(e.to_string()))?;
        stmt.exists(params![key])
            .map_err(|e| BackingStoreError::InternalError(e.to_string()))
    }
}

use rusqlite::{Connection, OptionalExtension, params};

#[derive(Debug)]
pub struct SqliteKvStore {
    conn: Connection,
}

#[cfg(feature = "rocksdb")]
struct RocksDbStore {
    db: rocksdb::DB,
}

/// A backing store that used RocksDB.
/// Works great but takes a really long time to compile vs sqlite.
#[cfg(feature = "rocksdb")]
impl BackingStore for RocksDbStore {
    fn new<P: AsRef<Path>>(path: P) -> Result<Self, BackingStoreError> {
        let rocksdb_cache = rocksdb::Cache::new_lru_cache(1024);
        let mut block_opts = BlockBasedOptions::default();
        block_opts.set_block_cache(&rocksdb_cache);

        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.set_db_write_buffer_size(1024);
        opts.set_block_based_table_factory(&block_opts);

        let db = rocksdb::DB::open(&opts, path)
            .map_err(|e| BackingStoreError::BackingStoreCreation(e.to_string()))?;

        Ok(Self { db })
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, BackingStoreError> {
        self.db
            .get(key)
            .map_err(|e| BackingStoreError::InternalError(e.to_string()))
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), BackingStoreError> {
        self.db
            .put(key, value)
            .map_err(|e| BackingStoreError::InternalError(e.to_string()))
    }

    fn contains_key(&self, key: &[u8]) -> Result<bool, BackingStoreError> {
        match self
            .db
            .get(key)
            .map_err(|e| BackingStoreError::InternalError(e.to_string()))?
        {
            Some(_) => Ok(true),
            None => Ok(false),
        }
    }
}

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("Invalid cache capacity.")]
    InvalidCapacity,
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Database error: {0}")]
    DbError(#[from] BackingStoreError),
    #[error("Serialization error: {0}")]
    BincodeEncodeError(#[from] bincode::error::EncodeError),
    #[error("Deserialization error: {0}")]
    BincodeDecodeError(#[from] bincode::error::DecodeError),
}

/// A cache where we can store the transactions that were issued for an account.
/// The goal of this cache is to allow only a limited amount of entries in memory.
/// Old entries are evicted to disk to preserve system resource.
///
/// Right now this has a pretty generic implementation (even though it stated out just as a transactions store),
/// and supports multiple backends for the disk storage engine.
#[derive(Debug)]
pub struct TransactionCache<
    S: BackingStore,
    K: Hash + Eq + Serialize,
    V: Serialize,
    const CAP: usize,
> {
    /// In memory cache of the transaction objects.
    cache: LruCache<K, V>,
    /// Database where transactions are evicted when memory cache gets full.
    db: S,
    /// We need to hold on to the temporary directory for as long as the cache is active.
    _db_dir: TempDir,
}

#[cfg(feature = "rocksdb")]
impl<'de, K: Hash + Eq + Serialize + Copy, V: Serialize + DeserializeOwned, const CAP: usize>
    TransactionCache<RocksDbStore, K, V, CAP>
{
    pub fn new() -> Result<Self, CacheError> {
        debug_assert!(CAP >= 1);

        let cache = LruCache::new(NonZeroUsize::new(CAP).ok_or(CacheError::InvalidCapacity)?);
        let db_dir = tempdir()?;
        let db = RocksDbStore::new(db_dir.path())?;

        Ok(Self {
            cache,
            db,
            _db_dir: db_dir,
        })
    }
}

impl<K: Hash + Eq + Serialize + Copy, V: Serialize + DeserializeOwned, const CAP: usize>
    TransactionCache<SqliteKvStore, K, V, CAP>
{
    pub fn new() -> Result<Self, CacheError> {
        debug_assert!(CAP >= 1);

        let cache = LruCache::new(NonZeroUsize::new(CAP).ok_or(CacheError::InvalidCapacity)?);
        let db_dir = tempdir()?;
        let sqlite = SqliteKvStore::new(format!("{}/my_db.db", db_dir.path().to_str().unwrap()))?;

        /*
        let db = sled::Config::default()
            .path(db_dir.path())
            .cache_capacity(256)
            .open()?;
        */

        Ok(Self {
            cache,
            db: sqlite,
            _db_dir: db_dir,
        })
    }
}

impl<
    S: BackingStore,
    K: Hash + Eq + Serialize + Copy,
    V: Serialize + DeserializeOwned,
    const CAP: usize,
> TransactionCache<S, K, V, CAP>
{
    /// Put a value in the cache. If the cache is full, the least recently used object will be evicted to the disk DB.
    pub fn put(&mut self, tx_id: K, entry: V) -> Result<(), CacheError> {
        // transaction already in cache; only need to update and promote its usage
        if self.cache.contains(&tx_id) {
            self.cache.put(tx_id, entry);
            return Ok(());
        }

        // cache is already full, the transaction is not in the cache so this put will evict the least recently used value.
        // we want to make sure the entry is evicted on disk rather than lost.
        // TOOD: as an improvement it probably would make more sense to evict more objects to disk instead of just one.
        if self.cache.len() == CAP {
            if let Some((tx_id_to_evict, entry_to_evict)) = self.cache.pop_lru() {
                let id_to_evict_bytes =
                    bincode::serde::encode_to_vec(tx_id_to_evict, bincode::config::standard())?;
                let entry_to_evict_bytes =
                    bincode::serde::encode_to_vec(&entry_to_evict, bincode::config::standard())?;
                self.db.put(&id_to_evict_bytes, &entry_to_evict_bytes)?;
                //self.db.flush()?;
            }
        }

        // the old item was evicted so there is room for the new one now.
        self.cache.put(tx_id, entry);

        Ok(())
    }

    /// Get a value from the cache. If the value is not in memory, it will be loaded from the disk database. When that happens, the least recently used item may be evicted.
    pub fn get_mut(&mut self, tx_id: &K) -> Result<Option<&mut V>, CacheError> {
        if self.cache.contains(tx_id) {
            return Ok(self.cache.get_mut(tx_id));
        }

        // the transaction is not in the cache. It's either on disk or doesn't exist. Check the db first.
        let tx_id_bytes = bincode::serde::encode_to_vec(tx_id, bincode::config::standard())?;

        if let Ok(Some(entry_bytes)) = self.db.get(&tx_id_bytes) {
            let (entry, _): (V, usize) =
                bincode::serde::decode_from_slice(&entry_bytes, bincode::config::standard())?;
            self.put(*tx_id, entry)?;
            Ok(self.cache.get_mut(tx_id))
        } else {
            // not in the db. Return None.
            Ok(None)
        }
    }

    // Check if there's an entry in the cache.
    pub fn contains_key(&self, tx_id: &K) -> Result<bool, CacheError> {
        if self.cache.contains(tx_id) {
            return Ok(true);
        }

        let tx_id_bytes = bincode::serde::encode_to_vec(tx_id, bincode::config::standard())?;
        Ok(self.db.contains_key(&tx_id_bytes)?)
    }
}

#[cfg(test)]
mod tests {
    //use crate::{account::FundingLogEntry, transaction_types::TransactionId};

    use super::*;

    impl<const CAP: usize> TransactionCache<SqliteKvStore, u16, u32, CAP> {
        pub(crate) fn get(&mut self, tx_id: &u16) -> Result<Option<&u32>, CacheError> {
            self.get_mut(tx_id)
                .map(|maybe_val| maybe_val.map(|val| &*val))
        }
    }

    #[test]
    fn should_evict_entries() {
        let mut cache = TransactionCache::<SqliteKvStore, u16, u32, 16>::new().unwrap();

        for i in 0..128 {
            cache.put(i, i as u32).unwrap();
        }

        assert_eq!(cache.cache.len(), 16);

        for i in 112..=127 {
            assert!(cache.cache.get(&i).is_some());
        }

        for i in 0..112 {
            assert!(cache.cache.get(&i).is_none());
        }
    }

    #[test]
    fn should_read_evicted_entries() {
        let mut cache = TransactionCache::<SqliteKvStore, u16, u32, 16>::new().unwrap();

        for i in 0..128 {
            cache.put(i, i as u32).unwrap();
        }

        for i in 0..128 {
            assert_eq!(*cache.get(&i).unwrap().unwrap(), i as u32)
        }
    }
}
