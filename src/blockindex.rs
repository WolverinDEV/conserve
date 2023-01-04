use std::{sync::{Arc, Mutex}, fmt::Debug, collections::HashSet, time::Instant};

use tracing::debug;

use crate::{BlockHash, Transport, Error, Result, blockdir::block_relpath, BlockDir};

/// Quick lookup index for meta information about
/// the archive block dir. Such index will be used for block deduplication.
pub trait BlockIndex: Send + Sync + Debug {
    fn contains_block(&self, hash: &BlockHash) -> Result<bool>;
    
    fn register_block(&self, hash: &BlockHash);
    fn delete_block(&self, hash: &BlockHash);
}

#[derive(Debug)]
pub struct FsBlockIndex {
    transport: Arc<dyn Transport>,
}

impl FsBlockIndex {
    pub fn new(transport: Arc<dyn Transport>) -> Self {
        Self {
            transport
        }
    }
}

impl BlockIndex for FsBlockIndex {
    fn contains_block(&self, hash: &BlockHash) -> Result<bool> {
        self.transport
            .is_file(&block_relpath(hash))
            .map_err(Error::from)
    }

    fn register_block(&self, _hash: &BlockHash) {
        // Nothing to do.
        // If the block gets created
        // contains_block will return true.
    }

    fn delete_block(&self, _hash: &BlockHash) {
        // Nothing to do.
        // If the block gets deleted from the transport target
        // contains_block will return false.
    }
}

pub struct CachedBlockIndex {
    transport: Arc<dyn Transport>,
    cache: Mutex<HashSet<BlockHash>>
}

impl CachedBlockIndex {
    pub fn load(transport: Arc<dyn Transport>) -> Result<Self> {
        let mut cache = HashSet::new();

        let begin = Instant::now();
        for block in BlockDir::open(transport.clone()).block_names()? {
            cache.insert(block);
        }
        
        debug!("Cache index time: {:#?} ({} entries)", begin.elapsed(), cache.len());
        
        Ok(CachedBlockIndex {
            transport,
            cache: Mutex::new(cache)
        })
    }
}

impl BlockIndex for CachedBlockIndex {
    fn contains_block(&self, hash: &BlockHash) -> Result<bool> {
        Ok(self.cache.lock().unwrap().contains(hash))
    }

    fn register_block(&self, hash: &BlockHash) {
        self.cache.lock().unwrap().insert(hash.clone());
    }

    fn delete_block(&self, hash: &BlockHash) {
        self.cache.lock().unwrap().remove(hash);
    }
}

impl Debug for CachedBlockIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedBlockIndex")
            .field("transport", &self.transport)
            //.field("cache", &self.cache)
            .finish()
    }
}