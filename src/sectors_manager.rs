use crate::domain::{SectorIdx, SectorVec};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;

#[async_trait::async_trait]
pub trait SectorsManager: Send + Sync {
    /// Returns 4096 bytes of sector data by index.
    async fn read_data(&self, idx: SectorIdx) -> SectorVec;

    /// Returns timestamp and write rank of the process which has saved this data.
    /// Timestamps and ranks are relevant for atomic register algorithm, and are described
    /// there.
    async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8);

    /// Writes a new data, along with timestamp and write rank to some sector.
    async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8));
}

const SECTOR_SIZE: usize = 4096;
const SECTORS_PREFIX: &str = "sectors";

/// Path parameter points to a directory to which this method has exclusive access.
pub async fn build_sectors_manager(path: PathBuf) -> Arc<dyn SectorsManager> {
    let mut sectors_path = path.clone();
    sectors_path.push(SECTORS_PREFIX);
    tokio::fs::create_dir_all(&sectors_path).await.unwrap();

    let mut metadata = HashMap::<SectorIdx, RwLock<(u64, u8)>>::new();

    let mut dir = tokio::fs::read_dir(&sectors_path).await.unwrap();
    while let Some(file) = dir
        .next_entry()
        .await
        .expect("could not read entry from SectorsManager while building")
    {
        let fname = file.file_name();
        let all_parts = fname
            .to_str()
            .expect("target format not proper: is not a string");
        if all_parts.len() != 2 * (8 + 8 + 1) {
            panic!("target_format_not_proper: wrong length");
        }
        let index = u64::from_str_radix(&all_parts[(0 * 2)..(8 * 2)], 16)
            .expect("sector index is not a valid hexstring");
        let timestamp = u64::from_str_radix(&all_parts[(8 * 2)..(16 * 2)], 16)
            .expect("timestamp not a valid hexstring");
        let rank = u8::from_str_radix(&all_parts[(16 * 2)..(17 * 2)], 16)
            .expect("rank not a valid hexstring");

        match metadata.get_mut(&index) {
            Some(lock) => {
                let mut value = lock.write().await;
                let (to_retain, to_delete) = if value.0 < timestamp {
                    ((timestamp, rank), *value)
                } else {
                    (*value, (timestamp, rank))
                };
                let mut p = sectors_path.clone();
                p.push(SectorsM::fname_for_entry(index, to_delete.0, to_delete.1));
                tokio::fs::remove_file(p).await.unwrap();
                *value = to_retain;
            }
            None => {
                metadata.insert(index, RwLock::new((timestamp, rank)));
            }
        };
    }

    Arc::new(SectorsM {
        root_dir: path,
        metadata: RwLock::new(metadata),
    })
}

struct SectorsM {
    root_dir: PathBuf,
    metadata: RwLock<HashMap<SectorIdx, RwLock<(u64, u8)>>>,
}

impl SectorsM {
    fn fname_for_entry(idx: SectorIdx, timestamp: u64, rank: u8) -> String {
        format!("{:016x}{:016x}{:02x}", idx, timestamp, rank)
    }
    fn path_for_entry(&self, idx: SectorIdx, timestamp: u64, rank: u8) -> PathBuf {
        let mut path = self.root_dir.clone();
        path.push(SECTORS_PREFIX);
        path.push(SectorsM::fname_for_entry(idx, timestamp, rank));
        path
    }
}

#[async_trait::async_trait]
impl SectorsManager for SectorsM {
    async fn read_data(&self, idx: SectorIdx) -> SectorVec {
        let metadata = self.metadata.read().await;
        let data = match metadata.get(&idx) {
            Some(lock) => {
                let (timestamp, rank) = {
                    let tup = lock.read().await;
                    (tup.0, tup.1)
                };
                let path = self.path_for_entry(idx, timestamp, rank);
                let mut file = tokio::fs::File::open(&path)
                    .await
                    .unwrap_or_else(|_| panic!("file {:?} should be present", path.display()));
                let mut buf = [0u8; SECTOR_SIZE];
                file.read_exact(&mut buf)
                    .await
                    .unwrap_or_else(|_| panic!("failed to read sector of {:?}", path.display()));
                buf
            }
            None => [0u8; SECTOR_SIZE],
        };
        SectorVec(Vec::<u8>::from(data))
    }
    async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8) {
        match self.metadata.read().await.get(&idx) {
            Some(lock) => {
                let idx_guard = lock.read().await;
                (idx_guard.0, idx_guard.1)
            }
            None => (0, 0),
        }
    }
    async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8)) {
        let (vec, new_timestamp, new_rank) = sector;
        let mut new_file =
            tokio::fs::File::create(self.path_for_entry(idx, *new_timestamp, *new_rank))
                .await
                .expect("couldn't create new file");
        new_file
            .write_all(&vec.0)
            .await
            .expect("couldn't write to new file");

        let mut metadata = self.metadata.write().await;
        let p = match metadata.get(&idx) {
            Some(lock) => {
                let mut x = lock.write().await;
                let (old_timestamp, old_rank) = *x;
                *x = (*new_timestamp, *new_rank);
                Some((old_timestamp, old_rank))
            }
            None => {
                metadata.insert(idx, (*new_timestamp, *new_rank).into());
                None
            }
        };
        if let Some((old_timestamp, old_rank)) = p {
            tokio::fs::remove_file(self.path_for_entry(idx, old_timestamp, old_rank))
                .await
                .expect("could not remove old file");
        }
    }
}
