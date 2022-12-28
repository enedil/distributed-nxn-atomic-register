use crate::domain::{SectorIdx, SectorVec};
use core::time;
use std::io::Error;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::timestamp;

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

/// Path parameter points to a directory to which this method has exclusive access.
pub async fn build_sectors_manager(_path: PathBuf) -> Arc<dyn SectorsManager> {
    unimplemented!()
}

struct SM {
    root_dir: PathBuf,
    root: tokio::fs::File,
}

impl SM {
    fn path_for_sector(&self, idx: SectorIdx, is_permanent: bool) -> PathBuf {
        let mut path = self.root_dir.clone();
        path.push(if is_permanent { "sectors" } else { "tmp" });
        path.push(format!("{:016x}", idx));
        path
    }
    fn path_for_sector_with_metadata(&self, idx: SectorIdx, is_permanent: bool, timestamp: u64, rank: u8) -> PathBuf {
        let mut base = self.path_for_sector(idx, is_permanent);
        base.push(format!("{:016x}:{:2x}", timestamp, rank));
        base
    }
    async fn get_metadata(&self, idx: SectorIdx) -> Option<(PathBuf, u64, u8)> {
        let f = SM::find_first_file(self.path_for_sector(idx, true))
            .await
            .unwrap();
        let fname = f.file_name()?.to_str()?;
        let mut parts = fname.split(":");
        let timestamp = u64::from_str_radix(parts.next()?, 16).ok()?;
        let rank = u8::from_str_radix(parts.next()?, 16).ok()?;
        Some((f.to_path_buf(), timestamp, rank))
    }
    async fn find_first_file(path: PathBuf) -> Result<PathBuf, Error> {
        let mut dir = tokio::fs::read_dir(path).await?;
        Ok(dir.next_entry().await?.unwrap().path())
    }
}

#[async_trait::async_trait]
impl SectorsManager for SM {
    async fn read_data(&self, idx: SectorIdx) -> SectorVec {
        let (path, _, _) = self.get_metadata(idx).await.unwrap();
        let mut file = tokio::fs::File::open(&path).await.unwrap();
        let mut buf = Vec::<u8>::from([0u8; 4096]);
        file.read_exact(&mut buf).await.unwrap();
        SectorVec(buf)
    }
    async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8) {
        let (_, timestamp, rank) = self.get_metadata(idx).await.unwrap();
        (timestamp, rank)
    }
    async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8)) {
        let tmp_path = self.path_for_sector_with_metadata(idx, false, sector.1, sector.2);
        // argh
        let mut f = tokio::fs::File::create(tmp_path.clone()).await.unwrap();
        f.write_all(&sector.0.0).await.unwrap();

        let old_metadata = self.get_metadata(idx).await;

        tokio::fs::copy(tmp_path, self.path_for_sector(idx, true)).await.unwrap();

        if let Some((path, _, _)) = old_metadata {

        }
        
        unimplemented!()
    }
}
