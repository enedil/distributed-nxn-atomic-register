
    use crate::domain::{SectorIdx, SectorVec};
    use std::path::PathBuf;
    use std::sync::Arc;
    use tokio::io::AsyncReadExt;
    use std::io::Error;

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
    pub async fn build_sectors_manager(path: PathBuf) -> Arc<dyn SectorsManager> {
        unimplemented!()
    }


    struct SM {
        root_dir: PathBuf,
        root: tokio::fs::File,
    }

    impl SM {
        fn path_for_sector(&self, idx: SectorIdx) -> PathBuf {
            let mut path = self.root_dir.clone();
            path.push("sectors");
            path.push(format!("{:016x}", idx));
            path
        }
        async fn get_metadata(&self, idx: SectorIdx) -> Option<(PathBuf, u64, u8)> {
            let f1 = SM::find_first_file(self.path_for_sector(idx)).await.unwrap();
            let f2 = SM::find_first_file(f1).await.unwrap();
            let timestamp_str = f2.file_name()?;
            let rank_str = f2.file_name()?;
            let timestmap = u64::from_str_radix(timestamp_str.to_str()?, 16).ok()?;
            let rank = u8::from_str_radix(rank_str.to_str()?, 16).ok()?;
            Some((f2, timestmap, rank))
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
            
        }
    }
