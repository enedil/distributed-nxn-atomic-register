use sha2::{self, Digest, Sha256};
use std::path::PathBuf;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

#[async_trait::async_trait]
/// A helper trait for small amount of durable metadata needed by the register algorithm
/// itself. Again, it is only for AtomicRegister definition. StableStorage in unit tests
/// is durable, as one could expect.
pub trait StableStorage: Send + Sync {
    async fn put(&mut self, key: &str, value: &[u8]) -> Result<(), String>;

    async fn get(&self, key: &str) -> Option<Vec<u8>>;

    async fn remove(&mut self, key: &str) -> bool;
}

struct SStorage {
    root_storage_dir: PathBuf,
    root: tokio::fs::File,
}

fn encode<T: AsRef<[u8]>>(data: T) -> String {
    let mut sha = Sha256::new();
    sha.update(&data);
    base64::encode_config(data, base64::URL_SAFE)
}

impl SStorage {
    fn path_for_key(&self, key: &str) -> Result<PathBuf, String> {
        if key.len() > 255 {
            return Err("key too long".to_string());
        }
        let mut p = self.root_storage_dir.clone();
        p.push("key".to_string() + &encode(key));
        Ok(p)
    }
    fn tempfile_for_value(&self, value: &[u8]) -> PathBuf {
        let path = "value".to_string() + &encode(value);
        let mut p = self.root_storage_dir.clone();
        p.push(path);
        p
    }
}

#[async_trait::async_trait]
impl StableStorage for SStorage {
    async fn put(&mut self, key: &str, value: &[u8]) -> Result<(), String> {
        if value.len() >= 0x10000 {
            Err("value too long".to_string())
        } else {
            let path = self.path_for_key(key)?;
            let tempfile_path = self.tempfile_for_value(value);
            let mut tempfile = tokio::fs::File::create(tempfile_path.clone())
                .await
                .unwrap();
            tempfile.write_all(value).await.unwrap();
            tempfile.sync_data().await.unwrap();
            tokio::fs::rename(tempfile_path, path).await.unwrap();
            self.root.sync_data().await.unwrap();
            Ok(())
        }
    }
    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        if let Ok(path) = self.path_for_key(key) {
            match tokio::fs::File::open(path).await {
                Ok(mut file) => {
                    let mut buf = vec![];
                    file.read_to_end(&mut buf).await.unwrap();
                    Some(buf)
                }
                Err(_) => None,
            }
        } else {
            None
        }
    }
    async fn remove(&mut self, key: &str) -> bool {
        if let Ok(path) = self.path_for_key(key) {
            match tokio::fs::remove_file(path).await {
                Ok(_) => {
                    self.root.sync_data().await.unwrap();
                    true
                }
                Err(_) => false,
            }
        } else {
            false
        }
    }
}

/// Creates a new instance of stable storage.
pub async fn build_stable_storage(root_storage_dir: PathBuf) -> Box<dyn StableStorage> {
    let root = tokio::fs::File::open(root_storage_dir.clone())
        .await
        .unwrap();
    Box::new(SStorage {
        root_storage_dir,
        root,
    })
}
