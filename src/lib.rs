mod domain;

pub use crate::domain::*;
pub use atomic_register_public::*;
pub use register_client_public::*;
pub use sectors_manager_public::*;
pub use stable_storage_public::*;
pub use transfer_public::*;


async fn handle_connection(config: Configuration, mut sock: tokio::net::TcpStream) {
    loop {
        let (cmd, ok) = transfer_public::deserialize_register_command(&mut sock, &config.hmac_system_key, &config.hmac_client_key).await.unwrap();
        if !ok {
            continue;
        }
        match cmd {
            RegisterCommand::Client(client) => handle_client(&config, client).await,
            RegisterCommand::System(system) => handle_system(&config, system).await,
        }
    }
}

async fn handle_client(config: &Configuration, cmd: ClientRegisterCommand) {

}

async fn handle_system(config: &Configuration, cmd: SystemRegisterCommand) {

}


pub async fn run_register_process(config: Configuration) {
    let (host, port) = &config.public.tcp_locations[(config.public.self_rank-1) as usize];
    let mut sock = tokio::net::TcpListener::bind(format!("{}:{}", host, port)).await.unwrap();

    //let mut clients = Vec::new();

    loop {
        let s = sock.accept().await.unwrap().0;
        tokio::spawn(handle_connection(config.clone(), s));
    }
}

pub mod atomic_register_public {
    use crate::{
        ClientRegisterCommand, OperationSuccess, RegisterClient, SectorsManager, StableStorage,
        SystemRegisterCommand,
    };
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    #[async_trait::async_trait]
    pub trait AtomicRegister: Send + Sync {
        /// Handle a client command. After the command is completed, we expect
        /// callback to be called. Note that completion of client command happens after
        /// delivery of multiple system commands to the register, as the algorithm specifies.
        ///
        /// This function corresponds to the handlers of Read and Write events in the
        /// (N,N)-AtomicRegister algorithm.
        async fn client_command(
            &mut self,
            cmd: ClientRegisterCommand,
            success_callback: Box<
                dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output = ()> + Send>>
                    + Send
                    + Sync,
            >,
        );

        /// Handle a system command.
        ///
        /// This function corresponds to the handlers of READ_PROC, VALUE, WRITE_PROC
        /// and ACK messages in the (N,N)-AtomicRegister algorithm.
        async fn system_command(&mut self, cmd: SystemRegisterCommand);
    }

    /// Idents are numbered starting at 1 (up to the number of processes in the system).
    /// Storage for atomic register algorithm data is separated into StableStorage.
    /// Communication with other processes of the system is to be done by register_client.
    /// And sectors must be stored in the sectors_manager instance.
    ///
    /// This function corresponds to the handlers of Init and Recovery events in the
    /// (N,N)-AtomicRegister algorithm.
    pub async fn build_atomic_register(
        self_ident: u8,
        metadata: Box<dyn StableStorage>,
        register_client: Arc<dyn RegisterClient>,
        sectors_manager: Arc<dyn SectorsManager>,
        processes_count: u8,
    ) -> Box<dyn AtomicRegister> {
        unimplemented!()
    }
}

pub mod sectors_manager_public {
    use crate::{SectorIdx, SectorVec};
    use std::path::PathBuf;
    use std::sync::Arc;

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
    pub fn build_sectors_manager(path: PathBuf) -> Arc<dyn SectorsManager> {
        unimplemented!()
    }
}

pub mod transfer_public {
    use crate::{RegisterCommand, MAGIC_NUMBER};
    use std::io::Error;
    use tokio::io::{AsyncRead, AsyncWrite, AsyncReadExt, AsyncWriteExt};
    use uuid::Uuid;
    use crate::domain::*;

    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;

    struct HmacAsyncReader<'a> {
        data: &'a mut (dyn AsyncRead + Send + Unpin),
        client_hmac: HmacSha256,
        system_hmac: HmacSha256
    }
    impl<'a> HmacAsyncReader<'a> {
        fn update_hmacs(&mut self, data: &[u8]) {
            self.client_hmac.update(&data);
            self.system_hmac.update(&data);
        }
        pub async fn read_until_magic(&mut self) -> Result<(), Error> {
            let mut buf = Vec::<u8>::new();
            while buf != MAGIC_NUMBER {
                let c = self.data.read_u8().await?;
                buf.push(c);
                if buf.len() > MAGIC_NUMBER.len() {
                    buf.drain(0..(MAGIC_NUMBER.len() - buf.len() - 1));
                }
            }
            self.update_hmacs(&buf);
            Ok(())
        }
        pub async fn read_exact(&mut self, slice: &mut [u8]) -> Result<(), Error> {
            self.data.read_exact(slice).await?;
            self.update_hmacs(&slice);
            Ok(())
        }
        pub async fn read_u8(&mut self) -> Result<u8, Error> {
            let mut buf = [0u8];
            self.data.read_exact(&mut buf).await?;
            self.update_hmacs(&buf);
            Ok(buf[0])
        }
        pub async fn read_u64(&mut self) -> Result<u64, Error> {
            let mut buf = [0u8; 8];
            self.data.read_exact(&mut buf).await?;
            self.update_hmacs(&buf);
            Ok(u64::from_be_bytes(buf))
        }
    }

    struct HmacAsyncWriter<'a> {
        writer: &'a mut (dyn AsyncWrite + Send + Unpin),
        hmac: HmacSha256,
    }
    impl<'a> HmacAsyncWriter<'a> {
        fn update_hmac(&mut self, data: &[u8]) {
            self.hmac.update(&data);
        }
        pub async fn write_all(&mut self, data: &[u8]) -> Result<(), Error> {
            self.update_hmac(data);
            self.writer.write_all(data).await
        }
        pub async fn write_u8(&mut self, data: u8) -> Result<(), Error> {
            let buf = [data];
            self.update_hmac(&buf);
            self.writer.write_u8(data).await
        }
        pub async fn write_u64(&mut self, data: u64) -> Result<(), Error> {
            let buf = data.to_be_bytes();
            self.update_hmac(&buf);
            self.writer.write_u64(data).await
        }
    }

    fn make_error() -> Error {
        Error::last_os_error()
    }

    pub async fn deserialize_register_command(
        data: &mut (dyn AsyncRead + Send + Unpin),
        hmac_system_key: &[u8; 64],
        hmac_client_key: &[u8; 32],
    ) -> Result<(RegisterCommand, bool), Error> {
        // TODO: 
        /*
        When a message which does not comply with the presented above formats is received, it shall be handled as follows:

        The solution shall slide over bytes in the stream until it detects a valid magic number. This marks a beginning of a message.
        If a message type is invalid, the solution shall discard the magic number and the following 4 bytes (8 bytes in total).
        In case of every other error, the solution shall consume the same number of bytes as if a message of this type was processed successfully.
        */        

        let system_hmac = HmacSha256::new_from_slice(hmac_system_key).or(Err(make_error()))?;
        let client_hmac = HmacSha256::new_from_slice(hmac_client_key).or(Err(make_error()))?;
        let mut reader = HmacAsyncReader{data, client_hmac, system_hmac};

        reader.read_until_magic().await?;

        let mut padding = [0u8; 2];
        reader.read_exact(&mut padding).await?;

        let process_rank = reader.read_u8().await?;
        let msg_type_u8 = reader.read_u8().await?;
        
        let payload = if 1 <= msg_type_u8 && msg_type_u8 <= 2 {
            let request_identifier = reader.read_u64().await?;
            let sector_idx = reader.read_u64().await?;

            let header = ClientCommandHeader{request_identifier, sector_idx};

            let content = match msg_type_u8 {
                1 => {
                    ClientRegisterCommandContent::Read
                },
                2 => {
                    let mut content = [0u8; 4096];
                    reader.read_exact(&mut content).await?;
                    ClientRegisterCommandContent::Write{data: SectorVec(Vec::from(content))}
                },
                _ => unreachable!(),
            };
            RegisterCommand::Client(ClientRegisterCommand{header, content})
        } else if 3 <= msg_type_u8 && msg_type_u8 <= 6 {
            let mut uuid_buf = [0u8; 16];
            reader.read_exact(&mut uuid_buf).await?;
            let uuid = Uuid::from_bytes(uuid_buf);
            let rid = reader.read_u64().await?;
            let sector_idx = reader.read_u64().await?;
            let header = SystemCommandHeader {
                process_identifier: process_rank,
                msg_ident: uuid,
                read_ident: rid,
                sector_idx,
            };
            let content = match msg_type_u8 {
                3 => SystemRegisterCommandContent::ReadProc,
                4..=5 => {
                    let timestamp = reader.read_u64().await?;
                    let mut padding = [0u8; 7];
                    reader.read_exact(&mut padding).await?;
                    let write_rank = reader.read_u8().await?;
                    let mut sector_data_arr = [0u8; 4096];
                    reader.read_exact(&mut sector_data_arr).await?;
                    let sector_data = SectorVec(Vec::from(sector_data_arr));
                    match msg_type_u8 {
                        4 => SystemRegisterCommandContent::Value { timestamp, write_rank, sector_data },
                        5 => SystemRegisterCommandContent::WriteProc { timestamp, write_rank, data_to_write: sector_data },
                        _ => unreachable!(),
                    }
                },
                6 => SystemRegisterCommandContent::Ack,
                _ => unreachable!(),
            };
            RegisterCommand::System(SystemRegisterCommand{header, content})
        } else {
            // TODO
            return Err(make_error());
        };

        let hmac = match payload {
            RegisterCommand::Client(_) => reader.client_hmac,
            RegisterCommand::System(_) => reader.system_hmac,
        };
        let mut hmac_buf = [0u8; 32];
        drop(reader.data);
        data.read_exact(&mut hmac_buf).await?;
        let ok = hmac.verify_slice(&hmac_buf).is_ok();
        Ok((payload, ok))
    }

    pub async fn serialize_register_command(
        cmd: &RegisterCommand,
        data: &mut (dyn AsyncWrite + Send + Unpin),
        hmac_key: &[u8],
    ) -> Result<(), Error> {
        let hmac = HmacSha256::new_from_slice(hmac_key).or(Err(make_error()))?;

        let mut writer = HmacAsyncWriter{writer: data, hmac};
        writer.write_all(&MAGIC_NUMBER).await?;
        
        let padding = [42u8, 24u8];
        writer.write_all(&padding).await?;
        match cmd {
            RegisterCommand::Client(client) => {
                // rest of the padding
                writer.write_u8(0u8).await?;
                writer.write_u8(match &client.content {
                    ClientRegisterCommandContent::Read => 1u8,
                    ClientRegisterCommandContent::Write { .. } => 2u8,
                }).await?;
                writer.write_u64(client.header.request_identifier).await?;
                writer.write_u64(client.header.sector_idx).await?;
                if let ClientRegisterCommandContent::Write{data} = &client.content {
                    writer.write_all(&data.0).await?;
                }
            }
            RegisterCommand::System(system) => {
                writer.write_u8(system.header.process_identifier).await?;
                writer.write_u8(match &system.content {
                    SystemRegisterCommandContent::ReadProc => 3u8,
                    SystemRegisterCommandContent::Value { .. } => 4u8,
                    SystemRegisterCommandContent::WriteProc { .. } => 5u8,
                    SystemRegisterCommandContent::Ack => 6u8,
                }).await?;
                writer.write_all(system.header.msg_ident.as_bytes()).await?;
                writer.write_u64(system.header.read_ident).await?;
                writer.write_u64(system.header.sector_idx).await?;
                writer.write_all(match &system.content {
                    SystemRegisterCommandContent::ReadProc => &[],
                    SystemRegisterCommandContent::Value { timestamp: _, write_rank: _, sector_data } => &sector_data.0,
                    SystemRegisterCommandContent::WriteProc { timestamp: _, write_rank: _, data_to_write } => &data_to_write.0,
                    SystemRegisterCommandContent::Ack => &[],
                }).await?;
            },
        };
        let computed_hmac = writer.hmac.finalize().into_bytes();
        drop(writer.writer);
        data.write_all(&computed_hmac).await?;
        Ok(())
    }
}

pub mod register_client_public {
    use crate::SystemRegisterCommand;
    use std::sync::Arc;

    #[async_trait::async_trait]
    /// We do not need any public implementation of this trait. It is there for use
    /// in AtomicRegister. In our opinion it is a safe bet to say some structure of
    /// this kind must appear in your solution.
    pub trait RegisterClient: core::marker::Send + core::marker::Sync {
        /// Sends a system message to a single process.
        async fn send(&self, msg: Send);

        /// Broadcasts a system message to all processes in the system, including self.
        async fn broadcast(&self, msg: Broadcast);
    }

    pub struct Broadcast {
        pub cmd: Arc<SystemRegisterCommand>,
    }

    pub struct Send {
        pub cmd: Arc<SystemRegisterCommand>,
        /// Identifier of the target process. Those start at 1.
        pub target: u8,
    }
}

pub mod stable_storage_public {
    #[async_trait::async_trait]
    /// A helper trait for small amount of durable metadata needed by the register algorithm
    /// itself. Again, it is only for AtomicRegister definition. StableStorage in unit tests
    /// is durable, as one could expect.
    pub trait StableStorage: Send + Sync {
        async fn put(&mut self, key: &str, value: &[u8]) -> Result<(), String>;

        async fn get(&self, key: &str) -> Option<Vec<u8>>;

        async fn remove(&mut self, key: &str) -> bool;
    }


    use base64;
use sha2::{self, Digest};
use std::path::PathBuf;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;


struct SStorage {
    root_storage_dir: PathBuf,
    root: tokio::fs::File,
}

fn encode<T: AsRef<[u8]>>(data: T) -> String {
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
        let k = sha2::Sha512::digest(value);
        let path = "value".to_string() + &encode(&k);
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
            tokio::fs::remove_file(path).await.unwrap();
            self.root.sync_data().await.unwrap();
            true
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
        root_storage_dir: root_storage_dir,
        root,
    })
}

}
