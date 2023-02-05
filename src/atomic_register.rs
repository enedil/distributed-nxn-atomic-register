use crate::{
    Broadcast, ClientCommandHeader, ClientRegisterCommand, ClientRegisterCommandContent,
    OperationSuccess, RegisterClient, SectorVec, SectorsManager, StableStorage,
    SystemCommandHeader, SystemRegisterCommand, SystemRegisterCommandContent,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::{alloc::System, collections::HashMap};
use uuid::{timestamp, Uuid};

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
            dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync,
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
    Box::new(ARegister::new(
        self_ident,
        metadata,
        register_client,
        sectors_manager,
        processes_count,
    ))
}

type ValType = Option<SectorVec>;

#[derive(Clone, Debug)]
enum AckOpt {
    NotAck,
    Ack,
}

struct ReadListElem {
    max_ts: u64,
    wr: u64,
    val: ValType,
}

#[derive(Clone, Debug)]
struct RegContent {
    timestamp: u64,
    wr: u8,
    val: ValType,
    rid: u64,
    readlist: Vec<Option<(u64, u8, ValType)>>,
    acklist: Vec<AckOpt>,
    reading: bool,
    writing: bool,
    writeval: ValType,
    readval: ValType,
    write_phase: bool,
}

impl RegContent {
    async fn new(reg: &mut ARegister) -> RegContent {
        unimplemented!();
        let n = reg.processes_count as usize;
        let r = RegContent {
            timestamp: 0,
            wr: 0,
            val: None,
            rid: 0,
            readlist: reg.fresh_readlist(),
            acklist: reg.fresh_acklist(),
            reading: false,
            writing: false,
            writeval: None,
            readval: None,
            write_phase: false,
        };
        reg.metadata.put("rid", &0u8.to_be_bytes()).await.unwrap();
        reg.metadata.put("rid", &0u8.to_be_bytes()).await.unwrap();
        reg.metadata.put("rid", &0u8.to_be_bytes()).await.unwrap();
        todo!();
        // jak robić store? retreive?
        r
    }
}

struct ARegister {
    self_ident: u8,
    metadata: Box<dyn StableStorage>,
    register_client: Arc<dyn RegisterClient>,
    sectors_manager: Arc<dyn SectorsManager>,
    processes_count: u8,

    rid: u64,

    // usize - sector index
    content: HashMap<usize, RegContent>,
}

impl ARegister {
    const FAILED_TO_GET_REGCONTENT: &str = "failed to get regcontent";

    fn new(
        self_ident: u8,
        metadata: Box<dyn StableStorage>,
        register_client: Arc<dyn RegisterClient>,
        sectors_manager: Arc<dyn SectorsManager>,
        processes_count: u8,
    ) -> ARegister {
        ARegister {
            self_ident,
            metadata,
            register_client,
            sectors_manager,
            processes_count,
            rid: 0,
            content: HashMap::new(),
        }
    }

    async fn try_init(&mut self, idx: usize) {
        if !self.content.contains_key(&idx) {
            let content = RegContent::new(self).await;
            self.content.insert(idx, content);
        }
    }

    fn get_mut(&mut self, idx: usize) -> &mut RegContent {
        self.content
            .get_mut(&idx)
            .expect(ARegister::FAILED_TO_GET_REGCONTENT)
    }

    fn refresh_lists(&mut self, idx: usize) {
        let fresh_readlist = self.fresh_readlist();
        let fresh_acklist = self.fresh_acklist();

        let mut regcontent = self.get_mut(idx);
        regcontent.readlist = fresh_readlist;
        regcontent.acklist = fresh_acklist;
    }

    async fn handle_read(&mut self, header: ClientCommandHeader) {
        let idx = header.sector_idx as usize;

        self.refresh_lists(idx);
        self.get_mut(idx).reading = true;
        self.store_rid().await;

        let cmd = SystemRegisterCommand {
            header: SystemCommandHeader {
                process_identifier: self.self_ident,
                sector_idx: header.sector_idx,
                read_ident: self.rid,
                msg_ident: Uuid::new_v4(),
            },
            content: SystemRegisterCommandContent::ReadProc,
        };
        let msg = Broadcast { cmd: Arc::new(cmd) };
        self.register_client.broadcast(msg).await;
    }

    async fn handle_write(&mut self, header: ClientCommandHeader, data: SectorVec) {
        let i = header.sector_idx as usize;

        self.get_mut(i).writeval = Some(data);
        self.refresh_lists(i);
        self.store_rid().await;
        self.get_mut(i).writing = true;
        let cmd = SystemRegisterCommand {
            header: SystemCommandHeader {
                process_identifier: self.self_ident,
                sector_idx: header.sector_idx,
                read_ident: self.rid,
                msg_ident: Uuid::new_v4(),
            },
            content: SystemRegisterCommandContent::ReadProc,
        };
        let msg = Broadcast { cmd: Arc::new(cmd) };
        self.register_client.broadcast(msg).await;
    }

    async fn handle_read_proc(&mut self, header: SystemCommandHeader) {
        let SystemCommandHeader {
            process_identifier,
            msg_ident,
            read_ident,
            sector_idx,
        } = header;

        let sector_data = self.sectors_manager.read_data(sector_idx).await;
        let (timestamp, write_rank) = self.sectors_manager.read_metadata(sector_idx).await;

        self.register_client
            .send(crate::Send {
                cmd: Arc::new(SystemRegisterCommand {
                    content: SystemRegisterCommandContent::Value {
                        timestamp,
                        write_rank,
                        sector_data,
                    },
                    header: SystemCommandHeader {
                        msg_ident,
                        process_identifier,
                        read_ident,
                        sector_idx,
                    },
                }),
                target: process_identifier,
            })
            .await;
    }

    async fn handle_value(
        &mut self,
        header: SystemCommandHeader,
        timestamp: u64,
        write_rank: u8,
        sector_data: SectorVec,
    ) {
        let SystemCommandHeader {
            process_identifier,
            msg_ident,
            read_ident,
            sector_idx,
        } = header;

        let rid = self.rid;
        let c = self.get_mut(sector_idx as usize);
        if read_ident == rid && !c.write_phase {
            c.readlist[process_identifier as usize] =
                Some((timestamp, write_rank, sector_data.into()));
            if (c.reading || c.writing)
                && 2 * readlist_size(&c.readlist) > (self.processes_count as usize)
            {
                c.readlist[self.self_ident as usize] = Some(todo!());
                todo!();
            }
        }
    }

    async fn handle_write_proc(
        &mut self,
        header: SystemCommandHeader,
        timestamp: u64,
        write_rank: u8,
        data_to_write: SectorVec,
    ) {
        let SystemCommandHeader {
            process_identifier,
            msg_ident,
            read_ident,
            sector_idx,
        } = header;

        let i = sector_idx as usize;
        let c = self.get_mut(i);
        if (timestamp, write_rank) > (c.timestamp, c.wr) {
            c.timestamp = timestamp;
            c.wr = write_rank;
            c.val = Some(data_to_write);
            self.store_wr_ts_val(i).await;
        }
    }

    async fn handle_ack(&mut self, header: SystemCommandHeader) {
        let SystemCommandHeader {
            process_identifier,
            msg_ident,
            read_ident,
            sector_idx,
        } = header;
        let i = header.sector_idx as usize;

        let processes_count = self.processes_count as usize;
        let rid = self.rid;

        let fresh_acklist = self.fresh_acklist();
        let c = self.get_mut(i);

        if read_ident == rid && c.write_phase {
            c.acklist[process_identifier as usize] = AckOpt::Ack;
            if 2 * acklist_size(&c.acklist) > processes_count {
                c.acklist = fresh_acklist;
                c.write_phase = false;
                if c.reading {
                    c.reading = false;
                    self.register_client.send(unimplemented!()).await;
                } else {
                    c.writing = false;
                    self.register_client.send(unimplemented!()).await;
                }
            }
        }
    }

    async fn store_rid(&mut self) {
        self.rid += 1;
        todo!();
    }

    async fn store_wr_ts_val(&mut self, i: usize) {
        unimplemented!();
    }

    fn fresh_readlist(&self) -> Vec<Option<(u64, u8, ValType)>> {
        let n = self.processes_count as usize;
        std::iter::repeat(None).take(n).collect::<Vec<_>>()
    }

    fn fresh_acklist(&self) -> Vec<AckOpt> {
        let n = self.processes_count as usize;
        std::iter::repeat(AckOpt::NotAck)
            .take(n)
            .collect::<Vec<_>>()
    }
}

fn readlist_size(readlist: &[Option<(u64, u8, ValType)>]) -> usize {
    let mut count = 0;
    for x in readlist.iter() {
        if let Some(..) = x {
            count += 1;
        }
    }
    count
}
fn acklist_size(readlist: &[AckOpt]) -> usize {
    let mut count = 0;
    for x in readlist.iter() {
        if let AckOpt::Ack = x {
            count += 1;
        }
    }
    count
}

#[async_trait::async_trait]
impl AtomicRegister for ARegister {
    async fn client_command(
        &mut self,
        cmd: ClientRegisterCommand,
        success_callback: Box<
            dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync,
        >,
    ) {
        self.try_init(cmd.header.sector_idx as usize).await;
        match cmd.content {
            ClientRegisterCommandContent::Read => self.handle_read(cmd.header).await,
            ClientRegisterCommandContent::Write { data } => {
                self.handle_write(cmd.header, data).await
            }
        }
    }

    async fn system_command(&mut self, cmd: SystemRegisterCommand) {
        self.try_init(cmd.header.sector_idx as usize).await;
        match cmd.content {
            SystemRegisterCommandContent::ReadProc => self.handle_read_proc(cmd.header).await,
            SystemRegisterCommandContent::Value {
                timestamp,
                write_rank,
                sector_data,
            } => {
                self.handle_value(cmd.header, timestamp, write_rank, sector_data)
                    .await
            }
            SystemRegisterCommandContent::WriteProc {
                timestamp,
                write_rank,
                data_to_write,
            } => {
                self.handle_write_proc(cmd.header, timestamp, write_rank, data_to_write)
                    .await
            }
            SystemRegisterCommandContent::Ack => self.handle_ack(cmd.header).await,
        };
    }
}
