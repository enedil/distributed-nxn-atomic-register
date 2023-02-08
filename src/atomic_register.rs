use crate::domain::ReadReturn;
use crate::{
    Broadcast, ClientCommandHeader, ClientRegisterCommand, ClientRegisterCommandContent,
    OperationReturn, OperationSuccess, RegisterClient, SectorIdx, SectorVec, SectorsManager,
    StableStorage, SystemCommandHeader, SystemRegisterCommand, SystemRegisterCommandContent,
};
use core::panic;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use uuid::Uuid;

type CallbackType =
    Box<dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

#[async_trait::async_trait]
pub trait AtomicRegister: Send + Sync {
    /// Handle a client command. After the command is completed, we expect
    /// callback to be called. Note that completion of client command happens after
    /// delivery of multiple system commands to the register, as the algorithm specifies.
    ///
    /// This function corresponds to the handlers of Read and Write events in the
    /// (N,N)-AtomicRegister algorithm.
    async fn client_command(&mut self, cmd: ClientRegisterCommand, success_callback: CallbackType);

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
    Box::new(
        ARegister::new(
            self_ident,
            metadata,
            register_client,
            sectors_manager,
            processes_count,
        )
        .await,
    )
}

type ValType = Option<SectorVec>;

#[derive(Clone, Debug)]
enum AckOpt {
    NotAck,
    Ack,
}

#[derive(Clone, Debug)]
struct WrTsVal {
    timestamp: u64,
    wr: u8,
    val: ValType,
}

#[derive(Clone, Debug, PartialEq)]
enum OperationStatus {
    Idle,
    Writing(SectorVec),
    Reading,
}

#[derive(Clone, Debug)]
struct RegContent {
    wrtsval: WrTsVal,
    readlist: Vec<Option<WrTsVal>>,
    acklist: Vec<AckOpt>,
    status: OperationStatus,
    readval: ValType,
    write_phase: bool,
}

impl RegContent {
    fn new(process_count: u8, status: OperationStatus) -> RegContent {
        RegContent {
            wrtsval: WrTsVal {
                timestamp: 0,
                wr: 0,
                val: None,
            },
            readlist: fresh_readlist(process_count),
            acklist: fresh_acklist(process_count),
            status,
            write_phase: false,
            readval: None,
        }
    }
    async fn from_storage(
        process_count: u8,
        status: OperationStatus,
        sectors_manager: &mut Arc<dyn SectorsManager>,
        sector_idx: u64,
    ) -> RegContent {
        let wrtsval = read_wr_ts_val(sectors_manager, sector_idx).await;
        let mut r = RegContent::new(process_count, status);
        r.wrtsval = wrtsval;
        r
    }
}

struct OngoingCommand {
    content: RegContent,
    command_uuid: Uuid,
    success_callback: Option<CallbackType>,
    sector_idx: usize,
    request_identifier: u64,
}

struct ARegister {
    self_ident: u8,
    metadata: Box<dyn StableStorage>,
    register_client: Arc<dyn RegisterClient>,
    sectors_manager: Arc<dyn SectorsManager>,
    process_count: u8,

    rid: u64,

    content: Option<OngoingCommand>,
}

async fn read_wr_ts_val(sectors_manager: &mut Arc<dyn SectorsManager>, idx: SectorIdx) -> WrTsVal {
    let ts_wr = sectors_manager.read_metadata(idx).await;
    let data = sectors_manager.read_data(idx).await;
    WrTsVal {
        val: Some(data),
        timestamp: ts_wr.0,
        wr: ts_wr.1,
    }
}

async fn write_wr_ts_val(
    sectors_manager: &mut Arc<dyn SectorsManager>,
    idx: SectorIdx,
    wrtsval: WrTsVal,
) {
    let WrTsVal { timestamp, wr, val } = wrtsval;
    sectors_manager
        .write(idx, &(val.unwrap(), timestamp, wr))
        .await;
    //todo!("sprawdź, czy .write działa dobrze");
}

fn from_le_bytes(v: Vec<u8>) -> u64 {
    assert_eq!(v.len(), 8);
    let mut buf = [0u8; 8];
    for (x, y) in std::iter::zip(&mut buf, v) {
        *x = y;
    }
    u64::from_le_bytes(buf)
}

impl ARegister {
    const RID_KEY: &str = "ridd";

    async fn new(
        self_ident: u8,
        metadata: Box<dyn StableStorage>,
        register_client: Arc<dyn RegisterClient>,
        sectors_manager: Arc<dyn SectorsManager>,
        process_count: u8,
    ) -> ARegister {
        let old_rid = metadata
            .get(ARegister::RID_KEY)
            .await
            .map(from_le_bytes)
            .unwrap_or(0);

        ARegister {
            self_ident,
            metadata,
            register_client,
            sectors_manager,
            process_count,
            rid: old_rid,
            content: None, // todo: recover
        }
    }

    async fn handle_read(&mut self, header: ClientCommandHeader, success_callback: CallbackType) {
        log::warn!("selfid={} handle_read {:?}", self.self_ident, header);

        let msg_ident = Uuid::new_v4();

        let content = RegContent::from_storage(
            self.process_count,
            OperationStatus::Reading,
            &mut self.sectors_manager,
            header.sector_idx,
        )
        .await;

        self.content = Some(OngoingCommand {
            content,
            command_uuid: msg_ident,
            success_callback: Some(success_callback),
            sector_idx: header.sector_idx as usize,
            request_identifier: header.request_identifier,
        });

        self.store_rid().await;

        let cmd = SystemRegisterCommand {
            header: SystemCommandHeader {
                process_identifier: self.self_ident,
                sector_idx: header.sector_idx,
                read_ident: self.rid,
                msg_ident,
            },
            content: SystemRegisterCommandContent::ReadProc,
        };

        let msg = Broadcast { cmd: Arc::new(cmd) };
        self.register_client.broadcast(msg).await;
    }

    async fn handle_write(
        &mut self,
        header: ClientCommandHeader,
        data: SectorVec,
        success_callback: CallbackType,
    ) {
        log::warn!("selfid={} handle_write {:?}", self.self_ident, header);

        let msg_ident = Uuid::new_v4();

        let content = RegContent::from_storage(
            self.process_count,
            OperationStatus::Writing(data),
            &mut self.sectors_manager,
            header.sector_idx,
        )
        .await;

        self.content = Some(OngoingCommand {
            content,
            command_uuid: msg_ident,
            success_callback: Some(success_callback),
            sector_idx: header.sector_idx as usize,
            request_identifier: header.request_identifier
        });

        self.store_rid().await;

        let cmd = SystemRegisterCommand {
            header: SystemCommandHeader {
                process_identifier: self.self_ident,
                sector_idx: header.sector_idx,
                read_ident: self.rid,
                msg_ident,
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
        // TODO: skąd czytać te dane?
        let (timestamp, write_rank) = self.sectors_manager.read_metadata(sector_idx).await;

        let msg = crate::Send {
            cmd: Arc::new(SystemRegisterCommand {
                content: SystemRegisterCommandContent::Value {
                    timestamp,
                    write_rank,
                    sector_data,
                },
                header: SystemCommandHeader {
                    msg_ident,
                    process_identifier: self.self_ident,
                    read_ident,
                    sector_idx,
                },
            }),
            target: process_identifier,
        };
        self.register_client.send(msg).await;
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

        let process_count = self.process_count as usize;

        match &mut self.content {
            Some(OngoingCommand {
                content,
                command_uuid,
                success_callback: _,
                sector_idx: current_sector_idx,
                request_identifier,
            }) => {
                if *command_uuid == msg_ident && sector_idx as usize == *current_sector_idx {
                    if read_ident == self.rid && !content.write_phase {
                        content.readlist[process_identifier as usize - 1] = Some(WrTsVal {
                            timestamp,
                            wr: write_rank,
                            val: Some(sector_data),
                        });
                        assert_readlist_correst(&content.readlist);

                        if content.status != OperationStatus::Idle
                            && 2 * readlist_size(&content.readlist) > process_count
                        {
                            content.readlist[self.self_ident as usize - 1] =
                                Some(content.wrtsval.clone());
                            assert_readlist_correst(&content.readlist);

                            let maxread =
                                readlist_highest(&content.readlist).expect("can't be empty");

                            content.readval = maxread.val.clone();

                            content.acklist = fresh_acklist(self.process_count);
                            content.readlist = fresh_readlist(self.process_count);

                            content.write_phase = true;

                            let cmd_content = match &content.status {
                                OperationStatus::Reading => {
                                    SystemRegisterCommandContent::WriteProc {
                                        timestamp: maxread.timestamp,
                                        write_rank: maxread.wr,
                                        data_to_write: content
                                            .readval
                                            .clone()
                                            .expect("at least one non-empty entry"),
                                    }
                                }
                                OperationStatus::Writing(writeval) => {
                                    content.wrtsval = WrTsVal {
                                        timestamp: maxread.timestamp + 1,
                                        wr: self.self_ident,
                                        val: Some(writeval.clone()),
                                    };
                                    write_wr_ts_val(
                                        &mut self.sectors_manager,
                                        sector_idx,
                                        content.wrtsval.clone(),
                                    )
                                    .await;
                                    SystemRegisterCommandContent::WriteProc {
                                        timestamp: content.wrtsval.timestamp,
                                        write_rank: content.wrtsval.wr,
                                        data_to_write: content.wrtsval.val.clone().unwrap(),
                                    }
                                }
                                OperationStatus::Idle => panic!("unreachable"),
                            };

                            self.register_client
                                .broadcast(Broadcast {
                                    cmd: Arc::new(SystemRegisterCommand {
                                        header: SystemCommandHeader {
                                            process_identifier: self.self_ident,
                                            msg_ident,
                                            read_ident,
                                            sector_idx,
                                        },
                                        content: cmd_content,
                                    }),
                                })
                                .await;
                        }
                    }
                } else {
                    self.skip_old_message();
                }
            }
            None => self.skip_old_message(),
        };
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

        let mut c = read_wr_ts_val(&mut self.sectors_manager, sector_idx).await;
        if (timestamp, write_rank) > (c.timestamp, c.wr) {
            c.timestamp = timestamp;
            c.wr = write_rank;
            c.val = Some(data_to_write);
            write_wr_ts_val(&mut self.sectors_manager, sector_idx, c).await;
        }

        let reply = crate::Send {
            cmd: Arc::new(SystemRegisterCommand {
                header: SystemCommandHeader {
                    process_identifier: self.self_ident,
                    msg_ident,
                    read_ident,
                    sector_idx,
                },
                content: SystemRegisterCommandContent::Ack,
            }),
            target: process_identifier,
        };

        self.register_client.send(reply).await;
    }

    fn skip_old_message(&self) {
        // this functions communicates, that this is a response to
        // an older command and should be skipped
    }

    async fn handle_ack(&mut self, header: SystemCommandHeader) {
        let SystemCommandHeader {
            process_identifier,
            msg_ident,
            read_ident,
            sector_idx,
        } = header;

        let processes_count = self.process_count as usize;
        let rid = self.rid;

        match &mut self.content {
            Some(OngoingCommand {
                content: c,
                command_uuid,
                success_callback,
                sector_idx: ongoing_sector_idx,
                request_identifier
            }) => {
                if *ongoing_sector_idx == sector_idx as usize && *command_uuid == msg_ident {
                    if read_ident == rid && c.write_phase {
                        c.acklist[process_identifier as usize - 1] = AckOpt::Ack;
                        if c.status != OperationStatus::Idle
                            && 2 * acklist_size(&c.acklist) > processes_count
                        {
                            c.acklist = fresh_acklist(self.process_count);
                            c.write_phase = false;

                            let op_return = match c.status {
                                OperationStatus::Reading => OperationReturn::Read(ReadReturn {
                                    read_data: c
                                        .readval
                                        .clone()
                                        .expect("now it should be something"),
                                }),
                                OperationStatus::Writing(..) => OperationReturn::Write,
                                OperationStatus::Idle => {
                                    panic!("unreachable");
                                }
                            };

                            let mut callback = None;
                            std::mem::swap(success_callback, &mut callback);
                            callback.unwrap()(OperationSuccess {
                                request_identifier: *request_identifier,
                                op_return,
                            })
                            .await;
                            self.content = None;
                        }
                    } else {
                        self.skip_old_message();
                    }
                } else {
                    self.skip_old_message();
                }
            }
            None => {
                // skip old message
            }
        }
    }

    async fn store_rid(&mut self) {
        self.rid += 1;
        self.metadata
            .put(ARegister::RID_KEY, &self.rid.to_le_bytes())
            .await
            .expect("failed writing rid to metadata storage");
    }
}

fn fresh_readlist(size: u8) -> Vec<Option<WrTsVal>> {
    let n = size as usize;
    std::iter::repeat(None).take(n).collect::<Vec<_>>()
}

fn fresh_acklist(size: u8) -> Vec<AckOpt> {
    let n = size as usize;
    std::iter::repeat(AckOpt::NotAck)
        .take(n)
        .collect::<Vec<_>>()
}

fn readlist_size(readlist: &[Option<WrTsVal>]) -> usize {
    let mut count = 0;
    for x in readlist.iter() {
        if let Some(..) = x {
            count += 1;
        }
    }
    count
}

fn acklist_size(acklist: &[AckOpt]) -> usize {
    let mut count = 0;
    for x in acklist.iter() {
        if let AckOpt::Ack = x {
            count += 1;
        }
    }
    count
}

fn assert_readlist_correst(readlist: &[Option<WrTsVal>]) {
    for x in readlist.iter() {
        if let Some(v) = x {
            assert!(v.val.is_some(), "readlist={:?}", readlist);
        }
    }
}

fn readlist_highest(readlist: &[Option<WrTsVal>]) -> Option<WrTsVal> {
    let mut current_max = readlist[0].clone();
    for entry in readlist.iter() {
        match &current_max {
            Some(curr) => match entry {
                Some(x) => {
                    if (x.timestamp, x.wr) > (curr.timestamp, curr.wr) {
                        current_max = Some(x.clone());
                    }
                }
                None => (),
            },
            None => {
                current_max = entry.clone();
            }
        }
    }
    current_max
}

#[async_trait::async_trait]
impl AtomicRegister for ARegister {
    async fn client_command(&mut self, cmd: ClientRegisterCommand, success_callback: CallbackType) {
        assert!(self.content.is_none());
        match cmd.content {
            ClientRegisterCommandContent::Read => {
                self.handle_read(cmd.header, success_callback).await
            }
            ClientRegisterCommandContent::Write { data } => {
                self.handle_write(cmd.header, data, success_callback).await
            }
        }
    }

    async fn system_command(&mut self, cmd: SystemRegisterCommand) {
        match cmd.content {
            SystemRegisterCommandContent::ReadProc => self.handle_read_proc(cmd.header).await,
            SystemRegisterCommandContent::Value {
                timestamp,
                write_rank,
                sector_data,
            } => {
                assert!(self.content.is_some());
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
            SystemRegisterCommandContent::Ack => {
                assert!(self.content.is_some());
                self.handle_ack(cmd.header).await
            }
        };
    }
}
