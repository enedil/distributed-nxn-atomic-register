mod atomic_register;
mod domain;
mod register_client;
mod sectors_manager;
mod stable_storage;
mod transfer;
use std::sync::Arc;

use async_channel::Receiver;
use async_channel::Sender;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Mutex;

pub use crate::atomic_register::*;
pub use crate::domain::*;
pub use crate::register_client::*;
pub use crate::sectors_manager::*;
pub use crate::stable_storage::*;
pub use crate::transfer::*;

enum RegisterCommandInternal {
    System(SystemRegisterCommand),
    Client((ClientRegisterCommand, Arc<Mutex<OwnedWriteHalf>>)),
}

fn sector_index_to_register_index(config: &PublicConfiguration, idx: usize) -> usize {
    idx % config.tcp_locations.len()
}

async fn handle_connection(
    config: Configuration,
    sock: tokio::net::TcpStream,
    nnars: Arc<Mutex<Vec<Sender<RegisterCommandInternal>>>>,
) {
    let (mut rx, tx) = sock.into_split();
    let writer = Arc::new(Mutex::new(tx));
    loop {
        let result =
            deserialize_register_command(&mut rx, &config.hmac_system_key, &config.hmac_client_key)
                .await;
        if let Ok((cmd, ok)) = result {
            let optype = match &cmd {
                RegisterCommand::Client(c) => Some(match c.content {
                    ClientRegisterCommandContent::Read => ClientOperationType::Read,
                    ClientRegisterCommandContent::Write { .. } => ClientOperationType::Write,
                }),
                RegisterCommand::System(_) => None,
            };
            if ok {
                let (msg, sector_idx) = match cmd {
                    RegisterCommand::Client(client) => {
                        let idx = client.header.sector_idx as usize;
                        (
                            RegisterCommandInternal::Client((client, writer.clone())),
                            idx,
                        )
                    }
                    RegisterCommand::System(system) => {
                        let idx = system.header.sector_idx as usize;
                        (RegisterCommandInternal::System(system), idx)
                    }
                };

                if (sector_idx as u64) < config.public.n_sectors {
                    let specific_sender = {
                        let nnar_vec = nnars.lock().await;
                        nnar_vec[sector_index_to_register_index(&config.public, sector_idx)]
                            .clone()
                    };
                    specific_sender.send(msg).await.expect("bbb");
                } else {
                    let w = &mut *writer.lock().await;
                    serialize_response_to_client(
                        w,
                        &config.hmac_system_key,
                        ClientResponse::InvalidSector(
                            optype.expect("system sent invalid sector index!"),
                        ),
                    )
                    .await.expect("coś sie nie udauo");
                }
            } else {
                let w = &mut *writer.lock().await;
                serialize_response_to_client(
                    w,
                    &config.hmac_system_key,
                    ClientResponse::InvalidHmac(optype.expect("system sent invalid hmac")),
                )
                .await.expect("coś się nie udauo");
            }
        } else {
            // continue
        }
    }
}

async fn client_task(
    ar: Arc<Mutex<Box<dyn AtomicRegister>>>,
    cmd: (ClientRegisterCommand, Arc<Mutex<OwnedWriteHalf>>),
    hmac_key: Arc<[u8; 64]>,
) {
    let (finish_tx, finish_rx) = async_channel::bounded(1);
    {
        let mut reg = ar.lock().await;
        reg.client_command(
            cmd.0,
            Box::new(|op_c| {
                Box::pin(async move {
                    let client_socket = &mut *cmd.1.lock().await;
                    serialize_response_to_client(
                        client_socket,
                        &hmac_key,
                        ClientResponse::Ok(op_c),
                    )
                    .await
                    .ok()
                    .map(|_| todo!());
                    finish_tx.send(()).await.unwrap()
                })
            }),
        )
        .await;
    }
    finish_rx.recv().await.expect("aaaa");
}

async fn handle_cmd(
    ar: Arc<Mutex<Box<dyn AtomicRegister>>>,
    c: Result<RegisterCommandInternal, async_channel::RecvError>,
    client_sender: Sender<(ClientRegisterCommand, Arc<Mutex<OwnedWriteHalf>>)>,
) {
    let cmd = c.expect("failed to read a command from atomic register");
    match cmd {
        RegisterCommandInternal::System(system) => {
            let mut reg = ar.lock().await;
            reg.system_command(system).await;
        }
        RegisterCommandInternal::Client((client, sender)) => {
            client_sender
                .send((client, sender))
                .await
                .expect("failed to send msg to client");
        }
    };
}

async fn process_register(
    reg: Box<dyn AtomicRegister>,
    rx: Receiver<RegisterCommandInternal>,
    hmac_system_key: Arc<[u8; 64]>,
) {
    let (client_sender, client_receiver) = async_channel::unbounded();
    let mut current_client_task: Option<tokio::task::JoinHandle<()>> = None;

    let regg = Arc::new(Mutex::new(reg));

    loop {
        match &mut current_client_task {
            Some(task_handle) => {
                tokio::select! {
                    cmd = rx.recv() => handle_cmd(regg.clone(), cmd, client_sender.clone()).await,
                    _ = task_handle => current_client_task = None,
                }
            }
            None => {
                tokio::select! {
                    cmd = rx.recv() => handle_cmd(regg.clone(), cmd, client_sender.clone()).await,
                    client_cmd = client_receiver.recv() =>
                        current_client_task = Some(tokio::spawn(client_task(regg.clone(), client_cmd.unwrap(), hmac_system_key.clone()))),
                }
            }
        }
    }
}

async fn make_atomic_register_process(
    config: &Configuration,
    sectors_manager: Arc<dyn SectorsManager>,
    register_client: Arc<dyn RegisterClient>,
    i: usize,
) -> (
    tokio::task::JoinHandle<()>,
    async_channel::Sender<RegisterCommandInternal>,
) {
    let self_ident = config.public.self_rank;

    let mut dir = config.public.storage_dir.clone();
    dir.push("naar");
    tokio::fs::create_dir(dir.clone()).await.unwrap();
    dir.push(format!("{:06x}", i));
    let stable_storage = build_stable_storage(dir).await;
    let reg = build_atomic_register(
        self_ident,
        stable_storage,
        register_client,
        sectors_manager,
        config.public.tcp_locations.len() as u8,
    )
    .await;

    let (tx, rx) = async_channel::unbounded();

    (
        tokio::spawn(process_register(reg, rx, Arc::new(config.hmac_system_key))),
        tx,
    )
}

pub async fn run_register_process(config: Configuration) {
    let (host, port) = &config.public.tcp_locations[(config.public.self_rank - 1) as usize];
    let sock = tokio::net::TcpListener::bind(format!("{}:{}", host, port))
        .await
        .unwrap();

    let sectors_manager = build_sectors_manager(config.public.storage_dir.clone()).await;
    let register_client = Arc::new(RegisterClientImpl::new(&config));

    let mut nnars = vec![];
    let mut join_handles = vec![];
    for i in 0..100 {
        // todo - czemu 100?
        let (jh, ar) = make_atomic_register_process(
            &config,
            sectors_manager.clone(),
            register_client.clone(),
            i,
        )
        .await;
        nnars.push(ar);
        join_handles.push(jh);
    }

    let nnars_shared = Arc::new(Mutex::new(nnars));

    loop {
        let s = sock.accept().await.unwrap().0;
        tokio::spawn(handle_connection(config.clone(), s, nnars_shared.clone()));
    }
}
