use tokio::sync::Mutex;

use crate::{serialize_register_command, Configuration, RegisterCommand, SystemRegisterCommand};
use std::{collections::HashSet, sync::Arc};

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

#[derive(Debug)]
pub struct Send {
    pub cmd: Arc<SystemRegisterCommand>,
    /// Identifier of the target process. Those start at 1.
    pub target: u8,
}

struct ClientInfo {
    location: (String, u16),
    stream: Option<tokio::net::TcpStream>,
    pending_messages: HashSet<Arc<SystemRegisterCommand>>,
    hmac_key: Arc<[u8; 64]>,
}

impl ClientInfo {
    fn new(location: (String, u16), hmac_key: Arc<[u8; 64]>) -> ClientInfo {
        ClientInfo {
            location,
            stream: None,
            pending_messages: HashSet::new(),
            hmac_key,
        }
    }

    async fn try_connect(&mut self) {
        log::trace!(
            "try connect: we have {} pending messages",
            self.pending_messages.len()
        );
        if self.stream.is_none() {
            // TODO zabezpiecz się przed panic, bo inaczej się skończy na braku bezpieczeństwa - dwóch klientów połączonych
            let addr = format!("{}:{}", self.location.0, self.location.1);
            let c = tokio::net::TcpStream::connect(addr).await;
            match c {
                Ok(stream) => {
                    self.stream = Some(stream);
                }
                Err(e) => {
                    log::error!("failed to connect: err={:?}", e);
                }
            }
        }
    }

    async fn try_send(&mut self) {
        log::trace!(
            "try send: pending messages are {:?}",
            self.pending_messages.len()
        );
        let mut reset_steam = false;
        if let Some(stream) = &mut self.stream {
            for msg in self.pending_messages.iter() {
                let cmd = RegisterCommand::System(msg.as_ref().clone());
                let mut buf: Vec<u8> = vec![];
                if let Err(e) =
                    serialize_register_command(&cmd, stream, self.hmac_key.as_ref()).await
                {
                    log::error!("failed to send message: err={:?}", e);
                    reset_steam = true;
                }
            }
        } else {
            log::error!("invalid stream!");
        }
        if reset_steam {
            self.stream = None;
            self.try_connect().await;
        }
    }

    async fn send_single_message(&mut self, msg: &SystemRegisterCommand) {
        log::trace!(
            "try send: pending messages are {:?}",
            self.pending_messages.len()
        );
        let mut reset_steam = false;
        if let Some(stream) = &mut self.stream {
            let cmd = RegisterCommand::System(msg.clone());

            if let Err(e) = serialize_register_command(&cmd, stream, self.hmac_key.as_ref()).await {
                log::error!("failed to send message: err={:?}", e);
                reset_steam = true;
            }
        } else {
            log::error!("invalid stream!");
        }
        if reset_steam {
            self.stream = None;
            self.try_connect().await;
        }
    }

    fn append_message(&mut self, msg: Arc<SystemRegisterCommand>) {
        if self.pending_messages.contains(&msg) {
            log::trace!(
                "found old message: {:?}, pending len = {}",
                msg,
                self.pending_messages.len()
            );
            self.pending_messages.remove(&msg);
        } else {
            self.pending_messages.insert(msg);
        }
    }
}

pub(crate) struct RegisterClientImpl {
    clients: Arc<Vec<Mutex<ClientInfo>>>,
    timer_handle: Option<tokio::task::JoinHandle<()>>,
    self_id: u8,
}

impl RegisterClientImpl {
    pub(crate) fn new(config: &Configuration) -> RegisterClientImpl {
        let hmac_key = Arc::new(config.hmac_system_key);
        let mut client = RegisterClientImpl {
            clients: Arc::new(
                config
                    .public
                    .tcp_locations
                    .iter()
                    .map(|x| Mutex::new(ClientInfo::new(x.clone(), hmac_key.clone())))
                    .collect(),
            ),
            timer_handle: None,
            self_id: config.public.self_rank,
        };
        client.run_timer();
        client
    }

    fn run_timer(&mut self) {
        // todo meeh
        let client_info = self.clients.clone();
        self.timer_handle = Some(tokio::task::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(100));

            loop {
                interval.tick().await;

                log::debug!("interval kicked");
                for guard in client_info.iter() {
                    let mut client = guard.lock().await;
                    client.try_connect().await;
                    client.try_send().await;
                }
            }
        }));
    }

    async fn send_append(&self, msg: Send) {
        let mut client = self.clients[msg.target as usize - 1].lock().await;

        client.try_connect().await;
        client.append_message(msg.cmd.clone());
        client.send_single_message(&msg.cmd).await;
    }
    async fn send_noappend(&self, msg: Send) {
        let mut client = self.clients[msg.target as usize - 1].lock().await;

        client.try_connect().await;
        client.send_single_message(&msg.cmd).await;
    }
}

#[async_trait::async_trait]
impl RegisterClient for RegisterClientImpl {
    async fn send(&self, msg: Send) {
        self.send_noappend(msg).await;
    }
    async fn broadcast(&self, msg: Broadcast) {
        for i in 1..=self.clients.len() {
            self.send_append(Send {
                target: i as u8,
                cmd: msg.cmd.clone(),
            })
            .await;
        }
    }
}
