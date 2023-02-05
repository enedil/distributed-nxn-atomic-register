use tokio::{io::AsyncWriteExt, stream, sync::Mutex};

use crate::{
    serialize_register_command, Configuration, PublicConfiguration, RegisterCommand,
    SystemCommandHeader, SystemRegisterCommand,
};
use std::{
    collections::{HashMap, HashSet},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    str::FromStr,
    sync::Arc,
};

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

struct ClientInfo {
    location: (String, u16),
    stream: Option<tokio::net::TcpStream>,
    pending_messages: HashMap<SystemCommandHeader, Arc<SystemRegisterCommand>>,
    hmac_key: Arc<[u8; 64]>,
}

impl ClientInfo {
    fn new(location: (String, u16), hmac_key: Arc<[u8; 64]>) -> ClientInfo {
        ClientInfo {
            location,
            stream: None,
            pending_messages: HashMap::new(),
            hmac_key,
        }
    }

    async fn try_connect(&mut self) {
        if self.stream.is_none() {
            let sockopt = tokio::net::TcpSocket::new_v4().ok();
            if let Some(sock) = sockopt {
                let ipaddropt = IpAddr::from_str(&self.location.0);
                if let Ok(ipaddr) = ipaddropt {
                    let addr = std::net::SocketAddr::new(ipaddr, self.location.1);
                    self.stream = sock.connect(addr).await.ok();
                }
            }
        }
    }

    async fn try_send(&mut self) {
        if let Some(stream) = &mut self.stream {
            for (_, msg) in self.pending_messages.iter() {
                let cmd = RegisterCommand::System(msg.as_ref().clone());
                if let Err(_) =
                    serialize_register_command(&cmd, stream, self.hmac_key.as_ref()).await
                {
                    // ignore
                }
            }
        }
    }

    fn append_message(&mut self, msg: Arc<SystemRegisterCommand>) {
        if self.pending_messages.contains_key(&msg.header) {
            self.pending_messages.remove(&msg.header);
        } else {
            self.pending_messages.insert(msg.header, msg);
        }
    }
}

pub(crate) struct RegisterClientImpl(
    Arc<Vec<Mutex<ClientInfo>>>,
    Option<tokio::task::JoinHandle<()>>,
);

impl RegisterClientImpl {
    pub(crate) fn new(config: &Configuration) -> RegisterClientImpl {
        let hmac_key = Arc::new(config.hmac_system_key);
        let mut client = RegisterClientImpl(
            Arc::new(
                config
                    .public
                    .tcp_locations
                    .iter()
                    .map(|x| Mutex::new(ClientInfo::new(x.clone(), hmac_key.clone())))
                    .collect(),
            ),
            None,
        );
        client.run_timer();
        client
    }

    fn run_timer(&mut self) {
        // meeh
        let client_info = self.0.clone();
        self.1 = Some(tokio::task::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(20));
            
            loop {
                interval.tick().await;

                for guard in client_info.iter() {
                    let mut client = guard.lock().await;
                    client.try_connect().await;
                    client.try_send().await;
                }
            }
        }));
    }
}

#[async_trait::async_trait]
impl RegisterClient for RegisterClientImpl {
    async fn send(&self, msg: Send) {
        let mut client = self.0[msg.target as usize - 1].lock().await;

        client.try_connect().await;
        client.append_message(msg.cmd);
        client.try_send().await;
    }
    async fn broadcast(&self, msg: Broadcast) {
        for i in 1..=self.0.len() {
            self.send(Send {
                target: i as u8,
                cmd: msg.cmd.clone(),
            })
            .await;
        }
    }
}
