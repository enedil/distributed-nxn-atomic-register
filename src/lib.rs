mod atomic_register;
mod domain;
mod register_client;
mod sectors_manager;
mod stable_storage;
mod transfer;

pub use crate::atomic_register::*;
pub use crate::domain::*;
pub use crate::register_client::*;
pub use crate::sectors_manager::*;
pub use crate::stable_storage::*;
pub use crate::transfer::*;

async fn handle_connection(config: Configuration, mut sock: tokio::net::TcpStream) {
    loop {
        let (cmd, ok) = deserialize_register_command(
            &mut sock,
            &config.hmac_system_key,
            &config.hmac_client_key,
        )
        .await
        .unwrap();
        if !ok {
            continue;
        }
        match cmd {
            RegisterCommand::Client(client) => handle_client(&config, client).await,
            RegisterCommand::System(system) => handle_system(&config, system).await,
        }
    }
}

async fn handle_client(_config: &Configuration, _cmd: ClientRegisterCommand) {}

async fn handle_system(_config: &Configuration, _cmd: SystemRegisterCommand) {}

pub async fn run_register_process(config: Configuration) {
    let (host, port) = &config.public.tcp_locations[(config.public.self_rank - 1) as usize];
    let sock = tokio::net::TcpListener::bind(format!("{}:{}", host, port))
        .await
        .unwrap();

    //let mut clients = Vec::new();

    loop {
        let s = sock.accept().await.unwrap().0;
        tokio::spawn(handle_connection(config.clone(), s));
    }
}
