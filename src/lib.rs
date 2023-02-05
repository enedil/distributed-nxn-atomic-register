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
        if !ok {}
        match cmd {
            RegisterCommand::Client(client) => handle_client(&config, client).await,
            RegisterCommand::System(system) => handle_system(&config, system).await,
        }
    }
}

async fn handle_client(config: &Configuration, cmd: ClientRegisterCommand) {
    unimplemented!()
}

async fn handle_system(config: &Configuration, cmd: SystemRegisterCommand) {
    unimplemented!()
}

pub async fn run_register_process(config: Configuration) {
    let (host, port) = &config.public.tcp_locations[(config.public.self_rank - 1) as usize];
    let sock = tokio::net::TcpListener::bind(format!("{}:{}", host, port))
        .await
        .unwrap();

    let sectors_manager = build_sectors_manager(config.public.storage_dir.clone()).await;

    let self_ident = config.public.self_rank;

    // let nnars = vec![];
    for i in 0..100 {
        let mut dir = config.public.storage_dir.clone();
        dir.push("naar");
        tokio::fs::create_dir(dir.clone()).await.unwrap();
        dir.push(format!("{:06x}", i));
        let stable_storage = build_stable_storage(dir).await;
        // let reg = build_atomic_register(
        //     self_ident,
        //     stable_storage,
        //     todo!("register client"),
        //     sectors_manager.clone(),
        //     config.public.tcp_locations.len() as u8,
        // )
        // .await;
        // nnars.push(reg);
    }

    loop {
        let s = sock.accept().await.unwrap().0;
        tokio::spawn(handle_connection(config.clone(), s));
    }
}
