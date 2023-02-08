use crate::domain::*;
use crate::{RegisterCommand, MAGIC_NUMBER};
use std::io::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use uuid::Uuid;

use hmac::{Hmac, Mac};
use sha2::Sha256;
type HmacSha256 = Hmac<Sha256>;

struct HmacAsyncReader<'a> {
    data: &'a mut (dyn AsyncRead + Send + Unpin),
    client_hmac: HmacSha256,
    system_hmac: HmacSha256,
    buf: Vec<u8>,
}
impl<'a> HmacAsyncReader<'a> {
    fn update_hmacs(&mut self, data: &[u8]) {
        self.client_hmac.update(data);
        self.system_hmac.update(data);
        if data.len() == 4096 {
            self.buf.extend_from_slice(&[42u8; 16]);
        } else {
            self.buf.extend_from_slice(data);
        }
    }
    pub async fn read_until_magic(&mut self) -> Result<(), Error> {
        let mut buf = Vec::<u8>::new();
        while buf != MAGIC_NUMBER {
            let c = self.data.read_u8().await?;
            buf.push(c);
            if buf.len() > MAGIC_NUMBER.len() {
                buf.drain(0..=(MAGIC_NUMBER.len() - buf.len()));
            }
        }
        self.update_hmacs(&buf);
        Ok(())
    }
    pub async fn read_exact(&mut self, slice: &mut [u8]) -> Result<(), Error> {
        self.data.read_exact(slice).await?;
        self.update_hmacs(slice);
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
    buf: Vec<u8>,
}
impl<'a> HmacAsyncWriter<'a> {
    fn update_hmac(&mut self, data: &[u8]) {
        self.hmac.update(data);
        if data.len() == 4096 {
            self.buf.extend_from_slice(&[42u8; 16]);
        } else {
            self.buf.extend_from_slice(data);
        }
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

    let system_hmac = HmacSha256::new_from_slice(hmac_system_key).map_err(|_| make_error())?;
    let client_hmac = HmacSha256::new_from_slice(hmac_client_key).map_err(|_| make_error())?;
    let mut reader = HmacAsyncReader {
        data,
        client_hmac,
        system_hmac,
        buf: vec![],
    };

    reader.read_until_magic().await?;

    let mut padding = [0u8; 2];
    reader.read_exact(&mut padding).await?;

    let process_rank = reader.read_u8().await?;
    let msg_type_u8 = reader.read_u8().await?;

    let payload = if (1..=2).contains(&msg_type_u8) {
        let request_identifier = reader.read_u64().await?;
        let sector_idx = reader.read_u64().await?;

        let header = ClientCommandHeader {
            request_identifier,
            sector_idx,
        };

        let content = match msg_type_u8 {
            1 => ClientRegisterCommandContent::Read,
            2 => {
                let mut content = [0u8; 4096];
                reader.read_exact(&mut content).await?;
                ClientRegisterCommandContent::Write {
                    data: SectorVec(Vec::from(content)),
                }
            }
            _ => unreachable!(),
        };
        RegisterCommand::Client(ClientRegisterCommand { header, content })
    } else if (3..=6).contains(&msg_type_u8) {
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
                    4 => SystemRegisterCommandContent::Value {
                        timestamp,
                        write_rank,
                        sector_data,
                    },
                    5 => SystemRegisterCommandContent::WriteProc {
                        timestamp,
                        write_rank,
                        data_to_write: sector_data,
                    },
                    _ => unreachable!(),
                }
            }
            6 => SystemRegisterCommandContent::Ack,
            _ => unreachable!(),
        };
        RegisterCommand::System(SystemRegisterCommand { header, content })
    } else {
        // TODO
        return Err(make_error());
    };

    let hmac = match payload {
        RegisterCommand::Client(_) => reader.client_hmac,
        RegisterCommand::System(_) => reader.system_hmac,
    };
    let x = reader.buf.clone();
    let mut hmac_buf = [0u8; 32];
    data.read_exact(&mut hmac_buf).await?;
    let correct_mac = hmac.clone().finalize().into_bytes();
    log::trace!("read   buf {:02x?}, len={}", x, x.len());
    log::trace!("   cmd={}", cmdname(&payload));
    log::trace!("hmac = {:02x?}\n", hmac_buf);

    let ok = hmac.verify_slice(&hmac_buf).is_ok();
    if !ok {
        log::error!(
            "received {:?} should be {:?}. ckey={:?} skey={:?}",
            hmac_buf,
            correct_mac,
            hmac_client_key,
            hmac_system_key
        );
    }
    Ok((payload, ok))
}

#[repr(u8)]
#[derive(Copy, Clone)]
pub(crate) enum ClientOperationType {
    Read,
    Write,
}

pub(crate) enum ClientResponse {
    Ok(OperationSuccess),
    InvalidHmac(ClientOperationType),
    InvalidSector(ClientOperationType),
}
impl ClientResponse {
    fn to_status(&self) -> StatusCode {
        match self {
            ClientResponse::Ok(_) => StatusCode::Ok,
            ClientResponse::InvalidHmac(_) => StatusCode::AuthFailure,
            ClientResponse::InvalidSector(_) => StatusCode::InvalidSectorIndex,
        }
    }
    fn op_type(&self) -> ClientOperationType {
        match self {
            ClientResponse::Ok(s) => match s.op_return {
                OperationReturn::Read(_) => ClientOperationType::Read,
                OperationReturn::Write => ClientOperationType::Write,
            },
            ClientResponse::InvalidHmac(x) => *x,
            ClientResponse::InvalidSector(x) => *x,
        }
    }
}

pub(crate) async fn serialize_response_to_client(
    data: &mut (dyn AsyncWrite + Send + Unpin),
    hmac_key: &[u8; 32],
    response: ClientResponse,
) -> Result<(), Error> {
    let hmac = HmacSha256::new_from_slice(hmac_key)
        .map_err(|_| make_error())
        .unwrap();

    let mut writer = HmacAsyncWriter {
        writer: data,
        hmac,
        buf: vec![],
    };
    writer.write_all(&MAGIC_NUMBER).await?;

    let padding = [31u8, 45u8];
    writer.write_all(&padding).await?;

    writer.write_u8(response.to_status() as u8).await?;
    writer.write_u8(response.op_type() as u8 + 0x41).await?;

    if let ClientResponse::Ok(success) = response {
        writer.write_u64(success.request_identifier).await?;

        if let OperationReturn::Read(buf) = success.op_return {
            writer.write_all(&buf.read_data.0).await?;
        };
    }

    let computed_hmac = writer.hmac.finalize().into_bytes();
    log::trace!("wrote1 buf={:02x?} len={}", writer.buf, writer.buf.len());
    data.write_all(&computed_hmac).await?;

    Ok(())
}

pub async fn serialize_register_command(
    cmd: &RegisterCommand,
    data: &mut (dyn AsyncWrite + Send + Unpin),
    hmac_key: &[u8],
) -> Result<(), Error> {
    let hmac = HmacSha256::new_from_slice(hmac_key).map_err(|_| make_error())?;

    let mut writer = HmacAsyncWriter {
        writer: data,
        hmac,
        buf: vec![],
    };
    writer.write_all(&MAGIC_NUMBER).await?;

    let padding = [42u8, 24u8];
    writer.write_all(&padding).await?;
    match cmd {
        RegisterCommand::Client(client) => {
            // rest of the padding
            writer.write_u8(0u8).await?;
            writer
                .write_u8(match &client.content {
                    ClientRegisterCommandContent::Read => 1u8,
                    ClientRegisterCommandContent::Write { .. } => 2u8,
                })
                .await?;
            writer.write_u64(client.header.request_identifier).await?;
            writer.write_u64(client.header.sector_idx).await?;
            if let ClientRegisterCommandContent::Write { data } = &client.content {
                writer.write_all(&data.0).await?;
            }
        }
        RegisterCommand::System(system) => {
            writer.write_u8(system.header.process_identifier).await?;
            writer
                .write_u8(match &system.content {
                    SystemRegisterCommandContent::ReadProc => 3u8,
                    SystemRegisterCommandContent::Value { .. } => 4u8,
                    SystemRegisterCommandContent::WriteProc { .. } => 5u8,
                    SystemRegisterCommandContent::Ack => 6u8,
                })
                .await?;
            writer.write_all(system.header.msg_ident.as_bytes()).await?;
            writer.write_u64(system.header.read_ident).await?;
            writer.write_u64(system.header.sector_idx).await?;

            let padding = [0x88u8; 7];
            match &system.content {
                SystemRegisterCommandContent::ReadProc => {},
                SystemRegisterCommandContent::Value {
                    timestamp,
                    write_rank,
                    sector_data,
                } => {
                    writer.write_u64(*timestamp).await?;
                    writer.write_all(&padding).await?;
                    writer.write_u8(*write_rank).await?;
                    writer.write_all(&sector_data.0).await?;
                },
                SystemRegisterCommandContent::WriteProc {
                    timestamp,
                    write_rank,
                    data_to_write,
                } => {
                    writer.write_u64(*timestamp).await?;
                    writer.write_all(&padding).await?;
                    writer.write_u8(*write_rank).await?;
                    writer.write_all(&data_to_write.0).await?;
                }
                SystemRegisterCommandContent::Ack => {},
            }
        }
    };
    let computed_hmac = writer.hmac.finalize().into_bytes();

    log::trace!("wrote2 buf={:02x?} len={}", writer.buf, writer.buf.len());
    log::trace!("   cmd={}", cmdname(cmd));
    log::trace!("hmac = {:02x?}\n", computed_hmac);
    data.write_all(&computed_hmac).await?;
    Ok(())
}

fn cmdname(cmd: &RegisterCommand) ->&str {
    match cmd {
        RegisterCommand::Client(x) => {
            match &x.content {
                ClientRegisterCommandContent::Read => "READ",
                ClientRegisterCommandContent::Write { data } => "WRITE",
            }
        },
        RegisterCommand::System(x) => {
            match &x.content {
                SystemRegisterCommandContent::ReadProc => "READ_PROC",
                SystemRegisterCommandContent::Value { .. } => "VALUE",
                SystemRegisterCommandContent::WriteProc { ..} => "WRITE_PROC",
                SystemRegisterCommandContent::Ack => "ACK",
            }
        },
    }
}