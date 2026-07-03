use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;

use crate::error::Error;
use crate::options::{SslMode, SslOptions};

const UTF8_MB4_GENERAL_CI: u16 = 0x002d;
const CLIENT_PROTOCOL_41: u32 = 512;
const CLIENT_SSL: u32 = 2048;
const CLIENT_SECURE_CONNECTION: u32 = 32768;
const CLIENT_PLUGIN_AUTH: u32 = 524288;

trait AsyncReadWrite: AsyncRead + AsyncWrite + Unpin {}

impl<T> AsyncReadWrite for T where T: AsyncRead + AsyncWrite + Unpin {}

pub struct PacketChannel {
    stream: Box<dyn AsyncReadWrite + Send + Sync>,
    seq: u8,
    tls_active: bool,
}

impl PacketChannel {
    pub async fn connect(host: &str, port: u16) -> Result<Self, Error> {
        let addr = format!("{host}:{port}");
        let stream = TcpStream::connect(&addr).await?;
        Ok(Self {
            stream: Box::new(stream),
            seq: 0,
            tls_active: false,
        })
    }

    pub fn from_stream(stream: TcpStream) -> Self {
        Self {
            stream: Box::new(stream),
            seq: 0,
            tls_active: false,
        }
    }

    pub async fn read_packet(&mut self) -> Result<Vec<u8>, Error> {
        let mut header = [0u8; 4];
        self.stream.read_exact(&mut header).await?;
        let len = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
        let seq = header[3];
        if seq != self.seq {
            return Err(Error::Protocol(format!(
                "invalid sequence {seq} != expected {}",
                self.seq
            )));
        }
        self.seq = self.seq.wrapping_add(1);
        let mut payload = vec![0u8; len];
        self.stream.read_exact(&mut payload).await?;
        Ok(payload)
    }

    pub async fn write_packet(&mut self, payload: &[u8]) -> Result<(), Error> {
        let len = payload.len();
        let header = [
            (len & 0xFF) as u8,
            ((len >> 8) & 0xFF) as u8,
            ((len >> 16) & 0xFF) as u8,
            self.seq,
        ];
        self.stream.write_all(&header).await?;
        self.stream.write_all(payload).await?;
        self.stream.flush().await?;
        self.seq = self.seq.wrapping_add(1);
        Ok(())
    }

    pub async fn write_command(&mut self, cmd: u8, payload: &[u8]) -> Result<(), Error> {
        let mut buf = Vec::with_capacity(1 + payload.len());
        buf.push(cmd);
        buf.extend_from_slice(payload);
        self.write_packet(&buf).await
    }

    pub async fn query(&mut self, sql: &str) -> Result<(), Error> {
        self.reset_seq();
        self.write_command(0x03, sql.as_bytes()).await?;
        let packet = self.read_packet().await?;
        check_error_packet(&packet, "query failed")?;
        if packet.first() == Some(&0x00) {
            return Ok(());
        }
        while self.read_packet().await?.first() != Some(&0xFE) {}
        Ok(())
    }

    pub fn reset_seq(&mut self) {
        self.seq = 0;
    }

    pub fn sequence(&self) -> u8 {
        self.seq
    }

    pub fn tls_active(&self) -> bool {
        self.tls_active
    }

    async fn upgrade_tls(&mut self, host: &str, options: &SslOptions) -> Result<(), Error> {
        let config = tls_config(options)?;
        let server_name = ServerName::try_from(host.to_string())
            .map_err(|e| Error::Ssl(format!("invalid TLS server name '{host}': {e}")))?;
        let (placeholder, _) = tokio::io::duplex(0);
        let placeholder = Box::new(placeholder);
        let stream = std::mem::replace(&mut self.stream, placeholder);
        let connector = TlsConnector::from(Arc::new(config));
        let tls_stream = connector
            .connect(server_name, stream)
            .await
            .map_err(|e| Error::Ssl(format!("TLS handshake failed: {e}")))?;
        self.stream = Box::new(tls_stream);
        self.tls_active = true;
        Ok(())
    }
}

pub async fn encode_binlog_dump_async(
    channel: &mut PacketChannel,
    server_id: u32,
    filename: &str,
    position: u32,
) -> Result<(), Error> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&position.to_le_bytes());
    payload.extend_from_slice(&0u16.to_le_bytes());
    payload.extend_from_slice(&server_id.to_le_bytes());
    payload.extend_from_slice(filename.as_bytes());
    payload.push(0);
    channel.write_command(0x12, &payload).await
}

pub async fn encode_register_slave(
    channel: &mut PacketChannel,
    server_id: u32,
    host: &str,
    username: &str,
    password: &str,
) -> Result<(), Error> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&server_id.to_le_bytes());
    payload.push(host.len() as u8);
    payload.extend_from_slice(host.as_bytes());
    payload.push(username.len() as u8);
    payload.extend_from_slice(username.as_bytes());
    payload.push(password.len() as u8);
    payload.extend_from_slice(password.as_bytes());
    payload.extend_from_slice(&3306u16.to_le_bytes());
    payload.extend_from_slice(&0u32.to_le_bytes()); // replication rank
    payload.extend_from_slice(&0u32.to_le_bytes()); // master server id
    channel.write_command(0x15, &payload).await?;
    let packet = channel.read_packet().await?;
    check_error_packet(&packet, "COM_REGISTER_SLAVE failed")?;
    Ok(())
}

pub struct Handshake {
    pub server_version: String,
    pub scramble: Vec<u8>,
    pub capabilities: u32,
    pub auth_plugin: String,
}

pub async fn handshake(channel: &mut PacketChannel) -> Result<Handshake, Error> {
    let packet = channel.read_packet().await?;
    check_error_packet(&packet, "handshake failed")?;
    parse_handshake(&packet)
}

pub async fn authenticate(
    channel: &mut PacketChannel,
    handshake: &Handshake,
    host: &str,
    username: &str,
    password: &str,
    ssl: &SslMode,
) -> Result<(), Error> {
    negotiate_tls(channel, handshake, host, ssl).await?;

    let auth = build_auth_packet(handshake, username, password, channel.tls_active())?;
    channel.write_packet(&auth).await?;
    let mut packet = channel.read_packet().await?;

    loop {
        match packet.first() {
            Some(0x00) => {
                drain_auth_tail(channel).await?;
                return Ok(());
            }
            Some(0xFF) => {
                check_error_packet(&packet, "authentication failed")?;
                channel.reset_seq();
                return Ok(());
            }
            Some(0x01) => {
                // AuthMoreData (caching_sha2_password)
                match packet.get(1) {
                    Some(0x03) => {
                        // Fast auth success. Drain any trailing OK the server may send
                        // before COM_* commands so packet sequence stays aligned.
                        drain_auth_tail(channel).await?;
                        return Ok(());
                    }
                    Some(0x04) => {
                        if !channel.tls_active() {
                            return Err(Error::Auth(
                                "caching_sha2_password full authentication requires TLS; use --tls-mode preferred or required"
                                    .into(),
                            ));
                        }
                        let mut response = password.as_bytes().to_vec();
                        response.push(0);
                        channel.write_packet(&response).await?;
                        packet = channel.read_packet().await?;
                    }
                    other => {
                        return Err(Error::Auth(format!(
                            "unexpected auth more data payload: {other:?}"
                        )));
                    }
                }
            }
            Some(0xFE) => {
                let plugin = read_null_string(&packet[1..]);
                let data_end = packet[1..]
                    .iter()
                    .skip(plugin.len() + 1)
                    .enumerate()
                    .find_map(|(i, &b)| if b == 0 { Some(i) } else { None })
                    .unwrap_or(0);
                let plugin_data = &packet[1 + plugin.len() + 1..1 + plugin.len() + 1 + data_end];
                let response = auth_switch_response(&plugin, password, plugin_data)?;
                channel.write_packet(&response).await?;
                packet = channel.read_packet().await?;
            }
            other => {
                return Err(Error::Auth(format!(
                    "unexpected auth response packet: {other:?}"
                )));
            }
        }
    }
}

async fn negotiate_tls(
    channel: &mut PacketChannel,
    handshake: &Handshake,
    host: &str,
    ssl: &SslMode,
) -> Result<(), Error> {
    let Some(options) = ssl.options() else {
        return Ok(());
    };
    if handshake.capabilities & CLIENT_SSL == 0 {
        return match ssl {
            SslMode::Preferred(_) => Ok(()),
            SslMode::Required(_) => Err(Error::Ssl(
                "server does not advertise CLIENT_SSL capability".into(),
            )),
            SslMode::Disabled => Ok(()),
        };
    }

    let request = build_ssl_request(true);
    channel.write_packet(&request).await?;
    channel.upgrade_tls(host, options).await
}

async fn drain_auth_tail(channel: &mut PacketChannel) -> Result<(), Error> {
    while let Ok(Ok(packet)) =
        tokio::time::timeout(std::time::Duration::from_millis(25), channel.read_packet()).await
    {
        check_error_packet(&packet, "authentication failed")?;
    }
    channel.reset_seq();
    Ok(())
}

fn build_auth_packet(
    handshake: &Handshake,
    username: &str,
    password: &str,
    use_ssl: bool,
) -> Result<Vec<u8>, Error> {
    let mut buf = Vec::new();
    let mut capabilities = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH;
    if use_ssl {
        capabilities |= CLIENT_SSL;
    }
    buf.extend_from_slice(&capabilities.to_le_bytes());
    buf.extend_from_slice(&1_000_000u32.to_le_bytes());
    buf.push(UTF8_MB4_GENERAL_CI as u8);
    buf.extend_from_slice(&[0u8; 19]);
    buf.extend_from_slice(&0u32.to_le_bytes()); // mariadb_ext_capabilities
    buf.extend_from_slice(username.as_bytes());
    buf.push(0);
    let scramble = &handshake.scramble;
    let hash = auth_response(handshake.auth_plugin.as_str(), password, scramble);
    buf.push(hash.len() as u8);
    buf.extend_from_slice(&hash);
    if handshake.capabilities & CLIENT_PLUGIN_AUTH != 0 {
        buf.extend_from_slice(handshake.auth_plugin.as_bytes());
        buf.push(0);
    }
    Ok(buf)
}

fn build_ssl_request(use_ssl: bool) -> Vec<u8> {
    let mut capabilities = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH;
    if use_ssl {
        capabilities |= CLIENT_SSL;
    }
    let mut buf = Vec::new();
    buf.extend_from_slice(&capabilities.to_le_bytes());
    buf.extend_from_slice(&1_000_000u32.to_le_bytes());
    buf.push(UTF8_MB4_GENERAL_CI as u8);
    buf.extend_from_slice(&[0u8; 19]);
    buf.extend_from_slice(&0u32.to_le_bytes());
    buf
}

fn auth_response(plugin: &str, password: &str, scramble: &[u8]) -> Vec<u8> {
    match plugin {
        "caching_sha2_password" => caching_sha2_password_response(password, scramble),
        _ => scramble_native_password(password, scramble),
    }
}

fn caching_sha2_password_response(password: &str, scramble: &[u8]) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    if password.is_empty() {
        return vec![0];
    }
    if scramble.is_empty() {
        let mut buf = password.as_bytes().to_vec();
        buf.push(0);
        return buf;
    }
    let stage1 = Sha256::digest(password.as_bytes());
    let stage2 = Sha256::digest(stage1);
    let stage3 = Sha256::digest([&stage2[..], scramble].concat());
    stage1
        .iter()
        .zip(stage3.iter())
        .map(|(a, b)| a ^ b)
        .collect()
}

fn scramble_native_password(password: &str, scramble: &[u8]) -> Vec<u8> {
    use sha1::{Digest, Sha1};
    let mut sha1_stage1 = Sha1::new();
    sha1_stage1.update(password.as_bytes());
    let stage1 = sha1_stage1.finalize();

    let mut sha1_stage2 = Sha1::new();
    sha1_stage2.update(stage1);
    let stage2 = sha1_stage2.finalize();

    let mut sha1_stage3 = Sha1::new();
    sha1_stage3.update(scramble);
    sha1_stage3.update(&stage2[..]);
    let stage3 = sha1_stage3.finalize();

    stage1
        .iter()
        .zip(stage3.iter())
        .map(|(a, b)| a ^ b)
        .collect()
}

fn auth_switch_response(
    plugin: &str,
    password: &str,
    plugin_data: &[u8],
) -> Result<Vec<u8>, Error> {
    if plugin == "mysql_native_password" {
        return Ok(scramble_native_password(password, plugin_data));
    }
    if plugin == "caching_sha2_password" {
        return Ok(caching_sha2_password_response(password, plugin_data));
    }
    Err(Error::Auth(format!("unsupported auth plugin: {plugin}")))
}

fn tls_config(options: &SslOptions) -> Result<ClientConfig, Error> {
    let mut roots = RootCertStore::empty();
    if let Some(ca) = &options.ca {
        let certs = load_certs(ca)?;
        let (added, ignored) = roots.add_parsable_certificates(certs);
        if added == 0 {
            return Err(Error::Ssl(format!(
                "no usable CA certificates found in {ca} (ignored {ignored})"
            )));
        }
    } else {
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }

    let builder = ClientConfig::builder().with_root_certificates(roots);
    match (&options.cert, &options.key) {
        (Some(cert), Some(key)) => {
            let certs = load_certs(cert)?;
            let key = load_private_key(key)?;
            builder
                .with_client_auth_cert(certs, key)
                .map_err(|e| Error::Ssl(format!("invalid client TLS certificate/key: {e}")))
        }
        (None, None) => Ok(builder.with_no_client_auth()),
        (Some(_), None) | (None, Some(_)) => Err(Error::Ssl(
            "--tls-cert and --tls-key must be provided together".into(),
        )),
    }
}

fn load_certs(path: &str) -> Result<Vec<CertificateDer<'static>>, Error> {
    let file = File::open(path)
        .map_err(|e| Error::Ssl(format!("failed to open TLS certificate file {path}: {e}")))?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| Error::Ssl(format!("failed to parse TLS certificates from {path}: {e}")))
}

fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>, Error> {
    let file = File::open(path)
        .map_err(|e| Error::Ssl(format!("failed to open TLS private key file {path}: {e}")))?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| Error::Ssl(format!("failed to parse TLS private key from {path}: {e}")))?
        .ok_or_else(|| Error::Ssl(format!("no private key found in {path}")))
}

fn parse_handshake(packet: &[u8]) -> Result<Handshake, Error> {
    if packet.len() < 15 {
        return Err(Error::Protocol("handshake too short".into()));
    }
    let protocol = packet[0];
    if protocol != 10 {
        return Err(Error::Protocol(format!("unexpected protocol {protocol}")));
    }
    let version_end = packet[1..]
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| Error::Protocol("missing server version".into()))?;
    let server_version = String::from_utf8_lossy(&packet[1..1 + version_end]).into_owned();

    let mut pos = 1 + version_end + 1;
    pos += 4; // connection id
    let scramble_part1 = packet
        .get(pos..pos + 8)
        .ok_or_else(|| Error::Protocol("handshake missing scramble part 1".into()))?;
    pos += 8;
    pos += 1; // filler

    let cap1 = u16::from_le_bytes([packet[pos], packet[pos + 1]]) as u32;
    pos += 2;
    pos += 1; // character set
    pos += 2; // status flags
    let cap2 = u16::from_le_bytes([packet[pos], packet[pos + 1]]) as u32;
    pos += 2;
    let capabilities = cap1 | (cap2 << 16);

    let auth_plugin_data_len = if capabilities & CLIENT_PLUGIN_AUTH != 0 {
        let len = packet
            .get(pos)
            .copied()
            .ok_or_else(|| Error::Protocol("handshake missing auth data length".into()))?
            as usize;
        pos += 1;
        len
    } else {
        8
    };

    let is_mariadb = server_version.to_ascii_lowercase().contains("mariadb");
    let scramble_part2_len = if is_mariadb {
        pos += 1; // reserved
        pos += 4; // mariadb extended capabilities
        std::cmp::max(13, auth_plugin_data_len as i32 - 8) as usize
    } else {
        pos += 10; // reserved
        auth_plugin_data_len.saturating_sub(8)
    };

    let scramble_part2 = if scramble_part2_len > 0 {
        packet
            .get(pos..pos + scramble_part2_len)
            .ok_or_else(|| Error::Protocol("handshake missing scramble part 2".into()))?
    } else {
        &[]
    };
    pos += scramble_part2_len;

    let mut scramble = scramble_part1.to_vec();
    scramble.extend_from_slice(scramble_part2);

    let auth_plugin = if capabilities & CLIENT_PLUGIN_AUTH != 0 {
        read_null_string(&packet[pos..])
    } else {
        "mysql_native_password".to_string()
    };

    Ok(Handshake {
        server_version,
        scramble,
        capabilities,
        auth_plugin,
    })
}

fn read_null_string(data: &[u8]) -> String {
    let end = data.iter().position(|&b| b == 0).unwrap_or(data.len());
    String::from_utf8_lossy(&data[..end]).into_owned()
}

pub fn check_error_packet(packet: &[u8], context: &str) -> Result<(), Error> {
    if packet.first() == Some(&0xFF) {
        let msg = if packet.len() > 3 {
            String::from_utf8_lossy(&packet[3..]).into_owned()
        } else {
            "unknown error".into()
        };
        return Err(Error::Protocol(format!("{context}: {msg}")));
    }
    Ok(())
}
