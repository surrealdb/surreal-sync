use std::io::Cursor;
use std::sync::Arc;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use tokio_rustls::rustls::crypto::{
    verify_tls12_signature, verify_tls13_signature, CryptoProvider,
};
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use tokio_rustls::rustls::{
    ClientConfig, DigitallySignedStruct, Error as RustlsError, RootCertStore, SignatureScheme,
};
use tokio_rustls::TlsConnector;

use crate::error::Error;
use crate::options::{SslMode, SslOptions};
use rsa::pkcs8::DecodePublicKey;

const UTF8_MB4_GENERAL_CI: u16 = 0x002d;
/// MySQL auth scramble / nonce length (trailing NUL from the handshake is trimmed).
const SCRAMBLE_LENGTH: usize = 20;
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
        let config = tls_config(options).await?;
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

/// COM_REGISTER_SLAVE replica host/username/password are protocol placeholders after client
/// auth; MySQL requires length-prefixed empty strings, not real credentials.
const REGISTER_SLAVE_UNUSED_FIELD: &str = "";

fn write_length_prefixed_string(buf: &mut Vec<u8>, value: &str) {
    buf.push(value.len() as u8);
    buf.extend_from_slice(value.as_bytes());
}

pub async fn encode_register_slave(
    channel: &mut PacketChannel,
    server_id: u32,
) -> Result<(), Error> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&server_id.to_le_bytes());
    write_length_prefixed_string(&mut payload, REGISTER_SLAVE_UNUSED_FIELD);
    write_length_prefixed_string(&mut payload, REGISTER_SLAVE_UNUSED_FIELD);
    write_length_prefixed_string(&mut payload, REGISTER_SLAVE_UNUSED_FIELD);
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

    let mut scramble = handshake.scramble.clone();
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
                        if channel.tls_active() {
                            let mut response = password.as_bytes().to_vec();
                            response.push(0);
                            channel.write_packet(&response).await?;
                        } else {
                            // Non-TLS full auth: RSA-encrypt the password with the
                            // server's public key (caching_sha2_password).
                            perform_caching_sha2_rsa_full_auth(channel, password, &scramble)
                                .await?;
                        }
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
                scramble = normalize_scramble(plugin_data);
                let response = auth_switch_response(&plugin, password, &scramble)?;
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

/// Request the server RSA public key and send an RSA-OAEP encrypted password
/// for `caching_sha2_password` full authentication over a plaintext connection.
async fn perform_caching_sha2_rsa_full_auth(
    channel: &mut PacketChannel,
    password: &str,
    scramble: &[u8],
) -> Result<(), Error> {
    // 0x02 = PublicKeyRequest
    channel.write_packet(&[0x02]).await?;
    let key_pkt = channel.read_packet().await?;
    check_error_packet(&key_pkt, "failed to get RSA public key")?;
    if key_pkt.first() != Some(&0x01) {
        return Err(Error::Auth(format!(
            "unexpected RSA key response prefix: {:02x}",
            key_pkt.first().copied().unwrap_or(0)
        )));
    }
    // AuthMoreData: [0x01, PEM..., optional 0x00]
    let pem_bytes = key_pkt[1..].strip_suffix(&[0x00]).unwrap_or(&key_pkt[1..]);
    let pub_key_pem = std::str::from_utf8(pem_bytes)
        .map_err(|e| Error::Auth(format!("invalid UTF-8 in RSA public key: {e}")))?;
    let pub_key = rsa::RsaPublicKey::from_public_key_pem(pub_key_pem)
        .map_err(|e| Error::Auth(format!("failed to parse RSA public key: {e}")))?;

    // XOR null-terminated password with the 20-byte scramble (repeating).
    let mut plain = password.as_bytes().to_vec();
    plain.push(0);
    if scramble.is_empty() {
        return Err(Error::Auth(
            "caching_sha2_password RSA full auth requires a non-empty scramble".into(),
        ));
    }
    for (i, byte) in plain.iter_mut().enumerate() {
        *byte ^= scramble[i % scramble.len()];
    }

    // MySQL uses RSAES-OAEP with SHA-1 (MySQL 8.0.5+).
    let padding = rsa::Oaep::new::<sha1::Sha1>();
    let encrypted = pub_key
        .encrypt(&mut rand_core::OsRng, padding, &plain)
        .map_err(|e| Error::Auth(format!("RSA encryption failed: {e}")))?;
    channel.write_packet(&encrypted).await
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
            // Preferred: server has no SSL — stay on plaintext.
            SslMode::Preferred(_) => Ok(()),
            SslMode::Required(_) => Err(Error::Ssl(
                "server does not advertise CLIENT_SSL capability; use a TLS-enabled MySQL server or --tls-mode preferred/disabled".into(),
            )),
            SslMode::Disabled => Ok(()),
        };
    }

    let request = build_ssl_request(true);
    channel.write_packet(&request).await?;
    channel.upgrade_tls(host, options).await.map_err(|e| match e {
        Error::Ssl(msg) if ssl.is_required() => Error::Ssl(format!(
            "{msg}; for self-signed servers pass --tls-ca <path> or omit --tls-ca to encrypt without public-CA verification"
        )),
        other => other,
    })
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

async fn tls_config(options: &SslOptions) -> Result<ClientConfig, Error> {
    // Ensure a process-wide rustls provider (needed when multiple crates pull rustls).
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    // With --tls-ca: verify the server against that CA.
    // Without --tls-ca: encrypt without requiring a public-CA match (MySQL REQUIRED semantics).
    let builder = if let Some(ca) = &options.ca {
        let mut roots = RootCertStore::empty();
        let certs = load_certs(ca).await?;
        let (added, ignored) = roots.add_parsable_certificates(certs);
        if added == 0 {
            return Err(Error::Ssl(format!(
                "no usable CA certificates found in {ca} (ignored {ignored})"
            )));
        }
        ClientConfig::builder().with_root_certificates(roots)
    } else {
        ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyServerCert::new()?))
    };

    match (&options.cert, &options.key) {
        (Some(cert), Some(key)) => {
            let certs = load_certs(cert).await?;
            let key = load_private_key(key).await?;
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

/// Encrypt-only verifier used when `--tls-ca` is omitted (MySQL `--ssl-mode=REQUIRED`).
#[derive(Debug)]
struct AcceptAnyServerCert {
    algorithms: tokio_rustls::rustls::crypto::WebPkiSupportedAlgorithms,
}

impl AcceptAnyServerCert {
    fn new() -> Result<Self, Error> {
        let provider = CryptoProvider::get_default().cloned().ok_or_else(|| {
            Error::Ssl("no rustls CryptoProvider installed; cannot build TLS client config".into())
        })?;
        Ok(Self {
            algorithms: provider.signature_verification_algorithms,
        })
    }
}

impl ServerCertVerifier for AcceptAnyServerCert {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        verify_tls12_signature(message, cert, dss, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        verify_tls13_signature(message, cert, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

async fn load_certs(path: &str) -> Result<Vec<CertificateDer<'static>>, Error> {
    let bytes = tokio::fs::read(path)
        .await
        .map_err(|e| Error::Ssl(format!("failed to open TLS certificate file {path}: {e}")))?;
    let mut reader = Cursor::new(bytes);
    rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| Error::Ssl(format!("failed to parse TLS certificates from {path}: {e}")))
}

async fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>, Error> {
    let bytes = tokio::fs::read(path)
        .await
        .map_err(|e| Error::Ssl(format!("failed to open TLS private key file {path}: {e}")))?;
    let mut reader = Cursor::new(bytes);
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
    scramble = normalize_scramble(&scramble);

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

/// Pad or truncate to the canonical 20-byte MySQL scramble (trailing handshake NUL
/// is dropped when the concatenated length is 21, matching mysql_async).
fn normalize_scramble(raw: &[u8]) -> Vec<u8> {
    let mut scramble = raw.to_vec();
    scramble.resize(SCRAMBLE_LENGTH, 0);
    scramble
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
