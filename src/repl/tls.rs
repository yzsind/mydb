//! mTLS transport for mydb-native replication.

use std::io::{BufReader as StdBufReader, Error, ErrorKind, Result as IoResult};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::repl::config::{ReplConfig, ReplTlsMode};

enum ReplStreamInner {
    Plain(TcpStream),
    ServerTls(tokio_rustls::server::TlsStream<TcpStream>),
    ClientTls(tokio_rustls::client::TlsStream<TcpStream>),
}

pub struct ReplStream {
    inner: ReplStreamInner,
    _reservation: crate::engine::executor::mem_budget::ControlledMemoryCharge,
}

impl ReplStream {
    pub fn transport_label(&self) -> String {
        match &self.inner {
            ReplStreamInner::Plain(_) => "plaintext-loopback".to_string(),
            ReplStreamInner::ServerTls(stream) => tls_label(stream.get_ref().1),
            ReplStreamInner::ClientTls(stream) => tls_label(stream.get_ref().1),
        }
    }
}

fn tls_label(
    connection: &tokio_rustls::rustls::ConnectionCommon<impl tokio_rustls::rustls::SideData>,
) -> String {
    let version = connection
        .protocol_version()
        .map(|version| format!("{version:?}"))
        .unwrap_or_else(|| "TLS".to_string());
    let suite = connection
        .negotiated_cipher_suite()
        .map(|suite| format!("{:?}", suite.suite()))
        .unwrap_or_else(|| "unknown-cipher".to_string());
    format!("{version}/{suite}")
}

fn reserve_connection() -> IoResult<crate::engine::executor::mem_budget::ControlledMemoryCharge> {
    crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
        crate::engine::executor::mem_budget::ControlledMemoryCategory::NativeReplicationTransient,
        256 * 1024,
    )
    .map_err(|err| Error::new(ErrorKind::OutOfMemory, err.message().to_string()))
}

impl AsyncRead for ReplStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
        match &mut self.get_mut().inner {
            ReplStreamInner::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            ReplStreamInner::ServerTls(stream) => Pin::new(stream).poll_read(cx, buf),
            ReplStreamInner::ClientTls(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ReplStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<IoResult<usize>> {
        match &mut self.get_mut().inner {
            ReplStreamInner::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            ReplStreamInner::ServerTls(stream) => Pin::new(stream).poll_write(cx, buf),
            ReplStreamInner::ClientTls(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        match &mut self.get_mut().inner {
            ReplStreamInner::Plain(stream) => Pin::new(stream).poll_flush(cx),
            ReplStreamInner::ServerTls(stream) => Pin::new(stream).poll_flush(cx),
            ReplStreamInner::ClientTls(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        match &mut self.get_mut().inner {
            ReplStreamInner::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            ReplStreamInner::ServerTls(stream) => Pin::new(stream).poll_shutdown(cx),
            ReplStreamInner::ClientTls(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

pub fn is_loopback_host(host: &str) -> bool {
    let host = host.trim().trim_start_matches('[').trim_end_matches(']');
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .map(|ip| ip.is_loopback())
            .unwrap_or(false)
        || host == Ipv4Addr::LOCALHOST.to_string()
        || host == Ipv6Addr::LOCALHOST.to_string()
}

fn should_use_tls(config: &ReplConfig, host: &str) -> IoResult<bool> {
    let complete = config.tls_ca.is_some() && config.tls_cert.is_some() && config.tls_key.is_some();
    let any = config.tls_ca.is_some() || config.tls_cert.is_some() || config.tls_key.is_some();
    if any && !complete {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "MYDB_REPL_TLS_CA, CERT and KEY must be configured together",
        ));
    }
    match config.tls_mode {
        ReplTlsMode::Disabled if !is_loopback_host(host) => Err(Error::new(
            ErrorKind::PermissionDenied,
            "plaintext native replication is allowed only on loopback",
        )),
        ReplTlsMode::Disabled => Ok(false),
        ReplTlsMode::VerifyIdentity if !complete => Err(Error::new(
            ErrorKind::InvalidInput,
            "verify_identity requires replication CA, certificate and key",
        )),
        ReplTlsMode::VerifyIdentity => Ok(true),
        ReplTlsMode::Auto if !is_loopback_host(host) && !complete => Err(Error::new(
            ErrorKind::PermissionDenied,
            "non-loopback native replication requires mTLS",
        )),
        ReplTlsMode::Auto => Ok(complete),
    }
}

pub fn validate_endpoint(config: &ReplConfig, host: &str) -> IoResult<()> {
    should_use_tls(config, host).map(|_| ())
}

pub async fn accept(
    stream: TcpStream,
    config: &ReplConfig,
    bind_host: &str,
) -> IoResult<ReplStream> {
    let reservation = reserve_connection()?;
    if !should_use_tls(config, bind_host)? {
        return Ok(ReplStream {
            inner: ReplStreamInner::Plain(stream),
            _reservation: reservation,
        });
    }
    let roots = load_roots(config.tls_ca.as_deref().expect("validated"))?;
    let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .map_err(|err| Error::new(ErrorKind::InvalidInput, format!("client CA: {err}")))?;
    let server = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(
            load_certs(config.tls_cert.as_deref().expect("validated"))?,
            load_key(config.tls_key.as_deref().expect("validated"))?,
        )
        .map_err(|err| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("server certificate: {err}"),
            )
        })?;
    let tls = tokio::time::timeout(
        Duration::from_secs(10),
        TlsAcceptor::from(Arc::new(server)).accept(stream),
    )
    .await
    .map_err(|_| Error::new(ErrorKind::TimedOut, "replication TLS handshake timed out"))??;
    Ok(ReplStream {
        inner: ReplStreamInner::ServerTls(tls),
        _reservation: reservation,
    })
}

pub async fn connect(stream: TcpStream, config: &ReplConfig, host: &str) -> IoResult<ReplStream> {
    let reservation = reserve_connection()?;
    if !should_use_tls(config, host)? {
        return Ok(ReplStream {
            inner: ReplStreamInner::Plain(stream),
            _reservation: reservation,
        });
    }
    let roots = load_roots(config.tls_ca.as_deref().expect("validated"))?;
    let client = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(
            load_certs(config.tls_cert.as_deref().expect("validated"))?,
            load_key(config.tls_key.as_deref().expect("validated"))?,
        )
        .map_err(|err| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("client certificate: {err}"),
            )
        })?;
    let name = config
        .tls_server_name
        .as_deref()
        .unwrap_or(host)
        .to_string();
    let server_name = ServerName::try_from(name).map_err(|_| {
        Error::new(
            ErrorKind::InvalidInput,
            "invalid replication TLS server name",
        )
    })?;
    let tls = tokio::time::timeout(
        Duration::from_secs(10),
        TlsConnector::from(Arc::new(client)).connect(server_name, stream),
    )
    .await
    .map_err(|_| Error::new(ErrorKind::TimedOut, "replication TLS handshake timed out"))??;
    Ok(ReplStream {
        inner: ReplStreamInner::ClientTls(tls),
        _reservation: reservation,
    })
}

fn load_certs(path: &Path) -> IoResult<Vec<CertificateDer<'static>>> {
    let mut reader = StdBufReader::new(std::fs::File::open(path)?);
    let certs = CertificateDer::pem_reader_iter(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| Error::new(ErrorKind::InvalidData, err))?;
    if certs.is_empty() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "certificate file is empty",
        ));
    }
    Ok(certs)
}

fn load_key(path: &Path) -> IoResult<PrivateKeyDer<'static>> {
    let mut reader = StdBufReader::new(std::fs::File::open(path)?);
    PrivateKeyDer::pem_reader_iter(&mut reader)
        .next()
        .transpose()
        .map_err(|err| Error::new(ErrorKind::InvalidData, err))?
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "private key file is empty"))
}

fn load_roots(path: &Path) -> IoResult<RootCertStore> {
    let mut roots = RootCertStore::empty();
    for cert in load_certs(path)? {
        roots
            .add(cert)
            .map_err(|err| Error::new(ErrorKind::InvalidInput, format!("CA certificate: {err}")))?;
    }
    Ok(roots)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_detection_is_strict() {
        assert!(is_loopback_host("127.0.0.1"));
        assert!(is_loopback_host("::1"));
        assert!(is_loopback_host("[::1]"));
        assert!(is_loopback_host("localhost"));
        assert!(!is_loopback_host("0.0.0.0"));
        assert!(!is_loopback_host("10.0.0.1"));
    }

    #[test]
    fn plaintext_is_rejected_off_loopback() {
        let mut config = ReplConfig::default();
        assert_eq!(
            should_use_tls(&config, "10.0.0.1").unwrap_err().kind(),
            ErrorKind::PermissionDenied
        );
        config.tls_mode = ReplTlsMode::Disabled;
        assert_eq!(
            should_use_tls(&config, "0.0.0.0").unwrap_err().kind(),
            ErrorKind::PermissionDenied
        );
        assert!(!should_use_tls(&config, "127.0.0.1").unwrap());
    }
}
