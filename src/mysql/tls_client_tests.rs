use super::*;
use mysql::prelude::Queryable;
use openssl::asn1::Asn1Time;
use openssl::bn::BigNum;
use openssl::ec::{EcGroup, EcKey};
use openssl::hash::MessageDigest;
use openssl::nid::Nid;
use openssl::pkey::{PKey, Private};
use openssl::rsa::Rsa;
use openssl::x509::extension::{
    BasicConstraints, ExtendedKeyUsage, KeyUsage, SubjectAlternativeName,
};
use openssl::x509::{X509NameBuilder, X509};
use std::time::Duration;

fn ec_key() -> PKey<Private> {
    PKey::from_ec_key(
        EcKey::generate(&EcGroup::from_curve_name(Nid::X9_62_PRIME256V1).unwrap()).unwrap(),
    )
    .unwrap()
}

fn certificate(key: &PKey<Private>, issuer: Option<(&X509, &PKey<Private>)>, name: &str) -> X509 {
    let mut subject = X509NameBuilder::new().unwrap();
    subject.append_entry_by_text("CN", name).unwrap();
    let subject = subject.build();
    let mut builder = X509::builder().unwrap();
    builder.set_version(2).unwrap();
    builder
        .set_serial_number(
            &BigNum::from_u32(if issuer.is_none() { 1 } else { 2 })
                .unwrap()
                .to_asn1_integer()
                .unwrap(),
        )
        .unwrap();
    builder.set_subject_name(&subject).unwrap();
    builder
        .set_issuer_name(
            issuer
                .map(|(cert, _)| cert.subject_name())
                .unwrap_or(&subject),
        )
        .unwrap();
    builder.set_pubkey(key).unwrap();
    builder
        .set_not_before(&Asn1Time::days_from_now(0).unwrap())
        .unwrap();
    builder
        .set_not_after(&Asn1Time::days_from_now(1).unwrap())
        .unwrap();
    if issuer.is_none() {
        builder
            .append_extension(BasicConstraints::new().critical().ca().build().unwrap())
            .unwrap();
        builder
            .append_extension(KeyUsage::new().key_cert_sign().crl_sign().build().unwrap())
            .unwrap();
    } else {
        builder
            .append_extension(BasicConstraints::new().critical().build().unwrap())
            .unwrap();
        builder
            .append_extension(
                KeyUsage::new()
                    .digital_signature()
                    .key_encipherment()
                    .build()
                    .unwrap(),
            )
            .unwrap();
        builder
            .append_extension(
                ExtendedKeyUsage::new()
                    .server_auth()
                    .client_auth()
                    .build()
                    .unwrap(),
            )
            .unwrap();
        let mut san = SubjectAlternativeName::new();
        san.dns(name);
        if name == "localhost" {
            san.ip("127.0.0.1");
        }
        let extension = san
            .build(&builder.x509v3_context(issuer.map(|(cert, _)| cert.as_ref()), None))
            .unwrap();
        builder.append_extension(extension).unwrap();
    }
    builder
        .sign(
            issuer.map(|(_, key)| key).unwrap_or(key),
            MessageDigest::sha256(),
        )
        .unwrap();
    builder.build()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mysql_tls_client_preserves_trust_hostname_and_identity_formats() {
    let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
    let ca_key = ec_key();
    let ca = certificate(&ca_key, None, "private test CA");
    let ca_der_path = dir.path().join("ca.der");
    std::fs::write(&ca_der_path, ca.to_der().unwrap()).unwrap();
    let ca_path = dir.path().join("ca.pem");
    std::fs::write(&ca_path, ca.to_pem().unwrap()).unwrap();
    let other_key = ec_key();
    let other_ca = certificate(&other_key, None, "untrusted CA");
    let other_ca_path = dir.path().join("other-ca.pem");
    std::fs::write(&other_ca_path, other_ca.to_pem().unwrap()).unwrap();
    let server_key = ec_key();
    let server_key_path = dir.path().join("server-key.pem");
    std::fs::write(
        &server_key_path,
        server_key.private_key_to_pem_pkcs8().unwrap(),
    )
    .unwrap();
    let rsa_key = PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap();
    let client_ec = ec_key();
    let identities = [
        (
            "pkcs1-pem",
            &rsa_key,
            rsa_key.rsa().unwrap().private_key_to_pem().unwrap(),
        ),
        (
            "pkcs8-pem",
            &rsa_key,
            rsa_key.private_key_to_pem_pkcs8().unwrap(),
        ),
        (
            "pkcs8-der",
            &rsa_key,
            rsa_key.private_key_to_pkcs8().unwrap(),
        ),
        (
            "sec1-pem",
            &client_ec,
            client_ec.ec_key().unwrap().private_key_to_pem().unwrap(),
        ),
        (
            "sec1-der",
            &client_ec,
            client_ec.ec_key().unwrap().private_key_to_der().unwrap(),
        ),
    ];
    let engine = Arc::new(crate::engine::StorageQueryEngine::open(dir.path().join("db")).unwrap());
    engine
        .execute("CREATE USER tls_client IDENTIFIED BY 'client-password' REQUIRE X509")
        .unwrap();
    for cert_der in [false, true] {
        for (format, key, encoded) in &identities {
            let client_cert = certificate(key, Some((&ca, &ca_key)), "client");
            let client_cert_path = dir.path().join(format!("{format}.cert.pem"));
            let client_key_path = dir.path().join(format!("{format}.key"));
            std::fs::write(
                &client_cert_path,
                if cert_der {
                    client_cert.to_der().unwrap()
                } else {
                    client_cert.to_pem().unwrap()
                },
            )
            .unwrap();
            std::fs::write(&client_key_path, encoded).unwrap();
            for (hostname, trusted, expected) in [
                ("localhost", true, true),
                ("localhost", false, false),
                ("wrong.invalid", true, false),
            ] {
                let server_cert = certificate(&server_key, Some((&ca, &ca_key)), hostname);
                let server_cert_path = dir.path().join("server-cert.pem");
                std::fs::write(&server_cert_path, server_cert.to_pem().unwrap()).unwrap();
                let mut roots = RootCertStore::empty();
                for cert in load_certs(&ca_path).unwrap() {
                    roots.add(cert).unwrap();
                }
                let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
                    .build()
                    .unwrap();
                let config = ServerConfig::builder()
                    .with_client_cert_verifier(verifier)
                    .with_single_cert(
                        load_certs(&server_cert_path).unwrap(),
                        load_private_key(&server_key_path).unwrap(),
                    )
                    .unwrap();
                let acceptor = TlsAcceptor::from(Arc::new(config));
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap();
                let server_engine = engine.clone();
                let server = tokio::spawn(async move {
                    let (stream, peer) = listener.accept().await.unwrap();
                    let mut session = crate::mysql::session::Session::new(
                        1,
                        peer,
                        stream,
                        Some(acceptor),
                        server_engine,
                        Arc::new(crate::engine::session_registry::SessionRegistry::new()),
                    );
                    let _ = session.run().await;
                });
                let identity =
                    mysql::ClientIdentity::new(client_cert_path.clone(), client_key_path.clone());
                let ssl = mysql::SslOpts::default()
                    .with_root_cert_path(Some(if trusted {
                        if cert_der {
                            ca_der_path.clone()
                        } else {
                            ca_path.clone()
                        }
                    } else {
                        other_ca_path.clone()
                    }))
                    .with_client_identity(Some(identity));
                let opts = mysql::OptsBuilder::new()
                    .ip_or_hostname(Some("127.0.0.1"))
                    .tcp_port(address.port())
                    .user(Some("tls_client"))
                    .pass(Some("client-password"))
                    .ssl_opts(Some(ssl))
                    .read_timeout(Some(Duration::from_secs(5)));
                let result = tokio::task::spawn_blocking(move || -> mysql::Result<Option<i64>> {
                    let mut client = mysql::Conn::new(opts)?;
                    client.query_first("SELECT 1")
                })
                .await
                .unwrap();
                assert_eq!(
                    result.is_ok(),
                    expected,
                    "{format}, {hostname}, trusted={trusted}: {result:?}"
                );
                if expected {
                    assert_eq!(result.unwrap(), Some(1));
                }
                tokio::time::timeout(Duration::from_secs(10), server)
                    .await
                    .unwrap()
                    .unwrap();
            }
        }
    }
}
