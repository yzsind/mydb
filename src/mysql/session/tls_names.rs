//! MySQL uses OpenSSL names in account TLS requirements and session status.
use tokio_rustls::rustls::CipherSuite;

pub(super) fn cipher_name(suite: CipherSuite) -> String {
    use CipherSuite::*;
    match suite {
        TLS13_AES_128_GCM_SHA256 => "TLS_AES_128_GCM_SHA256",
        TLS13_AES_256_GCM_SHA384 => "TLS_AES_256_GCM_SHA384",
        TLS13_CHACHA20_POLY1305_SHA256 => "TLS_CHACHA20_POLY1305_SHA256",
        TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256 => "ECDHE-RSA-AES128-GCM-SHA256",
        TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384 => "ECDHE-RSA-AES256-GCM-SHA384",
        TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256 => "ECDHE-ECDSA-AES128-GCM-SHA256",
        TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384 => "ECDHE-ECDSA-AES256-GCM-SHA384",
        TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256 => "ECDHE-RSA-CHACHA20-POLY1305",
        TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256 => "ECDHE-ECDSA-CHACHA20-POLY1305",
        _ => return format!("{suite:?}"),
    }
    .to_string()
}

pub(super) fn distinguished_name(name: &x509_parser::x509::X509Name<'_>) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    for rdn in name.iter_rdn() {
        for (index, attribute) in rdn.iter().enumerate() {
            out.push(if index == 0 { '/' } else { '+' });
            let oid = attribute.attr_type().to_id_string();
            let object = openssl::asn1::Asn1Object::from_str(&oid).ok();
            let label = object
                .as_ref()
                .filter(|object| object.nid() != openssl::nid::Nid::UNDEF)
                .and_then(|object| object.nid().short_name().ok())
                .unwrap_or(&oid);
            out.push_str(label);
            out.push('=');
            let bytes = attribute.as_slice();
            let general_ascii = attribute.attr_value().tag().0 == 27
                && bytes.len() % 4 == 0
                && bytes
                    .iter()
                    .enumerate()
                    .all(|(i, byte)| i % 4 == 3 || *byte == 0);
            for (i, &byte) in bytes.iter().enumerate() {
                if general_ascii && i % 4 != 3 {
                    continue;
                }
                match byte {
                    b'/' | b'+' => {
                        out.push('\\');
                        out.push(byte as char);
                    }
                    0x20..=0x7e => out.push(byte as char),
                    _ => {
                        let _ = write!(out, "\\x{byte:02X}");
                    }
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use x509_parser::prelude::FromDer;

    #[test]
    fn audit_1029_openssl_dn_and_cipher_names() {
        let mut builder = openssl::x509::X509NameBuilder::new().unwrap();
        builder.append_entry_by_text("C", "SE").unwrap();
        builder.append_entry_by_text("ST", "Stockholm").unwrap();
        builder.append_entry_by_text("CN", "client/a+b").unwrap();
        let der = builder.build().to_der().unwrap();
        let (_, name) = x509_parser::x509::X509Name::from_der(&der).unwrap();
        assert_eq!(
            distinguished_name(&name),
            "/C=SE/ST=Stockholm/CN=client\\/a\\+b"
        );
        assert_eq!(
            cipher_name(CipherSuite::TLS13_AES_128_GCM_SHA256),
            "TLS_AES_128_GCM_SHA256"
        );
        assert_eq!(
            cipher_name(CipherSuite::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384),
            "ECDHE-RSA-AES256-GCM-SHA384"
        );
    }
}
