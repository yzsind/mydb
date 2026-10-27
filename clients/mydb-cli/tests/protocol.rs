use mysql::{consts::CapabilityFlags, prelude::Queryable, Conn, OptsBuilder};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread,
    time::{Duration, Instant},
};

fn write_packet(s: &mut TcpStream, seq: u8, data: &[u8]) {
    let n = data.len();
    s.write_all(&[n as u8, (n >> 8) as u8, (n >> 16) as u8, seq])
        .unwrap();
    s.write_all(data).unwrap();
}
fn read_packet(s: &mut TcpStream) -> Option<Vec<u8>> {
    let mut h = [0; 4];
    if s.read_exact(&mut h).is_err() {
        return None;
    }
    let mut data = vec![0; h[0] as usize | ((h[1] as usize) << 8) | ((h[2] as usize) << 16)];
    s.read_exact(&mut data).unwrap();
    Some(data)
}
fn server_with_host(
    run: impl FnOnce(&mut TcpStream, bool) + Send + 'static,
    deprecated: bool,
    hostname: &str,
) -> (Conn, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = thread::spawn(move || {
        let (mut s, _) = listener.accept().unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let caps = CapabilityFlags::CLIENT_PROTOCOL_41
            | CapabilityFlags::CLIENT_SECURE_CONNECTION
            | CapabilityFlags::CLIENT_PLUGIN_AUTH
            | CapabilityFlags::CLIENT_MULTI_RESULTS
            | CapabilityFlags::CLIENT_MULTI_STATEMENTS
            | CapabilityFlags::CLIENT_DEPRECATE_EOF;
        let c = caps.bits();
        let mut h = vec![10];
        h.extend(b"8.0.46-fixture\0");
        h.extend(123u32.to_le_bytes());
        h.extend(b"12345678\0");
        h.extend((c as u16).to_le_bytes());
        h.push(45);
        h.extend(2u16.to_le_bytes());
        h.extend(((c >> 16) as u16).to_le_bytes());
        h.push(21);
        h.extend([0; 10]);
        h.extend(b"abcdefghijkl\0mysql_native_password\0");
        write_packet(&mut s, 0, &h);
        read_packet(&mut s).unwrap();
        write_packet(&mut s, 2, &[0, 0, 0, 2, 0, 0, 0]);
        run(&mut s, deprecated);
    });
    let opts = OptsBuilder::new()
        .ip_or_hostname(Some(hostname))
        .tcp_port(port)
        .user(Some("root"))
        .prefer_socket(false)
        .max_allowed_packet(Some(64 * 1024 * 1024))
        .read_timeout(Some(Duration::from_secs(5)))
        .additional_capabilities(if deprecated {
            CapabilityFlags::CLIENT_DEPRECATE_EOF
        } else {
            CapabilityFlags::empty()
        });
    (Conn::new(opts).unwrap(), handle)
}
fn server(
    run: impl FnOnce(&mut TcpStream, bool) + Send + 'static,
    deprecated: bool,
) -> (Conn, thread::JoinHandle<()>) {
    server_with_host(run, deprecated, "127.0.0.1")
}

#[test]
fn hostname_connect_tries_all_resolved_addresses() {
    let (mut conn, handle) = server_with_host(
        |s, deprecated| {
            read_packet(s).unwrap();
            result(s, &mut 1, deprecated, 0, false);
            read_packet(s);
        },
        false,
        "localhost",
    );
    assert_eq!(conn.query_first::<u32, _>("fixture").unwrap(), Some(42));
    drop(conn);
    handle.join().unwrap();
}

fn column() -> Vec<u8> {
    let mut c = vec![];
    for value in [b"def".as_slice(), b"", b"", b"", b"value", b""] {
        c.push(value.len() as u8);
        c.extend(value);
    }
    c.extend([12, 63, 0, 20, 0, 0, 0, 8, 0, 0, 0, 0, 0]);
    c
}
fn result(s: &mut TcpStream, seq: &mut u8, deprecated: bool, warnings: u16, more: bool) {
    let status: u16 = 2 | if more { 8 } else { 0 };
    for p in [vec![1], column()] {
        write_packet(s, *seq, &p);
        *seq += 1;
    }
    if !deprecated {
        write_packet(s, *seq, &[0xfe, 0, 0, 2, 0]);
        *seq += 1;
    }
    write_packet(s, *seq, &[2, b'4', b'2']);
    *seq += 1;
    let mut eof = if deprecated {
        vec![0xfe, 0, 0]
    } else {
        vec![0xfe]
    };
    if deprecated {
        eof.extend(status.to_le_bytes());
        eof.extend(warnings.to_le_bytes());
    } else {
        eof.extend(warnings.to_le_bytes());
        eof.extend(status.to_le_bytes());
    }
    write_packet(s, *seq, &eof);
    *seq += 1;
}

#[test]
fn completed_metadata_is_stable_across_result_boundaries() {
    for deprecated in [false, true] {
        let (mut conn, handle) = server(
            |s, deprecated| {
                assert_eq!(read_packet(s).unwrap()[0], 3);
                let mut seq = 1;
                result(s, &mut seq, deprecated, 7, true);
                write_packet(s, seq, &[0, 3, 0, 2, 0, 2, 0]);
                read_packet(s);
            },
            deprecated,
        );
        let mut query = conn.query_iter("fixture").unwrap();
        query.enable_result_metadata();
        {
            let mut set = query.iter().unwrap();
            assert_eq!(mysql::from_row::<u32>(set.next().unwrap().unwrap()), 42);
            assert!(set.next().is_none());
            assert_eq!(set.completed_result().unwrap().warnings(), 7);
        }
        {
            let mut set = query.iter().unwrap();
            assert!(set.next().is_none());
            let meta = set.completed_result().unwrap();
            assert_eq!(meta.affected_rows(), 3);
            assert_eq!(meta.warnings(), 2);
        }
        assert!(query.iter().is_none());
        drop(query);
        drop(conn);
        handle.join().unwrap();
    }
}

#[test]
fn error_in_later_result_is_not_lost() {
    let (mut conn, handle) = server(
        |s, deprecated| {
            read_packet(s).unwrap();
            let mut seq = 1;
            result(s, &mut seq, deprecated, 0, true);
            let mut error = vec![0xff, 0x28, 0x04];
            error.extend(b"#42000fixture error");
            write_packet(s, seq, &error);
            read_packet(s);
        },
        true,
    );
    let mut query = conn.query_iter("fixture").unwrap();
    query.enable_result_metadata();
    {
        let mut set = query.iter().unwrap();
        assert!(set.next().unwrap().is_ok());
        assert!(set.next().is_none());
    }
    {
        let mut set = query.iter().unwrap();
        assert!(matches!(set.next().unwrap(),Err(mysql::Error::MySqlError(e)) if e.code == 1064));
        assert!(set.completed_result().is_none());
    }
    drop(query);
    drop(conn);
    handle.join().unwrap();
}

#[test]
fn abort_never_drains_unfinished_result() {
    let (mut conn, handle) = server(
        |s, deprecated| {
            read_packet(s).unwrap();
            write_packet(s, 1, &[1]);
            write_packet(s, 2, &column());
            if !deprecated {
                write_packet(s, 3, &[0xfe, 0, 0, 2, 0]);
            }
            // Wait for a close, without ever sending a row or terminator.
            let start = Instant::now();
            assert!(read_packet(s).is_none());
            assert!(start.elapsed() < Duration::from_secs(2));
        },
        true,
    );
    let mut query = conn.query_iter("fixture").unwrap();
    let mut set = query.iter().unwrap();
    let start = Instant::now();
    set.abort();
    drop(set);
    drop(query);
    drop(conn);
    assert!(start.elapsed() < Duration::from_millis(500));
    handle.join().unwrap();
}
