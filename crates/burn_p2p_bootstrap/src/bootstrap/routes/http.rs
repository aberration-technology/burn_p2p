use super::*;

const HTTP_READ_TIMEOUT: Duration = Duration::from_secs(10);
const HTTP_WRITE_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_REQUEST_LINE_BYTES: usize = 8 * 1024;
const MAX_HEADER_LINE_BYTES: usize = 16 * 1024;
const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_BODY_BYTES: usize = 1024 * 1024;

pub(crate) fn write_event_stream(
    stream: &mut TcpStream,
    plan: &BootstrapPlan,
    state: &Arc<Mutex<BootstrapAdminState>>,
    remaining_work_units: Option<u64>,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Err(error) = stream.write_all(
        b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\n\r\n",
    ) {
        if is_sse_disconnect(&error) {
            return Ok(());
        }
        return Err(error.into());
    }
    if let Err(error) = stream.flush() {
        if is_sse_disconnect(&error) {
            return Ok(());
        }
        return Err(error.into());
    }

    loop {
        let diagnostics = current_diagnostics(plan, state, remaining_work_units);
        let payload = serde_json::to_string(&diagnostics)?;
        if let Err(error) = stream.write_all(format!("data: {payload}\n\n").as_bytes()) {
            if is_sse_disconnect(&error) {
                return Ok(());
            }
            return Err(error.into());
        }
        if let Err(error) = stream.flush() {
            if is_sse_disconnect(&error) {
                return Ok(());
            }
            return Err(error.into());
        }
        thread::sleep(Duration::from_secs(1));
    }
}

pub(crate) fn write_metrics_event_stream(
    stream: &mut TcpStream,
    plan: &BootstrapPlan,
    state: &Arc<Mutex<BootstrapAdminState>>,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Err(error) = stream.write_all(
        b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\n\r\n",
    ) {
        if is_sse_disconnect(&error) {
            return Ok(());
        }
        return Err(error.into());
    }
    if let Err(error) = stream.flush() {
        if is_sse_disconnect(&error) {
            return Ok(());
        }
        return Err(error.into());
    }

    loop {
        let payload = serde_json::to_string(&current_metrics_live_event(plan, state)?)?;
        if let Err(error) = stream.write_all(format!("data: {payload}\n\n").as_bytes()) {
            if is_sse_disconnect(&error) {
                return Ok(());
            }
            return Err(error.into());
        }
        if let Err(error) = stream.flush() {
            if is_sse_disconnect(&error) {
                return Ok(());
            }
            return Err(error.into());
        }
        thread::sleep(Duration::from_secs(1));
    }
}

#[cfg(feature = "artifact-publish")]
pub(crate) fn write_artifact_event_stream(
    stream: &mut TcpStream,
    state: &Arc<Mutex<BootstrapAdminState>>,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Err(error) = stream.write_all(
        b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\n\r\n",
    ) {
        if is_sse_disconnect(&error) {
            return Ok(());
        }
        return Err(error.into());
    }
    if let Err(error) = stream.flush() {
        if is_sse_disconnect(&error) {
            return Ok(());
        }
        return Err(error.into());
    }

    loop {
        if let Some(event) = current_artifact_live_event(state)? {
            let payload = serde_json::to_string(&event)?;
            if let Err(error) = stream.write_all(format!("data: {payload}\n\n").as_bytes()) {
                if is_sse_disconnect(&error) {
                    return Ok(());
                }
                return Err(error.into());
            }
            if let Err(error) = stream.flush() {
                if is_sse_disconnect(&error) {
                    return Ok(());
                }
                return Err(error.into());
            }
        }
        thread::sleep(Duration::from_secs(1));
    }
}

pub(crate) fn is_sse_disconnect(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::UnexpectedEof
    )
}

pub(crate) fn read_request(
    stream: &TcpStream,
) -> Result<Option<HttpRequest>, Box<dyn std::error::Error>> {
    stream.set_read_timeout(Some(HTTP_READ_TIMEOUT))?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let Some(request_line) = read_bounded_line(&mut reader, MAX_REQUEST_LINE_BYTES)? else {
        return Ok(None);
    };
    if request_line.trim().is_empty() {
        return Ok(None);
    }

    let mut parts = request_line.split_whitespace();
    let method = parts.next().ok_or("missing method")?.to_owned();
    let path = parts.next().ok_or("missing path")?.to_owned();
    let mut headers = BTreeMap::new();
    let mut header_bytes = 0usize;

    loop {
        let Some(line) = read_bounded_line(&mut reader, MAX_HEADER_LINE_BYTES)? else {
            break;
        };
        header_bytes = header_bytes
            .checked_add(line.len())
            .ok_or("request headers exceeded maximum size")?;
        if header_bytes > MAX_HEADER_BYTES {
            return Err(format!("request headers exceeded {MAX_HEADER_BYTES} bytes").into());
        }
        if line == "\r\n" || line.is_empty() {
            break;
        }

        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_owned());
        }
    }

    let content_length = match headers.get("content-length") {
        Some(value) => value
            .parse::<usize>()
            .map_err(|_| format!("invalid content-length header `{value}`"))?,
        None => 0,
    };
    if content_length > MAX_BODY_BYTES {
        return Err(format!("request body exceeded {MAX_BODY_BYTES} bytes").into());
    }
    let mut body = vec![0_u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body)?;
    }

    Ok(Some(HttpRequest {
        method,
        path,
        headers,
        body,
    }))
}

fn read_bounded_line(
    reader: &mut BufReader<TcpStream>,
    max_len: usize,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let mut bytes = Vec::new();
    loop {
        let mut byte = [0_u8; 1];
        match reader.read(&mut byte)? {
            0 => {
                if bytes.is_empty() {
                    return Ok(None);
                }
                break;
            }
            _ => {
                bytes.push(byte[0]);
                if bytes.len() > max_len {
                    return Err(format!("request line exceeded {max_len} bytes").into());
                }
                if byte[0] == b'\n' {
                    break;
                }
            }
        }
    }
    String::from_utf8(bytes)
        .map(Some)
        .map_err(|error| format!("request line was not utf-8: {error}").into())
}

pub(crate) fn write_json(
    stream: &mut TcpStream,
    value: &impl Serialize,
) -> Result<(), Box<dyn std::error::Error>> {
    write_response(
        stream,
        "200 OK",
        "application/json; charset=utf-8",
        serde_json::to_vec_pretty(value)?,
    )?;
    Ok(())
}

#[cfg_attr(not(feature = "artifact-publish"), allow(dead_code))]
pub(crate) fn write_json_for_method(
    stream: &mut TcpStream,
    method: &str,
    value: &impl Serialize,
) -> Result<(), Box<dyn std::error::Error>> {
    write_response_for_method(
        stream,
        method,
        "200 OK",
        "application/json; charset=utf-8",
        serde_json::to_vec_pretty(value)?,
    )?;
    Ok(())
}

pub(crate) fn write_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    write_response_with_headers(stream, status, content_type, &[], body)?;
    Ok(())
}

#[cfg_attr(not(feature = "artifact-publish"), allow(dead_code))]
pub(crate) fn write_response_for_method(
    stream: &mut TcpStream,
    method: &str,
    status: &str,
    content_type: &str,
    body: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    write_response_with_headers_for_method(stream, method, status, content_type, &[], body)?;
    Ok(())
}

pub(crate) fn write_response_with_headers(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    headers: &[(&str, String)],
    body: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    write_response_with_headers_for_method(stream, "GET", status, content_type, headers, body)
}

pub(crate) fn write_response_with_headers_for_method(
    stream: &mut TcpStream,
    method: &str,
    status: &str,
    content_type: &str,
    headers: &[(&str, String)],
    body: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    stream.set_write_timeout(Some(HTTP_WRITE_TIMEOUT))?;
    let mut wire = format!(
        concat!(
            "HTTP/1.1 {}\r\n",
            "Content-Type: {}\r\n",
            "Content-Length: {}\r\n",
            "Connection: close\r\n",
            "X-Content-Type-Options: nosniff\r\n",
            "Referrer-Policy: no-referrer\r\n",
        ),
        status,
        content_type,
        body.len()
    );
    for (name, value) in headers {
        wire.push_str(name);
        wire.push_str(": ");
        wire.push_str(value);
        wire.push_str("\r\n");
    }
    wire.push_str("\r\n");
    stream.write_all(wire.as_bytes())?;
    if method != "HEAD" {
        stream.write_all(&body)?;
    }
    stream.flush()?;
    Ok(())
}

#[cfg(feature = "artifact-publish")]
#[allow(dead_code)]
pub(crate) fn write_stream_response_with_headers<R: std::io::Read>(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    headers: &[(&str, String)],
    content_length: u64,
    mut reader: R,
) -> Result<(), Box<dyn std::error::Error>> {
    write_stream_response_with_headers_for_method(
        stream,
        "GET",
        status,
        content_type,
        headers,
        content_length,
        &mut reader,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::thread;

    #[test]
    fn read_request_ignores_blank_probe_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind listener");
        let addr = listener.local_addr().expect("listener addr");
        let client = thread::spawn(move || {
            let mut stream = TcpStream::connect(addr).expect("connect client");
            stream.write_all(b"\r\n").expect("write blank request line");
        });
        let (stream, _) = listener.accept().expect("accept client");

        let request = read_request(&stream).expect("read request");

        assert!(request.is_none());
        client.join().expect("join client");
    }
}

#[cfg(feature = "artifact-publish")]
pub(crate) fn write_stream_response_with_headers_for_method<R: std::io::Read>(
    stream: &mut TcpStream,
    method: &str,
    status: &str,
    content_type: &str,
    headers: &[(&str, String)],
    content_length: u64,
    mut reader: R,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut wire = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {content_length}\r\nConnection: close\r\n"
    );
    for (name, value) in headers {
        wire.push_str(name);
        wire.push_str(": ");
        wire.push_str(value);
        wire.push_str("\r\n");
    }
    wire.push_str("\r\n");
    stream.write_all(wire.as_bytes())?;
    if method != "HEAD" {
        std::io::copy(&mut reader, stream)?;
    }
    stream.flush()?;
    Ok(())
}

#[cfg(feature = "artifact-publish")]
#[allow(dead_code)]
pub(crate) fn write_file_response_with_headers(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    headers: &[(&str, String)],
    path: &std::path::Path,
    content_length: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    write_file_response_with_headers_for_method(
        stream,
        "GET",
        status,
        content_type,
        headers,
        path,
        content_length,
    )
}

#[cfg(feature = "artifact-publish")]
pub(crate) fn write_file_response_with_headers_for_method(
    stream: &mut TcpStream,
    method: &str,
    status: &str,
    content_type: &str,
    headers: &[(&str, String)],
    path: &std::path::Path,
    content_length: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = std::fs::File::open(path)?;
    let reader = std::io::BufReader::new(file);
    write_stream_response_with_headers_for_method(
        stream,
        method,
        status,
        content_type,
        headers,
        content_length,
        reader,
    )
}
