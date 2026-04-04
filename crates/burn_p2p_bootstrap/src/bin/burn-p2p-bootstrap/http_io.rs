fn write_event_stream(
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

fn write_metrics_event_stream(
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

fn write_artifact_event_stream(
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

fn is_sse_disconnect(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::UnexpectedEof
    )
}

fn read_request(stream: &TcpStream) -> Result<Option<HttpRequest>, Box<dyn std::error::Error>> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut request_line = String::new();
    if reader.read_line(&mut request_line)? == 0 {
        return Ok(None);
    }

    let mut parts = request_line.split_whitespace();
    let method = parts.next().ok_or("missing method")?.to_owned();
    let path = parts.next().ok_or("missing path")?.to_owned();
    let mut headers = BTreeMap::new();

    loop {
        let mut line = String::new();
        reader.read_line(&mut line)?;
        if line == "\r\n" || line.is_empty() {
            break;
        }

        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_owned());
        }
    }

    let content_length = headers
        .get("content-length")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);
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

fn write_json(
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

fn write_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    write_response_with_headers(stream, status, content_type, &[], body)?;
    Ok(())
}

fn write_response_with_headers(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    headers: &[(&str, String)],
    body: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut wire = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n",
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
    stream.write_all(&body)?;
    stream.flush()?;
    Ok(())
}

fn write_stream_response_with_headers<R: std::io::Read>(
    stream: &mut TcpStream,
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
    std::io::copy(&mut reader, stream)?;
    stream.flush()?;
    Ok(())
}

fn write_file_response_with_headers(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    headers: &[(&str, String)],
    path: &std::path::Path,
    content_length: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = std::fs::File::open(path)?;
    let reader = std::io::BufReader::new(file);
    write_stream_response_with_headers(
        stream,
        status,
        content_type,
        headers,
        content_length,
        reader,
    )
}
