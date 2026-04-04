fn handle_connection(
    mut stream: TcpStream,
    context: HttpServerContext,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(request) = read_request(&stream)? else {
        return Ok(());
    };
    let current_config = context
        .config
        .lock()
        .expect("daemon config should not be poisoned")
        .clone();

    if handle_get_route(&mut stream, &context, &current_config, &request)? {
        return Ok(());
    }
    if handle_artifact_publish_post_route(&mut stream, &context, &request)? {
        return Ok(());
    }
    if handle_browser_post_route(&mut stream, &context, &current_config, &request)? {
        return Ok(());
    }
    if handle_auth_post_route(&mut stream, &context, &request)? {
        return Ok(());
    }
    if handle_admin_post_route(&mut stream, &context, &request)? {
        return Ok(());
    }

    write_response(
        &mut stream,
        "404 Not Found",
        "text/plain; charset=utf-8",
        b"not found".to_vec(),
    )?;

    Ok(())
}

include!("get_routes.rs");
include!("post_routes.rs");
include!("portal_state.rs");
include!("trust_state.rs");
include!("admin_helpers.rs");
include!("artifact_routes.rs");
include!("metrics_routes.rs");
include!("http_io.rs");
