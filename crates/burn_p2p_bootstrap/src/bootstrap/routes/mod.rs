use super::*;

mod admin;
mod app_state;
mod artifacts;
mod get;
mod http;
mod metrics;
mod post;
mod trust_state;

pub(crate) use admin::*;
pub(crate) use app_state::*;
pub(crate) use artifacts::*;
pub(crate) use get::*;
pub(crate) use http::*;
pub(crate) use metrics::*;
pub(crate) use post::*;
pub(crate) use trust_state::*;

pub(crate) fn handle_connection(
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
