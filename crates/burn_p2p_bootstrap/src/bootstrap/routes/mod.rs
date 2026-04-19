use super::*;
use std::io::Error as IoError;
use std::sync::{Mutex, MutexGuard};

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

pub(crate) fn lock_shared<'a, T>(
    mutex: &'a Mutex<T>,
    label: &'static str,
) -> Result<MutexGuard<'a, T>, Box<dyn std::error::Error>> {
    mutex
        .lock()
        .map_err(|_| IoError::other(format!("{label} lock poisoned")).into())
}

pub(crate) fn with_admin_state<T, E, F>(
    state: &Arc<Mutex<BootstrapAdminState>>,
    f: F,
) -> Result<T, Box<dyn std::error::Error>>
where
    F: FnOnce(&BootstrapAdminState) -> Result<T, E>,
    E: std::fmt::Display,
{
    let state = lock_shared(state, "bootstrap admin state")?;
    f(&state).map_err(|err| IoError::other(err.to_string()).into())
}

pub(crate) fn handle_connection(
    mut stream: TcpStream,
    context: HttpServerContext,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(request) = read_request(&stream)? else {
        return Ok(());
    };
    let current_config = lock_shared(&context.config, "daemon config")?.clone();

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
