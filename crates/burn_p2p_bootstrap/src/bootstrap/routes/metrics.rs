use super::*;

pub(crate) fn handle_metrics_indexer_route(
    stream: &mut TcpStream,
    context: &HttpServerContext,
    request: &HttpRequest,
) -> Result<bool, Box<dyn std::error::Error>> {
    if request.method != "GET" {
        return Ok(false);
    }

    match request.path.as_str() {
        "/metrics/snapshot" => {
            if !cfg!(feature = "metrics-indexer") {
                write_response(
                    stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    b"metrics indexer disabled".to_vec(),
                )?;
                return Ok(true);
            }
            write_json(stream, &current_metrics_snapshots(&context.state)?)?;
            return Ok(true);
        }
        "/metrics/ledger" => {
            if !cfg!(feature = "metrics-indexer") {
                write_response(
                    stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    b"metrics indexer disabled".to_vec(),
                )?;
                return Ok(true);
            }
            write_json(stream, &current_metrics_ledger(&context.state)?)?;
            return Ok(true);
        }
        "/metrics/catchup" => {
            if !cfg!(feature = "metrics-indexer") {
                write_response(
                    stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    b"metrics indexer disabled".to_vec(),
                )?;
                return Ok(true);
            }
            write_json(stream, &current_metrics_catchup(&context.state)?)?;
            return Ok(true);
        }
        "/metrics/candidates" => {
            if !cfg!(feature = "metrics-indexer") {
                write_response(
                    stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    b"metrics indexer disabled".to_vec(),
                )?;
                return Ok(true);
            }
            write_json(stream, &current_metrics_candidates(&context.state)?)?;
            return Ok(true);
        }
        "/metrics/disagreements" => {
            if !cfg!(feature = "metrics-indexer") {
                write_response(
                    stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    b"metrics indexer disabled".to_vec(),
                )?;
                return Ok(true);
            }
            write_json(stream, &current_metrics_disagreements(&context.state)?)?;
            return Ok(true);
        }
        "/metrics/peer-windows" => {
            if !cfg!(feature = "metrics-indexer") {
                write_response(
                    stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    b"metrics indexer disabled".to_vec(),
                )?;
                return Ok(true);
            }
            write_json(stream, &current_metrics_peer_windows(&context.state)?)?;
            return Ok(true);
        }
        "/metrics/live" => {
            if !cfg!(feature = "metrics-indexer") {
                write_response(
                    stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    b"metrics indexer disabled".to_vec(),
                )?;
                return Ok(true);
            }
            write_metrics_event_stream(stream, &context.plan, &context.state)?;
            return Ok(true);
        }
        "/metrics/live/latest" => {
            if !cfg!(feature = "metrics-indexer") {
                write_response(
                    stream,
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    b"metrics indexer disabled".to_vec(),
                )?;
                return Ok(true);
            }
            write_json(
                stream,
                &current_metrics_live_event(&context.plan, &context.state)?,
            )?;
            return Ok(true);
        }
        _ => {}
    }

    if let Some(head_id) = request.path.strip_prefix("/metrics/heads/") {
        if !cfg!(feature = "metrics-indexer") {
            write_response(
                stream,
                "404 Not Found",
                "text/plain; charset=utf-8",
                b"metrics indexer disabled".to_vec(),
            )?;
            return Ok(true);
        }
        write_json(
            stream,
            &current_head_eval_reports(&context.state, &burn_p2p::HeadId::new(head_id)),
        )?;
        return Ok(true);
    }
    if let Some(experiment_id) = request.path.strip_prefix("/metrics/experiments/") {
        if !cfg!(feature = "metrics-indexer") {
            write_response(
                stream,
                "404 Not Found",
                "text/plain; charset=utf-8",
                b"metrics indexer disabled".to_vec(),
            )?;
            return Ok(true);
        }
        write_json(
            stream,
            &current_metrics_snapshots_for_experiment(
                &context.state,
                &burn_p2p::ExperimentId::new(experiment_id),
            )?,
        )?;
        return Ok(true);
    }
    if let Some(experiment_id) = request.path.strip_prefix("/metrics/catchup/") {
        if !cfg!(feature = "metrics-indexer") {
            write_response(
                stream,
                "404 Not Found",
                "text/plain; charset=utf-8",
                b"metrics indexer disabled".to_vec(),
            )?;
            return Ok(true);
        }
        write_json(
            stream,
            &current_metrics_catchup_for_experiment(
                &context.state,
                &burn_p2p::ExperimentId::new(experiment_id),
            )?,
        )?;
        return Ok(true);
    }
    if let Some(experiment_id) = request.path.strip_prefix("/metrics/candidates/") {
        if !cfg!(feature = "metrics-indexer") {
            write_response(
                stream,
                "404 Not Found",
                "text/plain; charset=utf-8",
                b"metrics indexer disabled".to_vec(),
            )?;
            return Ok(true);
        }
        write_json(
            stream,
            &current_metrics_candidates_for_experiment(
                &context.state,
                &burn_p2p::ExperimentId::new(experiment_id),
            )?,
        )?;
        return Ok(true);
    }
    if let Some(experiment_id) = request.path.strip_prefix("/metrics/disagreements/") {
        if !cfg!(feature = "metrics-indexer") {
            write_response(
                stream,
                "404 Not Found",
                "text/plain; charset=utf-8",
                b"metrics indexer disabled".to_vec(),
            )?;
            return Ok(true);
        }
        write_json(
            stream,
            &current_metrics_disagreements_for_experiment(
                &context.state,
                &burn_p2p::ExperimentId::new(experiment_id),
            )?,
        )?;
        return Ok(true);
    }
    if let Some(experiment_id) = request.path.strip_prefix("/metrics/peer-windows/") {
        if !cfg!(feature = "metrics-indexer") {
            write_response(
                stream,
                "404 Not Found",
                "text/plain; charset=utf-8",
                b"metrics indexer disabled".to_vec(),
            )?;
            return Ok(true);
        }
        if let Some((experiment_id, remainder)) = experiment_id.split_once('/')
            && let Some((revision_id, base_head_id)) = remainder.split_once('/')
        {
            match current_metrics_peer_window_detail(
                &context.state,
                &burn_p2p::ExperimentId::new(experiment_id),
                &burn_p2p::RevisionId::new(revision_id),
                &burn_p2p::HeadId::new(base_head_id),
            )? {
                Some(detail) => write_json(stream, &detail)?,
                None => {
                    write_response(
                        stream,
                        "404 Not Found",
                        "text/plain; charset=utf-8",
                        b"peer-window detail not found".to_vec(),
                    )?;
                }
            }
            return Ok(true);
        }
        write_json(
            stream,
            &current_metrics_peer_windows_for_experiment(
                &context.state,
                &burn_p2p::ExperimentId::new(experiment_id),
            )?,
        )?;
        return Ok(true);
    }

    Ok(false)
}

#[cfg(feature = "metrics-indexer")]
pub(crate) fn current_metrics_snapshots(
    state: &Arc<Mutex<BootstrapAdminState>>,
) -> Result<Vec<burn_p2p_metrics::MetricsSnapshot>, Box<dyn std::error::Error>> {
    Ok(state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .export_metrics_snapshots()?)
}

#[cfg(not(feature = "metrics-indexer"))]
pub(crate) fn current_metrics_snapshots(
    _state: &Arc<Mutex<BootstrapAdminState>>,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    Err("metrics indexer disabled".into())
}

#[cfg(feature = "metrics-indexer")]
pub(crate) fn current_metrics_snapshots_for_experiment(
    state: &Arc<Mutex<BootstrapAdminState>>,
    experiment_id: &burn_p2p::ExperimentId,
) -> Result<Vec<burn_p2p_metrics::MetricsSnapshot>, Box<dyn std::error::Error>> {
    Ok(state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .export_metrics_snapshots_for_experiment(experiment_id)?)
}

#[cfg(not(feature = "metrics-indexer"))]
pub(crate) fn current_metrics_snapshots_for_experiment(
    _state: &Arc<Mutex<BootstrapAdminState>>,
    _experiment_id: &burn_p2p::ExperimentId,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    Err("metrics indexer disabled".into())
}

#[cfg(feature = "metrics-indexer")]
pub(crate) fn current_metrics_ledger(
    state: &Arc<Mutex<BootstrapAdminState>>,
) -> Result<Vec<burn_p2p::MetricsLedgerSegment>, Box<dyn std::error::Error>> {
    Ok(state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .export_metrics_ledger_segments()?)
}

#[cfg(not(feature = "metrics-indexer"))]
pub(crate) fn current_metrics_ledger(
    _state: &Arc<Mutex<BootstrapAdminState>>,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    Err("metrics indexer disabled".into())
}

#[cfg(feature = "metrics-indexer")]
pub(crate) fn current_metrics_catchup(
    state: &Arc<Mutex<BootstrapAdminState>>,
) -> Result<Vec<burn_p2p_metrics::MetricsCatchupBundle>, Box<dyn std::error::Error>> {
    Ok(state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .export_metrics_catchup_bundles()?)
}

#[cfg(not(feature = "metrics-indexer"))]
pub(crate) fn current_metrics_catchup(
    _state: &Arc<Mutex<BootstrapAdminState>>,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    Err("metrics indexer disabled".into())
}

#[cfg(feature = "metrics-indexer")]
pub(crate) fn current_metrics_catchup_for_experiment(
    state: &Arc<Mutex<BootstrapAdminState>>,
    experiment_id: &burn_p2p::ExperimentId,
) -> Result<Vec<burn_p2p_metrics::MetricsCatchupBundle>, Box<dyn std::error::Error>> {
    Ok(state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .export_metrics_catchup_bundles_for_experiment(experiment_id)?)
}

#[cfg(not(feature = "metrics-indexer"))]
pub(crate) fn current_metrics_catchup_for_experiment(
    _state: &Arc<Mutex<BootstrapAdminState>>,
    _experiment_id: &burn_p2p::ExperimentId,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    Err("metrics indexer disabled".into())
}

#[cfg(feature = "metrics-indexer")]
pub(crate) fn current_metrics_candidates(
    state: &Arc<Mutex<BootstrapAdminState>>,
) -> Result<Vec<burn_p2p::ReducerCohortMetrics>, Box<dyn std::error::Error>> {
    Ok(state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .export_metrics_candidates()?)
}

#[cfg(not(feature = "metrics-indexer"))]
pub(crate) fn current_metrics_candidates(
    _state: &Arc<Mutex<BootstrapAdminState>>,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    Err("metrics indexer disabled".into())
}

#[cfg(feature = "metrics-indexer")]
pub(crate) fn current_metrics_candidates_for_experiment(
    state: &Arc<Mutex<BootstrapAdminState>>,
    experiment_id: &burn_p2p::ExperimentId,
) -> Result<Vec<burn_p2p::ReducerCohortMetrics>, Box<dyn std::error::Error>> {
    Ok(state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .export_metrics_candidates_for_experiment(experiment_id)?)
}

#[cfg(not(feature = "metrics-indexer"))]
pub(crate) fn current_metrics_candidates_for_experiment(
    _state: &Arc<Mutex<BootstrapAdminState>>,
    _experiment_id: &burn_p2p::ExperimentId,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    Err("metrics indexer disabled".into())
}

#[cfg(feature = "metrics-indexer")]
pub(crate) fn current_metrics_disagreements(
    state: &Arc<Mutex<BootstrapAdminState>>,
) -> Result<Vec<burn_p2p::ReducerCohortMetrics>, Box<dyn std::error::Error>> {
    Ok(state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .export_metrics_disagreements()?)
}

#[cfg(not(feature = "metrics-indexer"))]
pub(crate) fn current_metrics_disagreements(
    _state: &Arc<Mutex<BootstrapAdminState>>,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    Err("metrics indexer disabled".into())
}

#[cfg(feature = "metrics-indexer")]
pub(crate) fn current_metrics_disagreements_for_experiment(
    state: &Arc<Mutex<BootstrapAdminState>>,
    experiment_id: &burn_p2p::ExperimentId,
) -> Result<Vec<burn_p2p::ReducerCohortMetrics>, Box<dyn std::error::Error>> {
    Ok(state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .export_metrics_disagreements_for_experiment(experiment_id)?)
}

#[cfg(not(feature = "metrics-indexer"))]
pub(crate) fn current_metrics_disagreements_for_experiment(
    _state: &Arc<Mutex<BootstrapAdminState>>,
    _experiment_id: &burn_p2p::ExperimentId,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    Err("metrics indexer disabled".into())
}

#[cfg(feature = "metrics-indexer")]
pub(crate) fn current_metrics_peer_windows(
    state: &Arc<Mutex<BootstrapAdminState>>,
) -> Result<Vec<burn_p2p_metrics::PeerWindowDistributionSummary>, Box<dyn std::error::Error>> {
    Ok(state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .export_metrics_peer_window_distributions()?)
}

#[cfg(not(feature = "metrics-indexer"))]
pub(crate) fn current_metrics_peer_windows(
    _state: &Arc<Mutex<BootstrapAdminState>>,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    Err("metrics indexer disabled".into())
}

#[cfg(feature = "metrics-indexer")]
pub(crate) fn current_metrics_peer_windows_for_experiment(
    state: &Arc<Mutex<BootstrapAdminState>>,
    experiment_id: &burn_p2p::ExperimentId,
) -> Result<Vec<burn_p2p_metrics::PeerWindowDistributionSummary>, Box<dyn std::error::Error>> {
    Ok(state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .export_metrics_peer_window_distributions_for_experiment(experiment_id)?)
}

#[cfg(not(feature = "metrics-indexer"))]
pub(crate) fn current_metrics_peer_windows_for_experiment(
    _state: &Arc<Mutex<BootstrapAdminState>>,
    _experiment_id: &burn_p2p::ExperimentId,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    Err("metrics indexer disabled".into())
}

#[cfg(feature = "metrics-indexer")]
pub(crate) fn current_metrics_peer_window_detail(
    state: &Arc<Mutex<BootstrapAdminState>>,
    experiment_id: &burn_p2p::ExperimentId,
    revision_id: &burn_p2p::RevisionId,
    base_head_id: &burn_p2p::HeadId,
) -> Result<Option<burn_p2p_metrics::PeerWindowDistributionDetail>, Box<dyn std::error::Error>> {
    Ok(state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .export_metrics_peer_window_distribution_detail(experiment_id, revision_id, base_head_id)?)
}

#[cfg(not(feature = "metrics-indexer"))]
pub(crate) fn current_metrics_peer_window_detail(
    _state: &Arc<Mutex<BootstrapAdminState>>,
    _experiment_id: &burn_p2p::ExperimentId,
    _revision_id: &burn_p2p::RevisionId,
    _base_head_id: &burn_p2p::HeadId,
) -> Result<Option<serde_json::Value>, Box<dyn std::error::Error>> {
    Err("metrics indexer disabled".into())
}

#[cfg(feature = "metrics-indexer")]
pub(crate) fn current_metrics_live_event(
    plan: &BootstrapPlan,
    state: &Arc<Mutex<BootstrapAdminState>>,
) -> Result<burn_p2p_core::MetricsLiveEvent, Box<dyn std::error::Error>> {
    Ok(state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .export_metrics_live_event(plan.network_id())?)
}

#[cfg(not(feature = "metrics-indexer"))]
pub(crate) fn current_metrics_live_event(
    _plan: &BootstrapPlan,
    _state: &Arc<Mutex<BootstrapAdminState>>,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    Err("metrics indexer disabled".into())
}

pub(crate) fn current_head_eval_reports(
    state: &Arc<Mutex<BootstrapAdminState>>,
    head_id: &burn_p2p::HeadId,
) -> Vec<burn_p2p::HeadEvalReport> {
    state
        .lock()
        .expect("bootstrap admin state should not be poisoned")
        .export_head_eval_reports(head_id)
}
