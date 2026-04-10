use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, ensure};
use burn_p2p_testkit::formal_trace::{
    FormalProtocolAction, FormalProtocolEvent, FormalProtocolTrace, FormalTraceConformanceReport,
    FormalTraceScenario, check_protocol_trace, lower_protocol_trace, sample_protocol_trace,
    write_protocol_actions_json, write_protocol_trace_conformance_json, write_protocol_trace_json,
};

use crate::{
    artifacts::ArtifactLayout,
    cli::{FormalArgs, FormalExportArgs, FormalTraceFormatArg, FormalTraceScenarioArg},
    runner::{Workspace, command_available},
};

const FORMAL_PROJECT_DIR: &str = "formal/veil";

struct PreparedFormalTrace {
    scenario: FormalTraceScenario,
    extension: &'static str,
    output_path: PathBuf,
    trace: FormalProtocolTrace,
    conformance: FormalTraceConformanceReport,
    lowered_actions: Vec<FormalProtocolAction>,
}

struct FinalPromotionSummary {
    window_id: u64,
    base_head_id: String,
    head_id: String,
    aggregate_artifact_id: String,
    accepted_contributor_peer_ids: Vec<String>,
}

pub(crate) fn run_formal_check(workspace: &Workspace, args: FormalArgs) -> anyhow::Result<()> {
    let artifacts = ArtifactLayout::create(&workspace.root, "formal-check", args.common.profile)?;
    let project_root = formal_project_root(workspace);
    ensure!(
        project_root.exists(),
        "formal project root is missing at {}",
        project_root.display()
    );
    ensure!(
        resolve_lake_bin().is_some(),
        "lake is not available; install lean/lake (for example via elan) to run formal checks"
    );
    let lake = resolve_lake_bin().expect("checked above");
    workspace.run_in_dir(
        &artifacts,
        "lake-build",
        &project_root,
        &lake,
        &["build".into()],
        &BTreeMap::new(),
    )?;
    Ok(())
}

pub(crate) fn run_formal_modelcheck(workspace: &Workspace, args: FormalArgs) -> anyhow::Result<()> {
    let artifacts =
        ArtifactLayout::create(&workspace.root, "formal-modelcheck", args.common.profile)?;
    let project_root = formal_project_root(workspace);
    ensure!(
        project_root.exists(),
        "formal project root is missing at {}",
        project_root.display()
    );
    ensure!(
        resolve_lake_bin().is_some(),
        "lake is not available; install lean/lake (for example via elan) to run formal model checks"
    );
    let lake = resolve_lake_bin().expect("checked above");
    workspace.run_in_dir(
        &artifacts,
        "lake-build-modelcheck-smoke",
        &project_root,
        &lake,
        &["build".into(), "BurnP2P.ModelCheck.Smoke".into()],
        &BTreeMap::new(),
    )?;
    Ok(())
}

pub(crate) fn run_formal_export_trace(
    workspace: &Workspace,
    args: FormalExportArgs,
) -> anyhow::Result<()> {
    let artifacts =
        ArtifactLayout::create(&workspace.root, "formal-export-trace", args.common.profile)?;
    let prepared = prepare_formal_trace(&artifacts, &args)?;
    write_protocol_trace_conformance_json(
        artifacts.configs.join("formal-trace-conformance.json"),
        &prepared.conformance,
    )?;
    write_protocol_actions_json(
        artifacts.configs.join("formal-trace-actions.json"),
        &prepared.lowered_actions,
    )?;
    artifacts.write_json(
        "configs/formal-trace-manifest.json",
        &serde_json::json!({
            "scenario": prepared.scenario.slug(),
            "format": prepared.extension,
            "output": prepared.output_path
                .strip_prefix(&artifacts.root)
                .unwrap_or(&prepared.output_path)
                .display()
                .to_string(),
            "schema_version": prepared.trace.schema_version,
            "event_count": prepared.trace.events.len(),
            "conformance": {
                "accepted_candidate_count": prepared.conformance.accepted_candidate_count,
                "attestation_count": prepared.conformance.attestation_count,
                "quorum_certificate_count": prepared.conformance.quorum_certificate_count,
                "canonical_promotion_count": prepared.conformance.canonical_promotion_count,
                "lowered_action_count": prepared.conformance.lowered_action_count,
            }
        }),
    )?;
    println!("formal trace written to {}", prepared.output_path.display());
    Ok(())
}

pub(crate) fn run_formal_verify_trace(
    workspace: &Workspace,
    args: FormalExportArgs,
) -> anyhow::Result<()> {
    let artifacts =
        ArtifactLayout::create(&workspace.root, "formal-verify-trace", args.common.profile)?;
    let project_root = formal_project_root(workspace);
    ensure!(
        project_root.exists(),
        "formal project root is missing at {}",
        project_root.display()
    );
    ensure!(
        resolve_lake_bin().is_some(),
        "lake is not available; install lean/lake (for example via elan) to run formal checks"
    );
    let prepared = prepare_formal_trace(&artifacts, &args)?;
    let lean_fixture_path = artifacts.configs.join(format!(
        "formal-trace-{}-fixture.lean",
        prepared.scenario.slug()
    ));
    fs::write(
        &lean_fixture_path,
        render_trace_fixture_lean(&prepared.trace, &prepared.lowered_actions)?,
    )
    .with_context(|| format!("failed to write {}", lean_fixture_path.display()))?;
    write_protocol_trace_conformance_json(
        artifacts.configs.join("formal-trace-conformance.json"),
        &prepared.conformance,
    )?;
    write_protocol_actions_json(
        artifacts.configs.join("formal-trace-actions.json"),
        &prepared.lowered_actions,
    )?;
    let lake = resolve_lake_bin().expect("checked above");
    let lean_args = vec![
        "env".into(),
        "lean".into(),
        format!("--root={}", project_root.display()),
        lean_fixture_path.display().to_string(),
    ];
    workspace.run_in_dir(
        &artifacts,
        "lake-verify-trace-fixture",
        &project_root,
        &lake,
        &lean_args,
        &BTreeMap::new(),
    )?;
    artifacts.write_json(
        "configs/formal-trace-manifest.json",
        &serde_json::json!({
            "scenario": prepared.scenario.slug(),
            "format": prepared.extension,
            "output": prepared.output_path
                .strip_prefix(&artifacts.root)
                .unwrap_or(&prepared.output_path)
                .display()
                .to_string(),
            "fixture": lean_fixture_path
                .strip_prefix(&artifacts.root)
                .unwrap_or(&lean_fixture_path)
                .display()
                .to_string(),
            "schema_version": prepared.trace.schema_version,
            "event_count": prepared.trace.events.len(),
            "conformance": {
                "accepted_candidate_count": prepared.conformance.accepted_candidate_count,
                "attestation_count": prepared.conformance.attestation_count,
                "quorum_certificate_count": prepared.conformance.quorum_certificate_count,
                "canonical_promotion_count": prepared.conformance.canonical_promotion_count,
                "lowered_action_count": prepared.conformance.lowered_action_count,
            },
            "verified_by": "lake env lean",
        }),
    )?;
    println!(
        "formal trace verified with lean fixture {}",
        lean_fixture_path.display()
    );
    Ok(())
}

fn formal_project_root(workspace: &Workspace) -> std::path::PathBuf {
    workspace.root.join(FORMAL_PROJECT_DIR)
}

fn prepare_formal_trace(
    artifacts: &ArtifactLayout,
    args: &FormalExportArgs,
) -> anyhow::Result<PreparedFormalTrace> {
    let scenario = map_trace_scenario(args.scenario);
    let trace = sample_protocol_trace(scenario);
    let extension = match args.format {
        FormalTraceFormatArg::Json => "json",
        FormalTraceFormatArg::Cbor => "cbor",
    };
    let output_path =
        artifacts
            .configs
            .join(format!("formal-trace-{}.{}", scenario.slug(), extension));
    write_trace_payload(&output_path, &trace, args.format)?;
    let conformance =
        check_protocol_trace(&trace).context("formal trace failed protocol conformance checks")?;
    let lowered_actions = lower_protocol_trace(&trace);
    Ok(PreparedFormalTrace {
        scenario,
        extension,
        output_path,
        trace,
        conformance,
        lowered_actions,
    })
}

fn write_trace_payload(
    output_path: &Path,
    trace: &FormalProtocolTrace,
    format: FormalTraceFormatArg,
) -> anyhow::Result<()> {
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }
    match format {
        FormalTraceFormatArg::Json => write_protocol_trace_json(output_path, trace),
        FormalTraceFormatArg::Cbor => {
            let mut bytes = Vec::new();
            ciborium::into_writer(trace, &mut bytes)
                .context("failed to encode formal trace as cbor")?;
            fs::write(output_path, bytes)?;
            Ok(())
        }
    }
}

fn map_trace_scenario(value: FormalTraceScenarioArg) -> FormalTraceScenario {
    match value {
        FormalTraceScenarioArg::MnistSmoke => FormalTraceScenario::MnistSmoke,
        FormalTraceScenarioArg::ReducerAdversarial => FormalTraceScenario::ReducerAdversarial,
    }
}

fn render_trace_fixture_lean(
    trace: &FormalProtocolTrace,
    actions: &[FormalProtocolAction],
) -> anyhow::Result<String> {
    let validator_set = collect_validator_ids(trace);
    let validator_quorum_size = collect_validator_quorum_size(trace);
    let final_promotion = final_promotion_event(trace);
    let reducer_prefix_trace = reducer_prefix_trace(trace);
    let final_revoked_peers = collect_revoked_peers(trace);
    let final_head_assertion = match &final_promotion {
        Some(promotion) => format!(
            "example : (replayLoweredTrace generatedInitialState generatedTrace).promotedHead = some {{\n  windowId := {}\n  baseHeadId := {}\n  headId := {}\n  aggregateArtifactId := {}\n  acceptedContributorPeerIds := {}\n}} := by\n  native_decide\n",
            render_nat(promotion.window_id),
            render_lean_string(&promotion.base_head_id)?,
            render_lean_string(&promotion.head_id)?,
            render_lean_string(&promotion.aggregate_artifact_id)?,
            render_lean_string_list(&promotion.accepted_contributor_peer_ids)?,
        ),
        None => "example : (replayLoweredTrace generatedInitialState generatedTrace).promotedHead = none := by\n  native_decide\n".into(),
    };
    let reducer_prefix_assertion = match reducer_prefix_trace {
        Some(reducer_trace) => format!(
            "{}\n\nexample : (replayLoweredTrace generatedInitialState generatedReducerPrefixTrace).promotedHead = none := by\n  native_decide\n",
            render_protocol_trace("generatedReducerPrefixTrace", &reducer_trace)?
        ),
        None => String::new(),
    };
    let revoked_assertion = if final_revoked_peers.is_empty() {
        String::new()
    } else {
        format!(
            "example : (replayLoweredTrace generatedInitialState generatedTrace).revokedPeers = {} := by\n  native_decide\n",
            render_lean_string_list(&final_revoked_peers)?,
        )
    };

    Ok(format!(
        "import BurnP2P.Trace.Refinement\n\nopen BurnP2P.Protocol\nopen BurnP2P.Trace\n\n\
def generatedInitialState : State :=\n  {{\n    scope := {{\n      networkId := {}\n      studyId := {}\n      experimentId := {}\n      revisionId := {}\n    }}\n    authorityEpoch := 1\n    validatorSet := {}\n    validatorQuorumSize := {}\n    revokedPeers := []\n    candidateUpdates := []\n    reducerProposals := []\n    acceptedCandidates := []\n    rejectedCandidates := []\n    validatorAttestations := []\n    promotedHead := none\n  }}\n\n\
{}\n\n\
def generatedActions : List Action :=\n  {}\n\n\
example : lowerTraceActions generatedTrace = generatedActions := by\n  native_decide\n\n\
example : replayLoweredTrace generatedInitialState generatedTrace = generatedActions.foldl step generatedInitialState := by\n  native_decide\n\n\
{}\n\
example : (replayLoweredTrace generatedInitialState generatedTrace).promotedHead.map (fun promotion => promotion.acceptedContributorPeerIds) = {} := by\n  native_decide\n\n\
{}{}",
        render_lean_string(&trace.scope.network_id.to_string())?,
        render_lean_string(&trace.scope.study_id.to_string())?,
        render_lean_string(&trace.scope.experiment_id.to_string())?,
        render_lean_string(&trace.scope.revision_id.to_string())?,
        render_lean_string_list(&validator_set)?,
        render_nat(validator_quorum_size as u64),
        render_protocol_trace("generatedTrace", trace)?,
        render_action_list(actions)?,
        final_head_assertion,
        match &final_promotion {
            Some(promotion) => format!(
                "some {}",
                render_lean_string_list(&promotion.accepted_contributor_peer_ids)?
            ),
            None => "none".into(),
        },
        reducer_prefix_assertion,
        revoked_assertion,
    ))
}

fn render_protocol_trace(
    binding_name: &str,
    trace: &FormalProtocolTrace,
) -> anyhow::Result<String> {
    Ok(format!(
        "def {} : ProtocolTrace :=\n  {{\n    schemaVersion := {}\n    scenario := {}\n    events := {}\n  }}",
        binding_name,
        render_lean_string(&trace.schema_version)?,
        render_lean_string(trace.scenario.slug())?,
        render_event_list(&trace.events)?,
    ))
}

fn render_event_list(events: &[FormalProtocolEvent]) -> anyhow::Result<String> {
    let rendered = events
        .iter()
        .map(render_protocol_event)
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(format!("[{}]", rendered.join(", ")))
}

fn render_action_list(actions: &[FormalProtocolAction]) -> anyhow::Result<String> {
    let rendered = actions
        .iter()
        .map(render_protocol_action)
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(format!("[{}]", rendered.join(", ")))
}

fn render_protocol_event(event: &FormalProtocolEvent) -> anyhow::Result<String> {
    Ok(match event {
        FormalProtocolEvent::UpdatePublished {
            observed_at,
            peer_id,
            window_id,
            base_head_id,
            artifact_id,
        } => format!(
            ".updatePublished {} {} {} {} {}",
            render_lean_string(&observed_at.to_rfc3339())?,
            render_lean_string(&peer_id.to_string())?,
            render_nat(window_id.0),
            render_lean_string(&base_head_id.to_string())?,
            render_lean_string(&artifact_id.to_string())?,
        ),
        FormalProtocolEvent::ReducerProposalObserved {
            observed_at,
            reducer_peer_id,
            window_id,
            base_head_id,
            aggregate_artifact_id,
            contributor_peer_ids,
        } => format!(
            ".reducerProposalObserved {} {} {} {} {} {}",
            render_lean_string(&observed_at.to_rfc3339())?,
            render_lean_string(&reducer_peer_id.to_string())?,
            render_nat(window_id.0),
            render_lean_string(&base_head_id.to_string())?,
            render_lean_string(&aggregate_artifact_id.to_string())?,
            render_lean_string_list(
                &contributor_peer_ids
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            )?,
        ),
        FormalProtocolEvent::CandidateAccepted {
            observed_at,
            validator_peer_id,
            candidate_peer_id,
            window_id,
            artifact_id,
        } => format!(
            ".candidateAccepted {} {} {} {} {}",
            render_lean_string(&observed_at.to_rfc3339())?,
            render_lean_string(&validator_peer_id.to_string())?,
            render_lean_string(&candidate_peer_id.to_string())?,
            render_nat(window_id.0),
            render_lean_string(&artifact_id.to_string())?,
        ),
        FormalProtocolEvent::CandidateRejected {
            observed_at,
            validator_peer_id,
            candidate_peer_id,
            window_id,
            artifact_id,
            reason,
        } => format!(
            ".candidateRejected {} {} {} {} {} {}",
            render_lean_string(&observed_at.to_rfc3339())?,
            render_lean_string(&validator_peer_id.to_string())?,
            render_lean_string(&candidate_peer_id.to_string())?,
            render_nat(window_id.0),
            render_lean_string(&artifact_id.to_string())?,
            render_lean_string(reason)?,
        ),
        FormalProtocolEvent::ValidationAttested {
            observed_at,
            validator_peer_id,
            window_id,
            aggregate_artifact_id,
        } => format!(
            ".validationAttested {} {} {} {}",
            render_lean_string(&observed_at.to_rfc3339())?,
            render_lean_string(&validator_peer_id.to_string())?,
            render_nat(window_id.0),
            render_lean_string(&aggregate_artifact_id.to_string())?,
        ),
        FormalProtocolEvent::QuorumCertificateEmitted {
            observed_at,
            window_id,
            aggregate_artifact_id,
            validator_peer_ids,
        } => format!(
            ".quorumCertificateEmitted {} {} {} {}",
            render_lean_string(&observed_at.to_rfc3339())?,
            render_nat(window_id.0),
            render_lean_string(&aggregate_artifact_id.to_string())?,
            render_lean_string_list(
                &validator_peer_ids
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            )?,
        ),
        FormalProtocolEvent::CanonicalHeadPromoted {
            observed_at,
            window_id,
            base_head_id,
            head_id,
            aggregate_artifact_id,
            accepted_contributor_peer_ids,
        } => format!(
            ".canonicalHeadPromoted {} {} {} {} {} {}",
            render_lean_string(&observed_at.to_rfc3339())?,
            render_nat(window_id.0),
            render_lean_string(&base_head_id.to_string())?,
            render_lean_string(&head_id.to_string())?,
            render_lean_string(&aggregate_artifact_id.to_string())?,
            render_lean_string_list(
                &accepted_contributor_peer_ids
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            )?,
        ),
        FormalProtocolEvent::PeerRevoked {
            observed_at,
            peer_id,
            revocation_epoch,
        } => format!(
            ".peerRevoked {} {} {}",
            render_lean_string(&observed_at.to_rfc3339())?,
            render_lean_string(&peer_id.to_string())?,
            render_nat(revocation_epoch.0),
        ),
    })
}

fn render_protocol_action(action: &FormalProtocolAction) -> anyhow::Result<String> {
    Ok(match action {
        FormalProtocolAction::PublishUpdate {
            peer_id,
            window_id,
            base_head_id,
            artifact_id,
        } => format!(
            ".publishUpdate {} {} {} {}",
            render_lean_string(&peer_id.to_string())?,
            render_nat(window_id.0),
            render_lean_string(&base_head_id.to_string())?,
            render_lean_string(&artifact_id.to_string())?,
        ),
        FormalProtocolAction::PublishReducerProposal {
            peer_id,
            window_id,
            base_head_id,
            artifact_id,
        } => format!(
            ".publishReducerProposal {} {} {} {}",
            render_lean_string(&peer_id.to_string())?,
            render_nat(window_id.0),
            render_lean_string(&base_head_id.to_string())?,
            render_lean_string(&artifact_id.to_string())?,
        ),
        FormalProtocolAction::AcceptCandidate {
            validator_peer_id,
            candidate_peer_id,
            artifact_id,
        } => format!(
            ".acceptCandidate {} {} {}",
            render_lean_string(&validator_peer_id.to_string())?,
            render_lean_string(&candidate_peer_id.to_string())?,
            render_lean_string(&artifact_id.to_string())?,
        ),
        FormalProtocolAction::RejectCandidate {
            validator_peer_id,
            candidate_peer_id,
            artifact_id,
        } => format!(
            ".rejectCandidate {} {} {}",
            render_lean_string(&validator_peer_id.to_string())?,
            render_lean_string(&candidate_peer_id.to_string())?,
            render_lean_string(&artifact_id.to_string())?,
        ),
        FormalProtocolAction::AttestReduction {
            validator_peer_id,
            window_id,
            aggregate_artifact_id,
        } => format!(
            ".attestReduction {} {} {}",
            render_lean_string(&validator_peer_id.to_string())?,
            render_nat(window_id.0),
            render_lean_string(&aggregate_artifact_id.to_string())?,
        ),
        FormalProtocolAction::PromoteHead {
            window_id,
            base_head_id,
            head_id,
            aggregate_artifact_id,
        } => format!(
            ".promoteHead {} {} {} {}",
            render_nat(window_id.0),
            render_lean_string(&base_head_id.to_string())?,
            render_lean_string(&head_id.to_string())?,
            render_lean_string(&aggregate_artifact_id.to_string())?,
        ),
        FormalProtocolAction::RevokePeer {
            peer_id,
            revocation_epoch,
        } => format!(
            ".revokePeer {} {}",
            render_lean_string(&peer_id.to_string())?,
            render_nat(revocation_epoch.0),
        ),
    })
}

fn render_lean_string(value: &str) -> anyhow::Result<String> {
    serde_json::to_string(value).context("failed to encode lean string literal")
}

fn render_lean_string_list(values: &[String]) -> anyhow::Result<String> {
    let rendered = values
        .iter()
        .map(|value| render_lean_string(value))
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok(format!("[{}]", rendered.join(", ")))
}

fn render_nat(value: u64) -> String {
    value.to_string()
}

fn collect_validator_ids(trace: &FormalProtocolTrace) -> Vec<String> {
    let mut validator_ids = std::collections::BTreeSet::new();
    for event in &trace.events {
        match event {
            FormalProtocolEvent::CandidateAccepted {
                validator_peer_id, ..
            }
            | FormalProtocolEvent::CandidateRejected {
                validator_peer_id, ..
            }
            | FormalProtocolEvent::ValidationAttested {
                validator_peer_id, ..
            } => {
                validator_ids.insert(validator_peer_id.to_string());
            }
            FormalProtocolEvent::QuorumCertificateEmitted {
                validator_peer_ids, ..
            } => {
                validator_ids.extend(validator_peer_ids.iter().map(ToString::to_string));
            }
            _ => {}
        }
    }
    validator_ids.into_iter().collect()
}

fn collect_validator_quorum_size(trace: &FormalProtocolTrace) -> usize {
    trace
        .events
        .iter()
        .filter_map(|event| match event {
            FormalProtocolEvent::QuorumCertificateEmitted {
                validator_peer_ids, ..
            } => Some(validator_peer_ids.len()),
            _ => None,
        })
        .max()
        .unwrap_or(1)
}

fn final_promotion_event(trace: &FormalProtocolTrace) -> Option<FinalPromotionSummary> {
    trace.events.iter().rev().find_map(|event| match event {
        FormalProtocolEvent::CanonicalHeadPromoted {
            window_id,
            base_head_id,
            head_id,
            aggregate_artifact_id,
            accepted_contributor_peer_ids,
            ..
        } => Some(FinalPromotionSummary {
            window_id: window_id.0,
            base_head_id: base_head_id.to_string(),
            head_id: head_id.to_string(),
            aggregate_artifact_id: aggregate_artifact_id.to_string(),
            accepted_contributor_peer_ids: accepted_contributor_peer_ids
                .iter()
                .map(ToString::to_string)
                .collect(),
        }),
        _ => None,
    })
}

fn reducer_prefix_trace(trace: &FormalProtocolTrace) -> Option<FormalProtocolTrace> {
    let reducer_index = trace
        .events
        .iter()
        .position(|event| matches!(event, FormalProtocolEvent::ReducerProposalObserved { .. }))?;
    Some(FormalProtocolTrace {
        schema_version: trace.schema_version.clone(),
        scenario: trace.scenario,
        generated_at: trace.generated_at,
        source: format!("{}::reducer-prefix", trace.source),
        scope: trace.scope.clone(),
        events: trace.events[..=reducer_index].to_vec(),
    })
}

fn collect_revoked_peers(trace: &FormalProtocolTrace) -> Vec<String> {
    let mut revoked = Vec::new();
    for event in &trace.events {
        if let FormalProtocolEvent::PeerRevoked { peer_id, .. } = event {
            let value = peer_id.to_string();
            if !revoked.contains(&value) {
                revoked.push(value);
            }
        }
    }
    revoked
}

fn resolve_lake_bin() -> Option<String> {
    if let Ok(lake) = env::var("LAKE") {
        if !lake.trim().is_empty() {
            return Some(lake);
        }
    }
    if command_available("lake") {
        return Some("lake".into());
    }
    let home = env::var("HOME").ok()?;
    let elan_lake = std::path::Path::new(&home).join(".elan/bin/lake");
    if elan_lake.exists() {
        return Some(elan_lake.display().to_string());
    }
    None
}
