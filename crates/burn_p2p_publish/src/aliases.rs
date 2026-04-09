use std::collections::{BTreeMap, BTreeSet};

use burn_p2p_checkpoint::FsArtifactStore;
use burn_p2p_core::{
    ArtifactAlias, ArtifactAliasId, ArtifactAliasScope, ArtifactAliasSourceReason,
    ArtifactLiveEvent, ArtifactLiveEventKind, ArtifactProfile, ContentId, EvalProtocolManifest,
    ExperimentId, ExportJob, HeadDescriptor, HeadEvalReport, HeadEvalStatus, HeadId,
    PublicationMode, PublishedArtifactStatus, RevisionId, RunId,
};
use chrono::{DateTime, Utc};

use crate::{
    ArtifactAliasStatus, ArtifactRunSummary, ArtifactRunView, ExportRequest, HeadArtifactView,
    PublicationStore, PublishError, backends::should_eager_publish,
};

impl PublicationStore {
    /// Returns the current alias statuses ordered by experiment, scope, and alias name.
    pub fn alias_statuses(&self) -> Vec<ArtifactAliasStatus> {
        let latest_jobs_by_alias = self
            .state
            .export_jobs
            .iter()
            .filter_map(|job| {
                self.state
                    .aliases
                    .iter()
                    .find(|alias| {
                        alias.head_id == job.head_id
                            && alias.artifact_profile == job.artifact_profile
                    })
                    .map(|alias| (alias.artifact_alias_id.clone(), job.clone()))
            })
            .fold(
                BTreeMap::<ArtifactAliasId, ExportJob>::new(),
                |mut acc, (alias_id, job)| {
                    acc.entry(alias_id)
                        .and_modify(|existing| {
                            if job.queued_at > existing.queued_at {
                                *existing = job.clone();
                            }
                        })
                        .or_insert(job);
                    acc
                },
            );
        let mut rows = self
            .state
            .aliases
            .iter()
            .cloned()
            .map(|alias| {
                let alias_id = alias.artifact_alias_id.clone();
                let alias_history = self.alias_history_for_alias(&alias_id);
                let previous_head_id = alias_history
                    .iter()
                    .rev()
                    .find(|entry| entry.head_id != alias.head_id)
                    .map(|entry| entry.head_id.clone());
                ArtifactAliasStatus {
                    alias,
                    published_artifact: self
                        .state
                        .published_artifacts
                        .iter()
                        .filter(|record| {
                            record.artifact_alias_id.as_ref() == Some(&alias_id)
                                && record.status == PublishedArtifactStatus::Ready
                        })
                        .max_by_key(|record| record.created_at)
                        .cloned(),
                    latest_job: latest_jobs_by_alias.get(&alias_id).cloned(),
                    history_count: alias_history.len(),
                    previous_head_id,
                }
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            left.alias
                .experiment_id
                .cmp(&right.alias.experiment_id)
                .then_with(|| left.alias.scope.cmp(&right.alias.scope))
                .then_with(|| left.alias.alias_name.cmp(&right.alias.alias_name))
                .then_with(|| left.alias.head_id.cmp(&right.alias.head_id))
        });
        rows
    }

    /// Returns alias statuses filtered to one experiment.
    pub fn alias_statuses_for_experiment(
        &self,
        experiment_id: &ExperimentId,
    ) -> Vec<ArtifactAliasStatus> {
        self.alias_statuses()
            .into_iter()
            .filter(|row| &row.alias.experiment_id == experiment_id)
            .collect()
    }

    /// Returns run summaries for one experiment.
    pub fn run_summaries(
        &self,
        heads: &[HeadDescriptor],
        experiment_id: &ExperimentId,
    ) -> Result<Vec<ArtifactRunSummary>, PublishError> {
        let mut by_run = BTreeMap::<RunId, Vec<HeadDescriptor>>::new();
        for head in heads
            .iter()
            .filter(|head| &head.experiment_id == experiment_id)
        {
            by_run
                .entry(run_id_for_head(head)?)
                .or_default()
                .push(head.clone());
        }
        Ok(by_run
            .into_iter()
            .filter_map(|(run_id, run_heads)| {
                let latest_head_id = latest_head(&run_heads)?.head_id;
                let aliases = self.alias_statuses_for_run(heads, experiment_id, &run_id);
                let alias_history = self.alias_history_for_run(heads, experiment_id, &run_id);
                let published_artifact_count = self
                    .state
                    .published_artifacts
                    .iter()
                    .filter(|record| {
                        &record.experiment_id == experiment_id
                            && record.run_id.as_ref() == Some(&run_id)
                    })
                    .count();
                Some(ArtifactRunSummary {
                    experiment_id: experiment_id.clone(),
                    run_id,
                    latest_head_id,
                    alias_count: aliases.len(),
                    alias_history_count: alias_history.len(),
                    published_artifact_count,
                })
            })
            .collect())
    }

    /// Returns one run-scoped publication view.
    pub fn run_view(
        &self,
        heads: &[HeadDescriptor],
        reports: &[HeadEvalReport],
        experiment_id: &ExperimentId,
        run_id: &RunId,
    ) -> Result<ArtifactRunView, PublishError> {
        let run_heads = heads
            .iter()
            .filter(|head| &head.experiment_id == experiment_id)
            .filter_map(|head| {
                run_id_for_head(head)
                    .ok()
                    .filter(|candidate| candidate == run_id)
                    .map(|_| head.clone())
            })
            .collect::<Vec<_>>();
        Ok(ArtifactRunView {
            experiment_id: experiment_id.clone(),
            run_id: run_id.clone(),
            alias_history: self.alias_history_for_run(heads, experiment_id, run_id),
            aliases: self.alias_statuses_for_run(heads, experiment_id, run_id),
            eval_reports: reports
                .iter()
                .filter(|report| {
                    report.experiment_id == *experiment_id
                        && run_heads.iter().any(|head| head.head_id == report.head_id)
                })
                .cloned()
                .collect(),
            published_artifacts: self
                .state
                .published_artifacts
                .iter()
                .filter(|record| {
                    &record.experiment_id == experiment_id && record.run_id.as_ref() == Some(run_id)
                })
                .cloned()
                .collect(),
            heads: run_heads,
        })
    }

    /// Returns detail for one head together with its aliases and publications.
    pub fn head_view(
        &self,
        heads: &[HeadDescriptor],
        reports: &[HeadEvalReport],
        head_id: &HeadId,
    ) -> Result<HeadArtifactView, PublishError> {
        let head = heads
            .iter()
            .find(|head| &head.head_id == head_id)
            .cloned()
            .ok_or_else(|| PublishError::UnknownHead(head_id.clone()))?;
        let run_id = run_id_for_head(&head)?;
        let alias_history = self
            .state
            .alias_history
            .iter()
            .filter(|alias| {
                alias.experiment_id == head.experiment_id
                    && (alias.run_id.as_ref() == Some(&run_id) || alias.run_id.is_none())
            })
            .cloned()
            .collect::<Vec<_>>();
        let available_profiles = self
            .state
            .targets
            .iter()
            .flat_map(|target| target.allowed_artifact_profiles.iter().cloned())
            .collect::<BTreeSet<_>>();
        Ok(HeadArtifactView {
            eval_reports: reports
                .iter()
                .filter(|report| &report.head_id == head_id)
                .cloned()
                .collect(),
            aliases: self
                .state
                .aliases
                .iter()
                .filter(|alias| &alias.head_id == head_id)
                .cloned()
                .collect(),
            published_artifacts: self
                .state
                .published_artifacts
                .iter()
                .filter(|record| &record.head_id == head_id)
                .cloned()
                .collect(),
            head,
            run_id,
            available_profiles,
            alias_history,
            provider_peer_ids: Vec::new(),
        })
    }

    /// Synchronizes the built-in latest and best-validation aliases from canonical state.
    pub fn sync_aliases(
        &mut self,
        heads: &[HeadDescriptor],
        reports: &[HeadEvalReport],
        protocols: &[EvalProtocolManifest],
    ) -> Result<(), PublishError> {
        let mut next_aliases = Vec::<ArtifactAlias>::new();
        let grouped_heads = heads.iter().fold(
            BTreeMap::<(ExperimentId, RevisionId), Vec<HeadDescriptor>>::new(),
            |mut acc, head| {
                acc.entry((head.experiment_id.clone(), head.revision_id.clone()))
                    .or_default()
                    .push(head.clone());
                acc
            },
        );

        for ((experiment_id, _revision_id), revision_heads) in grouped_heads {
            let Some(latest_head) = latest_head(&revision_heads) else {
                continue;
            };
            let run_id = run_id_for_head(&latest_head)?;
            next_aliases.push(build_alias(AliasBuildSpec {
                experiment_id: &experiment_id,
                run_id: Some(&run_id),
                scope: ArtifactAliasScope::Run,
                alias_name: "latest/full",
                head_id: &latest_head.head_id,
                artifact_profile: ArtifactProfile::FullTrainingCheckpoint,
                resolved_at: latest_head.created_at,
                source_reason: ArtifactAliasSourceReason::LatestCanonicalHead,
            })?);
            next_aliases.push(build_alias(AliasBuildSpec {
                experiment_id: &experiment_id,
                run_id: Some(&run_id),
                scope: ArtifactAliasScope::Run,
                alias_name: "latest/serve",
                head_id: &latest_head.head_id,
                artifact_profile: ArtifactProfile::ServeCheckpoint,
                resolved_at: latest_head.created_at,
                source_reason: ArtifactAliasSourceReason::LatestCanonicalHead,
            })?);

            for protocol in protocols {
                if let Some(best_head) =
                    best_eval_head_for_protocol(&revision_heads, reports, protocol)?
                {
                    let alias_name =
                        format!("best_val/{}/full", protocol.eval_protocol_id.as_str());
                    next_aliases.push(build_alias(AliasBuildSpec {
                        experiment_id: &experiment_id,
                        run_id: Some(&run_id),
                        scope: ArtifactAliasScope::Run,
                        alias_name: &alias_name,
                        head_id: &best_head.head_id,
                        artifact_profile: ArtifactProfile::FullTrainingCheckpoint,
                        resolved_at: best_head.finished_at,
                        source_reason: ArtifactAliasSourceReason::BestValidation {
                            eval_protocol_id: protocol.eval_protocol_id.clone(),
                        },
                    })?);
                    let alias_name =
                        format!("best_val/{}/serve", protocol.eval_protocol_id.as_str());
                    next_aliases.push(build_alias(AliasBuildSpec {
                        experiment_id: &experiment_id,
                        run_id: Some(&run_id),
                        scope: ArtifactAliasScope::Run,
                        alias_name: &alias_name,
                        head_id: &best_head.head_id,
                        artifact_profile: ArtifactProfile::ServeCheckpoint,
                        resolved_at: best_head.finished_at,
                        source_reason: ArtifactAliasSourceReason::BestValidation {
                            eval_protocol_id: protocol.eval_protocol_id.clone(),
                        },
                    })?);
                }
            }
        }

        let latest_by_experiment = heads.iter().fold(
            BTreeMap::<ExperimentId, Vec<HeadDescriptor>>::new(),
            |mut acc, head| {
                acc.entry(head.experiment_id.clone())
                    .or_default()
                    .push(head.clone());
                acc
            },
        );
        for (experiment_id, experiment_heads) in latest_by_experiment {
            let Some(latest_head) = latest_head(&experiment_heads) else {
                continue;
            };
            next_aliases.push(build_alias(AliasBuildSpec {
                experiment_id: &experiment_id,
                run_id: None,
                scope: ArtifactAliasScope::Experiment,
                alias_name: "current/latest/full",
                head_id: &latest_head.head_id,
                artifact_profile: ArtifactProfile::FullTrainingCheckpoint,
                resolved_at: latest_head.created_at,
                source_reason: ArtifactAliasSourceReason::LatestCanonicalHead,
            })?);
            next_aliases.push(build_alias(AliasBuildSpec {
                experiment_id: &experiment_id,
                run_id: None,
                scope: ArtifactAliasScope::Experiment,
                alias_name: "current/latest/serve",
                head_id: &latest_head.head_id,
                artifact_profile: ArtifactProfile::ServeCheckpoint,
                resolved_at: latest_head.created_at,
                source_reason: ArtifactAliasSourceReason::LatestCanonicalHead,
            })?);
            let current_run_id = run_id_for_head(&latest_head)?;
            let current_run_heads = experiment_heads
                .iter()
                .filter(|head| run_id_for_head(head).ok().as_ref() == Some(&current_run_id))
                .cloned()
                .collect::<Vec<_>>();
            for protocol in protocols {
                if let Some(best_head) =
                    best_eval_head_for_protocol(&current_run_heads, reports, protocol)?
                {
                    let alias_name = format!(
                        "current/best_val/{}/full",
                        protocol.eval_protocol_id.as_str()
                    );
                    next_aliases.push(build_alias(AliasBuildSpec {
                        experiment_id: &experiment_id,
                        run_id: None,
                        scope: ArtifactAliasScope::Experiment,
                        alias_name: &alias_name,
                        head_id: &best_head.head_id,
                        artifact_profile: ArtifactProfile::FullTrainingCheckpoint,
                        resolved_at: best_head.finished_at,
                        source_reason: ArtifactAliasSourceReason::BestValidation {
                            eval_protocol_id: protocol.eval_protocol_id.clone(),
                        },
                    })?);
                    let alias_name = format!(
                        "current/best_val/{}/serve",
                        protocol.eval_protocol_id.as_str()
                    );
                    next_aliases.push(build_alias(AliasBuildSpec {
                        experiment_id: &experiment_id,
                        run_id: None,
                        scope: ArtifactAliasScope::Experiment,
                        alias_name: &alias_name,
                        head_id: &best_head.head_id,
                        artifact_profile: ArtifactProfile::ServeCheckpoint,
                        resolved_at: best_head.finished_at,
                        source_reason: ArtifactAliasSourceReason::BestValidation {
                            eval_protocol_id: protocol.eval_protocol_id.clone(),
                        },
                    })?);
                }
            }
        }

        next_aliases.sort_by(|left, right| {
            left.experiment_id
                .cmp(&right.experiment_id)
                .then_with(|| left.scope.cmp(&right.scope))
                .then_with(|| left.alias_name.cmp(&right.alias_name))
                .then_with(|| left.head_id.cmp(&right.head_id))
                .then_with(|| left.artifact_profile.cmp(&right.artifact_profile))
        });
        next_aliases.dedup_by(|left, right| left.artifact_alias_id == right.artifact_alias_id);

        let previous = self
            .state
            .aliases
            .iter()
            .map(|alias| (alias.artifact_alias_id.clone(), alias.clone()))
            .collect::<BTreeMap<_, _>>();
        for alias in &next_aliases {
            if previous
                .get(&alias.artifact_alias_id)
                .is_none_or(|existing| existing != alias)
            {
                self.state.alias_history.push(alias.clone());
                self.push_live_event(ArtifactLiveEvent {
                    event_id: ContentId::derive(&(
                        "artifact-live",
                        "alias-updated",
                        alias.artifact_alias_id.as_str(),
                        alias.head_id.as_str(),
                        alias.resolved_at.timestamp_micros(),
                    ))?,
                    kind: ArtifactLiveEventKind::AliasUpdated,
                    experiment_id: Some(alias.experiment_id.clone()),
                    run_id: alias.run_id.clone(),
                    head_id: Some(alias.head_id.clone()),
                    artifact_profile: Some(alias.artifact_profile.clone()),
                    publication_target_id: None,
                    alias_name: Some(alias.alias_name.clone()),
                    export_job_id: None,
                    published_artifact_id: None,
                    detail: Some(format!("{:?}", alias.source_reason)),
                    generated_at: Utc::now(),
                });
            }
        }
        self.state.aliases = next_aliases;
        self.persist_state()?;
        Ok(())
    }

    /// Synchronizes aliases and eagerly publishes aliases selected by the target policy.
    pub fn sync_aliases_and_eager_publications(
        &mut self,
        artifact_store: &FsArtifactStore,
        heads: &[HeadDescriptor],
        reports: &[HeadEvalReport],
        protocols: &[EvalProtocolManifest],
    ) -> Result<(), PublishError> {
        self.sync_aliases(heads, reports, protocols)?;
        let aliases = self.state.aliases.clone();
        let targets = self.state.targets.clone();
        for target in targets {
            if matches!(
                target.publication_mode,
                PublicationMode::Disabled | PublicationMode::LazyOnDemand
            ) {
                continue;
            }
            for alias in aliases
                .iter()
                .filter(|alias| should_eager_publish(alias, &target))
            {
                let request = ExportRequest {
                    requested_by_principal_id: None,
                    experiment_id: alias.experiment_id.clone(),
                    run_id: alias.run_id.clone(),
                    head_id: alias.head_id.clone(),
                    artifact_profile: alias.artifact_profile.clone(),
                    publication_target_id: target.publication_target_id.clone(),
                    artifact_alias_id: Some(alias.artifact_alias_id.clone()),
                };
                match self.request_export(artifact_store, heads, reports, request) {
                    Ok(_) => {}
                    Err(PublishError::DisallowedArtifactProfile { .. })
                    | Err(PublishError::TargetSizeLimitExceeded { .. })
                    | Err(PublishError::BrowserSnapshotUnsupported { .. })
                    | Err(PublishError::DisabledPublicationTarget(_)) => {}
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(())
    }

    pub(crate) fn alias_statuses_for_run(
        &self,
        heads: &[HeadDescriptor],
        experiment_id: &ExperimentId,
        run_id: &RunId,
    ) -> Vec<ArtifactAliasStatus> {
        let run_head_ids = heads
            .iter()
            .filter(|head| &head.experiment_id == experiment_id)
            .filter_map(|head| {
                run_id_for_head(head)
                    .ok()
                    .filter(|candidate| candidate == run_id)
                    .map(|_| head.head_id.clone())
            })
            .collect::<BTreeSet<_>>();
        self.alias_statuses()
            .into_iter()
            .filter(|row| {
                row.alias.experiment_id == *experiment_id
                    && (row.alias.run_id.as_ref() == Some(run_id)
                        || (row.alias.run_id.is_none()
                            && run_head_ids.contains(&row.alias.head_id)))
            })
            .collect()
    }

    pub(crate) fn alias_history_for_run(
        &self,
        heads: &[HeadDescriptor],
        experiment_id: &ExperimentId,
        run_id: &RunId,
    ) -> Vec<ArtifactAlias> {
        let run_head_ids = heads
            .iter()
            .filter(|head| &head.experiment_id == experiment_id)
            .filter_map(|head| {
                run_id_for_head(head)
                    .ok()
                    .filter(|candidate| candidate == run_id)
                    .map(|_| head.head_id.clone())
            })
            .collect::<BTreeSet<_>>();
        self.state
            .alias_history
            .iter()
            .filter(|alias| {
                alias.experiment_id == *experiment_id
                    && (alias.run_id.as_ref() == Some(run_id)
                        || (alias.run_id.is_none() && run_head_ids.contains(&alias.head_id)))
            })
            .cloned()
            .collect()
    }

    fn alias_history_for_alias(&self, alias_id: &ArtifactAliasId) -> Vec<ArtifactAlias> {
        self.state
            .alias_history
            .iter()
            .filter(|alias| &alias.artifact_alias_id == alias_id)
            .cloned()
            .collect()
    }
}

pub(crate) fn run_id_for_head(head: &HeadDescriptor) -> Result<RunId, burn_p2p_core::SchemaError> {
    RunId::derive(&(head.experiment_id.as_str(), head.revision_id.as_str()))
}

struct AliasBuildSpec<'a> {
    experiment_id: &'a ExperimentId,
    run_id: Option<&'a RunId>,
    scope: ArtifactAliasScope,
    alias_name: &'a str,
    head_id: &'a HeadId,
    artifact_profile: ArtifactProfile,
    resolved_at: DateTime<Utc>,
    source_reason: ArtifactAliasSourceReason,
}

fn build_alias(spec: AliasBuildSpec<'_>) -> Result<ArtifactAlias, burn_p2p_core::SchemaError> {
    let artifact_alias_id = ArtifactAliasId::derive(&(
        spec.experiment_id.as_str(),
        spec.run_id.map(RunId::as_str),
        format!("{:?}", spec.scope),
        spec.alias_name,
        format!("{:?}", spec.artifact_profile),
    ))?;
    Ok(ArtifactAlias {
        artifact_alias_id,
        experiment_id: spec.experiment_id.clone(),
        run_id: spec.run_id.cloned(),
        scope: spec.scope,
        alias_name: spec.alias_name.into(),
        head_id: spec.head_id.clone(),
        artifact_profile: spec.artifact_profile,
        resolved_at: spec.resolved_at,
        source_reason: spec.source_reason,
    })
}

fn latest_head(heads: &[HeadDescriptor]) -> Option<HeadDescriptor> {
    heads
        .iter()
        .max_by(|left, right| {
            left.global_step
                .cmp(&right.global_step)
                .then_with(|| left.created_at.cmp(&right.created_at))
                .then_with(|| left.head_id.cmp(&right.head_id))
        })
        .cloned()
}

fn best_eval_head_for_protocol(
    heads: &[HeadDescriptor],
    reports: &[HeadEvalReport],
    protocol: &EvalProtocolManifest,
) -> Result<Option<HeadEvalReport>, PublishError> {
    let primary = protocol
        .metric_defs
        .first()
        .ok_or(PublishError::MissingPrimaryEvalMetric)?;
    let head_ids = heads
        .iter()
        .map(|head| head.head_id.clone())
        .collect::<BTreeSet<_>>();
    let mut candidates = reports
        .iter()
        .filter(|report| {
            report.eval_protocol_id == protocol.eval_protocol_id
                && report.status == HeadEvalStatus::Completed
                && report.head_id.as_str() != ""
                && head_ids.contains(&report.head_id)
        })
        .filter_map(|report| {
            numeric_metric(report, &primary.metric_key).map(|value| (report, value))
        })
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Ok(None);
    }
    candidates.sort_by(|(left, left_value), (right, right_value)| {
        if primary.higher_is_better {
            left_value
                .partial_cmp(right_value)
                .unwrap_or(std::cmp::Ordering::Equal)
        } else {
            right_value
                .partial_cmp(left_value)
                .unwrap_or(std::cmp::Ordering::Equal)
        }
        .then_with(|| left.finished_at.cmp(&right.finished_at))
        .then_with(|| right.head_id.cmp(&left.head_id))
    });
    Ok(candidates.last().map(|(report, _)| (*report).clone()))
}

fn numeric_metric(report: &HeadEvalReport, metric_key: &str) -> Option<f64> {
    match report.metric_values.get(metric_key) {
        Some(burn_p2p_core::MetricValue::Integer(value)) => Some(*value as f64),
        Some(burn_p2p_core::MetricValue::Float(value)) => Some(*value),
        Some(burn_p2p_core::MetricValue::Bool(_))
        | Some(burn_p2p_core::MetricValue::Text(_))
        | None => None,
    }
}
