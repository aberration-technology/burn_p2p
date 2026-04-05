use std::fmt;

use clap::ValueEnum;
use serde::Serialize;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum Profile {
    Dev,
    Smoke,
    CiPr,
    CiIntegration,
    Nightly,
    RealBrowser,
}

impl fmt::Display for Profile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

impl Profile {
    pub fn label(self) -> &'static str {
        match self {
            Self::Dev => "dev",
            Self::Smoke => "smoke",
            Self::CiPr => "ci-pr",
            Self::CiIntegration => "ci-integration",
            Self::Nightly => "nightly",
            Self::RealBrowser => "real-browser",
        }
    }

    pub fn settings(self) -> ProfileSettings {
        match self {
            Self::Dev => ProfileSettings {
                trainer_windows: 1,
                chaos_events: 2,
                multiprocess_peers: 4,
                headless: true,
                browser_iterations: 3,
                browser_shard_count: 2,
                keep_artifacts_on_success: false,
            },
            Self::Smoke => ProfileSettings {
                trainer_windows: 2,
                chaos_events: 4,
                multiprocess_peers: 6,
                headless: true,
                browser_iterations: 4,
                browser_shard_count: 3,
                keep_artifacts_on_success: false,
            },
            Self::CiPr => ProfileSettings {
                trainer_windows: 2,
                chaos_events: 4,
                multiprocess_peers: 6,
                headless: true,
                browser_iterations: 4,
                browser_shard_count: 3,
                keep_artifacts_on_success: true,
            },
            Self::CiIntegration => ProfileSettings {
                trainer_windows: 3,
                chaos_events: 6,
                multiprocess_peers: 8,
                headless: true,
                browser_iterations: 5,
                browser_shard_count: 4,
                keep_artifacts_on_success: true,
            },
            Self::Nightly => ProfileSettings {
                trainer_windows: 5,
                chaos_events: 12,
                multiprocess_peers: 16,
                headless: true,
                browser_iterations: 6,
                browser_shard_count: 6,
                keep_artifacts_on_success: true,
            },
            Self::RealBrowser => ProfileSettings {
                trainer_windows: 2,
                chaos_events: 4,
                multiprocess_peers: 4,
                headless: false,
                browser_iterations: 5,
                browser_shard_count: 4,
                keep_artifacts_on_success: true,
            },
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ProfileSettings {
    pub trainer_windows: u32,
    pub chaos_events: u32,
    pub multiprocess_peers: u32,
    pub headless: bool,
    pub browser_iterations: u32,
    pub browser_shard_count: u32,
    pub keep_artifacts_on_success: bool,
}

#[cfg(test)]
mod tests {
    use super::Profile;

    #[test]
    fn ci_profiles_keep_artifacts() {
        assert!(Profile::CiPr.settings().keep_artifacts_on_success);
        assert!(Profile::Nightly.settings().keep_artifacts_on_success);
    }

    #[test]
    fn real_browser_profile_is_headed_by_default() {
        assert!(!Profile::RealBrowser.settings().headless);
    }
}
