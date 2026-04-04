fn ensure_synthetic_dataset(root: &Path) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(root)?;
    let project = SyntheticBootstrapProject {
        dataset_root: root.to_path_buf(),
        learning_rate: 1.0,
        target_model: 1.0,
    };
    let registration = project.dataset_registration()?;
    let plan = project.microshard_plan(&registration)?;
    let manifest = root.join("fetch-manifest.json");
    if !manifest.exists() {
        let manifest_value = ShardFetchManifest::from_microshards(
            &plan.dataset_view,
            &plan.microshards,
            |ordinal| match ordinal {
                0 => b"3.5".to_vec(),
                _ => b"6.5".to_vec(),
            },
        );
        std::fs::write(&manifest, serde_json::to_vec_pretty(&manifest_value)?)?;
        for entry in &manifest_value.entries {
            let bytes = match entry.ordinal {
                0 => b"3.5".to_vec(),
                _ => b"6.5".to_vec(),
            };
            std::fs::write(root.join(&entry.locator), bytes)?;
        }
    }
    Ok(())
}
