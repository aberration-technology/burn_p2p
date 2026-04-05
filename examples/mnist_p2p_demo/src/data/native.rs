use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use burn::data::{
    dataloader::batcher::Batcher,
    dataloader::{DataLoader, DataLoaderBuilder},
    dataset::{
        Dataset, InMemDataset,
        vision::{MnistDataset, MnistItem},
    },
};
use burn::tensor::backend::Backend;
use burn_p2p::{
    AssignmentLease, DatasetRegistration, DatasetSizing, MicroShardId, MicroShardPlan,
    burn::{BurnShardedDataset, BurnShardedDatasetConfig},
};

use super::{EVAL_RECORDS_FILE, MnistBatch, MnistBatcher, MnistDatasetConfig, MnistRecord};
use crate::model::IMAGE_DIM;

impl<B: Backend> Batcher<B, MnistRecord, MnistBatch<B>> for MnistBatcher {
    fn batch(&self, items: Vec<MnistRecord>, device: &B::Device) -> MnistBatch<B> {
        self.batch_records(items, device)
    }
}

#[derive(Clone, Debug)]
pub struct PreparedMnistData {
    pub dataset_root: PathBuf,
    pub train_dataset: BurnShardedDataset<MnistRecord>,
    pub registration: DatasetRegistration,
    pub microshard_plan: MicroShardPlan,
    pub train_records: Vec<MnistRecord>,
    pub eval_records: Vec<MnistRecord>,
    pub batch_size: usize,
    pub shard_examples: BTreeMap<MicroShardId, usize>,
}

impl PreparedMnistData {
    pub fn build_train_loader<B: Backend>(
        &self,
        device: &B::Device,
    ) -> Arc<dyn DataLoader<B, MnistBatch<B>>> {
        DataLoaderBuilder::new(MnistBatcher)
            .batch_size(self.batch_size)
            .build(InMemDataset::new(self.train_records.clone()))
            .to_device(device)
    }

    pub fn build_eval_loader<B: Backend>(
        &self,
        device: &B::Device,
    ) -> Arc<dyn DataLoader<B, MnistBatch<B>>> {
        DataLoaderBuilder::new(MnistBatcher)
            .batch_size(self.batch_size)
            .build(InMemDataset::new(self.eval_records.clone()))
            .to_device(device)
    }

    pub fn examples_for_lease(&self, lease: &AssignmentLease) -> usize {
        lease
            .microshards
            .iter()
            .fold(0_usize, |sum, microshard_id| {
                sum + self
                    .shard_examples
                    .get(microshard_id)
                    .copied()
                    .unwrap_or_default()
            })
    }

    pub fn eval_records_path(&self) -> PathBuf {
        self.dataset_root.join(EVAL_RECORDS_FILE)
    }
}

pub fn prepare_mnist_dataset(
    root: &Path,
    config: &MnistDatasetConfig,
) -> anyhow::Result<PreparedMnistData> {
    fs::create_dir_all(root)?;

    let train_records =
        collect_balanced_subset(&MnistDataset::train(), config.train_examples_per_digit)?;
    let eval_records =
        collect_balanced_subset(&MnistDataset::test(), config.eval_examples_per_digit)?;
    fs::write(
        root.join(EVAL_RECORDS_FILE),
        serde_json::to_vec_pretty(&eval_records)?,
    )?;
    let sizing = dataset_sizing(&train_records);
    let train_dataset = BurnShardedDataset::write_local(
        root,
        &train_records,
        BurnShardedDatasetConfig::new("mnist-demo-train")
            .with_microshards(config.microshard_count)
            .with_sizing(sizing)
            .with_format("mnist-record-shards")
            .with_manifest_metadata_entry("name", "MNIST downstream demo")
            .with_manifest_metadata_entry("train_examples", train_records.len().to_string())
            .with_manifest_metadata_entry("eval_examples", eval_records.len().to_string())
            .with_view_metadata_entry("shape", "1x28x28")
            .with_view_metadata_entry("normalized", "true"),
    )?;

    Ok(PreparedMnistData {
        dataset_root: root.to_path_buf(),
        registration: train_dataset.registration().clone(),
        microshard_plan: train_dataset.microshard_plan().clone(),
        shard_examples: train_dataset.shard_examples().clone(),
        train_dataset,
        train_records,
        eval_records,
        batch_size: config.batch_size,
    })
}

fn collect_balanced_subset<D>(dataset: &D, per_digit: usize) -> anyhow::Result<Vec<MnistRecord>>
where
    D: Dataset<MnistItem>,
{
    let mut counts = [0_usize; 10];
    let mut items = Vec::with_capacity(per_digit * 10);
    for index in 0..dataset.len() {
        let Some(item) = dataset.get(index) else {
            continue;
        };
        let digit = item.label as usize;
        if digit >= 10 || counts[digit] >= per_digit {
            continue;
        }
        counts[digit] += 1;
        items.push(mnist_record(item));
        if counts.iter().all(|count| *count >= per_digit) {
            break;
        }
    }

    if counts.iter().all(|count| *count >= per_digit) {
        return Ok(items);
    }

    anyhow::bail!(
        "could not collect balanced mnist subset: {:?} < {} per digit",
        counts,
        per_digit
    )
}

fn mnist_record(item: MnistItem) -> MnistRecord {
    let mut pixels = Vec::with_capacity(IMAGE_DIM);
    for row in item.image {
        for pixel in row {
            pixels.push(pixel / 255.0);
        }
    }
    MnistRecord {
        label: item.label,
        pixels,
    }
}

fn dataset_sizing(records: &[MnistRecord]) -> DatasetSizing {
    DatasetSizing {
        total_examples: records.len() as u64,
        total_tokens: (records.len() * IMAGE_DIM) as u64,
        total_bytes: (records.len() * (IMAGE_DIM * std::mem::size_of::<f32>() + 1)) as u64,
    }
}
