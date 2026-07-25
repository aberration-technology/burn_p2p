//! Backend-generic canonical tensor identity for Burn modules.
#![forbid(unsafe_code)]

use std::{
    collections::BTreeMap,
    io::{self, Write},
};

use burn::{module::Module, prelude::Backend};
use burn_p2p_core::{ContentId, codec::multihash_from_sha256_digest};
use burn_store::{Collector, TensorSnapshot};
use serde::{
    Serialize,
    ser::{SerializeSeq, SerializeStruct},
};
use sha2::{Digest, Sha256};

/// Errors returned while deriving canonical model tensor identity.
#[derive(Debug, thiserror::Error)]
pub enum TensorIdentityError {
    /// Canonical schema serialization failed.
    #[error("schema error: {0}")]
    Schema(#[from] burn_p2p_core::SchemaError),
}

/// Computes the canonical digest of a module's float tensor layout and values.
///
/// The representation is identical to the canonical
/// [`burn_p2p_core::FlattenedTensorPack`] checksum, but values are serialized
/// one parameter tensor at a time. Peak host memory is therefore bounded by the
/// largest individual parameter tensor instead of a second model-sized buffer.
pub fn module_tensor_digest<B, M>(
    module: &M,
    model_schema_hash: ContentId,
) -> Result<ContentId, TensorIdentityError>
where
    B: Backend,
    M: Module<B>,
{
    let snapshots = collect_float_snapshots::<B, M>(module);
    let layout_hash = float_parameter_layout_hash(&snapshots)?;
    let wire = StreamingFlattenedTensorPack {
        model_schema_hash: &model_schema_hash,
        layout_hash: &layout_hash,
        values: StreamingFloatValues {
            snapshots: &snapshots,
        },
    };
    let mut writer = Sha256Writer::default();
    ciborium::ser::into_writer(&wire, &mut writer).map_err(burn_p2p_core::SchemaError::Encode)?;
    Ok(ContentId::from_multihash(multihash_from_sha256_digest(
        writer.finalize(),
    )))
}

fn collect_float_snapshots<B, M>(module: &M) -> BTreeMap<String, TensorSnapshot>
where
    B: Backend,
    M: Module<B>,
{
    let mut collector = Collector::default();
    module.visit(&mut collector);

    let mut snapshots = BTreeMap::new();
    for snapshot in collector.into_tensors() {
        if snapshot.dtype.is_float() {
            snapshots.insert(snapshot.full_path(), snapshot);
        }
    }
    snapshots
}

fn float_parameter_layout_hash(
    snapshots: &BTreeMap<String, TensorSnapshot>,
) -> Result<ContentId, TensorIdentityError> {
    let layout = snapshots
        .iter()
        .map(|(path, snapshot)| (path.as_str(), snapshot.shape.as_slice()))
        .collect::<Vec<_>>();
    Ok(ContentId::derive(&layout)?)
}

struct StreamingFlattenedTensorPack<'a> {
    model_schema_hash: &'a ContentId,
    layout_hash: &'a ContentId,
    values: StreamingFloatValues<'a>,
}

impl Serialize for StreamingFlattenedTensorPack<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("FlattenedTensorPack", 3)?;
        state.serialize_field("model_schema_hash", self.model_schema_hash)?;
        state.serialize_field("layout_hash", self.layout_hash)?;
        state.serialize_field("values", &self.values)?;
        state.end()
    }
}

struct StreamingFloatValues<'a> {
    snapshots: &'a BTreeMap<String, TensorSnapshot>,
}

impl Serialize for StreamingFloatValues<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let value_count = self
            .snapshots
            .values()
            .map(|snapshot| snapshot.shape.iter().product::<usize>())
            .sum();
        let mut sequence = serializer.serialize_seq(Some(value_count))?;
        for snapshot in self.snapshots.values() {
            let data = snapshot.to_data().map_err(serde::ser::Error::custom)?;
            for value in data.iter::<f64>() {
                sequence.serialize_element(&(value as f32))?;
            }
        }
        sequence.end()
    }
}

#[derive(Default)]
struct Sha256Writer(Sha256);

impl Sha256Writer {
    fn finalize(self) -> impl AsRef<[u8]> {
        self.0.finalize()
    }
}

impl Write for Sha256Writer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use burn::{
        backend::NdArray,
        module::{Module, ModuleMapper, Param},
        nn::{Linear, LinearConfig},
        tensor::{Device, Tensor, backend::Backend},
    };
    use burn_p2p_core::{ContentId, FlattenedTensorPack};
    use burn_store::Collector;

    use super::module_tensor_digest;

    type TestBackend = NdArray<f32>;

    #[derive(Module, Debug)]
    struct TinyModel<B: Backend> {
        linear: Linear<B>,
    }

    impl<B: Backend> TinyModel<B> {
        fn new(device: &B::Device) -> Self {
            Self {
                linear: LinearConfig::new(4, 2).init(device),
            }
        }
    }

    struct FillMapper(f32);

    impl<B: Backend> ModuleMapper<B> for FillMapper {
        fn map_float<const D: usize>(&mut self, param: Param<Tensor<B, D>>) -> Param<Tensor<B, D>> {
            param.map(|tensor| tensor.zeros_like() + self.0)
        }
    }

    fn flattened_pack(
        model: &TinyModel<TestBackend>,
        model_schema_hash: ContentId,
    ) -> FlattenedTensorPack {
        let mut collector = Collector::default();
        model.visit(&mut collector);
        let mut snapshots = collector
            .into_tensors()
            .into_iter()
            .filter(|snapshot| snapshot.dtype.is_float())
            .map(|snapshot| (snapshot.full_path(), snapshot))
            .collect::<Vec<_>>();
        snapshots.sort_by(|left, right| left.0.cmp(&right.0));
        let layout_hash = ContentId::derive(
            &snapshots
                .iter()
                .map(|(path, snapshot)| (path.as_str(), snapshot.shape.as_slice()))
                .collect::<Vec<_>>(),
        )
        .expect("layout hash");
        let values = snapshots
            .iter()
            .flat_map(|(_, snapshot)| {
                snapshot
                    .to_data()
                    .expect("tensor data")
                    .iter::<f64>()
                    .map(|value| value as f32)
                    .collect::<Vec<_>>()
            })
            .collect();
        FlattenedTensorPack::new(model_schema_hash, layout_hash, values)
    }

    #[test]
    fn streaming_digest_matches_flattened_pack_and_tracks_values() {
        let device = Device::<TestBackend>::default();
        let schema = ContentId::new("semantic-schema");
        let mut first_mapper = FillMapper(0.25);
        let first = TinyModel::<TestBackend>::new(&device).map(&mut first_mapper);
        let mut second_mapper = FillMapper(0.5);
        let second = TinyModel::<TestBackend>::new(&device).map(&mut second_mapper);
        let expected = flattened_pack(&first, schema.clone())
            .checksum()
            .expect("flattened checksum");

        assert_eq!(
            module_tensor_digest::<TestBackend, _>(&first, schema.clone())
                .expect("streaming digest"),
            expected,
        );
        assert_ne!(
            module_tensor_digest::<TestBackend, _>(&second, schema).expect("changed digest"),
            expected,
        );
    }
}
