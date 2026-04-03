use burn::{
    backend::NdArray,
    module::{Module, ModuleMapper, Param},
    nn::{Linear, LinearConfig},
    tensor::{Tensor, backend::Backend},
};
use burn_p2p_engine::{BurnMergeCandidate, apply_root_ema_modules, merge_weighted_mean_modules};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};

#[derive(Module, Debug)]
struct BenchModel<B: Backend> {
    encoder: Linear<B>,
    decoder: Linear<B>,
}

impl<B: Backend> BenchModel<B> {
    fn new(device: &B::Device) -> Self {
        Self {
            encoder: LinearConfig::new(256, 256).init(device),
            decoder: LinearConfig::new(256, 256).init(device),
        }
    }
}

type TestBackend = NdArray<f32>;

#[derive(Debug)]
struct FillMapper {
    value: f32,
}

impl<B: Backend> ModuleMapper<B> for FillMapper {
    fn map_float<const D: usize>(&mut self, param: Param<Tensor<B, D>>) -> Param<Tensor<B, D>> {
        param.map(|tensor| tensor.zeros_like() + self.value)
    }
}

fn fill_model(model: BenchModel<TestBackend>, value: f32) -> BenchModel<TestBackend> {
    let mut mapper = FillMapper { value };
    model.map(&mut mapper)
}

fn bench_weighted_merge(c: &mut Criterion) {
    let device = <TestBackend as Backend>::Device::default();
    let base = fill_model(BenchModel::<TestBackend>::new(&device), 0.0);
    let total_params = 2_u64 * ((256_u64 * 256_u64) + 256_u64);
    let mut group = c.benchmark_group("engine_weighted_merge");

    for candidate_count in [2_usize, 8, 16] {
        let models = (0..candidate_count)
            .map(|index| {
                fill_model(
                    BenchModel::<TestBackend>::new(&device),
                    1.0 + (index as f32),
                )
            })
            .collect::<Vec<_>>();
        group.throughput(Throughput::Elements(total_params * candidate_count as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{candidate_count}_candidates")),
            &candidate_count,
            |b, _| {
                b.iter(|| {
                    let candidates = models
                        .iter()
                        .enumerate()
                        .map(|(index, model)| BurnMergeCandidate {
                            module: model,
                            weight: 1.0 + (index as f64),
                        })
                        .collect::<Vec<_>>();
                    merge_weighted_mean_modules::<TestBackend, _>(
                        black_box(&base),
                        black_box(candidates.as_slice()),
                    )
                    .expect("merge")
                    .expect("merged model");
                });
            },
        );
    }

    group.finish();
}

fn bench_root_ema(c: &mut Criterion) {
    let device = <TestBackend as Backend>::Device::default();
    let base = fill_model(BenchModel::<TestBackend>::new(&device), 2.0);
    let merged = fill_model(BenchModel::<TestBackend>::new(&device), 6.0);
    let total_params = 2_u64 * ((256_u64 * 256_u64) + 256_u64);

    let mut group = c.benchmark_group("engine_root_ema");
    group.throughput(Throughput::Elements(total_params));
    group.bench_function("single_root_ema", |b| {
        b.iter(|| {
            apply_root_ema_modules::<TestBackend, _>(
                black_box(&base),
                black_box(&merged),
                black_box(0.25),
            )
            .expect("root ema");
        });
    });
    group.finish();
}

criterion_group!(benches, bench_weighted_merge, bench_root_ema);
criterion_main!(benches);
