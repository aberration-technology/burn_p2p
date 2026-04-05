use crate::{
    args::Args,
    core::run_core_demo,
    correctness::{export::MnistRunExport, report::build_run_export},
};

pub fn run_demo(args: &Args) -> anyhow::Result<MnistRunExport> {
    let run = run_core_demo(args)?;
    build_run_export(&run)
}
