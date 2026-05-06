import argparse
import json
import os
from pathlib import Path

import torch
from safetensors.torch import load, load_file, save, save_file
from torchvision.datasets import MNIST


def _state_dict_cpu(model):
    return {name: tensor.detach().cpu() for name, tensor in model.state_dict().items()}


def _metric_map(loss: float, accuracy: float, examples: int):
    return {
        "loss": float(loss),
        "accuracy": float(accuracy),
        "examples": int(examples),
    }


class TorchMnistWorkload:
    def __init__(self, config):
        self.config = config
        self.device = self._resolve_device(config.get("preferred_device", "auto"))
        self.hidden_size = int(config.get("hidden_size", 64))
        self.learning_rate = float(config.get("learning_rate", 0.05))
        self.train_batch_size = int(config.get("train_batch_size", 32))
        self.eval_batch_size = int(config.get("eval_batch_size", 64))
        self.eval_limit = int(config.get("eval_limit", 256))
        self.mnist_root = Path(config["mnist_root"])
        self._eval_dataset = MNIST(self.mnist_root, train=False, download=True)

    def runtime_device(self):
        if self.device.type == "cuda":
            return "python-torch-cuda"
        if self.device.type == "mps":
            return "python-torch-mps"
        return "python-torch-cpu"

    def capability_probe(self):
        model = self._create_model().to(self.device)
        optimizer = self._make_optimizer(model)
        batch = torch.randn(128, 1, 28, 28, device=self.device)
        targets = torch.randint(0, 10, (128,), device=self.device)
        criterion = torch.nn.CrossEntropyLoss()

        if self.device.type == "cuda":
            torch.cuda.synchronize()
        start = torch.cuda.Event(enable_timing=True) if self.device.type == "cuda" else None
        end = torch.cuda.Event(enable_timing=True) if self.device.type == "cuda" else None

        import time

        t0 = time.perf_counter()
        if start is not None:
            start.record()
        for _ in range(30):
            optimizer.zero_grad(set_to_none=True)
            logits = model(batch)
            loss = criterion(logits, targets)
            loss.backward()
            optimizer.step()
        if end is not None:
            end.record()
            torch.cuda.synchronize()
        elapsed = time.perf_counter() - t0
        examples_per_second = (30 * batch.shape[0]) / max(elapsed, 1e-9)
        return {
            "preferred_backends": [self.runtime_device()],
            "work_units_per_second": float(examples_per_second),
            "target_window_seconds": 1,
        }

    def init_model(self, _device_name):
        model = self._create_model().to(self.device)
        return {
            "model": model,
            "optimizer": self._make_optimizer(model),
        }

    def train_window(self, state, batches):
        return self._train_steps(state, batches, None)

    def run_inner_loop(self, state, job):
        steps = int(job["num_inner_steps"])
        metrics = self._train_steps(state, job["batches"], steps)
        return {
            "steps_completed": int(metrics["train_steps"]),
            "metrics": metrics,
        }

    def _train_steps(self, state, batches, max_steps):
        model = state["model"]
        optimizer = state["optimizer"]
        criterion = torch.nn.CrossEntropyLoss()
        model.train()

        total_loss = 0.0
        total_correct = 0
        total_examples = 0
        total_steps = 0

        while max_steps is None or total_steps < max_steps:
            progressed = False
            for batch in batches:
                for shard_path in self._batch_shard_paths(batch):
                    shard = load_file(shard_path)
                    images = shard["images"].to(self.device, dtype=torch.float32) / 255.0
                    labels = shard["labels"].to(self.device, dtype=torch.long)
                    for start in range(0, labels.shape[0], self.train_batch_size):
                        if max_steps is not None and total_steps >= max_steps:
                            break
                        batch_images = images[start : start + self.train_batch_size]
                        batch_labels = labels[start : start + self.train_batch_size]
                        optimizer.zero_grad(set_to_none=True)
                        logits = model(batch_images)
                        loss = criterion(logits, batch_labels)
                        loss.backward()
                        optimizer.step()

                        progressed = True
                        total_steps += 1
                        batch_size = int(batch_labels.shape[0])
                        total_examples += batch_size
                        total_loss += float(loss.detach().cpu().item()) * batch_size
                        total_correct += int(
                            (logits.argmax(dim=1) == batch_labels).sum().detach().cpu().item()
                        )
                    if max_steps is not None and total_steps >= max_steps:
                        break
                if max_steps is not None and total_steps >= max_steps:
                    break
            if max_steps is None or total_steps >= max_steps:
                break
            if not progressed:
                raise ValueError("training batches did not produce any optimizer steps")

        average_loss = total_loss / max(total_examples, 1)
        accuracy = total_correct / max(total_examples, 1)
        metrics = _metric_map(average_loss, accuracy, total_examples)
        metrics["train_accuracy"] = float(accuracy)
        metrics["train_steps"] = int(total_steps)
        return metrics

    def evaluate(self, state, split):
        del split
        model = state["model"]
        criterion = torch.nn.CrossEntropyLoss()
        model.eval()

        images = self._eval_dataset.data[: self.eval_limit].to(self.device, dtype=torch.float32) / 255.0
        labels = self._eval_dataset.targets[: self.eval_limit].to(self.device, dtype=torch.long)

        total_loss = 0.0
        total_correct = 0
        total_examples = 0
        with torch.no_grad():
            for start in range(0, labels.shape[0], self.eval_batch_size):
                batch_images = images[start : start + self.eval_batch_size]
                batch_labels = labels[start : start + self.eval_batch_size]
                logits = model(batch_images)
                loss = criterion(logits, batch_labels)
                batch_size = int(batch_labels.shape[0])
                total_examples += batch_size
                total_loss += float(loss.detach().cpu().item()) * batch_size
                total_correct += int(
                    (logits.argmax(dim=1) == batch_labels).sum().detach().cpu().item()
                )

        average_loss = total_loss / max(total_examples, 1)
        accuracy = total_correct / max(total_examples, 1)
        metrics = _metric_map(average_loss, accuracy, total_examples)
        metrics["validation_accuracy"] = float(accuracy)
        return metrics

    def load_model_artifact(self, state, artifact_bytes):
        tensors = load(artifact_bytes)
        state["model"].load_state_dict(tensors)
        state["optimizer"] = self._make_optimizer(state["model"])
        return state

    def load_model_artifact_path(self, state, artifact_path):
        state["model"].load_state_dict(load_file(artifact_path))
        state["optimizer"] = self._make_optimizer(state["model"])
        return state

    def materialize_model_artifact(self, state):
        return save(_state_dict_cpu(state["model"]))

    def materialize_model_artifact_path(self, state, artifact_path):
        save_file(_state_dict_cpu(state["model"]), artifact_path)

    def merge_candidate_models(self, base_state, candidates, policy):
        del policy
        if not candidates:
            return None

        base_state_dict = _state_dict_cpu(base_state["model"])
        merged_state_dict = {}
        total_weight = 0.0
        for candidate in candidates:
            total_weight += max(
                float(candidate["sample_weight"]) * float(candidate["quality_weight"]), 0.0
            )
        if total_weight <= 1e-9:
            merged_state_dict = base_state_dict
        else:
            for name, base_tensor in base_state_dict.items():
                merged = torch.zeros_like(base_tensor, dtype=torch.float32)
                for candidate in candidates:
                    candidate_weight = max(
                        float(candidate["sample_weight"])
                        * float(candidate["quality_weight"]),
                        0.0,
                    )
                    candidate_tensor = candidate["model"]["model"].state_dict()[name].detach().cpu()
                    merged = merged + candidate_tensor.to(torch.float32) * (
                        candidate_weight / total_weight
                    )
                merged_state_dict[name] = merged.to(base_tensor.dtype)

        merged_model = self._create_model().to(self.device)
        merged_model.load_state_dict(merged_state_dict)
        return {
            "model": merged_model,
            "optimizer": self._make_optimizer(merged_model),
        }

    def apply_single_root_ema(self, base_state, merged_state, policy):
        if policy not in ("Ema", "QualityWeightedEma"):
            return merged_state

        base_state_dict = _state_dict_cpu(base_state["model"])
        merged_state_dict = _state_dict_cpu(merged_state["model"])
        for name, base_tensor in base_state_dict.items():
            merged_tensor = merged_state_dict[name].to(torch.float32)
            base_tensor = base_tensor.to(torch.float32)
            merged_state_dict[name] = (0.75 * merged_tensor + 0.25 * base_tensor).to(
                merged_state_dict[name].dtype
            )
        merged_state["model"].load_state_dict(merged_state_dict)
        merged_state["optimizer"] = self._make_optimizer(merged_state["model"])
        return merged_state

    def reconcile_canonical_model(self, local_state, canonical_state, strategy):
        if strategy == "Replace":
            canonical_state["optimizer"] = self._make_optimizer(canonical_state["model"])
            return canonical_state

        if not isinstance(strategy, dict) or "RootEma" not in strategy:
            raise ValueError(f"unsupported reconcile strategy {strategy!r}")

        canonical_weight = float(strategy["RootEma"]["canonical_weight"])
        local_weight = 1.0 - canonical_weight
        local_state_dict = _state_dict_cpu(local_state["model"])
        canonical_state_dict = _state_dict_cpu(canonical_state["model"])
        blended = {}
        for name, canonical_tensor in canonical_state_dict.items():
            local_tensor = local_state_dict[name].to(torch.float32)
            blended[name] = (
                canonical_tensor.to(torch.float32) * canonical_weight
                + local_tensor * local_weight
            ).to(canonical_tensor.dtype)
        canonical_state["model"].load_state_dict(blended)
        canonical_state["optimizer"] = self._make_optimizer(canonical_state["model"])
        return canonical_state

    def apply_patch(self, patch):
        values = patch.get("values", {})
        if "learning_rate" not in values:
            return {"Rejected": "missing learning_rate patch"}
        value = values["learning_rate"]
        if not isinstance(value, dict) or "Float" not in value:
            return {"Rejected": "learning_rate patch must be a float"}
        self.learning_rate = float(value["Float"])
        return "Applied"

    def _resolve_device(self, preferred):
        if preferred == "cuda" and torch.cuda.is_available():
            return torch.device("cuda")
        if preferred == "mps" and torch.backends.mps.is_available():
            return torch.device("mps")
        if preferred == "auto":
            if torch.cuda.is_available():
                return torch.device("cuda")
            if torch.backends.mps.is_available():
                return torch.device("mps")
        return torch.device("cpu")

    def _create_model(self):
        return torch.nn.Sequential(
            torch.nn.Flatten(),
            torch.nn.Linear(28 * 28, self.hidden_size),
            torch.nn.ReLU(),
            torch.nn.Linear(self.hidden_size, 10),
        )

    def _make_optimizer(self, model):
        return torch.optim.SGD(model.parameters(), lr=self.learning_rate)

    def _batch_shard_paths(self, batch):
        if batch.get("kind") == "micro_epoch":
            payload = batch.get("payload", {})
            shard_paths = payload.get("shard_paths", [])
            if not isinstance(shard_paths, list):
                raise ValueError("micro_epoch payload.shard_paths must be a list")
            return shard_paths
        shard_paths = batch.get("shard_paths", [])
        if not isinstance(shard_paths, list):
            raise ValueError("cached batch shard_paths must be a list")
        return shard_paths


def prepare_dataset(dataset_root: Path, mnist_root: Path, sample_count: int, shard_count: int):
    dataset_root.mkdir(parents=True, exist_ok=True)
    mnist_root.mkdir(parents=True, exist_ok=True)
    train_dataset = MNIST(mnist_root, train=True, download=True)
    _ = MNIST(mnist_root, train=False, download=True)

    images = train_dataset.data[:sample_count].clone()
    labels = train_dataset.targets[:sample_count].clone()
    shard_size = max(sample_count // shard_count, 1)
    for ordinal in range(shard_count):
        start = ordinal * shard_size
        end = sample_count if ordinal == shard_count - 1 else min(
            sample_count, start + shard_size
        )
        shard_bytes = save(
            {
                "images": images[start:end],
                "labels": labels[start:end],
            }
        )
        (dataset_root / f"{ordinal:05}.bin").write_bytes(shard_bytes)


def run_diloco_command_job(config):
    from burn_p2p_python_runtime.worker import _load_parameter_pack, _write_parameter_pack

    job_path = Path(os.environ["BURN_P2P_DILOCO_JOB_MANIFEST"])
    result_path = Path(os.environ["BURN_P2P_DILOCO_RESULT_MANIFEST"])
    job = json.loads(job_path.read_text(encoding="utf-8"))
    workload = TorchMnistWorkload(config)
    state = workload.init_model(workload.runtime_device())
    _load_parameter_pack(job["base_parameter_pack_path"], state["model"])
    metrics = workload._train_steps(state, job["batches"], int(job["num_inner_steps"]))
    steps_completed = int(metrics["train_steps"])
    if job.get("require_exact_steps", True) and steps_completed != int(job["num_inner_steps"]):
        raise ValueError(
            f"command inner loop completed {steps_completed} steps, requested {job['num_inner_steps']}"
        )
    _write_parameter_pack(
        job["output_parameter_pack_path"],
        state["model"],
        job["model_schema_hash"],
    )
    result_path.write_text(
        json.dumps(
            {
                "steps_completed": steps_completed,
                "metrics": metrics,
                "local_parameter_pack_path": job["output_parameter_pack_path"],
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def main():
    parser = argparse.ArgumentParser()
    subcommands = parser.add_subparsers(dest="command", required=True)

    prepare = subcommands.add_parser("prepare-dataset")
    prepare.add_argument("--dataset-root", required=True)
    prepare.add_argument("--mnist-root", required=True)
    prepare.add_argument("--sample-count", type=int, required=True)
    prepare.add_argument("--shard-count", type=int, required=True)

    diloco_job = subcommands.add_parser("diloco-command-job")
    diloco_job.add_argument("--config-json", required=True)

    args = parser.parse_args()
    if args.command == "prepare-dataset":
        prepare_dataset(
            Path(args.dataset_root),
            Path(args.mnist_root),
            args.sample_count,
            args.shard_count,
        )
        return
    if args.command == "diloco-command-job":
        run_diloco_command_job(json.loads(args.config_json))
        return
    raise SystemExit(f"unknown command {args.command}")


if __name__ == "__main__":
    main()
