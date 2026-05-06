import argparse
import fnmatch
import hashlib
import importlib
import json
from pathlib import Path
import socket
import struct
import sys
import traceback


PROTOCOL_VERSION = 2
PARAMETER_PACK_FORMAT = "burn-p2p-python-flattened-parameter-pack-v1"


def _import_factory(spec: str):
    if ":" not in spec:
        raise ValueError(f"factory spec must use module:attr form, got {spec!r}")
    module_name, attr_name = spec.split(":", 1)
    module = importlib.import_module(module_name)
    return getattr(module, attr_name)


def _metric_value(value):
    if isinstance(value, (bool, int, float, str)):
        return value
    raise TypeError(f"unsupported metric value {value!r}")


def _metric_map(values):
    return {key: _metric_value(value) for key, value in values.items()}


def _patch_outcome_rejected(message: str):
    return {"Rejected": message}


def _recv_exact(sock, size: int) -> bytes:
    chunks = []
    remaining = size
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise EOFError("python rpc socket closed")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _recv_message(sock):
    header = sock.recv(8)
    if not header:
        return None
    while len(header) < 8:
        chunk = sock.recv(8 - len(header))
        if not chunk:
            raise EOFError("python rpc socket closed during frame header")
        header += chunk
    (size,) = struct.unpack("!Q", header)
    return json.loads(_recv_exact(sock, size))


def _send_message(sock, payload):
    encoded = json.dumps(payload).encode("utf-8")
    sock.sendall(struct.pack("!Q", len(encoded)))
    sock.sendall(encoded)


def _torch_module_entries_from_state(state):
    if isinstance(state, dict) and "modules" in state:
        modules = state["modules"]
        if not isinstance(modules, dict) or not modules:
            raise TypeError("{'modules': ...} must contain a non-empty module mapping")
        entries = []
        for module_name in sorted(modules):
            module = modules[module_name]
            if not isinstance(module_name, str) or not module_name:
                raise TypeError("module names must be non-empty strings")
            if "." in module_name:
                raise TypeError("module names must not contain '.'")
            if not hasattr(module, "state_dict") or not hasattr(module, "load_state_dict"):
                raise TypeError(f"module {module_name!r} does not support state_dict I/O")
            entries.append((module_name, module))
        return entries
    if isinstance(state, dict) and "model" in state:
        state = state["model"]
    if hasattr(state, "state_dict") and hasattr(state, "load_state_dict"):
        return [("", state)]
    raise TypeError(
        "generic parameter-pack support requires a torch model, {'model': model}, "
        "or {'modules': {'name': module}}"
    )


def _state_dict_filter(config):
    config = config or {}
    return {
        "include_globs": [str(pattern) for pattern in config.get("include_globs") or []],
        "exclude_globs": [str(pattern) for pattern in config.get("exclude_globs") or []],
    }


def _filter_allows_state_key(full_name: str, config) -> bool:
    config = _state_dict_filter(config)
    include_globs = config["include_globs"]
    exclude_globs = config["exclude_globs"]
    if include_globs and not any(
        fnmatch.fnmatchcase(full_name, pattern) for pattern in include_globs
    ):
        return False
    if any(fnmatch.fnmatchcase(full_name, pattern) for pattern in exclude_globs):
        return False
    return True


def _torch_float_layout(model, state_dict_filter=None):
    import torch

    items = []
    excluded_float_keys = []
    ignored_non_float_keys = []
    for module_name, module in _torch_module_entries_from_state(model):
        state_dict = module.state_dict()
        for key in sorted(state_dict):
            full_name = f"{module_name}.{key}" if module_name else key
            tensor = state_dict[key].detach()
            if not torch.is_floating_point(tensor):
                ignored_non_float_keys.append(full_name)
                continue
            if not _filter_allows_state_key(full_name, state_dict_filter):
                excluded_float_keys.append(full_name)
                continue
            shape = [int(dim) for dim in tensor.shape]
            numel = int(tensor.numel())
            items.append(
                {
                    "name": full_name,
                    "module": module_name,
                    "key": key,
                    "shape": shape,
                    "dtype": str(tensor.dtype),
                    "numel": numel,
                }
            )
    layout_for_hash = [
        {
            "name": item["name"],
            "shape": item["shape"],
            "dtype": item["dtype"],
            "numel": item["numel"],
        }
        for item in items
    ]
    encoded = json.dumps(layout_for_hash, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return (
        "torch-layout-" + hashlib.sha256(encoded).hexdigest(),
        items,
        excluded_float_keys,
        ignored_non_float_keys,
    )


def _parameter_pack_plan(model, state_dict_filter=None):
    layout_hash, layout, excluded_float_keys, ignored_non_float_keys = _torch_float_layout(
        model, state_dict_filter
    )
    return {
        "uses_custom_parameter_pack_hooks": False,
        "layout_hash": layout_hash,
        "included_keys": [item["name"] for item in layout],
        "excluded_float_keys": excluded_float_keys,
        "ignored_non_float_keys": ignored_non_float_keys,
        "parameter_count": sum(int(item["numel"]) for item in layout),
    }


def _write_parameter_pack(path, model, model_schema_hash, state_dict_filter=None):
    import torch

    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    layout_hash, layout, excluded_float_keys, ignored_non_float_keys = _torch_float_layout(
        model, state_dict_filter
    )
    values_path = path / "values.f32le"
    parameter_count = 0
    state_dicts = {
        module_name: module.state_dict()
        for module_name, module in _torch_module_entries_from_state(model)
    }
    with values_path.open("wb") as values_file:
        for item in layout:
            tensor = (
                state_dicts[item["module"]][item["key"]]
                .detach()
                .to(device="cpu", dtype=torch.float32)
                .contiguous()
                .reshape(-1)
            )
            values = tensor.tolist()
            parameter_count += len(values)
            for start in range(0, len(values), 65536):
                chunk = values[start : start + 65536]
                values_file.write(struct.pack(f"<{len(chunk)}f", *chunk))
    manifest = {
        "format": PARAMETER_PACK_FORMAT,
        "model_schema_hash": str(model_schema_hash),
        "layout_hash": layout_hash,
        "parameter_count": parameter_count,
        "values_f32_le": "values.f32le",
        "state_dict_filter": _state_dict_filter(state_dict_filter),
        "included_keys": [item["name"] for item in layout],
        "excluded_float_keys": excluded_float_keys,
        "ignored_non_float_keys": ignored_non_float_keys,
    }
    (path / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def _read_pack_manifest(path):
    path = Path(path)
    manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("format") != PARAMETER_PACK_FORMAT:
        raise ValueError(f"unsupported parameter-pack format {manifest.get('format')!r}")
    values_path = path / manifest["values_f32_le"]
    values_bytes = values_path.read_bytes()
    expected_bytes = int(manifest["parameter_count"]) * 4
    if len(values_bytes) != expected_bytes:
        raise ValueError(
            f"parameter-pack byte length {len(values_bytes)} does not match {expected_bytes}"
        )
    return manifest, values_bytes


def _load_parameter_pack(path, model, state_dict_filter=None):
    import torch

    manifest, values_bytes = _read_pack_manifest(path)
    layout_hash, layout, _, _ = _torch_float_layout(model, state_dict_filter)
    if manifest["layout_hash"] != layout_hash:
        raise ValueError(
            f"parameter-pack layout {manifest['layout_hash']} does not match model layout {layout_hash}"
        )
    values = []
    for start in range(0, len(values_bytes), 4 * 65536):
        chunk = values_bytes[start : start + 4 * 65536]
        values.extend(struct.unpack(f"<{len(chunk) // 4}f", chunk))
    if len(values) != int(manifest["parameter_count"]):
        raise ValueError("decoded parameter count does not match manifest")

    module_entries = _torch_module_entries_from_state(model)
    state_dicts = {module_name: module.state_dict() for module_name, module in module_entries}
    offset = 0
    for item in layout:
        numel = int(item["numel"])
        existing = state_dicts[item["module"]][item["key"]]
        restored = torch.tensor(
            values[offset : offset + numel],
            dtype=torch.float32,
            device=existing.device,
        ).reshape(item["shape"])
        state_dicts[item["module"]][item["key"]] = restored.to(
            dtype=existing.dtype, device=existing.device
        )
        offset += numel
    if offset != len(values):
        raise ValueError(f"unused parameter-pack values: consumed {offset}, found {len(values)}")
    for module_name, module in module_entries:
        module.load_state_dict(state_dicts[module_name])


class WorkerServer:
    def __init__(self, factory_spec: str, config_json: str):
        factory = _import_factory(factory_spec)
        config = json.loads(config_json)
        self.workload = factory(config)
        self.models = {}
        self.next_model_id = 1

    def serve_forever(self, host: str, port: int):
        with socket.create_connection((host, port)) as sock:
            while True:
                request = _recv_message(sock)
                if request is None:
                    return
                if not request:
                    continue
                response = None
                try:
                    response = {
                        "id": request["id"],
                        "ok": True,
                        "result": self.dispatch(request["method"], request.get("params")),
                    }
                except SystemExit:
                    response = {
                        "id": request.get("id", 0),
                        "ok": True,
                        "result": {"ok": True},
                    }
                    _send_message(sock, response)
                    return
                except Exception as exc:
                    response = {
                        "id": request.get("id", 0),
                        "ok": False,
                        "error": f"{exc}\n{traceback.format_exc()}",
                    }
                _send_message(sock, response)

    def new_model(self, model_state):
        model_id = f"m{self.next_model_id}"
        self.next_model_id += 1
        self.models[model_id] = model_state
        return model_id

    def dispatch(self, method: str, params):
        params = params or {}
        if method == "hello":
            return {
                "protocol_version": PROTOCOL_VERSION,
                "workload_name": getattr(
                    self.workload, "workload_name", type(self.workload).__name__
                ),
                "capabilities": [
                    "artifact_path_io",
                    "diloco_checkpoint_job",
                    "parameter_pack_export",
                    "parameter_pack_import",
                    "exact_step_budget",
                ],
            }
        if method == "capability_probe":
            return {
                "runtime_device": self.workload.runtime_device(),
                "capability": self.workload.capability_probe(),
            }
        if method == "init_model":
            model = self.workload.init_model(params["device"])
            return {"model_id": self.new_model(model)}
        if method == "parameter_pack_plan":
            uses_custom_hooks = hasattr(
                self.workload, "export_parameter_pack_path"
            ) or hasattr(self.workload, "import_parameter_pack_path")
            model = self.workload.init_model(params["device"])
            try:
                plan = _parameter_pack_plan(model, params.get("state_dict_filter"))
                plan["uses_custom_parameter_pack_hooks"] = uses_custom_hooks
                return plan
            except Exception as exc:
                if uses_custom_hooks:
                    return {
                        "uses_custom_parameter_pack_hooks": True,
                        "layout_hash": None,
                        "included_keys": [],
                        "excluded_float_keys": [],
                        "ignored_non_float_keys": [],
                        "parameter_count": 0,
                        "generic_plan_error": str(exc),
                    }
                raise
        if method == "train_window":
            model_id = params["model_id"]
            metrics = self.workload.train_window(self.models[model_id], params["batches"])
            return {"metrics": _metric_map(metrics)}
        if method == "evaluate":
            model_id = params["model_id"]
            metrics = self.workload.evaluate(self.models[model_id], params["split"])
            return {"metrics": _metric_map(metrics)}
        if method == "apply_patch":
            if not hasattr(self.workload, "apply_patch"):
                return _patch_outcome_rejected(
                    "python workload does not support runtime patches"
                )
            return self.workload.apply_patch(params)
        if method == "load_model_artifact":
            model_id = params["model_id"]
            artifact_path = params["artifact_path"]
            if hasattr(self.workload, "load_model_artifact_path"):
                updated = self.workload.load_model_artifact_path(
                    self.models[model_id], artifact_path
                )
            else:
                with open(artifact_path, "rb") as artifact_file:
                    artifact_bytes = artifact_file.read()
                updated = self.workload.load_model_artifact(
                    self.models[model_id], artifact_bytes
                )
            if updated is not None:
                self.models[model_id] = updated
            return {"ok": True}
        if method == "materialize_model_artifact":
            model_id = params["model_id"]
            artifact_path = params["artifact_path"]
            if hasattr(self.workload, "materialize_model_artifact_path"):
                self.workload.materialize_model_artifact_path(
                    self.models[model_id], artifact_path
                )
            else:
                artifact_bytes = self.workload.materialize_model_artifact(
                    self.models[model_id]
                )
                with open(artifact_path, "wb") as artifact_file:
                    artifact_file.write(artifact_bytes)
            return {"ok": True}
        if method == "export_parameter_pack":
            model_id = params["model_id"]
            parameter_pack_path = params["parameter_pack_path"]
            model_schema_hash = params["model_schema_hash"]
            if hasattr(self.workload, "export_parameter_pack_path"):
                self.workload.export_parameter_pack_path(
                    self.models[model_id], parameter_pack_path, model_schema_hash
                )
            else:
                _write_parameter_pack(
                    parameter_pack_path,
                    self.models[model_id],
                    model_schema_hash,
                    params.get("state_dict_filter"),
                )
            return {"ok": True}
        if method == "import_parameter_pack":
            parameter_pack_path = params["parameter_pack_path"]
            device = params["device"]
            if hasattr(self.workload, "import_parameter_pack_path"):
                imported = self.workload.import_parameter_pack_path(
                    device, parameter_pack_path
                )
            else:
                imported = self.workload.init_model(device)
                _load_parameter_pack(
                    parameter_pack_path, imported, params.get("state_dict_filter")
                )
            return {"model_id": self.new_model(imported)}
        if method == "run_inner_loop":
            model_id = params["model_id"]
            if not hasattr(self.workload, "run_inner_loop"):
                raise ValueError("python workload does not implement run_inner_loop")
            result = self.workload.run_inner_loop(self.models[model_id], params) or {}
            if "state" in result:
                self.models[model_id] = result["state"]
            output_pack_path = params["output_parameter_pack_path"]
            if not Path(output_pack_path, "manifest.json").exists():
                _write_parameter_pack(
                    output_pack_path,
                    self.models[model_id],
                    params["model_schema_hash"],
                    params.get("state_dict_filter"),
                )
            steps_completed = int(result.get("steps_completed", params["num_inner_steps"]))
            if params.get("require_exact_steps") and steps_completed != int(
                params["num_inner_steps"]
            ):
                raise ValueError(
                    f"python inner loop completed {steps_completed} steps, requested {params['num_inner_steps']}"
                )
            return {
                "steps_completed": steps_completed,
                "metrics": _metric_map(result.get("metrics", {})),
                "inner_optimizer_state_path": result.get("inner_optimizer_state_path"),
                "inner_optimizer_state_encoding": result.get(
                    "inner_optimizer_state_encoding"
                ),
            }
        if method == "merge_candidate_models":
            base_model = self.models[params["base_model_id"]]
            candidates = []
            for candidate in params["candidates"]:
                candidates.append(
                    {
                        "peer_id": candidate["peer_id"],
                        "head_id": candidate["head_id"],
                        "artifact_id": candidate["artifact_id"],
                        "model": self.models[candidate["model_id"]],
                        "sample_weight": candidate["sample_weight"],
                        "quality_weight": candidate["quality_weight"],
                    }
                )
            merged = self.workload.merge_candidate_models(
                base_model, candidates, params["policy"]
            )
            if merged is None:
                return {"model_id": None}
            return {"model_id": self.new_model(merged)}
        if method == "apply_single_root_ema":
            base_model = self.models[params["base_model_id"]]
            merged_model_id = params["merged_model_id"]
            merged_model = self.models[merged_model_id]
            updated = self.workload.apply_single_root_ema(
                base_model, merged_model, params["policy"]
            )
            if updated is not None:
                self.models[merged_model_id] = updated
            return {"model_id": merged_model_id}
        if method == "reconcile_canonical_model":
            local_model = self.models[params["local_model_id"]]
            canonical_model_id = params["canonical_model_id"]
            canonical_model = self.models[canonical_model_id]
            updated = self.workload.reconcile_canonical_model(
                local_model, canonical_model, params["strategy"]
            )
            if updated is not None:
                self.models[canonical_model_id] = updated
            return {"model_id": canonical_model_id}
        if method == "release_model":
            self.models.pop(params["model_id"], None)
            return {"ok": True}
        if method == "shutdown":
            raise SystemExit(0)
        raise ValueError(f"unknown method {method!r}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--factory", required=True)
    parser.add_argument("--config-json", required=True)
    parser.add_argument("--connect-host", required=True)
    parser.add_argument("--connect-port", required=True, type=int)
    args = parser.parse_args()
    server = WorkerServer(args.factory, args.config_json)
    server.serve_forever(args.connect_host, args.connect_port)


if __name__ == "__main__":
    main()
