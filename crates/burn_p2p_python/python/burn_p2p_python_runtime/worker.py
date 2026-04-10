import argparse
import importlib
import json
import socket
import struct
import sys
import traceback


PROTOCOL_VERSION = 1


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
            }
        if method == "capability_probe":
            return {
                "runtime_device": self.workload.runtime_device(),
                "capability": self.workload.capability_probe(),
            }
        if method == "init_model":
            model = self.workload.init_model(params["device"])
            return {"model_id": self.new_model(model)}
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
