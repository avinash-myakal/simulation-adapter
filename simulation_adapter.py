# =========================================
# file: simulation_adapter.py
# =========================================
from __future__ import annotations

import importlib.util
import json
import logging
import os
import sys
import threading
import time
import uuid
from typing import Any, Dict

import requests
from flask import Flask, jsonify, request

def load_simulation_model() -> Any:
    base_dir = os.path.dirname(__file__)
    file_path = os.path.join(base_dir, "mm_final_energy_sim.py")
    module_name = "mm_final_energy_sim"
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load spec for {file_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)  # type: ignore[attr-defined]
    return module

def public_base_url(default_service: str) -> str:
    url = os.getenv("ADAPTER_PUBLIC_URL")
    if url:
        return url.rstrip("/")
    host = os.getenv("ADAPTER_PUBLIC_HOST") or os.getenv("ADAPTER_SERVICE") or default_service
    port = os.getenv("ADAPTER_PUBLIC_PORT") or os.getenv("ADAPTER_PORT", "8000")
    return f"http://{host}:{port}"

def _registry_post(url: str, payload: Dict[str, Any], timeout: float = 5.0) -> requests.Response:
    return requests.post(url, json=payload, timeout=timeout)

def register_with_registry(app_name: str, description: str, endpoints: list[str], logger: logging.Logger) -> None:
    registry = os.getenv("REGISTRY_ENDPOINT")
    if not registry:
        logger.info("REGISTRY_ENDPOINT not set; skipping auto-registration")
        return
    base_url = public_base_url(default_service=f"{app_name}-adapter")
    version = os.getenv("MODEL_VERSION", "1.0.0")
    max_workers = int(os.getenv("MODEL_MAX_WORKERS", "1"))
    used_workers = int(os.getenv("MODEL_USED_WORKERS", "0"))
    new_api_url = f"{registry.rstrip('/')}/registry/"
    old_api_url = f"{registry.rstrip('/')}/models/register"

    def attempt_once() -> bool:
        try:
            r = _registry_post(new_api_url, {
                "name": app_name, "uri": base_url, "version": version,
                "max_workers": max_workers, "used_workers": used_workers,
            })
            if r.status_code == 404:
                raise FileNotFoundError
            r.raise_for_status()
            logger.info("Registered with registry (new API) at %s", new_api_url)
            return True
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.warning("New API registration failed: %s", e)
        try:
            r = _registry_post(old_api_url, {
                "name": app_name, "description": description, "url": base_url, "endpoints": endpoints,
            })
            r.raise_for_status()
            logger.info("Registered with registry (old API) at %s", old_api_url)
            return True
        except Exception as e:
            logger.warning("Old API registration failed: %s", e)
            return False

    if not attempt_once():
        logger.info("Will retry registration in background until success...")

    def heartbeat() -> None:
        while True:
            ok = attempt_once()
            time.sleep(60 if ok else 10)

    threading.Thread(target=heartbeat, daemon=True).start()

class RunState:
    PENDING = "PENDING"; INITIALISED = "INITIALISED"; RUNNING = "RUNNING"; SUCCEEDED = "SUCCEEDED"; ERROR = "ERROR"

class SimulationModelAdapter:
    def __init__(self) -> None:
        self.logger = logging.getLogger("SimulationAdapter")
        logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
        self._runs: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
        try:
            self.model_module = load_simulation_model()
            self.logger.info("Energy simulation model loaded")
        except Exception as e:
            self.logger.error("Failed to load energy simulation model module: %s", e)
            raise

    def create_run(self) -> str:
        run_id = f"{os.getenv('RUN_ID_PREFIX', 'run')}-{uuid.uuid4()}"
        with self._lock:
            self._runs[run_id] = {"state": RunState.PENDING, "config": {}, "thread": None, "result": None, "error": None}
        return run_id

    def initialise_run(self, run_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
        with self._lock:
            run = self._runs.get(run_id)
            if not run:
                raise KeyError(f"Run {run_id} does not exist")
            if run["state"] in [RunState.PENDING, RunState.INITIALISED]:
                run["state"] = RunState.INITIALISED
            return run

    def start_run(self, run_id: str) -> Dict[str, Any]:
        with self._lock:
            run = self._runs.get(run_id)
            if not run:
                raise KeyError(f"Run {run_id} does not exist")
            if run["state"] in [RunState.RUNNING, RunState.SUCCEEDED, RunState.ERROR]:
                return run
            env = os.environ.copy(); env["RUN_ID"] = run_id
            run["state"] = RunState.RUNNING
            t = threading.Thread(target=self._exec_run, args=(run_id, env), daemon=True)
            run["thread"] = t; t.start()
            return run

    def _exec_run(self, run_id: str, env: Dict[str, str]) -> None:
        try:
            original = os.environ.copy(); os.environ.update(env)
            try:
                # DO NOT redirect stdout; let print() flow to Gunicorn/Docker logs
                fn = getattr(self.model_module, "run_energy_simulation_from_env", None) \
                     or getattr(self.model_module, "run_energy_model_from_env", None)
                if fn is None:
                    raise AttributeError("Neither run_energy_simulation_from_env nor run_energy_model_from_env found")
                result = fn()
            finally:
                os.environ.clear(); os.environ.update(original)

            out = result if isinstance(result, dict) else {"message": "Energy simulation completed"}
            with self._lock:
                run = self._runs.get(run_id)
                if run:
                    run["state"] = RunState.SUCCEEDED; run["result"] = out
        except Exception as e:
            self.logger.exception("Run %s failed", run_id)
            with self._lock:
                run = self._runs.get(run_id)
                if run:
                    run["state"] = RunState.ERROR; run["error"] = str(e)

    def get_run(self, run_id: str) -> Dict[str, Any]:
        with self._lock:
            run = self._runs.get(run_id)
            if not run:
                raise KeyError(f"Run {run_id} does not exist")
            return run

    def remove_run(self, run_id: str) -> None:
        with self._lock:
            self._runs.pop(run_id, None)

def create_app() -> Flask:
    adapter = SimulationModelAdapter()
    app = Flask(__name__)
    register_with_registry(
        app_name=os.getenv("MODEL_NAME", "energy_sim_model"),
        description=os.getenv("MODEL_DESCRIPTION", "Runs EnergyPlus over IDFs from MinIO and writes XLSX results"),
        endpoints=[
            "/model/request",
            "/model/initialize/<run_id>",
            "/model/run/<run_id>",
            "/model/status/<run_id>",
            "/model/results/<run_id>",
            "/model/remove/<run_id>",
        ],
        logger=adapter.logger,
    )

    @app.get("/health")
    def health(): return jsonify({"status": "ok"})

    @app.post("/model/request")
    def request_model(): return jsonify({"run_id": adapter.create_run()})

    @app.post("/model/initialize/<run_id>")
    def initialise(run_id: str):
        try:
            cfg = request.get_json() or {}
            run = adapter.initialise_run(run_id, cfg)
            return jsonify({"run_id": run_id, "state": run["state"], "config": run["config"]})
        except KeyError as e:
            return jsonify({"error": str(e)}), 404

    @app.post("/model/run/<run_id>")
    def run_model(run_id: str):
        try:
            run = adapter.start_run(run_id)
            code = 400 if run["state"] == RunState.ERROR else 200
            return jsonify({"run_id": run_id, "state": run["state"], "error": run.get("error")}), code
        except KeyError as e:
            return jsonify({"error": str(e)}), 404

    @app.get("/model/status/<run_id>")
    def status(run_id: str):
        try:
            run = adapter.get_run(run_id)
            return jsonify({"run_id": run_id, "state": run["state"], "error": run.get("error")})
        except KeyError as e:
            return jsonify({"error": str(e)}), 404

    @app.get("/model/results/<run_id>")
    def results(run_id: str):
        try:
            run = adapter.get_run(run_id)
            if run["state"] == RunState.SUCCEEDED:
                return jsonify({"run_id": run_id, "result": run["result"]})
            if run["state"] == RunState.ERROR:
                return jsonify({"run_id": run_id, "error": run.get("error")}), 400
            return jsonify({"run_id": run_id, "state": run["state"]}), 202
        except KeyError as e:
            return jsonify({"error": str(e)}), 404

    @app.post("/model/remove/<run_id>")
    def remove(run_id: str):
        adapter.remove_run(run_id)
        return jsonify({"status": "removed", "run_id": run_id})

    return app

from flask import Flask
app = create_app()

if __name__ == "__main__":
    app.run(host=os.getenv("ADAPTER_HOST", "0.0.0.0"), port=int(os.getenv("ADAPTER_PORT", "8000")), debug=False)
