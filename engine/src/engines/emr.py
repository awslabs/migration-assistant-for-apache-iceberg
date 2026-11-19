# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""EMR Serverless client — submit, poll, and cancel Spark job runs."""

import os




def _load_config() -> dict:
    """Settings from environment variables, with local-development defaults."""
    return {
        "application_id": os.getenv("ENGINE_EMR_APPLICATION_ID", ""),
        "execution_role_arn": os.getenv("ENGINE_EMR_EXECUTION_ROLE_ARN", ""),
        "log_uri": os.getenv("ENGINE_EMR_LOG_URI", ""),
        "region": os.getenv("ENGINE_EMR_REGION", "us-east-1"),
    }


_config = _load_config()


def resolve_script_path(script_name: str) -> str:
    """Resolve the S3 path to an EMR Spark script (resolved lazily at call time).

    Prefers ENGINE_EMR_SCRIPTS_URI, then ENGINE_EMR_LOG_URI, then the
    config log_uri. Avoids a double '/scripts/' if the URI already ends with
    '/scripts'. Reading env at call time (not from the frozen module config)
    ensures values pushed via runtime settings are honored.
    """
    scripts_uri = os.getenv("ENGINE_EMR_SCRIPTS_URI", "").rstrip("/")
    if not scripts_uri:
        scripts_uri = (os.getenv("ENGINE_EMR_LOG_URI", "") or
                       _config.get("log_uri", "")).rstrip("/")
    if scripts_uri.endswith("/scripts"):
        return f"{scripts_uri}/{script_name}"
    return f"{scripts_uri}/scripts/{script_name}"


class EmrServerlessClient:
    """Thin wrapper around the EMR Serverless API."""

    def __init__(self, application_id: str = "", execution_role_arn: str = "",
                 region: str = ""):
        self.application_id = application_id or _config["application_id"]
        self.execution_role_arn = (
            execution_role_arn or _config["execution_role_arn"])
        self.region = region or _config["region"]
        # Cached, shared client (timeouts/retries/pool + reuse). See utils.aws.
        from ..utils.aws import get_client
        self._client = get_client("emr-serverless", self.region)

    @classmethod
    def from_pipeline(cls, pipeline: dict) -> "EmrServerlessClient":
        """Build a client using the pipeline spec's EMR overrides, if any.

        EMR Serverless applications are REGIONAL and identified by an
        application_id that is unique to one region. The app injects the
        user-configured EMR settings into spec.strategy.emr
        (region/application_id/execution_role_arn) so job submission targets the
        right application instead of the engine's deploy-time env defaults —
        otherwise StartJobRun fails with ResourceNotFoundException ("Application
        does not exist") when the engine's default region/app-id differ. Any
        absent value falls back to the env/config default.
        """
        import json as _json
        cfg: dict = {}
        spec = (pipeline or {}).get("spec_json")
        if isinstance(spec, str):
            try:
                spec = _json.loads(spec)
            except (ValueError, TypeError):
                spec = {}
        if isinstance(spec, dict):
            cfg = ((spec.get("strategy") or {}).get("emr") or {})
        return cls(
            application_id=cfg.get("application_id", ""),
            execution_role_arn=cfg.get("execution_role_arn", ""),
            region=cfg.get("region", ""))

    def submit_job(self, name: str, script_path: str,
                   script_args: list[str] = None,
                   spark_properties: dict[str, str] = None,
                   packages: list[str] = None,
                   py_files: list[str] = None) -> str:
        """Submit a Spark job run. Returns the job_run_id.

        Args:
            packages: optional list of Maven coordinates to pass as
                      --packages (e.g. ["org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0"]).
                      EMR Serverless resolves these from Maven Central at job start.
            py_files: optional list of S3 paths to pass as --py-files.
                      The spark_sql_builder.py is always included automatically.
        """
        # Resolve config lazily — settings may have been pushed after startup
        app_id = self.application_id or os.getenv("ENGINE_EMR_APPLICATION_ID", "")
        role_arn = self.execution_role_arn or os.getenv("ENGINE_EMR_EXECUTION_ROLE_ARN", "")

        # Fail fast with a clear message rather than passing empties to boto3,
        # which raises a cryptic "Invalid length ... valid min length: 20".
        if not app_id:
            raise ValueError(
                "EMR application ID is not configured "
                "(ENGINE_EMR_APPLICATION_ID).")
        if not role_arn:
            raise ValueError(
                "EMR execution role ARN is not configured "
                "(ENGINE_EMR_EXECUTION_ROLE_ARN).")
        spark_submit = {
            "entryPoint": script_path,
        }
        if script_args:
            spark_submit["entryPointArguments"] = script_args

        props = dict(spark_properties or {})

        # Build sparkSubmitParameters: --conf entries + optional --packages + --py-files
        params_parts = []
        if props:
            params_parts.extend(f"--conf {k}={v}" for k, v in props.items())
        if packages:
            params_parts.append(f"--packages {','.join(packages)}")

        # Always include spark_sql_builder.py as a py-file dependency
        all_py_files = [resolve_script_path("spark_sql_builder.py")]
        if py_files:
            all_py_files.extend(py_files)
        params_parts.append(f"--py-files {','.join(all_py_files)}")

        if params_parts:
            spark_submit["sparkSubmitParameters"] = " ".join(params_parts)

        job_config = {
            "sparkSubmit": spark_submit,
        }

        kwargs = {
            "applicationId": app_id,
            "executionRoleArn": role_arn,
            "jobDriver": job_config,
            "name": name,
        }

        # Prefer the live env value: the app pushes the log URI at runtime (derived
        # from the common logs bucket), and the module-level _config is frozen at
        # import, so reading it first would ignore a pushed change.
        log_uri = os.getenv("ENGINE_EMR_LOG_URI", "") or _config.get("log_uri", "")
        if log_uri:
            kwargs["configurationOverrides"] = {
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri": log_uri,
                    }
                }
            }

        resp = self._client.start_job_run(**kwargs)
        return resp["jobRunId"]

    def get_job_status(self, job_run_id: str) -> dict:
        """Get the current state of a job run.

        Returns dict with keys: state, state_details, created_at,
        updated_at, total_resource_utilization.
        """
        resp = self._client.get_job_run(
            applicationId=self.application_id or os.getenv("ENGINE_EMR_APPLICATION_ID", ""),
            jobRunId=job_run_id,
        )
        jr = resp["jobRun"]
        return {
            "state": jr["state"],
            "state_details": jr.get("stateDetails", ""),
            "created_at": str(jr.get("createdAt", "")),
            "updated_at": str(jr.get("updatedAt", "")),
            "total_resource_utilization": jr.get(
                "totalResourceUtilization", {}),
        }

    def cancel_job(self, job_run_id: str) -> None:
        """Cancel a running job run."""
        self._client.cancel_job_run(
            applicationId=self.application_id or os.getenv("ENGINE_EMR_APPLICATION_ID", ""),
            jobRunId=job_run_id,
        )
