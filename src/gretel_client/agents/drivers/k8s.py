"""
Support for running local docker workers.
"""

from __future__ import annotations

import base64
import itertools
import json
import os
import traceback

from pathlib import Path
from threading import Thread
from time import sleep
from typing import Optional, TYPE_CHECKING

from kubernetes import client, config
from kubernetes.client import ApiClient, BatchV1Api, CoreV1Api

from gretel_client.agents.drivers.driver import Driver
from gretel_client.config import get_logger
from gretel_client.docker import get_container_auth

logger = get_logger(__name__)

GRETEL_WORKER_SA = os.getenv("GRETEL_WORKER_SA", "gretel-agent")
GRETEL_WORKER_NAMESPACE = os.getenv("GRETEL_WORKER_NAMESPACE", "gretel-workers")
GRETEL_AGENT_SECRET_NAME = os.getenv("GRETEL_AGENT_SECRET_NAME", "gretel-agent-secret")
GRETEL_PULL_SECRET = os.getenv("GRETEL_PULL_SECRET", "gretel-pull-secret")
LIVENESS_FILE = os.getenv("LIVENESS_FILE", "/tmp/liveness.txt")


if TYPE_CHECKING:
    from gretel_client.agents.agent import AgentConfig, Job


def _base64_str(my_str: str) -> str:
    return base64.b64encode(my_str.encode("ascii")).decode("ascii")


class Kubernetes(Driver):
    """Run a worker using a Kubernetes daemon.

    This driver is suitable for running Gretel Workers in an on premise cloud
    """

    def __init__(
        self,
        agent_config: AgentConfig,
        batch_api: BatchV1Api,
        core_api: CoreV1Api,
    ):
        self._agent_config = agent_config
        self._batch_api = batch_api
        self._core_api = core_api
        worker = KubernetesDriverDaemon(
            agent_config=agent_config, core_api=self._core_api
        )
        worker.update_pull_secret_thread()
        worker.update_liveness_file_thread()

    @classmethod
    def from_config(cls, agent_config: AgentConfig) -> Kubernetes:
        config.load_incluster_config()
        return cls(agent_config, BatchV1Api(ApiClient()), CoreV1Api(ApiClient()))

    def schedule(self, job: Job) -> client.V1Job:
        return self._create_kubernetes_job(job)

    def active(self, unit: client.V1Job) -> bool:
        if unit:
            return self._is_job_active(unit)
        return False

    def clean(self, unit: client.V1Job):
        self._delete_kubernetes_job(unit)

    def shutdown(self, unit: client.V1Job):
        self._delete_kubernetes_job(unit)

    def _create_kubernetes_job(self, agent_job: Job) -> client.V1Job:
        """Creates a job for the input job config in the cluster pointed to by the input Api Client."""
        logger.info(f"Creating job:{agent_job.uid} in Kubernetes cluster.")
        kubernetes_job = self._build_job(agent_job)
        try:
            response = self._batch_api.create_namespaced_job(
                body=kubernetes_job, namespace=GRETEL_WORKER_NAMESPACE
            )
            return response
        except client.ApiException as ex:
            err_resp = json.loads(ex.body)
            if err_resp.get("reason") == "AlreadyExists":
                logger.warning(f"job={agent_job.uid} already scheduled. Skipping.")
            else:
                logger.error(f"Could not deploy job={agent_job.uid}")
                logger.error(err_resp)
                logger.error(traceback.format_exc())
                raise KubernetesError(
                    f"Count not create job name={agent_job.uid} namespace={GRETEL_WORKER_NAMESPACE}"
                ) from ex
            return None
        except Exception as ex:
            logger.error(traceback.format_exc())
            raise ex

    def _build_job(self, job_config: Job) -> client.V1Job:
        memory_limit_in_gb = os.environ.get("MEMORY_LIMIT_IN_GB", 14)
        res_limits = {"memory": f"{memory_limit_in_gb}Gi"}
        if job_config.needs_gpu:
            res_limits["nvidia.com/gpu"] = "1"

        env = []
        if job_config.env_vars:
            env = [
                client.V1EnvVar(name=k, value=v) for k, v in job_config.env_vars.items()
            ]
        env.append(client.V1EnvVar(name="GRETEL_STAGE", value=job_config.gretel_stage))

        args = list(itertools.chain.from_iterable(job_config.params.items()))

        container = client.V1Container(
            name=job_config.uid,
            image=job_config.container_image,
            resources=client.V1ResourceRequirements(limits=res_limits),
            args=args,
            env=env,
            image_pull_policy="IfNotPresent",
            env_from=[
                client.V1EnvFromSource(
                    secret_ref=client.V1SecretEnvSource(name=GRETEL_AGENT_SECRET_NAME)
                )
            ],
        )

        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(
                labels={"app": "gretel-jobs-worker"},
                namespace=GRETEL_WORKER_NAMESPACE,
            ),
            spec=client.V1PodSpec(
                restart_policy="Never",
                containers=[container],
                service_account_name=GRETEL_WORKER_SA,
                image_pull_secrets=[
                    client.V1LocalObjectReference(name=GRETEL_PULL_SECRET)
                ],
            ),
        )
        gpu_node_selector = os.getenv("GPU_NODE_SELECTOR", "")
        if job_config.needs_gpu and gpu_node_selector:
            pod_spec: client.V1PodSpec = template.spec
            if pod_spec:
                try:
                    selector_dict = json.loads(gpu_node_selector)
                except json.decoder.JSONDecodeError as ex:
                    raise KubernetesError(
                        "Could not deserialize JSON for GPU_NODE_SELECTOR"
                    ) from ex
                if not isinstance(selector_dict, dict):
                    raise KubernetesError(
                        f"The GPU_NODE_SELECTOR was not a JSON dictionary, received {gpu_node_selector}"
                    )
                if selector_dict:
                    pod_spec.node_selector = selector_dict

        spec = client.V1JobSpec(
            template=template, backoff_limit=0, ttl_seconds_after_finished=86400
        )

        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(name=job_config.uid),
            spec=spec,
        )

        return job

    def _delete_kubernetes_job(self, job: Optional[client.V1Job]):
        """Deletes the input V1Job in the cluster pointed to by the input Api Client."""
        if not job:
            logger.warning("Could not delete job, no metadata found")
            return
        logger.info(f"Deleting job:{job.metadata.name} from Kubernetes cluster.")
        try:
            self._batch_api.delete_namespaced_job(
                name=job.metadata.name,
                namespace=job.metadata.namespace,
                propagation_policy="Background",
            )
        except client.ApiException as ex:
            err_resp = json.loads(ex.body)
            if err_resp.get("reason") == "NotFound":
                logger.warning(
                    f"attempted to delete job={job.metadata.name} but job does not exist."
                )
            else:
                logger.error(f"Could not delete job={job.metadata.name}")
                logger.error(err_resp)
                raise KubernetesError(
                    f"Could not delete job name={job.metadata.name} namespace={GRETEL_WORKER_NAMESPACE}"
                ) from ex

    def _is_job_active(self, job: client.V1Job) -> bool:
        job_resp: client.V1Job = self._batch_api.read_namespaced_job(
            job.metadata.name, namespace=GRETEL_WORKER_NAMESPACE
        )
        if not job_resp:
            return False
        status: client.V1JobStatus = job_resp.status
        if status and status and status.active is not None:
            return status.active > 0
        return False


class KubernetesDriverDaemon:
    """
    Represents background tasks performed by the k8s driver
    """

    def __init__(
        self,
        agent_config: AgentConfig,
        core_api: CoreV1Api,
    ):
        self._agent_config = agent_config
        config.load_incluster_config()
        self._core_api = core_api

    def update_pull_secret_thread(self) -> None:
        thread = Thread(target=self._run_pull_secret_thread, daemon=True)
        thread.start()

    def _run_pull_secret_thread(self) -> None:
        while True:
            self._update_pull_secrets()
            sleep(300)

    def update_liveness_file_thread(self) -> None:
        thread = Thread(target=self._run_liveness_file_thread, daemon=True)
        thread.start()

    def _run_liveness_file_thread(self) -> None:
        while True:
            self._update_liveness_file()
            sleep(30)

    def _update_liveness_file(self) -> None:
        Path(LIVENESS_FILE).touch()

    def _create_secret_body(self) -> client.V1Secret:
        auth, server = get_container_auth()
        username = auth.get("username")
        password = auth.get("password")
        config_json = json.dumps(
            {
                "auths": {
                    server: {
                        "username": username,
                        "password": password,
                        "email": "unused",
                        "auth": _base64_str(f"{username}:{password}"),
                    }
                }
            }
        )
        data = {".dockerconfigjson": _base64_str(config_json)}
        secret = client.V1Secret(
            metadata=client.V1ObjectMeta(name=GRETEL_PULL_SECRET),
            data=data,
            type="kubernetes.io/dockerconfigjson",
        )
        return secret

    def _update_pull_secrets(self) -> None:
        try:
            self._core_api.read_namespaced_secret(
                GRETEL_PULL_SECRET, GRETEL_WORKER_NAMESPACE
            )
            secret = self._create_secret_body()
            logger.info("Updating pull secret")
            self._core_api.patch_namespaced_secret(
                GRETEL_PULL_SECRET, GRETEL_WORKER_NAMESPACE, secret
            )

        except client.ApiException as ex:
            err_resp = json.loads(ex.body)
            if err_resp["reason"] == "NotFound":
                logger.info("Creating pull secret")
                secret = self._create_secret_body()
                self._core_api.create_namespaced_secret(
                    namespace=GRETEL_WORKER_NAMESPACE, body=secret
                )


class KubernetesError(Exception):
    ...
