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
from typing import Optional, Type, TYPE_CHECKING, TypeVar

from kubernetes import client, config
from kubernetes.client import ApiClient, BatchV1Api, CoreV1Api

from gretel_client.agents.drivers.driver import Driver
from gretel_client.config import get_logger
from gretel_client.docker import get_container_auth

logger = get_logger(__name__)

GRETEL_WORKER_SA = os.getenv("GRETEL_WORKER_SA", "gretel-agent")
GRETEL_WORKER_NAMESPACE = os.getenv("GRETEL_WORKER_NAMESPACE", "gretel-workers")
GRETEL_PULL_SECRET = os.getenv("GRETEL_PULL_SECRET", "gretel-pull-secret")
LIVENESS_FILE = os.getenv("LIVENESS_FILE", "/tmp/liveness.txt")

T = TypeVar("T")

GPU_NODE_SELECTOR_ENV_NAME = "GPU_NODE_SELECTOR"
CPU_NODE_SELECTOR_ENV_NAME = "CPU_NODE_SELECTOR"
GPU_TOLERATIONS_ENV_NAME = "GPU_TOLERATIONS"
CPU_TOLERATIONS_ENV_NAME = "CPU_TOLERATIONS"
CPU_COUNT_ENV_NAME = "GRETEL_CPU_COUNT"


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

    def _create_kubernetes_job(self, agent_job: Job) -> Optional[client.V1Job]:
        """Creates a job for the input job config in the cluster pointed to by the input Api Client."""
        logger.info(f"Creating job:{agent_job.uid} in Kubernetes cluster.")
        kubernetes_job = self._build_job(agent_job)
        try:
            created_k8s_job: Optional[client.V1Job] = None
            try:
                k8s_job = self._batch_api.create_namespaced_job(
                    body=kubernetes_job, namespace=GRETEL_WORKER_NAMESPACE
                )
                created_k8s_job = k8s_job
            except client.ApiException as ex:
                err_resp = json.loads(ex.body)
                if err_resp.get("reason") != "AlreadyExists":
                    raise
                logger.warning(
                    f"job={agent_job.uid} already scheduled. Skipping job deployment."
                )
                k8s_job = self._batch_api.read_namespaced_job(
                    name=kubernetes_job.metadata.name,
                    namespace=GRETEL_WORKER_NAMESPACE,
                )

            job_secret = self._build_secret(agent_job, k8s_job)
            try:
                self._core_api.create_namespaced_secret(
                    body=job_secret,
                    namespace=GRETEL_WORKER_NAMESPACE,
                )
            except client.ApiException as ex:
                err_resp = json.loads(ex.body)
                if err_resp.get("reason") != "AlreadyExists":
                    raise
                logger.warning(
                    f"secret={agent_job.uid} already created. Skipping secret creation."
                )

            return created_k8s_job
        except client.ApiException as ex:
            err_resp = json.loads(ex.body)
            logger.error(f"Could not deploy job={agent_job.uid}")
            logger.error(err_resp)
            logger.error(traceback.format_exc())
            raise KubernetesError(
                f"Could not create job name={agent_job.uid} namespace={GRETEL_WORKER_NAMESPACE}"
            ) from ex
        except Exception as ex:
            logger.error(traceback.format_exc())
            raise ex

    def _build_job(self, job_config: Job) -> client.V1Job:
        memory_limit_in_gb = os.environ.get("MEMORY_LIMIT_IN_GB", 14)
        resource_requests = {"memory": f"{memory_limit_in_gb}Gi"}
        if job_config.needs_gpu:
            resource_requests["nvidia.com/gpu"] = "1"

        env = []
        if job_config.env_vars:
            env = [
                client.V1EnvVar(name=k, value=v) for k, v in job_config.env_vars.items()
            ]
        env.append(
            client.V1EnvVar(name="GRETEL_ENDPOINT", value=job_config.gretel_endpoint)
        )
        env.append(client.V1EnvVar(name="GRETEL_STAGE", value=job_config.gretel_stage))

        # Make a copy here in case we need to change the requests but not the limits
        limits = resource_requests.copy()
        # TODO: Separate value for CPU and GPU jobs?
        gretel_cpu_count = os.getenv(CPU_COUNT_ENV_NAME)
        resource_requests["cpu"] = "1"
        if gretel_cpu_count:
            if not gretel_cpu_count.isdigit():
                raise KubernetesError(
                    f"Gretel CPU Count must be an integer, instead received {gretel_cpu_count}"
                )
            env.append(client.V1EnvVar(name=CPU_COUNT_ENV_NAME, value=gretel_cpu_count))
            resource_requests["cpu"] = gretel_cpu_count

        args = list(itertools.chain.from_iterable(job_config.params.items()))

        container = client.V1Container(
            name=job_config.uid,
            image=job_config.container_image,
            resources=client.V1ResourceRequirements(
                requests=resource_requests, limits=limits
            ),
            args=args,
            env=env,
            image_pull_policy="IfNotPresent",
            env_from=[
                client.V1EnvFromSource(
                    secret_ref=client.V1SecretEnvSource(
                        name=job_config.uid, optional=False
                    ),
                ),
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
                automount_service_account_token=False,
                image_pull_secrets=[
                    client.V1LocalObjectReference(name=GRETEL_PULL_SECRET)
                ],
            ),
        )

        if job_config.needs_gpu:
            self._add_selector_if_present(template, GPU_NODE_SELECTOR_ENV_NAME)
            self._add_tolerations_if_present(template, GPU_TOLERATIONS_ENV_NAME)
        else:
            self._add_selector_if_present(template, CPU_NODE_SELECTOR_ENV_NAME)
            self._add_tolerations_if_present(template, CPU_TOLERATIONS_ENV_NAME)

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

    def _build_secret(self, job_config: Job, k8s_job: client.V1Job) -> client.V1Secret:
        return client.V1Secret(
            api_version="v1",
            kind="Secret",
            metadata=client.V1ObjectMeta(
                name=job_config.uid,
                owner_references=[
                    client.V1OwnerReference(
                        api_version=k8s_job.api_version,
                        kind=k8s_job.kind,
                        name=k8s_job.metadata.name,
                        uid=k8s_job.metadata.uid,
                        block_owner_deletion=False,
                        controller=False,
                    ),
                ],
            ),
            string_data=job_config.secret_env,
        )

    def _add_selector_if_present(
        self, template: client.V1PodTemplateSpec, env_var_name: str
    ) -> None:
        selector_dict = self._parse_kube_env_var(env_var_name, dict)

        if selector_dict:
            pod_spec: client.V1PodSpec = template.spec
            pod_spec.node_selector = selector_dict

    def _add_tolerations_if_present(
        self, template: client.V1PodTemplateSpec, env_var_name: str
    ) -> None:
        tolerations_list = self._parse_kube_env_var(env_var_name, list)

        if tolerations_list:
            pod_spec: client.V1PodSpec = template.spec
            pod_spec.tolerations = tolerations_list

    def _parse_kube_env_var(
        self, env_var_name: str, expected_type: Type[T]
    ) -> Optional[T]:
        env_var_value = os.getenv(env_var_name, "")
        if not env_var_value:
            return None
        try:
            result = json.loads(env_var_value)
        except json.decoder.JSONDecodeError as ex:
            raise KubernetesError(
                f"Could not deserialize JSON for {env_var_name}"
            ) from ex
        if not isinstance(result, expected_type):
            raise KubernetesError(
                f"The {env_var_name} variable was not a JSON {expected_type.__name__}, received {env_var_value}"
            )
        return result

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
