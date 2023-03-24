"""
Support for running local docker workers.
"""

from __future__ import annotations

import base64
import itertools
import json
import os
import traceback

from threading import Thread
from time import sleep
from typing import List, Optional, Tuple, Type, TYPE_CHECKING, TypeVar

from kubernetes import client, config
from kubernetes.client import ApiClient, BatchV1Api, CoreV1Api

from gretel_client.agents.drivers.driver import Driver
from gretel_client.config import get_logger
from gretel_client.docker import get_container_auth

logger = get_logger(__name__)

T = TypeVar("T")

OVERRIDE_CERT_NAME = "override-cert"

WORKER_NAMESPACE_ENV_NAME = "GRETEL_WORKER_NAMESPACE"
PULL_SECRET_ENV_NAME = "GRETEL_PULL_SECRET"
GPU_NODE_SELECTOR_ENV_NAME = "GPU_NODE_SELECTOR"
CPU_NODE_SELECTOR_ENV_NAME = "CPU_NODE_SELECTOR"
GPU_TOLERATIONS_ENV_NAME = "GPU_TOLERATIONS"
CPU_TOLERATIONS_ENV_NAME = "CPU_TOLERATIONS"
CPU_COUNT_ENV_NAME = "GRETEL_CPU_COUNT"
CA_CERT_CONFIGMAP_ENV_NAME = "GRETEL_CA_CERT_CONFIGMAP_NAME"
IMAGE_REGISTRY_OVERRIDE_HOST_ENV_NAME = "IMAGE_REGISTRY_OVERRIDE_HOST"


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
        self._load_env_and_set_vars()
        worker = KubernetesDriverDaemon(
            agent_config=agent_config, core_api=self._core_api
        )
        worker.start_update_pull_secret_thread()

    def _load_env_and_set_vars(self):
        self._memory_limit_in_gb = os.getenv("MEMORY_LIMIT_IN_GB") or "14"
        self._gretel_cpu_count = os.getenv(CPU_COUNT_ENV_NAME) or "1"
        self._cert_override = os.getenv(CA_CERT_CONFIGMAP_ENV_NAME)

        self._gpu_node_selector = self._parse_kube_env_var(
            GPU_NODE_SELECTOR_ENV_NAME, dict
        )
        self._cpu_node_selector = self._parse_kube_env_var(
            CPU_NODE_SELECTOR_ENV_NAME, dict
        )
        self._gpu_tolerations = self._parse_kube_env_var(GPU_TOLERATIONS_ENV_NAME, list)
        self._cpu_tolerations = self._parse_kube_env_var(CPU_TOLERATIONS_ENV_NAME, list)

        self._gretel_worker_sa = os.getenv("GRETEL_WORKER_SA") or "gretel-agent"
        self._gretel_worker_secret_name = (
            os.getenv("GRETEL_WORKER_SECRET_NAME") or "gretel-worker-secret"
        )
        self._gretel_worker_namespace = (
            os.getenv(WORKER_NAMESPACE_ENV_NAME) or "gretel-workers"
        )
        self._gretel_pull_secret = (
            os.getenv(PULL_SECRET_ENV_NAME) or "gretel-pull-secret"
        )
        self._override_host = os.environ.get(IMAGE_REGISTRY_OVERRIDE_HOST_ENV_NAME)

    def _parse_kube_env_var(
        self, env_var_name: str, expected_type: Type[T]
    ) -> Optional[T]:
        env_var_value = os.getenv(env_var_name)
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
                    body=kubernetes_job, namespace=self._gretel_worker_namespace
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
                    namespace=self._gretel_worker_namespace,
                )

            job_secret = self._build_secret(agent_job, k8s_job)
            try:
                self._core_api.create_namespaced_secret(
                    body=job_secret,
                    namespace=self._gretel_worker_namespace,
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
                f"Could not create job name={agent_job.uid} namespace={self._gretel_worker_namespace}"
            ) from ex
        except Exception as ex:
            logger.error(traceback.format_exc())
            raise ex

    def _build_job(self, job_config: Job) -> client.V1Job:
        resource_requests, limits = self._setup_resources(job_config)

        env = self._setup_environment_variables(job_config)

        args = list(itertools.chain.from_iterable(job_config.params.items()))

        image = self._resolve_image(job_config)

        container = client.V1Container(
            name=job_config.uid,
            image=image,
            resources=client.V1ResourceRequirements(
                requests=resource_requests, limits=limits
            ),
            args=args,
            env=env,
            image_pull_policy="IfNotPresent",
            env_from=[
                client.V1EnvFromSource(
                    secret_ref=client.V1SecretEnvSource(
                        name=self._gretel_worker_secret_name
                    )
                ),
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
                namespace=self._gretel_worker_namespace,
            ),
            spec=client.V1PodSpec(
                restart_policy="Never",
                containers=[container],
                service_account_name=self._gretel_worker_sa,
                automount_service_account_token=False,
                image_pull_secrets=[
                    client.V1LocalObjectReference(name=self._gretel_pull_secret)
                ],
            ),
        )

        self._setup_certificate(container, template)

        if job_config.needs_gpu:
            self._add_selector_if_present(template, self._gpu_node_selector)
            self._add_tolerations_if_present(template, self._gpu_tolerations)
        else:
            self._add_selector_if_present(template, self._cpu_node_selector)
            self._add_tolerations_if_present(template, self._cpu_tolerations)

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

    def _setup_resources(self, job_config: Job) -> Tuple[dict, dict]:
        resource_requests = {"memory": f"{self._memory_limit_in_gb}Gi"}
        if job_config.needs_gpu:
            resource_requests["nvidia.com/gpu"] = "1"

        limits = resource_requests.copy()

        if not self._gretel_cpu_count.isdigit():
            raise KubernetesError(
                f"Gretel CPU Count must be an integer, instead received {self._gretel_cpu_count}"
            )
        resource_requests["cpu"] = self._gretel_cpu_count

        return resource_requests, limits

    def _setup_environment_variables(self, job_config: Job) -> List[client.V1EnvVar]:
        env = []
        if job_config.env_vars:
            env = [
                client.V1EnvVar(name=k, value=v) for k, v in job_config.env_vars.items()
            ]
        env.append(
            client.V1EnvVar(name="GRETEL_ENDPOINT", value=job_config.gretel_endpoint)
        )
        env.append(client.V1EnvVar(name="GRETEL_STAGE", value=job_config.gretel_stage))

        gretel_cpu_count = self._gretel_cpu_count
        if gretel_cpu_count:
            env.append(client.V1EnvVar(name=CPU_COUNT_ENV_NAME, value=gretel_cpu_count))

        return env

    def _setup_certificate(
        self, container: client.V1Container, template: client.V1PodTemplateSpec
    ):
        if not self._cert_override:
            return
        if container.volume_mounts is None:
            container.volume_mounts = []
        container.volume_mounts += [
            client.V1VolumeMount(
                mount_path="/usr/local/share/ca-certificates/",
                name=OVERRIDE_CERT_NAME,
            )
        ]

        pod_spec: client.V1PodSpec = template.spec

        if pod_spec.volumes is None:
            pod_spec.volumes = []
        pod_spec.volumes += [
            client.V1Volume(
                name=OVERRIDE_CERT_NAME,
                config_map=client.V1ConfigMapVolumeSource(
                    name=self._cert_override,
                    default_mode=0o644,
                    optional=True,
                ),
            )
        ]

    def _resolve_image(self, job_config: Job) -> str:
        image = job_config.container_image
        if self._override_host:
            image_parts = image.split("/")
            if len(image_parts) > 1:
                image_parts[0] = self._override_host
                image = "/".join(image_parts)
        return image

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
        self, template: client.V1PodTemplateSpec, selector_dict: dict
    ) -> None:
        if selector_dict:
            pod_spec: client.V1PodSpec = template.spec
            pod_spec.node_selector = selector_dict

    def _add_tolerations_if_present(
        self, template: client.V1PodTemplateSpec, tolerations_list: list
    ) -> None:
        if tolerations_list:
            pod_spec: client.V1PodSpec = template.spec
            pod_spec.tolerations = tolerations_list

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
                    f"Could not delete job name={job.metadata.name} namespace={self._gretel_worker_namespace}"
                ) from ex

    def _is_job_active(self, job: client.V1Job) -> bool:
        job_resp: client.V1Job = self._batch_api.read_namespaced_job(
            job.metadata.name, namespace=self._gretel_worker_namespace
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
        self, agent_config: AgentConfig, core_api: CoreV1Api, sleep_length: int = 300
    ):
        self._agent_config = agent_config
        config.load_incluster_config()
        self._core_api = core_api
        self.sleep_length = sleep_length
        self._gretel_worker_namespace = (
            os.getenv(WORKER_NAMESPACE_ENV_NAME) or "gretel-workers"
        )
        self._gretel_pull_secret = (
            os.getenv(PULL_SECRET_ENV_NAME) or "gretel-pull-secret"
        )
        self._override_host = os.getenv(IMAGE_REGISTRY_OVERRIDE_HOST_ENV_NAME)

    def start_update_pull_secret_thread(self) -> None:
        thread = Thread(target=self._run_pull_secret_thread, daemon=True)
        thread.start()

    def _run_pull_secret_thread(self) -> None:
        while True:
            try:
                self._update_pull_secrets()
            # We don't want the thread to die unless a
            # keyboard interrupt occurs
            except KeyboardInterrupt as ex:
                logger.info("Exiting early")
                raise ex
            except Exception:
                logger.exception("Error updating pull secret")
            sleep(self.sleep_length)

    def _create_secret_body(self) -> client.V1Secret:
        auth, server = get_container_auth()
        if self._override_host:  # We need to authenticate to the appropriate registry
            server = self._override_host
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
            metadata=client.V1ObjectMeta(name=self._gretel_pull_secret),
            data=data,
            type="kubernetes.io/dockerconfigjson",
        )
        return secret

    def _update_pull_secrets(self) -> None:
        try:
            self._core_api.read_namespaced_secret(
                self._gretel_pull_secret, self._gretel_worker_namespace
            )
            secret = self._create_secret_body()
            logger.info("Updating pull secret")
            self._core_api.patch_namespaced_secret(
                self._gretel_pull_secret, self._gretel_worker_namespace, secret
            )

        except client.ApiException as ex:
            err_resp = json.loads(ex.body)
            if err_resp["reason"] == "NotFound":
                logger.info("Creating pull secret")
                secret = self._create_secret_body()
                self._core_api.create_namespaced_secret(
                    namespace=self._gretel_worker_namespace, body=secret
                )
            else:
                raise ex


class KubernetesError(Exception):
    ...
