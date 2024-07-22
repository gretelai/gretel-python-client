"""
Support for running local docker workers.
"""

from __future__ import annotations

import base64
import contextlib
import datetime
import json
import math
import os
import traceback

from _thread import interrupt_main
from copy import deepcopy
from operator import itemgetter
from pathlib import Path
from threading import Condition, Thread
from time import sleep
from typing import List, Optional, Tuple, Type, TYPE_CHECKING, TypeVar

from kubernetes import client, config
from kubernetes.client import ApiClient, ApiException, BatchV1Api, CoreV1Api
from kubernetes.utils.quantity import parse_quantity
from kubernetes.watch.watch import Watch

from gretel_client.agents.drivers.driver import Driver
from gretel_client.config import ClientConfig, get_logger
from gretel_client.docker import get_container_auth

logger = get_logger(__name__)

T = TypeVar("T")

OVERRIDE_CERT_NAME = "override-cert"

WORKER_NAMESPACE_ENV_NAME = "GRETEL_WORKER_NAMESPACE"
WORKER_RESOURCES_ENV_NAME = "GRETEL_WORKER_RESOURCES"
CPU_MODEL_WORKER_RESOURCES_ENV_NAME = "GRETEL_MODEL_WORKER_CPU_RESOURCES"
GPU_MODEL_WORKER_RESOURCES_ENV_NAME = "GRETEL_MODEL_WORKER_GPU_RESOURCES"
WORKER_MEMORY_GB_ENV_NAME = "MEMORY_LIMIT_IN_GB"
GRETEL_MODEL_TYPE_ENV_NAME = "GRETEL_MODEL_TYPE"
PULL_SECRET_ENV_NAME = "GRETEL_PULL_SECRET"
PULL_SECRETS_ENV_NAME = "GRETEL_PULL_SECRETS"
GPU_NODE_SELECTOR_ENV_NAME = "GPU_NODE_SELECTOR"
CPU_NODE_SELECTOR_ENV_NAME = "CPU_NODE_SELECTOR"
GPU_TOLERATIONS_ENV_NAME = "GPU_TOLERATIONS"
CPU_TOLERATIONS_ENV_NAME = "CPU_TOLERATIONS"
GPU_AFFINITY_ENV_NAME = "GPU_AFFINITY"
CPU_AFFINITY_ENV_NAME = "CPU_AFFINITY"
CPU_COUNT_ENV_NAME = "GRETEL_CPU_COUNT"
CA_CERT_CONFIGMAP_ENV_NAME = "GRETEL_CA_CERT_CONFIGMAP_NAME"
IMAGE_REGISTRY_OVERRIDE_HOST_ENV_NAME = "IMAGE_REGISTRY_OVERRIDE_HOST"
IMAGE_REGISTRY_OVERRIDE_TAG_ENV_NAME = "IMAGE_REGISTRY_OVERRIDE_TAG"
USES_DOCKERHUB_IMAGE_FORMAT_ENV_NAME = "USES_DOCKERHUB_IMAGE_FORMAT"
PREVENT_AUTOSCALER_EVICTION_ENV_NAME = "PREVENT_AUTOSCALER_EVICTION"
WORKER_POD_LABELS_ENV_NAME = "WORKER_POD_LABELS"
WORKER_POD_ANNOTATIONS_ENV_NAME = "WORKER_POD_ANNOTATIONS"
COMMON_ENV_SECRET_NAME_ENV_NAME = "GRETEL_COMMON_ENV_SECRET_NAME"
COMMON_DATA_SECRET_NAME_ENV_NAME = "GRETEL_COMMON_DATA_SECRET_NAME"
COMMON_DATA_MOUNT_PATH_ENV_NAME = "GRETEL_COMMON_DATA_MOUNT_PATH"
LIVENESS_FILE_ENV_NAME = "LIVENESS_FILE"
GPU_SEC_CONTEXT_ENV_NAME = "GPU_WORKER_SECURITY_CONTEXT"
CPU_SEC_CONTEXT_ENV_NAME = "CPU_WORKER_SECURITY_CONTEXT"
JOB_BACKOFF_LIMIT_ENV_NAME = "JOB_BACKOFF_LIMIT"

GRETEL_CLUSTER_SECRET_NAME_ENV_NAME = "GRETEL_CLUSTER_SECRET_NAME"
GRETEL_CLUSTER_ID_KEY = "GRETEL_CLUSTER_ID"

LAST_UPDATED_ANNOTATION = "lastUpdated"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
if TYPE_CHECKING:
    from gretel_client.agents.agent import AgentConfig, Job


class FakeKubeResponse:
    def __init__(self, obj):
        self.data = json.dumps(obj)


def _base64_str(my_str: str) -> str:
    return base64.b64encode(my_str.encode("ascii")).decode("ascii")


def _create_secret_json_b64(server: str, username: str, password: str):
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
    return data


def _ensure_valid_quantity(qstr: str, label: str):
    try:
        parse_quantity(qstr)
    except ValueError as ex:
        raise KubernetesError(f"{label} must be a valid quantity, got: {qstr}") from ex


def _set_default_resources(resources: dict) -> dict:
    resources["requests"].setdefault("cpu", "1")
    # Memory request default should be 14Gi, but if a limit is specified, use that.
    resources["requests"].setdefault(
        "memory", resources["limits"].get("memory", "14Gi")
    )
    resources["requests"].setdefault(
        "ephemeral-storage", resources["limits"].get("ephemeral-storage", "50Gi")
    )
    resources["limits"].setdefault("memory", resources["requests"].get("memory"))
    return resources


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
        self._agent_config = agent_config
        self._worker = None
        self._load_env_and_set_vars()

    def _load_and_setup_worker_resources(self):
        requests, limits = itemgetter("requests", "limits")(
            self._load_resources_from_env_var(WORKER_RESOURCES_ENV_NAME)
        )

        if env_mem_limit := os.getenv(WORKER_MEMORY_GB_ENV_NAME, "").strip():
            try:
                if int(env_mem_limit) <= 0:
                    raise ValueError("memory limit must be positive")
            except ValueError as ex:
                raise KubernetesError(
                    f"{WORKER_MEMORY_GB_ENV_NAME} must be a positive integer, got: {env_mem_limit}"
                ) from ex

            if "memory" in requests or "memory" in limits:
                logger.warning(
                    f"ignoring '{WORKER_MEMORY_GB_ENV_NAME}' environment variable as memory resources are set via '{WORKER_RESOURCES_ENV_NAME}'"
                )
            else:
                quantity_str = f"{env_mem_limit.strip()}Gi"
                requests["memory"] = quantity_str
                limits["memory"] = quantity_str

        if env_cpu_count := os.getenv(CPU_COUNT_ENV_NAME, "").strip():
            try:
                if int(env_cpu_count) <= 0:
                    raise ValueError("CPU count must be positive")
            except ValueError as ex:
                raise KubernetesError(
                    f"Gretel CPU Count must be a positive integer, instead received {env_cpu_count}"
                ) from ex

            if "cpu" in requests or "cpu" in limits:
                logger.warning(
                    f"ignoring '{CPU_COUNT_ENV_NAME}' as CPU resources are set via '{WORKER_RESOURCES_ENV_NAME}'"
                )
            else:
                requests["cpu"] = env_cpu_count
                limits["cpu"] = env_cpu_count

        self._cpu_worker_resources = {"requests": requests, "limits": limits}
        self._gpu_worker_resources = {"requests": requests, "limits": limits}

        # Override top level resources (or defaults) with granular values
        cpu_worker_requests, cpu_worker_limits = itemgetter("requests", "limits")(
            self._load_resources_from_env_var(CPU_MODEL_WORKER_RESOURCES_ENV_NAME)
        )
        if cpu_worker_requests or cpu_worker_limits:
            self._cpu_worker_resources = {
                "requests": cpu_worker_requests,
                "limits": cpu_worker_limits,
            }
        gpu_worker_requests, gpu_worker_limits = itemgetter("requests", "limits")(
            self._load_resources_from_env_var(GPU_MODEL_WORKER_RESOURCES_ENV_NAME)
        )
        if gpu_worker_requests or gpu_worker_limits:
            self._gpu_worker_resources = {
                "requests": gpu_worker_requests,
                "limits": gpu_worker_limits,
            }

        self._cpu_worker_resources = _set_default_resources(self._cpu_worker_resources)
        self._gpu_worker_resources = _set_default_resources(self._gpu_worker_resources)

    def _load_resources_from_env_var(self, env_var_name: str) -> dict:
        requests, limits = {}, {}

        if env_resources := self._parse_kube_env_var(env_var_name, dict):
            requests.update(
                **{k: str(v) for k, v in env_resources.pop("requests", {}).items()}
            )
            limits.update(
                **{k: str(v) for k, v in env_resources.pop("limits", {}).items()}
            )
            if env_resources:
                raise KubernetesError(
                    f"resources specified via '{env_var_name}' invalid: only 'requests' and 'limits' are allowed, extra keys: {','.join(env_resources.keys())}"
                )
            # Ensure that quantities supplied via env resources are valid
            for k, v in requests.items():
                _ensure_valid_quantity(v, f"requests.{k} in '{env_var_name}' value")
            for k, v in limits.items():
                _ensure_valid_quantity(v, f"limits.{k} in '{env_var_name}' value")

        return {"requests": requests, "limits": limits}

    def _load_env_and_set_vars(self, restart_worker: bool = False):
        self._load_and_setup_worker_resources()
        self._cert_override = os.getenv(CA_CERT_CONFIGMAP_ENV_NAME)
        self._common_env_secret_name = os.getenv(COMMON_ENV_SECRET_NAME_ENV_NAME)
        self._common_data_secret_name = os.getenv(COMMON_DATA_SECRET_NAME_ENV_NAME)
        self._common_data_mount_path = os.getenv(COMMON_DATA_MOUNT_PATH_ENV_NAME)

        self._gpu_node_selector = self._parse_kube_env_var(
            GPU_NODE_SELECTOR_ENV_NAME, dict
        )
        self._cpu_node_selector = self._parse_kube_env_var(
            CPU_NODE_SELECTOR_ENV_NAME, dict
        )
        self._gpu_tolerations = self._parse_kube_env_var(GPU_TOLERATIONS_ENV_NAME, list)
        self._cpu_tolerations = self._parse_kube_env_var(CPU_TOLERATIONS_ENV_NAME, list)

        self._gpu_affinity = self._parse_kube_env_var(GPU_AFFINITY_ENV_NAME, dict)
        self._cpu_affinity = self._parse_kube_env_var(CPU_AFFINITY_ENV_NAME, dict)

        self._gretel_worker_sa = os.getenv("GRETEL_WORKER_SA") or "gretel-agent"
        self._gretel_worker_secret_name = (
            os.getenv("GRETEL_WORKER_SECRET_NAME") or "gretel-worker-secret"
        )
        self._gpu_security_context = self._parse_kube_env_var(
            GPU_SEC_CONTEXT_ENV_NAME, dict
        )
        self._cpu_security_context = self._parse_kube_env_var(
            CPU_SEC_CONTEXT_ENV_NAME, dict
        )
        self._gretel_worker_namespace = (
            os.getenv(WORKER_NAMESPACE_ENV_NAME) or "gretel-workers"
        )
        self._gretel_pull_secret = (
            os.getenv(PULL_SECRET_ENV_NAME) or "gretel-pull-secret"
        )
        pull_secrets_list = self._parse_kube_env_var(PULL_SECRETS_ENV_NAME, list) or []
        self._gretel_pull_secrets = [
            {"name": secret_name} for secret_name in pull_secrets_list
        ] or [{"name": self._gretel_pull_secret}]

        self._worker_pod_labels = (
            self._parse_kube_env_var(WORKER_POD_LABELS_ENV_NAME, dict) or {}
        )
        if "app" in self._worker_pod_labels:
            logger.warning(
                f"User-provided worker pod label app={self._worker_pod_labels['app']} will be ignored, 'app' is a reserved label key and cannot be user-specified"
            )
        self._worker_pod_annotations = (
            self._parse_kube_env_var(WORKER_POD_ANNOTATIONS_ENV_NAME, dict) or {}
        )
        if os.environ.get(PREVENT_AUTOSCALER_EVICTION_ENV_NAME) == "true":
            if (
                "cluster-autoscaler.kubernetes.io/safe-to-evict"
                not in self._worker_pod_annotations
            ):
                self._worker_pod_annotations[
                    "cluster-autoscaler.kubernetes.io/safe-to-evict"
                ] = "false"
            else:
                logger.warning(
                    f"ignoring '{PREVENT_AUTOSCALER_EVICTION_ENV_NAME}' setting as '{WORKER_POD_ANNOTATIONS_ENV_NAME}' already contains key 'cluster-autoscaler.kubernetes.io/safe-to-evict'"
                )

        if restart_worker or self._worker is None:
            if self._worker is not None:
                self._worker.stop()

            self._worker = KubernetesDriverDaemon(
                agent_config=self._agent_config, core_api=self._core_api
            )
            self._worker.start_update_pull_secret_thread()
        self._job_backoff_limit = int(os.getenv(JOB_BACKOFF_LIMIT_ENV_NAME, "0"))

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

    def has_errored(self, unit: client.V1Job) -> bool:
        if unit:
            return self._has_job_errored(unit)
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
        resource_requests, resource_limits = self._setup_resources(job_config)
        resources = client.V1ResourceRequirements(
            requests=resource_requests,
            limits=resource_limits,
        )

        env = self._setup_environment_variables(job_config, resources)

        # K8s agent supports secret environment via creating an ephemeral secret, so
        # no need for the insecure --worker_token argument.
        # This breaks compatibility with very old (pre-2023) images, but this seems
        # tolerable, especially for hybrid users.
        job_params = dict(job_config.params)
        job_params.pop("--worker_token", None)
        args = [
            arg for k, v in job_params.items() for arg in ((k, v) if v != "" else (k,))
        ]

        image = self._resolve_image(job_config)

        env_from = [
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
        ]

        if self._common_env_secret_name:
            env_from.append(
                client.V1EnvFromSource(
                    secret_ref=client.V1SecretEnvSource(
                        name=self._common_env_secret_name,
                    )
                )
            )

        volumes = []
        volume_mounts = []
        if self._common_data_secret_name:
            volumes.append(
                client.V1Volume(
                    name="common-data",
                    secret=client.V1SecretVolumeSource(
                        secret_name=self._common_data_secret_name,
                    ),
                )
            )
            volume_mounts.append(
                client.V1VolumeMount(
                    name="common-data",
                    mount_path=self._common_data_mount_path,
                )
            )
        run_path = "/run"
        for name, path in [("tmp", "/tmp"), ("run", run_path)]:
            volumes.append(
                client.V1Volume(
                    name=name,
                    empty_dir=client.V1EmptyDirVolumeSource(),
                )
            )
            volume_mounts.append(
                client.V1VolumeMount(
                    name=name,
                    mount_path=path,
                )
            )

        container = client.V1Container(
            name=job_config.uid,
            image=image,
            resources=resources,
            args=args,
            env=env,
            image_pull_policy="IfNotPresent",
            env_from=env_from,
            volume_mounts=volume_mounts,
        )
        labels = deepcopy(self._worker_pod_labels)
        labels["app"] = "gretel-jobs-worker"
        annotations = deepcopy(self._worker_pod_annotations)
        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(
                labels=labels,
                namespace=self._gretel_worker_namespace,
                annotations=annotations,
            ),
            spec=client.V1PodSpec(
                restart_policy="Never",
                containers=[container],
                service_account_name=self._gretel_worker_sa,
                automount_service_account_token=False,
                image_pull_secrets=self._gretel_pull_secrets,
                volumes=volumes,
            ),
        )

        self._setup_certificate(container, template)

        if job_config.needs_gpu:
            self._add_selector_if_present(template, self._gpu_node_selector)
            self._add_tolerations_if_present(template, self._gpu_tolerations)
            self._add_affinity_if_present(template, self._gpu_affinity)
            self._add_security_context_if_present(
                template, self._gpu_security_context, run_path
            )
        else:
            self._add_selector_if_present(template, self._cpu_node_selector)
            self._add_tolerations_if_present(template, self._cpu_tolerations)
            self._add_affinity_if_present(template, self._cpu_affinity)
            self._add_security_context_if_present(
                template, self._cpu_security_context, run_path
            )

        spec = client.V1JobSpec(
            template=template,
            backoff_limit=self._job_backoff_limit,
            ttl_seconds_after_finished=86400,
        )

        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(name=job_config.uid),
            spec=spec,
        )

        return job

    def _setup_resources(self, job_config: Job) -> Tuple[dict, dict]:
        worker_resources = deepcopy(self._cpu_worker_resources)
        if job_config.needs_gpu:
            worker_resources = deepcopy(self._gpu_worker_resources)
            worker_resources["requests"]["nvidia.com/gpu"] = "1"
            worker_resources["limits"]["nvidia.com/gpu"] = "1"

        return worker_resources["requests"], worker_resources["limits"]

    def _setup_environment_variables(
        self, job_config: Job, resources: client.V1ResourceRequirements
    ) -> List[client.V1EnvVar]:
        env = []
        if job_config.env_vars:
            env = [
                client.V1EnvVar(name=k, value=v) for k, v in job_config.env_vars.items()
            ]
        env.append(
            client.V1EnvVar(name="GRETEL_ENDPOINT", value=job_config.gretel_endpoint)
        )
        env.append(client.V1EnvVar(name="GRETEL_STAGE", value=job_config.gretel_stage))

        env.append(
            client.V1EnvVar(
                name=GRETEL_MODEL_TYPE_ENV_NAME, value=job_config.model_type
            )
        )

        if cpu_limit := resources.limits.get("cpu"):
            cpu_quantity = parse_quantity(cpu_limit)
            cpu_count = max(math.floor(cpu_quantity), 1)
            env.append(client.V1EnvVar(name=CPU_COUNT_ENV_NAME, value=str(cpu_count)))

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
        if registry_host := self._worker.get_registry_host():
            image_parts = image.split("/")
            # Special case for pull through cache registries,
            # which are mirrored from DockerHub. DockerHub doesn't allow slashes in the name
            # We'll instead release the images with gretelai/image-separated-by-dashes
            if registry_host and self._worker.uses_dockerhub_format:
                start_index = 0
                if len(image_parts) != 1 and any(
                    ["." in image_parts[0], ":" in image_parts[0]]
                ):
                    start_index = 1
                image_name_with_dashes = "-".join(image_parts[start_index:])
                image = f"{registry_host}/{image_name_with_dashes}"

            elif len(image_parts) > 1:
                image_parts[0] = registry_host
                image = "/".join(image_parts)
            elif len(image_parts) == 1 and image_parts[0].startswith("models"):
                image = f"{registry_host}/{image_parts[0]}"
            # The override_tag will take precedence over a sha256 value if specified, or
            # over the original tag
            # Matches image_name:latest@sha256:abc12345
            if self._worker.override_tag and image.count(":") == 2:
                image = image.split(":")[0] + ":" + self._worker.override_tag
            # Matches image_name@sha256:abc12345
            elif self._worker.override_tag and "@sha256" in image:
                image = image.split("@sha256")[0] + ":" + self._worker.override_tag
            # Matches image_name:abc12345
            elif self._worker.override_tag and image.count(":") == 1:
                image = image.split(":")[0] + ":" + self._worker.override_tag

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

    def _add_affinity_if_present(
        self, template: client.V1PodTemplateSpec, affinity_dict: dict
    ) -> None:
        if affinity_dict:
            pod_spec: client.V1PodSpec = template.spec
            pod_spec.affinity = affinity_dict

    def _add_tolerations_if_present(
        self, template: client.V1PodTemplateSpec, tolerations_list: list
    ) -> None:
        if tolerations_list:
            pod_spec: client.V1PodSpec = template.spec
            pod_spec.tolerations = tolerations_list

    def _add_security_context_if_present(
        self,
        template: client.V1PodTemplateSpec,
        security_context: dict,
        running_path: str,
    ) -> None:
        if security_context:
            pod_spec: client.V1PodSpec = template.spec
            container: client.V1Container = pod_spec.containers[0]
            container.security_context = security_context
            # We need to run from a /run emptydir if we are not running as root
            # or readOnlyRootFileSystem is true, but older containers won't
            # necessarily have this set
            container.working_dir = running_path
            # Set the home directory to be the writable /run path as well
            container.env.append(client.V1EnvVar(name="HOME", value=running_path))

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
        job_resp: Optional[client.V1Job] = self._maybe_read_namespaced_job(job)
        if not job_resp:
            return False
        status: Optional[client.V1JobStatus] = job_resp.status
        # If a job doesn't have a status, this can only happen because it doesn't have
        # a status *yet*, and a job about to launch should be treated as active.
        if not status:
            return True
        # Only the job conditions provide an authoritative answer whether or not the job
        # has terminated. A job otherwise can have some failed attempts and no active
        # attempts in spite of still being active, e.g., in between retries.
        return all(
            not cond.status
            for cond in (status.conditions or [])
            if cond.type in ("Complete", "Failed")
        )

    def _has_job_errored(self, job: client.V1Job):
        job_resp: Optional[client.V1Job] = self._maybe_read_namespaced_job(job)
        if not job_resp:  # Assume a job that doesn't exist is not "errored"
            return False
        status: Optional[client.V1JobStatus] = job_resp.status
        if status:
            return bool(status.failed)
        return False  # Assume a job with no status hasn't failed

    def _maybe_read_namespaced_job(self, job: client.V1Job) -> Optional[client.V1Job]:
        try:
            return self._batch_api.read_namespaced_job(
                job.metadata.name, namespace=self._gretel_worker_namespace
            )
        except Exception:
            logger.exception(f"Error reading job {job.metadata.name}")

        return None


class KubernetesDriverDaemon:
    """
    Represents background tasks performed by the k8s driver
    """

    def __init__(
        self,
        agent_config: AgentConfig,
        core_api: CoreV1Api,
        sleep_length: int = 3600,
        sleep_length_retry: int = 30,
    ):
        self._agent_config = agent_config
        config.load_incluster_config()
        self._core_api = core_api
        self.sleep_length = sleep_length
        self.sleep_length_retry = sleep_length_retry
        self._gretel_worker_namespace = (
            os.getenv(WORKER_NAMESPACE_ENV_NAME) or "gretel-workers"
        )
        self._gretel_pull_secret = (
            os.getenv(PULL_SECRET_ENV_NAME) or "gretel-pull-secret"
        )
        self._override_host = os.getenv(IMAGE_REGISTRY_OVERRIDE_HOST_ENV_NAME)
        self.override_tag = os.getenv(IMAGE_REGISTRY_OVERRIDE_TAG_ENV_NAME)
        self.uses_dockerhub_format = (
            os.getenv(USES_DOCKERHUB_IMAGE_FORMAT_ENV_NAME) == "true"
        )

        self._registry_host = ""
        self._registry_host_available = Condition()

        self._stop = False
        self._stop_cond = Condition()

        self._liveness_file_path = Path(
            os.getenv(LIVENESS_FILE_ENV_NAME) or "/tmp/liveness.txt"
        )

        self._pull_secret_thread = Thread(
            target=self._run_pull_secret_thread, daemon=True
        )

        self._minutes_stale = 11 * 60  # 11 hours

        self._cluster_id = agent_config.cluster_guid
        self._cluster_secret_name = os.getenv(GRETEL_CLUSTER_SECRET_NAME_ENV_NAME)
        if self._cluster_secret_name and not self._cluster_id:
            raise ValueError(
                "a cluster secret name is configured, but no cluster ID was specified in the environment"
            )

        self._monitor_cluster_secret_thread = Thread(
            target=self._run_monitor_cluster_secret_thread,
            daemon=True,
        )

    @property
    def session(self) -> ClientConfig:
        return self._agent_config.session

    def stop(self):
        with self._stop_cond:
            self._stop = True
            self._stop_cond.notify_all()

    def _should_stop(self) -> bool:
        with self._stop_cond:
            return self._stop

    def _sleep(self, secs):
        with self._stop_cond:
            if self._stop:
                return
            self._stop_cond.wait(timeout=secs)

    def get_registry_host(self) -> str:
        if self._override_host:
            return self._override_host

        with self._registry_host_available:
            while not self._registry_host:
                logger.info(
                    "Waiting for image pull credentials to be updated for the first time ..."
                )
                self._registry_host_available.wait()
            return self._registry_host

    def start_update_pull_secret_thread(self) -> None:
        healthcheck_thread = Thread(target=self._run_healthcheck_thread, daemon=True)
        self._pull_secret_thread.start()
        self._monitor_cluster_secret_thread.start()
        healthcheck_thread.start()
        with self._stop_cond:
            self._stop = False

    def _run_pull_secret_thread(self) -> None:
        while not self._should_stop():
            sleep_length = self.sleep_length
            try:
                self._update_pull_secrets()
            # We don't want the thread to die unless a
            # test emitted exception occurs
            except StopThread as ex:
                logger.info("Exiting early")
                raise ex
            except BaseException:
                logger.exception("Error updating pull secret")
                sleep_length = self.sleep_length_retry

            self._sleep(sleep_length)

    def _create_secret_body(self) -> client.V1Secret:
        auth, server = get_container_auth(session=self.session)
        if self._override_host:  # We need to authenticate to the appropriate registry
            server = self._override_host
        with self._registry_host_available:
            self._registry_host = server
            self._registry_host_available.notify_all()

        username = auth.get("username")
        password = auth.get("password")
        data = _create_secret_json_b64(server, username, password)
        current_time_str = datetime.datetime.now().strftime(DATETIME_FORMAT)
        secret = client.V1Secret(
            metadata=client.V1ObjectMeta(
                name=self._gretel_pull_secret,
                annotations={LAST_UPDATED_ANNOTATION: current_time_str},
            ),
            data=data,
            type="kubernetes.io/dockerconfigjson",
        )
        return secret

    def _read_secret(self) -> client.V1Secret:
        return self._core_api.read_namespaced_secret(
            self._gretel_pull_secret, self._gretel_worker_namespace
        )

    def _update_pull_secrets(self) -> None:
        try:
            # Read the secret to see if we need to create it
            self._read_secret()
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

    def _run_healthcheck_thread(self) -> None:
        """This thread monitors the pull secret thread, in case that
        thread unexpectedly dies, or stops writing the pull secret"""
        while not self._should_stop():
            # Check if the pull secret exists and won't expire
            try:
                self._run_healthchecks()
                self._update_liveness_file()
            except Exception as e:
                logger.warning("Healthcheck Failed: %s", str(e))

            sleep(self.sleep_length_retry)

    def _run_healthchecks(self) -> bool:
        try:
            secret = self._read_secret()
        except client.ApiException as ex:
            raise PullSecretError("Could not find pull secret") from ex

        # First check update time
        metadata: client.V1ObjectMeta = secret.metadata
        if not metadata:
            raise PullSecretError("Metadata not populated for pull secret")
        annotations: dict[str, str] = metadata.annotations
        if not annotations:
            raise PullSecretError("Annotations not set for pull secret metadata")
        updated_time_str: datetime.datetime = annotations.get(LAST_UPDATED_ANNOTATION)
        if not updated_time_str:
            raise PullSecretError("No update time found for pull secret")
        updated_time = datetime.datetime.strptime(updated_time_str, DATETIME_FORMAT)
        minutes_since_last_update = (
            datetime.datetime.now(tz=updated_time.tzinfo) - updated_time
        ).seconds // 60

        if minutes_since_last_update > self._minutes_stale:
            raise PullSecretError(
                f"Pull secret is stale, last updated {minutes_since_last_update} minutes ago."
            )

        # Next check if the pull secret thread is alive
        if not self._pull_secret_thread.is_alive():
            raise PullSecretError("The pull secret thread died.")
        return True

    def _update_liveness_file(self):
        self._liveness_file_path.touch()

    def _run_monitor_cluster_secret_thread(self):
        if not self._cluster_secret_name:
            return
        while not self._should_stop():
            try:
                self._monitor_cluster_secret()
            except ClusterSecretError:
                interrupt_main()
                raise
            except ApiException as ex:
                if ex.status == 410:
                    # This can happen on every watch, it just means we need to restart
                    # the watch.
                    continue
                logger.exception("Error monitoring cluster secret, trying again")
            except:
                logger.exception("Error monitoring cluster secret, trying again")

            self._sleep(10)

    def _check_cluster_secret(self, cluster_secret: client.V1Secret):
        cluster_id_b64 = cluster_secret.data.get(GRETEL_CLUSTER_ID_KEY) or ""
        cluster_id = base64.b64decode(cluster_id_b64).decode("ascii")
        if cluster_id != self._cluster_id:
            raise ClusterSecretError(
                f"cluster ID in cluster secret has changed from '{self._cluster_id}' to '{cluster_id}'"
            )

    def _monitor_cluster_secret(self):
        list_kwargs = {
            "namespace": self._gretel_worker_namespace,
            "field_selector": f"metadata.name={self._cluster_secret_name}",
        }

        secrets_list: client.V1SecretList = self._core_api.list_namespaced_secret(
            **list_kwargs
        )
        cluster_secrets = [
            s
            for s in secrets_list.items
            if s.metadata.name == self._cluster_secret_name
        ]
        if not cluster_secrets:
            raise ClusterSecretError(
                f"cluster secret {self._cluster_secret_name} not found"
            )
        cluster_secret = cluster_secrets[0]
        self._check_cluster_secret(cluster_secret)

        w = Watch()
        with contextlib.closing(
            w.stream(
                self._core_api.list_namespaced_secret,
                resource_version=secrets_list.metadata.resource_version,
                **list_kwargs,
            )
        ) as events_stream:
            for event in events_stream:
                object: client.V1Secret = event["object"]
                if object.metadata.name != self._cluster_secret_name:
                    continue

                if event["type"] == "DELETED":
                    raise ClusterSecretError(
                        f"cluster secret {self._cluster_secret_name} was deleted"
                    )

                self._check_cluster_secret(object)


class KubernetesError(Exception): ...


class PullSecretError(Exception): ...


class ClusterSecretError(Exception): ...


class StopThread(Exception): ...
