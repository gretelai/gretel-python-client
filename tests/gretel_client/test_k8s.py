import datetime
import json
import os
import uuid

from base64 import b64decode
from contextlib import contextmanager
from functools import wraps
from typing import Callable
from unittest import main, TestCase
from unittest.mock import MagicMock, Mock, patch

from kubernetes.client import (
    ApiException,
    BatchV1Api,
    CoreV1Api,
    V1ConfigMapVolumeSource,
    V1Container,
    V1EnvVar,
    V1Job,
    V1JobCondition,
    V1JobSpec,
    V1JobStatus,
    V1ObjectMeta,
    V1PodSpec,
    V1PodTemplateSpec,
    V1Secret,
    V1Volume,
    V1VolumeMount,
)

from gretel_client.agents.agent import AgentConfig, Job
from gretel_client.agents.drivers.k8s import (
    CA_CERT_CONFIGMAP_ENV_NAME,
    CPU_AFFINITY_ENV_NAME,
    CPU_COUNT_ENV_NAME,
    CPU_MODEL_WORKER_RESOURCES_ENV_NAME,
    CPU_NODE_SELECTOR_ENV_NAME,
    CPU_SEC_CONTEXT_ENV_NAME,
    CPU_TOLERATIONS_ENV_NAME,
    DATETIME_FORMAT,
    GPU_AFFINITY_ENV_NAME,
    GPU_MODEL_WORKER_RESOURCES_ENV_NAME,
    GPU_NODE_SELECTOR_ENV_NAME,
    GPU_SEC_CONTEXT_ENV_NAME,
    GPU_TOLERATIONS_ENV_NAME,
    IMAGE_REGISTRY_OVERRIDE_HOST_ENV_NAME,
    IMAGE_REGISTRY_OVERRIDE_TAG_ENV_NAME,
    JOB_BACKOFF_LIMIT_ENV_NAME,
    Kubernetes,
    KubernetesDriverDaemon,
    KubernetesError,
    LAST_UPDATED_ANNOTATION,
    OVERRIDE_CERT_NAME,
    PREVENT_AUTOSCALER_EVICTION_ENV_NAME,
    PULL_SECRETS_ENV_NAME,
    PullSecretError,
    StopThread,
    USES_DOCKERHUB_IMAGE_FORMAT_ENV_NAME,
    WORKER_MEMORY_GB_ENV_NAME,
    WORKER_POD_ANNOTATIONS_ENV_NAME,
    WORKER_POD_LABELS_ENV_NAME,
    WORKER_RESOURCES_ENV_NAME,
)


def get_mock_job(instance_type: str = "cpu-standard") -> dict:
    return {
        "run_id": "run-id",
        "job_type": "run",
        "container_image": "555.dkr.ecr.us-east-1.amazonaws.com/gretelai/transforms",
        "worker_token": "abcdef1243",
        "instance_type": instance_type,
    }


def patch_auth(func: Callable):
    @wraps(func)
    def inner_func(*args, **kwargs):
        with (
            patch("kubernetes.config.load_incluster_config", lambda: None),
            patch(
                "gretel_client.agents.agent.AgentConfig._lookup_max_jobs_active"
            ) as lookup_max_jobs_active_mock,
        ):
            lookup_max_jobs_active_mock.return_value = 1
            return func(*args, **kwargs)

    return inner_func


@contextmanager
def patch_gpu_environ(var_value: str):
    with patch.dict(os.environ, {GPU_NODE_SELECTOR_ENV_NAME: var_value}):
        yield


@contextmanager
def patch_cpu_environ(var_value: str):
    with patch.dict(os.environ, {CPU_NODE_SELECTOR_ENV_NAME: var_value}):
        yield


@contextmanager
def patch_worker_resources(var_value: str):
    with patch.dict(os.environ, {WORKER_RESOURCES_ENV_NAME: var_value}):
        yield


@contextmanager
def patch_cpu_worker_resources(var_value: str):
    with patch.dict(os.environ, {CPU_MODEL_WORKER_RESOURCES_ENV_NAME: var_value}):
        yield


@contextmanager
def patch_gpu_worker_resources(var_value: str):
    with patch.dict(os.environ, {GPU_MODEL_WORKER_RESOURCES_ENV_NAME: var_value}):
        yield


@contextmanager
def patch_gpu_toleration_environ(var_value: str):
    with patch.dict(
        os.environ,
        {GPU_TOLERATIONS_ENV_NAME: var_value},
    ):
        yield


@contextmanager
def patch_cpu_toleration_environ(var_value: str):
    with patch.dict(
        os.environ,
        {CPU_TOLERATIONS_ENV_NAME: var_value},
    ):
        yield


@contextmanager
def patch_env(var_name: str, var_value: str):
    with patch.dict(
        os.environ,
        {var_name: var_value},
    ):
        yield


@contextmanager
def patch_cpu_count_environ(var_value: str):
    with patch.dict(os.environ, {CPU_COUNT_ENV_NAME: var_value}):
        yield


@contextmanager
def patch_worker_memory_gb_environ(var_value: str):
    with patch.dict(os.environ, {WORKER_MEMORY_GB_ENV_NAME: var_value}):
        yield


@contextmanager
def patch_cert_env(configmap_name: str):
    with patch.dict(
        os.environ,
        {
            CA_CERT_CONFIGMAP_ENV_NAME: configmap_name,
        },
    ):
        yield


@contextmanager
def patch_image_registry(override_url: str, override_tag: str = ""):
    with patch.dict(
        os.environ,
        {
            IMAGE_REGISTRY_OVERRIDE_HOST_ENV_NAME: override_url,
            IMAGE_REGISTRY_OVERRIDE_TAG_ENV_NAME: override_tag,
            USES_DOCKERHUB_IMAGE_FORMAT_ENV_NAME: "true",
        },
    ):
        yield


@contextmanager
def patch_autoscaler_env_var(annotation_val: str):
    with patch.dict(
        os.environ,
        {
            PREVENT_AUTOSCALER_EVICTION_ENV_NAME: annotation_val,
        },
    ):
        yield


class TestKubernetesDriver(TestCase):
    @patch_auth
    def setUp(self) -> None:
        session = MagicMock()
        # Fake container auth return.
        session.get_api.return_value.get_container_login.return_value = {
            "data": {
                "auth": {"username": "abc", "password": "efg"},
                "registry": "123",
            }
        }
        self.config = AgentConfig(
            driver="k8s",
            creds=[],
            env_vars={"MY_KEY": "MY_VALUE", "OTHER_KEY": "OTHER_VALUE"},
            session=session,
        )
        self.batch_api = Mock()
        self.core_api = Mock()
        self.job = Job.from_dict(get_mock_job(), self.config)
        self.k8s_job = V1Job(
            metadata=V1ObjectMeta(name="johnny", namespace="gretel-workers")
        )
        self.driver = Kubernetes(
            self.config,
            batch_api=self.batch_api,
            core_api=self.core_api,
        )

    def reload_env_and_build_job(self, job: Job, restart_worker: bool = False) -> V1Job:
        self.driver._load_env_and_set_vars(restart_worker=restart_worker)
        return self.driver._build_job(job)

    def test_job_active_none(self):
        self.assertFalse(self.driver.active(None))

    def test_schedule_job(self):
        self.driver.schedule(self.job)

        self.batch_api.create_namespaced_job.assert_called_once()
        assert 2 == self.core_api.read_namespaced_secret.call_count

    def _create_api_exception(self, status: int, data: str) -> ApiException:
        http_resp = Mock()
        http_resp.status = status
        http_resp.data = data
        return ApiException(status, "", http_resp)

    def _stub_api_exception_for_batch(self, status: int, data: str) -> None:
        exception = self._create_api_exception(status, data)
        self.batch_api.create_namespaced_job.side_effect = exception
        self.batch_api.delete_namespaced_job.side_effect = exception

    def test_schedule_job_with_errors_unknown(self):
        self._stub_api_exception_for_batch(500, "{}")

        with self.assertRaisesRegex(
            KubernetesError, "Could not create job name=run-id"
        ):
            self.driver.schedule(self.job)

        self.batch_api.create_namespaced_job.assert_called_once()
        assert 2 == self.core_api.read_namespaced_secret.call_count

    def test_schedule_job_with_errors_non_api_exception(self):
        exception = RecursionError("ahhh")
        self.batch_api.create_namespaced_job.side_effect = exception
        self.batch_api.delete_namespaced_job.side_effect = exception

        with self.assertRaises(RecursionError):
            self.driver.schedule(self.job)

        self.batch_api.create_namespaced_job.assert_called_once()
        assert 2 == self.core_api.read_namespaced_secret.call_count

    def test_schedule_job_with_errors_already_exists(self):
        self._stub_api_exception_for_batch(405, '{"reason":"AlreadyExists"}')
        self.batch_api.read_namespaced_job.return_value = V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=V1ObjectMeta(
                name=self.job.uid,
                uid=str(uuid.uuid4()),
            ),
        )
        self.core_api.create_namespaced_secret.return_value = V1Secret()

        result = self.driver.schedule(self.job)
        self.assertIsNone(result)

        self.batch_api.create_namespaced_job.assert_called_once()
        assert 2 == self.core_api.read_namespaced_secret.call_count

    def test_schedule_job_secret_already_exists(self):
        self.core_api.create_namespaced_secret.side_effect = self._create_api_exception(
            405, '{"reason":"AlreadyExists"}'
        )

        result = self.driver.schedule(self.job)
        assert result is not None
        self.batch_api.create_namespaced_job.assert_called_once()
        self.core_api.create_namespaced_secret.assert_called_once()

    def test_schedule_job_secret_error(self):
        self.core_api.create_namespaced_secret.side_effect = self._create_api_exception(
            500, '{"reason":"SomethingBad"}'
        )
        with self.assertRaisesRegex(KubernetesError, "Could not create job"):
            self.driver.schedule(self.job)

    @patch_gpu_environ("")
    def test_build_job_gpu_req(self):
        job = Job.from_dict(get_mock_job("gpu-standard"), self.config)

        k8s_job = self.reload_env_and_build_job(job)

        job_spec: V1JobSpec = k8s_job.spec
        job_template: V1PodTemplateSpec = job_spec.template
        expected_resources = {
            "memory": "14Gi",
            "nvidia.com/gpu": "1",
        }
        self.assertEqual(
            expected_resources,
            job_template.spec.containers[0].resources.limits,
        )
        expected_resources["cpu"] = "1"
        expected_resources["ephemeral-storage"] = "50Gi"
        self.assertEqual(
            expected_resources,
            job_template.spec.containers[0].resources.requests,
        )

    @patch_gpu_environ('{"cloud.google.com/gke-accelerator":"nvidia-tesla-t4"}')
    def test_build_job_gpu_req_node_selector_set(self):
        job = Job.from_dict(get_mock_job("gpu-standard"), self.config)

        k8s_job = self.reload_env_and_build_job(job)

        job_spec: V1JobSpec = k8s_job.spec
        job_template: V1PodTemplateSpec = job_spec.template
        self.assertEqual(
            {"memory": "14Gi", "nvidia.com/gpu": "1"},
            job_template.spec.containers[0].resources.limits,
        )

        self.assertEqual(
            {"cloud.google.com/gke-accelerator": "nvidia-tesla-t4"},
            job_template.spec.node_selector,
        )

    @patch_cpu_toleration_environ('[{"key":"dedicated", "value": "gretel"}]')
    @patch_gpu_toleration_environ('[{"key":"dedicated", "value": "gretel"}]')
    def test_build_job_tolerations_set(self):
        for job_type in ["cpu-standard", "gpu-standard"]:
            with self.subTest(job_type):
                job_data = get_mock_job(job_type)
                job = Job.from_dict(job_data, self.config)

                k8s_job = self.reload_env_and_build_job(job)

                job_spec: V1JobSpec = k8s_job.spec
                job_template: V1PodTemplateSpec = job_spec.template

                self.assertEqual(
                    [{"key": "dedicated", "value": "gretel"}],
                    job_template.spec.tolerations,
                )

    @patch_env(
        CPU_AFFINITY_ENV_NAME,
        '{"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms":[{"matchExpressions":[{"key":"gretel.ai/node-type","operator":"In","values":["cpu-standard"]}]}]}}}',
    )
    @patch_env(
        GPU_AFFINITY_ENV_NAME,
        '{"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms":[{"matchExpressions":[{"key":"gretel.ai/node-type","operator":"In","values":["gpu-standard"]}]}]}}}',
    )
    def test_build_job_tolerations_set_2(self):
        for job_type in ["cpu-standard", "gpu-standard"]:
            with self.subTest(job_type):
                job_data = get_mock_job(job_type)
                job = Job.from_dict(job_data, self.config)

                k8s_job = self.reload_env_and_build_job(job)

                job_spec: V1JobSpec = k8s_job.spec
                job_template: V1PodTemplateSpec = job_spec.template

                self.assertEqual(
                    {
                        "nodeAffinity": {
                            "requiredDuringSchedulingIgnoredDuringExecution": {
                                "nodeSelectorTerms": [
                                    {
                                        "matchExpressions": [
                                            {
                                                "key": "gretel.ai/node-type",
                                                "operator": "In",
                                                "values": [job_type],
                                            }
                                        ]
                                    }
                                ]
                            }
                        }
                    },
                    job_template.spec.affinity,
                )

    @patch_env(JOB_BACKOFF_LIMIT_ENV_NAME, "3")
    def test_backoff_set_above_zero(self):
        job = Job.from_dict(get_mock_job("gpu-standard"), self.config)

        k8s_job = self.reload_env_and_build_job(job)

        job_spec: V1JobSpec = k8s_job.spec
        assert job_spec.backoff_limit == 3

    def test_build_job_cpu_req_node_selector_set(self):
        with patch_cpu_environ('{"selector":"my-cpu-node"}'):
            self.driver._load_env_and_set_vars()
            job = Job.from_dict(get_mock_job(), self.config)

            k8s_job = self.driver._build_job(job)

            job_spec: V1JobSpec = k8s_job.spec
            assert job_spec.backoff_limit == 0
            job_template: V1PodTemplateSpec = job_spec.template
            self.assertEqual(
                {"memory": "14Gi"},
                job_template.spec.containers[0].resources.limits,
            )

            self.assertEqual(
                {"selector": "my-cpu-node"},
                job_template.spec.node_selector,
            )

    def test_build_job_gpu_req_node_selector_set_invalid(self):
        with patch_gpu_environ("{"):
            with self.assertRaisesRegex(KubernetesError, "Could not deserialize JSON"):
                Kubernetes(
                    self.config,
                    batch_api=self.batch_api,
                    core_api=self.core_api,
                )

    def test_build_job_set_security_context(self):
        with patch.dict(
            os.environ,
            {
                GPU_SEC_CONTEXT_ENV_NAME: json.dumps(
                    {"runAsNonRoot": True, "runAsUser": 1001}
                )
            },
        ):
            job = Job.from_dict(get_mock_job("gpu-standard"), self.config)
            k8s_job = self.reload_env_and_build_job(job)
            job_spec: V1JobSpec = k8s_job.spec
            job_template: V1PodTemplateSpec = job_spec.template
            pod_spec: V1PodSpec = job_template.spec
            container: V1Container = pod_spec.containers[0]
            security_context: dict = container.security_context
            assert security_context.get("runAsNonRoot") is True
            assert security_context.get("runAsUser") == 1001
            assert V1EnvVar(name="HOME", value="/run") in container.env

    def test_build_job_cpu_set_security_context(self):
        with patch.dict(
            os.environ,
            {
                CPU_SEC_CONTEXT_ENV_NAME: json.dumps(
                    {"runAsNonRoot": False, "runAsUser": 1003}
                ),
                GPU_SEC_CONTEXT_ENV_NAME: json.dumps(
                    {"runAsNonRoot": False, "runAsUser": 1002}
                ),
            },
        ):
            job = Job.from_dict(get_mock_job("cpu-standard"), self.config)
            k8s_job = self.reload_env_and_build_job(job)
            job_spec: V1JobSpec = k8s_job.spec
            job_template: V1PodTemplateSpec = job_spec.template
            pod_spec: V1PodSpec = job_template.spec
            container: V1Container = pod_spec.containers[0]
            security_context: dict = container.security_context
            assert security_context.get("runAsNonRoot") is False
            assert security_context.get("runAsUser") == 1003

    def test_build_job_node_toleration_set_invalid(self):
        with patch_cpu_toleration_environ("{"):
            with self.assertRaisesRegex(KubernetesError, "Could not deserialize JSON"):
                Kubernetes(
                    self.config,
                    batch_api=self.batch_api,
                    core_api=self.core_api,
                )

    def test_build_job_gpu_req_node_selector_set_not_dictionary(self):
        for test_val in ["1", "[1,2,3]", "[]"]:
            with patch_gpu_environ(test_val):
                with self.assertRaisesRegex(
                    KubernetesError,
                    "The GPU_NODE_SELECTOR variable was not a JSON dict",
                ):
                    self.driver._load_env_and_set_vars()

    @patch_cpu_environ("{")
    def test_build_job_cpu_req_node_selector_set_invalid(self):
        with self.assertRaisesRegex(KubernetesError, "Could not deserialize JSON"):
            self.driver._load_env_and_set_vars()

    def test_build_job_cpu_req_node_selector_set_not_dictionary(self):
        for test_val in ["1", "[1,2,3]", "[]"]:
            with patch_cpu_environ(test_val):
                with self.assertRaisesRegex(
                    KubernetesError,
                    "The CPU_NODE_SELECTOR variable was not a JSON dict",
                ):
                    self.driver._load_env_and_set_vars()

    def test_tolerations_set_not_list(self):
        for test_val in ["1", '{"key":"val"}', '"abc"']:
            with patch_cpu_toleration_environ(test_val):
                with self.assertRaisesRegex(
                    KubernetesError, "The CPU_TOLERATIONS variable was not a JSON list"
                ):
                    self.driver._load_env_and_set_vars()
            with patch_gpu_toleration_environ(test_val):
                with self.assertRaisesRegex(
                    KubernetesError, "The GPU_TOLERATIONS variable was not a JSON list"
                ):
                    self.driver._load_env_and_set_vars()

    def test_cpu_count_not_set_properly(self):
        for val in ["5.3", "-1", "something"]:
            with patch_cpu_count_environ(val):
                job = Job.from_dict(get_mock_job(), self.config)
                with self.assertRaisesRegex(
                    KubernetesError,
                    f"Gretel CPU Count must be a positive integer, instead received {val}",
                ):
                    self.reload_env_and_build_job(job)

    def test_cpu_count_set_properly(self):
        with patch_cpu_count_environ("5"):
            job = Job.from_dict(get_mock_job(), self.config)
            k8s_job = self.reload_env_and_build_job(job)

            job_spec: V1JobSpec = k8s_job.spec
            job_template: V1PodTemplateSpec = job_spec.template
            self.assertEqual(
                {"memory": "14Gi", "cpu": "5", "ephemeral-storage": "50Gi"},
                job_template.spec.containers[0].resources.requests,
            )
            self.assertEqual(
                {"memory": "14Gi", "cpu": "5"},
                job_template.spec.containers[0].resources.limits,
            )
            self.assertEqual(
                "5",
                next(
                    (
                        v.value
                        for v in job_template.spec.containers[0].env
                        if v.name == CPU_COUNT_ENV_NAME
                    )
                ),
            )

    def test_worker_resources_set_not_dictionary(self):
        for test_val in ["1", "[1,2,3]", "[]"]:
            with patch_worker_resources(test_val):
                with self.assertRaisesRegex(
                    KubernetesError,
                    "The GRETEL_WORKER_RESOURCES variable was not a JSON dict",
                ):
                    self.driver._load_env_and_set_vars()

    def test_worker_resources_set_invalid_fields(self):
        with patch_worker_resources(
            json.dumps({"requests": {}, "limits": {}, "more": {}})
        ):
            with self.assertRaisesRegex(
                KubernetesError,
                "resources specified via 'GRETEL_WORKER_RESOURCES' invalid:.*more",
            ):
                self.driver._load_env_and_set_vars()

    def test_worker_resources_set_invalid_quantity(self):
        with patch_worker_resources(
            json.dumps({"requests": {}, "limits": {"cpu": "1x"}})
        ):
            with self.assertRaisesRegex(
                KubernetesError,
                "limits.cpu in 'GRETEL_WORKER_RESOURCES' value must be a valid quantity, got: 1x",
            ):
                self.driver._load_env_and_set_vars()

    def test_worker_resources_set_ignores_legacy_settings(self):
        with (
            patch_worker_resources(
                json.dumps({"requests": {"cpu": "3.5", "memory": "12Gi"}, "limits": {}})
            ),
            patch_cpu_count_environ("2"),
            patch_worker_memory_gb_environ("14"),
        ):
            job = Job.from_dict(get_mock_job(), self.config)
            k8s_job = self.reload_env_and_build_job(job)

            job_spec: V1JobSpec = k8s_job.spec
            job_template: V1PodTemplateSpec = job_spec.template
            self.assertEqual(
                {"memory": "12Gi", "cpu": "3.5", "ephemeral-storage": "50Gi"},
                job_template.spec.containers[0].resources.requests,
            )
            self.assertEqual(
                {"memory": "12Gi"},
                job_template.spec.containers[0].resources.limits,
            )
            self.assertNotIn(
                CPU_COUNT_ENV_NAME,
                (v.name for v in job_template.spec.containers[0].env),
            )

    def test_worker_resources_set_sets_cpu_count_from_cpu_limit(self):
        with patch_worker_resources(
            json.dumps(
                {
                    "requests": {"cpu": "1.5", "memory": "12Gi"},
                    "limits": {"cpu": "3.5", "memory": "12Gi"},
                }
            )
        ):
            job = Job.from_dict(get_mock_job(), self.config)
            k8s_job = self.reload_env_and_build_job(job)

            job_spec: V1JobSpec = k8s_job.spec
            job_template: V1PodTemplateSpec = job_spec.template
            self.assertEqual(
                {"memory": "12Gi", "cpu": "1.5", "ephemeral-storage": "50Gi"},
                job_template.spec.containers[0].resources.requests,
            )
            self.assertEqual(
                {"memory": "12Gi", "cpu": "3.5"},
                job_template.spec.containers[0].resources.limits,
            )
            self.assertEqual(
                "3",
                next(
                    (
                        v.value
                        for v in job_template.spec.containers[0].env
                        if v.name == CPU_COUNT_ENV_NAME
                    )
                ),
            )

    def test_worker_resources_partially_set_uses_legacy_cpu(self):
        with (
            patch_worker_resources(
                json.dumps({"requests": {"memory": "12Gi"}, "limits": {}})
            ),
            patch_cpu_count_environ("2"),
            patch_worker_memory_gb_environ("14"),
        ):
            job = Job.from_dict(get_mock_job(), self.config)
            k8s_job = self.reload_env_and_build_job(job)

            job_spec: V1JobSpec = k8s_job.spec
            job_template: V1PodTemplateSpec = job_spec.template
            self.assertEqual(
                {"memory": "12Gi", "cpu": "2", "ephemeral-storage": "50Gi"},
                job_template.spec.containers[0].resources.requests,
            )
            self.assertEqual(
                {"memory": "12Gi", "cpu": "2"},
                job_template.spec.containers[0].resources.limits,
            )
            self.assertEqual(
                "2",
                next(
                    (
                        v.value
                        for v in job_template.spec.containers[0].env
                        if v.name == CPU_COUNT_ENV_NAME
                    )
                ),
            )

    def test_worker_resources_partially_set_uses_legacy_memory(self):
        with (
            patch_worker_resources(
                json.dumps({"requests": {"cpu": "3.5"}, "limits": {}})
            ),
            patch_cpu_count_environ("2"),
            patch_worker_memory_gb_environ("14"),
        ):
            job = Job.from_dict(get_mock_job(), self.config)
            k8s_job = self.reload_env_and_build_job(job)

            job_spec: V1JobSpec = k8s_job.spec
            job_template: V1PodTemplateSpec = job_spec.template
            self.assertEqual(
                {"memory": "14Gi", "cpu": "3.5", "ephemeral-storage": "50Gi"},
                job_template.spec.containers[0].resources.requests,
            )
            self.assertEqual(
                {"memory": "14Gi"},
                job_template.spec.containers[0].resources.limits,
            )
            self.assertNotIn(
                CPU_COUNT_ENV_NAME,
                (v.name for v in job_template.spec.containers[0].env),
            )

    def test_worker_resources_granular_cpu(self):
        with (
            patch_cpu_worker_resources(
                json.dumps(
                    {
                        "requests": {"cpu": "3", "memory": "8Gi"},
                        "limits": {"cpu": "3", "memory": "8Gi"},
                    }
                )
            ),
        ):
            job = Job.from_dict(get_mock_job(), self.config)
            k8s_job = self.reload_env_and_build_job(job)

            job_spec: V1JobSpec = k8s_job.spec
            job_template: V1PodTemplateSpec = job_spec.template
            self.assertEqual(
                {"memory": "8Gi", "cpu": "3", "ephemeral-storage": "50Gi"},
                job_template.spec.containers[0].resources.requests,
            )
            self.assertEqual(
                {"memory": "8Gi", "cpu": "3"},
                job_template.spec.containers[0].resources.limits,
            )

    def test_worker_resources_granular_gpu(self):
        with (
            patch_gpu_worker_resources(
                json.dumps(
                    {
                        "requests": {"cpu": "3", "memory": "8Gi"},
                        "limits": {"cpu": "3", "memory": "8Gi"},
                    }
                )
            ),
        ):
            job = Job.from_dict(get_mock_job("gpu-standard"), self.config)
            k8s_job = self.reload_env_and_build_job(job)

            job_spec: V1JobSpec = k8s_job.spec
            job_template: V1PodTemplateSpec = job_spec.template
            self.assertEqual(
                {
                    "memory": "8Gi",
                    "cpu": "3",
                    "nvidia.com/gpu": "1",
                    "ephemeral-storage": "50Gi",
                },
                job_template.spec.containers[0].resources.requests,
            )
            self.assertEqual(
                {"memory": "8Gi", "cpu": "3", "nvidia.com/gpu": "1"},
                job_template.spec.containers[0].resources.limits,
            )

    def test_worker_resources_granular_cpu_override(self):
        with (
            patch_worker_resources(
                json.dumps(
                    {
                        "requests": {"cpu": "6", "memory": "16Gi"},
                        "limits": {"cpu": "6", "memory": "16Gi"},
                    }
                )
            ),
            patch_cpu_worker_resources(
                json.dumps(
                    {
                        "requests": {"cpu": "3", "memory": "8Gi"},
                        "limits": {"cpu": "3", "memory": "8Gi"},
                    }
                )
            ),
        ):
            job = Job.from_dict(get_mock_job(), self.config)
            k8s_job = self.reload_env_and_build_job(job)

            job_spec: V1JobSpec = k8s_job.spec
            job_template: V1PodTemplateSpec = job_spec.template
            self.assertEqual(
                {"memory": "8Gi", "cpu": "3", "ephemeral-storage": "50Gi"},
                job_template.spec.containers[0].resources.requests,
            )
            self.assertEqual(
                {"memory": "8Gi", "cpu": "3"},
                job_template.spec.containers[0].resources.limits,
            )

    def test_worker_resources_granular_gpu_override(self):
        with (
            patch_worker_resources(
                json.dumps(
                    {
                        "requests": {"cpu": "6", "memory": "16Gi"},
                        "limits": {"cpu": "6", "memory": "16Gi"},
                    }
                )
            ),
            patch_gpu_worker_resources(
                json.dumps(
                    {
                        "requests": {"cpu": "3", "memory": "8Gi"},
                        "limits": {"cpu": "3", "memory": "8Gi"},
                    }
                )
            ),
        ):
            job = Job.from_dict(get_mock_job("gpu-standard"), self.config)
            k8s_job = self.reload_env_and_build_job(job)

            job_spec: V1JobSpec = k8s_job.spec
            job_template: V1PodTemplateSpec = job_spec.template
            self.assertEqual(
                {
                    "memory": "8Gi",
                    "cpu": "3",
                    "nvidia.com/gpu": "1",
                    "ephemeral-storage": "50Gi",
                },
                job_template.spec.containers[0].resources.requests,
            )
            self.assertEqual(
                {"memory": "8Gi", "cpu": "3", "nvidia.com/gpu": "1"},
                job_template.spec.containers[0].resources.limits,
            )

    def test_is_job_active_true_then_false(self):
        self.batch_api.read_namespaced_job.side_effect = [
            V1Job(
                status=V1JobStatus(
                    active=1,
                    conditions=[
                        V1JobCondition(type="Complete", status=False),
                    ],
                )
            ),
            V1Job(
                status=V1JobStatus(
                    active=0,
                    conditions=[
                        V1JobCondition(type="Complete", status=True),
                    ],
                )
            ),
        ]
        self.assertTrue(self.driver.active(self.k8s_job))
        self.assertFalse(self.driver.active(self.k8s_job))

    def test_is_job_active_true_then_empty(self):
        self.batch_api.read_namespaced_job.side_effect = [
            V1Job(status=V1JobStatus(active=1)),
            None,
        ]
        self.assertTrue(self.driver.active(self.k8s_job))
        self.assertFalse(self.driver.active(self.k8s_job))

    def test_is_job_active_true_then_empty_status(self):
        self.batch_api.read_namespaced_job.side_effect = [
            V1Job(status=V1JobStatus(active=1)),
            V1Job(status=V1JobStatus()),
        ]
        self.assertTrue(self.driver.active(self.k8s_job))
        self.assertTrue(self.driver.active(self.k8s_job))

    def test_is_job_active_true_then_failed(self):
        self.batch_api.read_namespaced_job.side_effect = [
            V1Job(
                status=V1JobStatus(
                    conditions=[
                        V1JobCondition(type="Complete", status=False),
                        V1JobCondition(type="Failed", status=False),
                    ],
                )
            ),
            V1Job(
                status=V1JobStatus(
                    conditions=[
                        V1JobCondition(type="Complete", status=False),
                        V1JobCondition(type="Failed", status=True),
                    ],
                )
            ),
        ]
        self.assertTrue(self.driver.active(self.k8s_job))
        self.assertFalse(self.driver.active(self.k8s_job))

    def test_delete_job_successful_clean(self):
        self.driver.clean(self.k8s_job)
        self.batch_api.delete_namespaced_job.assert_called_once()

    def test_delete_job_successful_shutdown(self):
        self.driver.shutdown(self.k8s_job)
        self.batch_api.delete_namespaced_job.assert_called_once()

    def test_delete_job_warns_empty_job_passed(self):
        self.driver.clean(None)

    def test_delete_job_fails_not_found(self):
        self._stub_api_exception_for_batch(404, '{"reason":"NotFound"}')
        self.driver.clean(self.k8s_job)

    def test_delete_job_fails_error(self):
        self._stub_api_exception_for_batch(405, '{"reason":"oops"}')
        with self.assertRaisesRegex(
            KubernetesError, "Could not delete job name=johnny"
        ):
            self.driver.clean(self.k8s_job)

    @patch_auth
    @patch(
        "gretel_client.agents.drivers.k8s.KubernetesDriverDaemon.start_update_pull_secret_thread",
        lambda *args: None,
    )
    def test_contruct_driver(self):
        driver = Kubernetes.from_config(self.config)
        self.assertIsInstance(driver._batch_api, BatchV1Api)
        self.assertIsInstance(driver._core_api, CoreV1Api)

    @patch_auth
    def test_daemon_create_secret(self):
        core_api = Mock()
        daemon = KubernetesDriverDaemon(self.config, core_api)
        core_api.patch_namespaced_secret.side_effect = self._create_api_exception(
            500, '{"reason":"NotFound"}'
        )
        daemon._update_pull_secrets()
        core_api.patch_namespaced_secret.assert_called_once()
        core_api.create_namespaced_secret.assert_called_once()

    @patch_auth
    def test_daemon_create_secret_loop_with_exceptions(self):
        core_api = Mock()
        daemon = KubernetesDriverDaemon(
            self.config, core_api, sleep_length=0, sleep_length_retry=0
        )
        core_api.patch_namespaced_secret.side_effect = [
            self._create_api_exception(500, '{"reason":"NotFound"}'),
            self._create_api_exception(403, '{"reason":"Forbidden"}'),
            Exception(),
            StopThread(),
        ]
        with self.assertRaises(StopThread):
            daemon._run_pull_secret_thread()

        assert 4 == core_api.patch_namespaced_secret.call_count
        assert 1 == core_api.create_namespaced_secret.call_count

    @patch_cert_env("my-cert-configmap")
    def test_build_job_with_custom_certs(self):
        job = Job.from_dict(get_mock_job(), self.config)

        k8s_job = self.reload_env_and_build_job(job)

        job_spec: V1JobSpec = k8s_job.spec
        pod_template: V1PodTemplateSpec = job_spec.template
        pod_spec: V1PodSpec = pod_template.spec
        container: V1Container = pod_spec.containers[0]

        assert len(container.volume_mounts) == 3
        mount: V1VolumeMount = container.volume_mounts[-1]
        assert mount.name == OVERRIDE_CERT_NAME
        assert mount.mount_path == "/usr/local/share/ca-certificates/"

        assert len(pod_spec.volumes) == 3
        volume: V1Volume = pod_spec.volumes[-1]
        assert volume.name == OVERRIDE_CERT_NAME
        config_map: V1ConfigMapVolumeSource = volume.config_map
        assert config_map.optional
        assert config_map.name == "my-cert-configmap"
        assert config_map.default_mode == 0o0644

    @patch_image_registry("shiny-new-reg.example.ai/gretelai123")
    @patch_auth
    @patch_env(
        PULL_SECRETS_ENV_NAME,
        json.dumps(["personal-secret", "gretel-pull-secret"]),
    )
    def test_build_job_image_url_override(self):
        job = Job.from_dict(get_mock_job(), self.config)
        k8s_job = self.reload_env_and_build_job(job, restart_worker=True)

        job_spec: V1JobSpec = k8s_job.spec
        pod_template: V1PodTemplateSpec = job_spec.template
        pod_spec: V1PodSpec = pod_template.spec
        container: V1Container = pod_spec.containers[0]

        assert (
            container.image
            == "shiny-new-reg.example.ai/gretelai123/gretelai-transforms"
        )
        assert pod_spec.image_pull_secrets == [
            {"name": "personal-secret"},
            {"name": "gretel-pull-secret"},
        ]

    @patch_auth
    def test_build_job_params_set_correctly(self):
        job = Job.from_dict(get_mock_job(), self.config)
        k8s_job = self.reload_env_and_build_job(job, restart_worker=True)

        job_spec: V1JobSpec = k8s_job.spec
        pod_template: V1PodTemplateSpec = job_spec.template
        pod_spec: V1PodSpec = pod_template.spec
        container: V1Container = pod_spec.containers[0]

        assert container.args == ["--worker-token", job.worker_token]

    @patch_auth
    def test_build_job_params_set_correctly_with_empty_arg_val(self):
        job = Job.from_dict(get_mock_job(), self.config)
        job.disable_cloud_logging = True
        job.disable_cloud_report_scores = True
        job.artifact_endpoint = "s3://davey-jones-bucket/"
        k8s_job = self.reload_env_and_build_job(job, restart_worker=True)

        job_spec: V1JobSpec = k8s_job.spec
        pod_template: V1PodTemplateSpec = job_spec.template
        pod_spec: V1PodSpec = pod_template.spec
        container: V1Container = pod_spec.containers[0]

        assert container.args == [
            "--worker-token",
            job.worker_token,
            "--artifact-endpoint",
            job.artifact_endpoint,
            "--disable-cloud-logging",
            "--disable-cloud-report-scores",
        ]

    @patch_image_registry("shiny-new-reg.example.ai")
    @patch_auth
    @patch_env(
        PULL_SECRETS_ENV_NAME,
        json.dumps(["personal-secret", "gretel-pull-secret"]),
    )
    def test_resolve_image_models(self):
        job = Job.from_dict(get_mock_job("gpu-standard"), self.config)
        original_image = "models:latest"
        job.container_image = original_image
        self.reload_env_and_build_job(job, restart_worker=True)

        image = self.driver._resolve_image(job)

        assert image == "shiny-new-reg.example.ai/models:latest"

    @patch_image_registry("shiny-new-reg.example.ai")
    @patch_auth
    @patch_env(
        PULL_SECRETS_ENV_NAME,
        json.dumps(["personal-secret", "gretel-pull-secret"]),
    )
    def test_resolve_image_only_one_part(self):
        job = Job.from_dict(get_mock_job("gpu-standard"), self.config)
        original_image = "my.ecr/busybox:latest"
        job.container_image = original_image
        self.reload_env_and_build_job(job, restart_worker=True)

        image = self.driver._resolve_image(job)

        assert image == "shiny-new-reg.example.ai/busybox:latest"

    @patch_image_registry("shiny-new-reg.example.ai", override_tag="dev")
    @patch_auth
    def test_resolve_image_dont_malform_with_sha(self):
        job = Job.from_dict(get_mock_job("gpu-standard"), self.config)
        original_image = "my.ecr/busybox:latest@sha256:abc12345"
        job.container_image = original_image
        self.reload_env_and_build_job(job, restart_worker=True)

        image = self.driver._resolve_image(job)

        assert image == "shiny-new-reg.example.ai/busybox:dev"

    @patch_image_registry("shiny-new-reg.example.ai", override_tag="dev")
    @patch_auth
    def test_resolve_image_dont_malform_with_sha_and_no_tag(self):
        job = Job.from_dict(get_mock_job("gpu-standard"), self.config)
        original_image = "my.ecr/busybox@sha256:abc12345"
        job.container_image = original_image
        self.reload_env_and_build_job(job, restart_worker=True)

        image = self.driver._resolve_image(job)

        assert image == "shiny-new-reg.example.ai/busybox:dev"

    @patch_image_registry("shiny-new-reg.example.ai", override_tag="latest")
    @patch_auth
    def test_resolve_image_dont_malform_with_only_tag(self):
        job = Job.from_dict(get_mock_job("gpu-standard"), self.config)
        original_image = "my.ecr/busybox:dev"
        job.container_image = original_image
        self.reload_env_and_build_job(job, restart_worker=True)

        image = self.driver._resolve_image(job)

        assert image == "shiny-new-reg.example.ai/busybox:latest"

    @patch_auth
    @patch_image_registry("shiny-new-reg.example.ai")
    def test_create_secret_with_override(self):
        worker = KubernetesDriverDaemon(self.config, self.core_api)
        secret = worker._create_secret_body()
        decoded_str = b64decode(secret.data[".dockerconfigjson"]).decode("utf-8")
        dockerconfig_json = json.loads(decoded_str)
        assert "auths" in dockerconfig_json
        assert list(dockerconfig_json["auths"].keys())[0] == "shiny-new-reg.example.ai"

    @patch_auth
    @patch_image_registry("shiny-new-reg.example.ai/gretelai")
    def test_special_case_gretelai(self):
        job = Job.from_dict(get_mock_job("gpu-standard"), self.config)
        original_image = "gcc/my-workflow-image:latest"
        job.container_image = original_image
        daemon = KubernetesDriverDaemon(self.config, self.core_api)
        self.driver._worker = daemon
        image = self.driver._resolve_image(job)

        assert image == "shiny-new-reg.example.ai/gretelai/gcc-my-workflow-image:latest"

    @patch_auth
    @patch_image_registry("shiny-new-reg.example.ai/gretelai")
    def test_special_case_gretelai_models(self):
        job = Job.from_dict(get_mock_job("gpu-standard"), self.config)
        original_image = "models:latest"
        job.container_image = original_image
        daemon = KubernetesDriverDaemon(self.config, self.core_api)
        self.driver._worker = daemon
        image = self.driver._resolve_image(job)

        assert image == "shiny-new-reg.example.ai/gretelai/models:latest"

    @patch_autoscaler_env_var("true")
    def test_annotation_set_true(self):
        job = Job.from_dict(get_mock_job(), self.config)
        k8s_job = self.reload_env_and_build_job(job)
        job_spec: V1JobSpec = k8s_job.spec
        pod_template: V1PodTemplateSpec = job_spec.template
        metadata: V1ObjectMeta = pod_template.metadata
        assert {
            "cluster-autoscaler.kubernetes.io/safe-to-evict": "false"
        } == metadata.annotations

    @patch_autoscaler_env_var("false")
    def test_annotation_set_false(self):
        job = Job.from_dict(get_mock_job(), self.config)
        k8s_job = self.reload_env_and_build_job(job)
        job_spec: V1JobSpec = k8s_job.spec
        pod_template: V1PodTemplateSpec = job_spec.template
        metadata: V1ObjectMeta = pod_template.metadata
        assert {} == metadata.annotations

    @patch_env(WORKER_POD_LABELS_ENV_NAME, '{"foo": "bar"}')
    @patch_env(WORKER_POD_ANNOTATIONS_ENV_NAME, '{"baz": "qux"}')
    @patch_autoscaler_env_var("true")
    def test_worker_pod_metadata(self):
        job = Job.from_dict(get_mock_job(), self.config)
        k8s_job = self.reload_env_and_build_job(job)
        job_spec: V1JobSpec = k8s_job.spec
        pod_template: V1PodTemplateSpec = job_spec.template
        metadata: V1ObjectMeta = pod_template.metadata
        assert {"foo": "bar", "app": "gretel-jobs-worker"} == metadata.labels
        assert {
            "baz": "qux",
            "cluster-autoscaler.kubernetes.io/safe-to-evict": "false",
        } == metadata.annotations

    @patch_env(
        WORKER_POD_ANNOTATIONS_ENV_NAME,
        '{"cluster-autoscaler.kubernetes.io/safe-to-evict": "true"}',
    )
    @patch_autoscaler_env_var("true")
    def test_worker_pod_annotations_overrides_safe_to_evict(self):
        job = Job.from_dict(get_mock_job(), self.config)
        k8s_job = self.reload_env_and_build_job(job)
        job_spec: V1JobSpec = k8s_job.spec
        pod_template: V1PodTemplateSpec = job_spec.template
        metadata: V1ObjectMeta = pod_template.metadata
        assert {
            "cluster-autoscaler.kubernetes.io/safe-to-evict": "true"
        } == metadata.annotations

    @patch_auth
    def test_healthcheck_thread_invalid_k8s_object(self) -> None:
        daemon = KubernetesDriverDaemon(self.config, self.core_api)
        self.core_api.read_namespaced_secret.return_value = V1Secret()
        with self.assertRaisesRegex(
            PullSecretError, "Metadata not populated for pull secret"
        ):
            daemon._run_healthchecks()

        self.core_api.read_namespaced_secret.return_value = V1Secret(
            metadata=V1ObjectMeta(annotations={LAST_UPDATED_ANNOTATION: None})
        )
        with self.assertRaisesRegex(
            PullSecretError, "No update time found for pull secret"
        ):
            daemon._run_healthchecks()

    @patch_auth
    def test_healthcheck_thread_secret_too_old(self) -> None:
        daemon = KubernetesDriverDaemon(self.config, self.core_api)
        daemon._pull_secret_thread = Mock()
        daemon._pull_secret_thread.is_alive.return_value = True
        updated_time = datetime.datetime.now() - datetime.timedelta(
            minutes=daemon._minutes_stale + 3
        )

        self.core_api.read_namespaced_secret.return_value = V1Secret(
            metadata=V1ObjectMeta(
                annotations={
                    LAST_UPDATED_ANNOTATION: updated_time.strftime(DATETIME_FORMAT)
                }
            )
        )
        with self.assertRaisesRegex(
            PullSecretError, "Pull secret is stale, last updated.*minutes ago"
        ):
            daemon._run_healthchecks()

    @patch_auth
    def test_healthcheck_thread_thread_dead(self) -> None:
        daemon = KubernetesDriverDaemon(self.config, self.core_api)
        daemon._pull_secret_thread = Mock()
        daemon._pull_secret_thread.is_alive.return_value = False
        updated_time = datetime.datetime.now() - datetime.timedelta(minutes=60)

        self.core_api.read_namespaced_secret.return_value = V1Secret(
            metadata=V1ObjectMeta(
                annotations={
                    LAST_UPDATED_ANNOTATION: updated_time.strftime(DATETIME_FORMAT)
                }
            )
        )
        with self.assertRaisesRegex(PullSecretError, "The pull secret thread died."):
            daemon._run_healthchecks()

    @patch_auth
    def test_healthcheck_thread_thread_alive(self) -> None:
        daemon = KubernetesDriverDaemon(self.config, self.core_api)
        daemon._pull_secret_thread = Mock()
        daemon._pull_secret_thread.is_alive.return_value = True
        updated_time = datetime.datetime.now() - datetime.timedelta(minutes=60)

        self.core_api.read_namespaced_secret.return_value = V1Secret(
            metadata=V1ObjectMeta(
                annotations={
                    LAST_UPDATED_ANNOTATION: updated_time.strftime(DATETIME_FORMAT)
                }
            )
        )
        assert daemon._run_healthchecks()


if __name__ == "__main__":
    main()
