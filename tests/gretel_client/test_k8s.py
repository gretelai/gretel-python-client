import os

from contextlib import contextmanager
from functools import wraps
from typing import Callable
from unittest import TestCase
from unittest.mock import Mock, patch

from kubernetes.client import (
    ApiException,
    BatchV1Api,
    CoreV1Api,
    V1Job,
    V1JobSpec,
    V1JobStatus,
    V1ObjectMeta,
    V1PodTemplateSpec,
)

from gretel_client.agents.agent import AgentConfig, Job
from gretel_client.agents.drivers.k8s import (
    CPU_NODE_SELECTOR_ENV_NAME,
    CPU_TOLERATIONS_ENV_NAME,
    GPU_NODE_SELECTOR_ENV_NAME,
    GPU_TOLERATIONS_ENV_NAME,
    Kubernetes,
    KubernetesDriverDaemon,
    KubernetesError,
)


def get_mock_job(instance_type: str = "cpu-standard") -> dict:
    return {
        "run_id": "run-id",
        "job_type": "run",
        "container_image": "gretelai/transforms",
        "worker_token": "abcdef1243",
        "instance_type": instance_type,
    }


def patch_auth(func: Callable):
    @wraps(func)
    def inner_func(*args, **kwargs):
        with patch("kubernetes.config.load_incluster_config", lambda: None), patch(
            "gretel_client.agents.agent.get_session_config"
        ) as agent_get_session_mock, patch(
            "gretel_client.docker.get_session_config"
        ) as driver_get_session_mock:
            driver_get_session_mock.return_value.get_api.return_value.get_container_login.return_value = {
                "data": {
                    "auth": {"username": "abc", "password": "efg"},
                    "registry": "123",
                }
            }
            agent_get_session_mock.return_value.get_api.return_value.users_me.return_value = {
                "data": {"me": {"service_limits": {"max_job_runtime": 1}}}
            }
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
def patch_toleration_environ(var_value: str):
    with patch.dict(
        os.environ,
        {GPU_TOLERATIONS_ENV_NAME: var_value, CPU_TOLERATIONS_ENV_NAME: var_value},
    ):
        yield


class TestKubernetesDriver(TestCase):
    @patch_auth
    def setUp(self) -> None:
        os.environ["SHOULD_RUN_THREAD_ONCE"] = "true"
        self.config = AgentConfig(
            driver="k8s",
            creds=[],
            env_vars={"MY_KEY": "MY_VALUE", "OTHER_KEY": "OTHER_VALUE"},
        )
        self.batch_api = Mock()
        self.core_api = Mock()
        self.job = Job.from_dict(get_mock_job(), self.config)
        self.k8s_job = V1Job(
            metadata=V1ObjectMeta(name="johnny", namespace="gretel-workers")
        )
        self.driver = Kubernetes(
            self.config, batch_api=self.batch_api, core_api=self.core_api
        )

    def test_job_active_none(self):
        self.assertFalse(self.driver.active(None))

    def test_schedule_job(self):
        self.driver.schedule(self.job)

        self.batch_api.create_namespaced_job.assert_called_once()
        self.core_api.read_namespaced_secret.assert_called_once()

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
            KubernetesError, "Count not create job name=run-id"
        ):
            self.driver.schedule(self.job)

        self.batch_api.create_namespaced_job.assert_called_once()
        self.core_api.read_namespaced_secret.assert_called_once()

    def test_schedule_job_with_errors_non_api_exception(self):
        exception = RecursionError("ahhh")
        self.batch_api.create_namespaced_job.side_effect = exception
        self.batch_api.delete_namespaced_job.side_effect = exception

        with self.assertRaises(RecursionError):
            self.driver.schedule(self.job)

        self.batch_api.create_namespaced_job.assert_called_once()
        self.core_api.read_namespaced_secret.assert_called_once()

    def test_schedule_job_with_errors_already_exists(self):
        self._stub_api_exception_for_batch(405, '{"reason":"AlreadyExists"}')

        result = self.driver.schedule(self.job)
        self.assertIsNone(result)

        self.batch_api.create_namespaced_job.assert_called_once()
        self.core_api.read_namespaced_secret.assert_called_once()

    def test_build_job_gpu_req(self):
        with patch_gpu_environ(""):
            job = Job.from_dict(get_mock_job("gpu-standard"), self.config)

            k8s_job = self.driver._build_job(job)

            job_spec: V1JobSpec = k8s_job.spec
            job_template: V1PodTemplateSpec = job_spec.template
            self.assertEqual(
                {"memory": "14Gi", "nvidia.com/gpu": "1"},
                job_template.spec.containers[0].resources.limits,
            )

    def test_build_job_gpu_req_node_selector_set(self):
        with patch_gpu_environ(
            '{"cloud.google.com/gke-accelerator":"nvidia-tesla-t4"}'
        ):
            job = Job.from_dict(get_mock_job("gpu-standard"), self.config)

            k8s_job = self.driver._build_job(job)

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

    def test_build_job_tolerations_set(self):
        for job_data in [get_mock_job(), get_mock_job("gpu-standard")]:
            with patch_toleration_environ(
                '[{"key":"dedicated", "value": "gpu-standard"}]'
            ):
                job = Job.from_dict(job_data, self.config)

                k8s_job = self.driver._build_job(job)

                job_spec: V1JobSpec = k8s_job.spec
                job_template: V1PodTemplateSpec = job_spec.template

                self.assertEqual(
                    [{"key": "dedicated", "value": "gpu-standard"}],
                    job_template.spec.tolerations,
                )

    def test_build_job_cpu_req_node_selector_set(self):
        with patch_cpu_environ('{"selector":"my-cpu-node"}'):
            job = Job.from_dict(get_mock_job(), self.config)

            k8s_job = self.driver._build_job(job)

            job_spec: V1JobSpec = k8s_job.spec
            job_template: V1PodTemplateSpec = job_spec.template
            self.assertEqual(
                {"memory": "14Gi"},
                job_template.spec.containers[0].resources.limits,
            )
            print(job_template.spec)
            self.assertEqual(
                {"selector": "my-cpu-node"},
                job_template.spec.node_selector,
            )

    def test_build_job_gpu_req_node_selector_set_invalid(self):
        with patch_gpu_environ("{"):
            job = Job.from_dict(get_mock_job("gpu-standard"), self.config)
            with self.assertRaisesRegex(KubernetesError, "Could not deserialize JSON"):
                self.driver._build_job(job)

    def test_build_job_node_toleration_set_invalid(self):
        with patch_toleration_environ("{"):
            job = Job.from_dict(get_mock_job(), self.config)
            with self.assertRaisesRegex(KubernetesError, "Could not deserialize JSON"):
                self.driver._build_job(job)

    def test_build_job_gpu_req_node_selector_set_not_dictionary(self):
        for test_val in ["1", "[1,2,3]", "[]"]:
            with patch_gpu_environ(test_val):
                job = Job.from_dict(get_mock_job("gpu-standard"), self.config)
                with self.assertRaisesRegex(
                    KubernetesError,
                    "The GPU_NODE_SELECTOR variable was not a JSON dict",
                ):
                    self.driver._build_job(job)

    def test_build_job_cpu_req_node_selector_set_invalid(self):
        with patch_cpu_environ("{"):
            job = Job.from_dict(get_mock_job(), self.config)
            with self.assertRaisesRegex(KubernetesError, "Could not deserialize JSON"):
                self.driver._build_job(job)

    def test_build_job_cpu_req_node_selector_set_not_dictionary(self):
        for test_val in ["1", "[1,2,3]", "[]"]:
            with patch_cpu_environ(test_val):
                job = Job.from_dict(get_mock_job(), self.config)
                with self.assertRaisesRegex(
                    KubernetesError,
                    "The CPU_NODE_SELECTOR variable was not a JSON dict",
                ):
                    self.driver._build_job(job)

    def test_tolerations_set_not_list(self):
        for test_val in ["1", '{"key":"val"}', '"abc"']:
            with patch_toleration_environ(test_val):
                job = Job.from_dict(get_mock_job(), self.config)
                with self.assertRaisesRegex(
                    KubernetesError, "The CPU_TOLERATIONS variable was not a JSON list"
                ):
                    self.driver._build_job(job)
                job = Job.from_dict(get_mock_job("gpu-standard"), self.config)
                with self.assertRaisesRegex(
                    KubernetesError, "The GPU_TOLERATIONS variable was not a JSON list"
                ):
                    self.driver._build_job(job)

    def test_is_job_active_true_then_false(self):
        self.batch_api.read_namespaced_job.side_effect = [
            V1Job(status=V1JobStatus(active=1)),
            V1Job(status=V1JobStatus(active=0)),
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
        "gretel_client.agents.drivers.k8s.KubernetesDriverDaemon.update_pull_secret_thread",
        lambda *args: None,
    )
    @patch(
        "gretel_client.agents.drivers.k8s.KubernetesDriverDaemon.update_liveness_file_thread",
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
