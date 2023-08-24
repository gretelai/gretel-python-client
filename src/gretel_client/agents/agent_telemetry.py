from opentelemetry import metrics
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from prometheus_client import start_http_server

# Service name is required for most backends
AGENT_SERVICE_NAME = "gretel-agent"

# Initialize PrometheusMetricReader which pulls metrics from the SDK
# on-demand to respond to scrape requests
reader = PrometheusMetricReader()
resource = Resource(attributes={SERVICE_NAME: AGENT_SERVICE_NAME})
provider = MeterProvider(resource=resource, metric_readers=[reader])
metrics.set_meter_provider(provider)
meter = metrics.get_meter(__name__)
JOB_COUNTER = meter.create_counter(
    name="jobs_scheduled", unit="count", description="Count of jobs scheduled"
)
JOB_COUNTER.add(0, {"error": False})
MAX_WORKER_GAUGE = meter.create_up_down_counter(name="max_workers", unit="count")
ACTIVE_JOBS_COUNTER = meter.create_up_down_counter(name="active_jobs", unit="count")


def setup_prometheus(port: int = 8080):
    # Start Prometheus client
    start_http_server(port=port)


def increment_job_count(error: bool = False):
    JOB_COUNTER.add(1, attributes={"error": error})


def set_max_workers(
    max_workers: int,
    previous_max_workers: int = 0,
):
    """There is only an observable version of the gauge, so we simply use an up down counter
    and subtract the previous value if it's greater than zero"""
    if previous_max_workers > 0:
        MAX_WORKER_GAUGE.add(-previous_max_workers)
    MAX_WORKER_GAUGE.add(max_workers)


def increase_active_jobs():
    ACTIVE_JOBS_COUNTER.add(1)


def decrease_active_jobs():
    ACTIVE_JOBS_COUNTER.add(-1)
