import pytest

from gretel_client.projects.models import read_model_config
from gretel_client.tuner.sampler import ACTGANConfigSampler


@pytest.mark.parametrize(
    "Sampler,default_config", [(ACTGANConfigSampler, "synthetics/tabular-actgan")]
)
def test_sampler_create_config(Sampler, default_config):
    """Test that each sampler can create a valid model config."""
    sampler = Sampler()
    base_config = read_model_config(default_config)
    sample_kw = {
        "_".join(k.split("_")[:-1]): v[0]
        for k, v in sampler.dict().items()
        if k != "base_model_config"
    }
    sample_config = sampler.create_config(**sample_kw)
    base_params = set(list(base_config["models"][0].values())[0]["params"].keys())
    sample_params = set(list(sample_config["models"][0].values())[0]["params"].keys())
    assert base_params == sample_params
