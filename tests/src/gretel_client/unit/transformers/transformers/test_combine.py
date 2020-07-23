from gretel_client.transformers import DataTransformPipeline, DataPath, CombineConfig
from gretel_client.transformers.base import FieldRef


def test_combine(record_and_meta_2):
    xf_combine = CombineConfig(combine=FieldRef(['latitude', 'city', 'state']), separator=", ")

    data_paths = [
        DataPath(input='dni', xforms=xf_combine, output='everything'),
    ]

    xf = DataTransformPipeline(data_paths)

    check_aw = xf.transform_record(record_and_meta_2)
    assert check_aw['record'] == {'everything': 'He loves 8.8.8.8 for DNS, 112.221, San Diego, California'}
