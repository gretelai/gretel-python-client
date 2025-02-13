import pandas as pd

from gretel_client import create_project, poll

project = create_project()

# create a synthetic model using a default synthetic config from
#   https://github.com/gretelai/gretel-blueprints/blob/main/config_templates/gretel/synthetics/tabular-actgan.yml
#
#   Providing a data_source will override the datasource from the template. If the data source is a local
#   file, then it will automatically be uploaded to Gretel Cloud as part of the submission step
model = project.create_model_obj(
    model_config="synthetics/tabular-actgan",
    data_source="https://gretel-public-website.s3.us-west-2.amazonaws.com/datasets/USAdultIncome5k.csv",
)

# submit the model to Gretel Cloud for training
model.submit()

# wait for the model to training
poll(model)

# read out a preview data from the synthetic model
pd.read_csv(model.get_artifact_link("data_preview"), compression="gzip")
