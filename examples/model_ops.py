import pandas as pd

from gretel_client import create_project
from gretel_client.helpers import poll

project = create_project()

# create a synthetic model using a default synthetic config from
#   https://github.com/gretelai/gretel-blueprints/blob/main/config_templates/gretel/synthetics/default.yml
model = project.create_model_obj(model_config="synthetics/default")

# this will override the datasource set from the template or blueprint
model.data_source = "https://gretel-public-website.s3.us-west-2.amazonaws.com/datasets/USAdultIncome5k.csv"

# submit the model to Gretel Cloud for training
model.submit(upload_data_source=True)

# wait for the model to training
poll(model)

# read out a preview data from the synthetic model
pd.read_csv(model.get_artifact_link("data_preview"), compression='gzip')
