from gretel_client import Gretel

# connect to your Gretel account
gretel = Gretel(api_key="prompt")

# train a deep generative model from scratch and update the config via kwargs
trained = gretel.submit_train(
    base_config="tabular-actgan",
    data_source="https://gretel-public-website.s3-us-west-2.amazonaws.com/datasets/USAdultIncome5k.csv",
    params={"epochs": 500},
)

# view synthetic data quality scores
print(trained.report)

# display the full report in your browser
trained.report.display_in_browser()

# fetch and inspect the synthetic data used in the report
df_report_synth = trained.fetch_report_synthetic_data()
print(df_report_synth.head())

# generate synthetic data from a trained model
generated = gretel.submit_generate(trained.model_id, num_records=1000)

# inspect the synthetic data
print(generated.synthetic_data.head())
