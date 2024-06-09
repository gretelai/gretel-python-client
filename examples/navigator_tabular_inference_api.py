from gretel_client import Gretel

gretel = Gretel(api_key="prompt")

# list available backend models for Navigator Tabular
print(gretel.factories.get_navigator_model_list("tabular"))

# the `backend_model` argument is optional and defaults "gretelai/auto"
tabular = gretel.factories.initialize_navigator_api(
    "tabular", backend_model="gretelai/auto"
)

prompt = """\
Generate customer bank transaction data. Include the following columns:
- customer_name
- customer_id
- transaction_date
- transaction_amount
- transaction_type
- transaction_category
- account_balance
"""

# generate tabular data from a natural language prompt
df = tabular.generate(prompt, num_records=25)

# add column to the generated table using the `edit` method
edit_prompt = """\
Add the following column to the provided table:

- customer_address
"""
df_edited = tabular.edit(edit_prompt, seed_data=df)
