from gretel_client import Gretel

gretel = Gretel(api_key="prompt")

# list available backend models for Navigator LLM
print(gretel.factories.get_navigator_model_list("natural_language"))

llm = gretel.factories.initialize_navigator_api("natural_language")

print(
    llm.generate(
        prompt="Tell me a funny joke about data scientists.",
        temperature=0.5,
        max_tokens=100,
    )
)
