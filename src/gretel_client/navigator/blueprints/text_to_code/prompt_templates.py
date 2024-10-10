TEXT_PROMPT = """\
Your task is to generate the natural language component of a text-to-{lang} dataset, \
carefully following the given context and instructions.

### Context:
{context}

### Instructions:
    * Generate text related to {lang} code based on the given context.
    * Do NOT return any code in the response.
    * Return only the requested text, without any additional comments or instructions.

### Text:
"""


CODE_PROMPT = """\
Your task is to generate {lang} code that corresponds to the text and context given below.

### Text:
{{text}}

### Context:
{context}

### Instructions:
    * Remember to base your response on the given context.
    * Include ONLY a SINGLE block of code WITHOUT ANY additional text.
    
### Code:    
"""


FIELD_GENERATION_PROMPT = """\
Your task is to generate a `{name}` field in a dataset based on the given description and context.

### Description:
{description}

### Context:
{context}

### Instructions:
    * Generate `{name}` as described above.
    * Remember to base your response on the given context.
    {parser_instructions}
    
### Response:
"""
