"""
Use the 'labels' parameter to target the entities Gretel has identified in your data.
Use the 'minimum_score' parameter to transform only high quality matches.
"""
from gretel_client.transformers import (
    RedactWithLabelConfig,
    DataPath,
    DataTransformPipeline,
    Score,
)


record = {
    'record': {
        'summary': 'John Doe <john.doe@spacely.com> works at Spacely Sprockets. Jane Doe used to work at Example.com.',
        'dni': 'He loves 8.8.8.8 for DNS',
        'city': 'San Diego',
        'state': 'California',
        'stuff': 'nothing labeled here',
        'latitude': 112.221
    },
    'metadata': {
        'gretel_id': '2732c7ed44a8402f899a01e52a931985',
        'fields': {
            'summary': {
                'ner': {
                    'labels': [
                        {
                            'start': 0,
                            'end': 8,
                            'score': 0.8,
                            'text': 'John Doe',
                            'label': 'person_name',
                        },
                        {
                            'start': 60,
                            'end': 68,
                            'score': 0.8,
                            'text': 'Jane Doe',
                            'label': 'person_name',
                        },
                        {
                            'start': 10,
                            'end': 30,
                            'score': 0.9,
                            'text': 'john.doe@spacely.com',
                            'label': 'email_address',
                        },
                        {
                            'start': 41,
                            'end': 58,
                            'score': 0.7,
                            'text': 'Spacely Sprockets',
                            'label': 'company_name',
                        },
                        {
                            'start': 85,
                            'end': 96,
                            'score': 0.8,
                            'text': 'Example.com',
                            'label': 'company_name',
                        }
                    ]
                }
            },
            'dni': {
                'ner': {
                    'labels': [
                        {
                            'start': 9,
                            'end': 16,
                            'score': 1.0,
                            'text': '8.8.8.8',
                            'label': 'ip_address'
                        }
                    ]
                }
            },
            'city': {
                'ner': {
                    'labels': [
                        {
                            'start': 0,
                            'end': 9,
                            'score': 1.0,
                            'text': 'San Diego',
                            'label': 'location_city'
                        }
                    ]
                }
            },
            'state': {
                'ner': {
                    'labels': [
                        {
                            'start': 0,
                            'end': 10,
                            'score': 1.0,
                            'text': 'California',
                            'label': 'location_state'
                        }
                    ]
                }
            },
            'latitude': {
                'ner': {
                    'labels': [
                        {
                            'start': 0,
                            'end': 7,
                            'score': 1,
                            'text': '112.221',
                            'label': 'latitude'
                        }
                    ]
                }
            }
        }
    }
}

entity_xf_list = [
    # Replace names with PERSON_NAME. Should be applied to all.
    RedactWithLabelConfig(labels=['person_name']),

    # Replace names with COMPANY_NAME. Should be applied to Example.com but not Spacely Sprockets.
    RedactWithLabelConfig(labels=['company_name'], minimum_score=Score.HIGH),
]

data_paths = [
    DataPath(input='summary', xforms=entity_xf_list),
    # Transforms should be no-ops for all these, no matching entities.
    DataPath(input='dni', xforms=entity_xf_list),
    DataPath(input='city', xforms=entity_xf_list),
    DataPath(input='state', xforms=entity_xf_list),
    DataPath(input='stuff', xforms=entity_xf_list),
    DataPath(input='latitude', xforms=entity_xf_list)
]

xf = DataTransformPipeline(data_paths)

check = xf.transform_record(record).get('record')

print(check)

assert check == {
    'summary': 'PERSON_NAME <john.doe@spacely.com> works at Spacely Sprockets. PERSON_NAME used to work at '
               'COMPANY_NAME.',
    'dni': 'He loves 8.8.8.8 for DNS',
    'city': 'San Diego',
    'state': 'California',
    'stuff': 'nothing labeled here',
    'latitude': 112.221
}
