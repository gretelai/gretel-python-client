import pytest


@pytest.fixture(scope='session')
def records_conditional():
    return [
        {'record':
            {
                'first_name': 'Alex',
                'last_name': 'Watson',
                'user_id': '0003',
                'dni': 'He loves 8.8.8.8 for DNS',
                'city': 'San Diego',
                'state': 'California',
                'lat': 112.22134,
                'lon': 135.76433,
                'user_consent': '1'
            },
            'metadata': {
                'gretel_id': '2732c7ed44a8402f899a01e52a931985',
                'fields': {
                    'lat': {'ner': {'labels': [
                        {'start': 0, 'end': 9,
                         'label': 'latitude', 'score': 1,
                         'source': 'regex',
                         'text': '12.22134'}]}},
                    'lon': {'ner': {'labels': [
                        {'start': 0, 'end': 9,
                         'label': 'longitude', 'score': 1,
                         'source': 'regex',
                         'text': '135.76433'}]}}}}
        },
        {'record':
            {
                'first_name': 'Alex',
                'last_name': 'Ehrath',
                'user_id': '0013',
                'dni': 'He loves 192.168.8.254 for DNS',
                'city': 'San Marcos',
                'state': 'California',
                'lat': 35.659491,
                'lon': 139.72785,
                'user_consent': '0'
            },
            'metadata': {
                'gretel_id': '2732c7ed44a8402f899a01e52a931985',
                'fields': {
                    'lat': {'ner': {'labels': [
                        {'start': 0, 'end': 9,
                         'label': 'latitude', 'score': 1,
                         'source': 'regex',
                         'text': '35.659491'}]}},
                    'lon': {'ner': {'labels': [
                        {'start': 0, 'end': 9,
                         'label': 'longitude', 'score': 1,
                         'source': 'regex',
                         'text': '139.72785'}]}}}}
        }
    ]


@pytest.fixture(scope='session')
def record_dirty_fpe_check():
    return {'Address': '317 Massa. Av.',
            'City': 'Didim',
            'Country': 'Eritrea',
            'Credit Card': '601128 2195205 818',
            'Customer ID': '169/61*009 38-34',
            'Date': '2019-10-08',
            'Name': 'Grimes, Bo H.',
            'Zipcode': '745558'
            }


@pytest.fixture(scope='session')
def record_meta_data_check():
    return {'record': {'Address': '317 Massa. Av.',
                       'City': 'Didim',
                       'Country': 'Eritrea',
                       'Credit Card': '6011282195205818',
                       'Customer ID': '16961009 3834',
                       'Date': '2019-10-08',
                       'Name': 'Grimes, Bo H.',
                       'Zipcode': '745558'},
            'metadata': {
                'gretel_id': '2732c7ed44a8402f899a01e52a931985',
                'fields': {
                    'Country': {
                        'ner': {
                            'labels': [{
                                'start': 0,
                                'end': 7,
                                'label': 'location',
                                'score': None,
                                'source': 'spacy',
                                'text': 'Eritrea'
                            }]
                        }
                    },
                    'Name': {'ner': {
                        'labels': [
                            {'start': 0,
                             'end': 6,
                             'label': 'person_name',
                             'score': None,
                             'source': 'spacy',
                             'text': 'Grimes'}]}},
                    'Credit Card': {'ner': {
                        'labels': [
                            {'start': 0,
                             'end': 16,
                             'label': 'credit_card_number',
                             'score': 1,
                             'source': 'regex.credit_card',
                             'text': '6011282195205818'}]}},
                    'Date': {'ner': {
                        'labels': [
                            {'start': 0,
                             'end': 10,
                             'label': 'date',
                             'score': 1.0,
                             'source': 'datetime',
                             'text': '2019-10-08'}]}}}}}


@pytest.fixture(scope='session')
def records_date_tweak():
    return [
        {
            'first_name': 'Alex',
            'last_name': 'Watson',
            'user_id': '0003',
            'dni': 'He loves 8.8.8.8 for DNS',
            'city': 'San Diego',
            'state': 'California',
            'created': '2016-06-17T18:58:41Z'
        },
        {
            'first_name': 'Alex',
            'last_name': 'Ehrath',
            'user_id': '0013',
            'dni': 'He loves 192.168.8.254 for DNS',
            'city': 'San Marcos',
            'state': 'California',
            'created': '2016-06-17'
        }
    ]


@pytest.fixture(scope='session')
def record_and_meta_2():
    record = {
        'summary': 'Alex Watson <alex@gretel.ai> works at Gretel. Alexander Ehrath used to work at Qualcomm.',
        'dni': 'He loves 8.8.8.8 for DNS',
        'city': 'San Diego',
        'state': 'California',
        'stuff': 'nothing labeled here',
        'latitude': 112.221
    }

    meta = {
        'gretel_id': '2732c7ed44a8402f899a01e52a931985',
        'fields': {
            'summary': {
                'ner': {
                    'labels': [
                        {
                            'start': 0,
                            'end': 11,
                            'score': 0.8,
                            'text': 'Alex Watson',
                            'label': 'person_name',
                        },
                        {
                            'start': 46,
                            'end': 62,
                            'score': 0.8,
                            'text': 'Alexander Ehrath',
                            'label': 'person_name',
                        },
                        {
                            'start': 13,
                            'end': 27,
                            'score': 0.9,
                            'text': 'alex@gretel.ai',
                            'label': 'email_address',
                        },
                        {
                            'start': 38,
                            'end': 44,
                            'score': 0.7,
                            'text': 'Gretel',
                            'label': 'company_name',
                        },
                        {
                            'start': 79,
                            'end': 87,
                            'score': 0.8,
                            'text': 'Qualcomm',
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

    return {'record': record, 'metadata': meta}


@pytest.fixture(scope='session')
def safecast_test_bucket():
    records = {
        'records':
            [{'id': 'rrvewdk3dwb3',
              'ingest_time': 'foo',
              'data':
                  {'payload.device_urn': 'pointcast:10002', 'payload.device_class': 'pointcast',
                   'payload.device_sn': 'Pointcast #10002', 'payload.device': 10002,
                   'payload.when_captured': '2020-03-10T23:58:55Z', 'payload.loc_lat': 35.659491,
                   'payload.loc_lon': 139.72785, 'payload.loc_alt': 92, 'payload.loc_olc': '8Q7XMP5H+Q4X',
                   'payload.env_temp': 21.1, 'payload.bat_voltage': 7.64, 'payload.dev_comms_failures': 534,
                   'payload.dev_restarts': 648, 'payload.dev_free_memory': 53636, 'payload.dev_ntp_count': 1,
                   'payload.dev_last_failure': 'FAILsdcard', 'payload.service_uploaded': '2020-03-10T23:58:55Z',
                   'payload.service_transport': 'pointcast:122.212.234.10',
                   'payload.service_md5': 'abf7a122a5a0c20588d239199c8c6d7f',
                   'payload.service_handler': 'i-051cab8ec0fe30bcd', 'payload.ip_address': '122.212.234.10',
                   'payload.ip_country_code': 'JP', 'payload.ip_city': 'Shibuya',
                   'payload.ip_country_name': 'Japan', 'payload.ip_subdivision': 'Tokyo',
                   'payload.location': '35.659491,139.72785',
                   'origin': 'arn:aws:sns:us-west-2:985752656544:ingest-measurements-prd'}, 'metadata': {'fields': {
                    'payload.service_transport': {'ner': {'labels': [
                        {'start': 10, 'end': 24, 'label': 'ip_address', 'score': 1, 'source': 'regex',
                         'text': '122.212.234.10'}]}}, 'payload.ip_address': {'ner': {'labels': [
                        {'start': 0, 'end': 14, 'label': 'ip_address', 'score': 1, 'source': 'regex',
                         'text': '122.212.234.10'}]}}, 'payload.loc_lat': {'ner': {'labels': [
                        {'start': 0, 'end': 9, 'label': 'latitude', 'score': 1, 'source': 'regex',
                         'text': '35.659491'}]}}, 'payload.loc_lon': {'ner': {'labels': [
                        {'start': 0, 'end': 9, 'label': 'longitude', 'score': 1, 'source': 'regex',
                         'text': '139.72785'}]}}}}},
             {'id': '6gwrzlk665hv',
              'ingest_time': 'foo',
              'data':
                  {'payload.device_urn': 'pointcast:10002',
                   'payload.device_class': 'pointcast',
                   'payload.device_sn': 'Pointcast #10002',
                   'payload.device': 10002,
                   'payload.when_captured': '2020-03-10T23:58:54Z',
                   'payload.loc_lat': 35.659491,
                   'payload.loc_lon': 139.72785, 'payload.loc_alt': 92,
                   'payload.loc_olc': '8Q7XMP5H+Q4X',
                   'payload.lnd_7128ec': 25,
                   'payload.service_uploaded': '2020-03-10T23:58:54Z',
                   'payload.service_transport': 'pointcast:122.212.234.10',
                   'payload.service_md5': 'cb93606463ba99994f832177e39dc6a5',
                   'payload.service_handler': 'i-051a2a353509414f0',
                   'payload.ip_address': '122.212.234.10',
                   'payload.ip_country_code': 'JP',
                   'payload.ip_city': 'Shibuya',
                   'payload.ip_country_name': 'Japan',
                   'payload.ip_subdivision': 'Tokyo',
                   'payload.location': '35.659491,139.72785',
                   'origin': 'arn:aws:sns:us-west-2:985752656544:ingest-measurements-prd'},
              'metadata': {'fields': {'payload.service_transport': {'ner': {
                  'labels': [{'start': 10, 'end': 24, 'label': 'ip_address',
                              'score': 1, 'source': 'regex',
                              'text': '122.212.234.10'}]}},
                  'payload.ip_address': {'ner': {'labels': [
                      {'start': 0, 'end': 14,
                       'label': 'ip_address', 'score': 1,
                       'source': 'regex',
                       'text': '122.212.234.10'}]}},
                  'payload.loc_lat': {'ner': {'labels': [
                      {'start': 0, 'end': 9,
                       'label': 'latitude', 'score': 1,
                       'source': 'regex',
                       'text': '35.659491'}]}},
                  'payload.loc_lon': {'ner': {'labels': [
                      {'start': 0, 'end': 9,
                       'label': 'longitude', 'score': 1,
                       'source': 'regex',
                       'text': '139.72785'}]}}}}},
             {'id': 'p6vn0zp1yja1',
              'ingest_time': 'foo',
              'data':
                  {'payload.device_urn': 'pointcast:10002', 'payload.device_class': 'pointcast',
                   'payload.device_sn': 'Pointcast #10002', 'payload.device': 10002,
                   'payload.when_captured': '2020-03-10T23:58:54Z', 'payload.loc_lat': 35.659491,
                   'payload.loc_lon': 139.72785, 'payload.loc_alt': 92, 'payload.loc_olc': '8Q7XMP5H+Q4X',
                   'payload.lnd_7318u': 12, 'payload.service_uploaded': '2020-03-10T23:58:54Z',
                   'payload.service_transport': 'pointcast:122.212.234.10',
                   'payload.service_md5': '72f2b7cf2132bcc50ea68a2b6bdb6e2d',
                   'payload.service_handler': 'i-0c65ac97805549e0d', 'payload.ip_address': '122.212.234.10',
                   'payload.ip_country_code': 'JP', 'payload.ip_city': 'Shibuya',
                   'payload.ip_country_name': 'Japan', 'payload.ip_subdivision': 'Tokyo',
                   'payload.location': '35.659491,139.72785',
                   'origin': 'arn:aws:sns:us-west-2:985752656544:ingest-measurements-prd'}, 'metadata': {'fields': {
                 'payload.service_transport': {'ner': {'labels': [
                     {'start': 10, 'end': 24, 'label': 'ip_address', 'score': 1, 'source': 'regex',
                      'text': '122.212.234.10'}]}}, 'payload.ip_address': {'ner': {'labels': [
                     {'start': 0, 'end': 14, 'label': 'ip_address', 'score': 1, 'source': 'regex',
                      'text': '122.212.234.10'}]}}, 'payload.loc_lat': {'ner': {'labels': [
                     {'start': 0, 'end': 9, 'label': 'latitude', 'score': 1, 'source': 'regex', 'text': '35.659491'}]}},
                 'payload.loc_lon': {'ner': {'labels': [
                     {'start': 0, 'end': 9, 'label': 'longitude', 'score': 1, 'source': 'regex',
                      'text': '139.72785'}]}}}}}
             ]
    }
    return {'data': records}


@pytest.fixture(scope='session')
def safecast_test_bucket2():
    records = {
        'records':
            [{'id': 'rrvewdk3dwb3',
              'ingest_time': 'foo',
              'data':
                  {'payload.device_urn': 'pointcast:10002', 'payload.device_class': 'pointcast',
                   'payload.device_sn': 'Pointcast #10002', 'payload.device': 10002,
                   'payload.when_captured': '2020-03-10T23:58:55Z', 'payload.loc_lat': 35.659491,
                   'payload.loc_lon': 139.72785, 'payload.loc_alt': 92, 'payload.loc_olc': '8Q7XMP5H+Q4X',
                   'payload.env_temp': 21.1, 'payload.bat_voltage': 7.64, 'payload.dev_comms_failures': 534,
                   'payload.dev_restarts': 648, 'payload.dev_free_memory': 53636, 'payload.dev_ntp_count': 1,
                   'payload.dev_last_failure': 'FAILsdcard', 'payload.service_uploaded': '2020-03-10T23:58:55Z',
                   'payload.service_transport': 'pointcast:122.212.234.10',
                   'payload.service_md5': 'abf7a122a5a0c20588d239199c8c6d7f',
                   'payload.service_handler': 'i-051cab8ec0fe30bcd', 'payload.ip_address': '122.212.234.10',
                   'payload.ip_country_code': 'JP', 'payload.ip_city': 'Shibuya',
                   'payload.ip_country_name': 'Japan', 'payload.ip_subdivision': 'Tokyo',
                   'payload.location': '35.659491,139.72785',
                   'origin': 'arn:aws:sns:us-west-2:985752656544:ingest-measurements-prd'}, 'metadata': {'fields': {
                    'payload.service_transport': {'ner': {'labels': [
                        {'start': 10, 'end': 24, 'label': 'ip_address', 'score': 1, 'source': 'regex',
                         'text': '122.212.234.10'}]}}, 'payload.ip_address': {'ner': {'labels': [
                        {'start': 0, 'end': 14, 'label': 'ip_address', 'score': 1, 'source': 'regex',
                         'text': '122.212.234.10'}]}}, 'payload.loc_lat': {'ner': {'labels': [
                        {'start': 0, 'end': 9, 'label': 'latitude', 'score': 1, 'source': 'regex',
                         'text': '35.659491'}]}}, 'payload.loc_lon': {'ner': {'labels': [
                        {'start': 0, 'end': 9, 'label': 'longitude', 'score': 1, 'source': 'regex',
                         'text': '139.72785'}]}}}}},
             {'id': '6gwrzlk665hv',
              'ingest_time': 'foo',
              'data':
                  {'payload.device_urn': 'pointcast:10002',
                   'payload.device_class': 'pointcast',
                   'payload.device_sn': 'Pointcast #10002',
                   'payload.device': 10002,
                   'payload.when_captured': '2020-03-10T23:58:54Z',
                   'payload.loc_lat': 35.659491,
                   'payload.loc_lon': 139.72785, 'payload.loc_alt': 92,
                   'payload.loc_olc': '8Q7XMP5H+Q4X',
                   'payload.lnd_7128ec': 25,
                   'payload.service_uploaded': '2020-03-10T23:58:54Z',
                   'payload.service_transport': 'pointcast:122.212.234.10',
                   'payload.service_md5': 'cb93606463ba99994f832177e39dc6a5',
                   'payload.service_handler': 'i-051a2a353509414f0',
                   'payload.ip_address': '122.212.234.10',
                   'payload.ip_country_code': 'JP',
                   'payload.ip_city': 'Shibuya',
                   'payload.ip_country_name': 'Japan',
                   'payload.ip_subdivision': 'Tokyo',
                   'payload.location': '35.659491,139.72785',
                   'origin': 'arn:aws:sns:us-west-2:985752656544:ingest-measurements-prd'},
              'metadata': {'fields': {'payload.service_transport': {'ner': {
                  'labels': [{'start': 10, 'end': 24, 'label': 'ip_address',
                              'score': 1, 'source': 'regex',
                              'text': '122.212.234.10'}]}},
                  'payload.ip_address': {'ner': {'labels': [
                      {'start': 0, 'end': 14,
                       'label': 'ip_address', 'score': 1,
                       'source': 'regex',
                       'text': '122.212.234.10'}]}},
                  'payload.loc_lat': {'ner': {'labels': [
                      {'start': 0, 'end': 9,
                       'label': 'latitude', 'score': 1,
                       'source': 'regex',
                       'text': '35.659491'}]}},
                  'payload.loc_lon': {'ner': {'labels': [
                      {'start': 0, 'end': 9,
                       'label': 'longitude', 'score': 1,
                       'source': 'regex',
                       'text': '139.72785'}]}}}}},
             {'id': 'p6vn0zp1yja1',
              'ingest_time': 'foo',
              'data':
                  {'payload.device_urn': 'pointcast:10002', 'payload.device_class': 'pointcast',
                   'payload.device_sn': 'Pointcast #10002', 'payload.device': 10002,
                   'payload.when_captured': '2020-03-10T23:58:54Z', 'payload.loc_lat': 35.659491,
                   'payload.loc_lon': 139.72785, 'payload.loc_alt': 92, 'payload.loc_olc': '8Q7XMP5H+Q4X',
                   'payload.lnd_7318u': 12, 'payload.service_uploaded': '2020-03-10T23:58:54Z',
                   'payload.service_transport': 'pointcast:122.212.234.10',
                   'payload.service_md5': '72f2b7cf2132bcc50ea68a2b6bdb6e2d',
                   'payload.service_handler': 'i-0c65ac97805549e0d', 'payload.ip_address': '122.212.234.10',
                   'payload.ip_country_code': 'JP', 'payload.ip_city': 'Shibuya',
                   'payload.ip_country_name': 'Japan', 'payload.ip_subdivision': 'Tokyo',
                   'payload.location': '35.659491,139.72785',
                   'origin': 'arn:aws:sns:us-west-2:985752656544:ingest-measurements-prd'}, 'metadata': {'fields': {
                 'payload.service_transport': {'ner': {'labels': [
                     {'start': 10, 'end': 24, 'label': 'ip_address', 'score': 1, 'source': 'regex',
                      'text': '122.212.234.10'}]}}, 'payload.ip_address': {'ner': {'labels': [
                     {'start': 0, 'end': 14, 'label': 'ip_address', 'score': 1, 'source': 'regex',
                      'text': '122.212.234.10'}]}}, 'payload.loc_lat': {'ner': {'labels': [
                     {'start': 0, 'end': 9, 'label': 'latitude', 'score': 1, 'source': 'regex', 'text': '35.659491'}]}},
                 'payload.loc_lon': {'ner': {'labels': [
                     {'start': 0, 'end': 9, 'label': 'longitude', 'score': 1, 'source': 'regex',
                      'text': '139.72785'}]}}}}},
             {'id': 'lnj9o0xo6euz',
              'ingest_time': 'foo',
              'data':
                  {'payload.device_urn': 'geigiecast:62007',
                   'payload.device_class': 'geigiecast',
                   'payload.device_sn': 'bGeigiecast #62007',
                   'payload.device': 62007,
                   'payload.when_captured': '2020-03-10T23:58:50Z',
                   'payload.loc_lat': 34.48273, 'payload.loc_lon': 136.16316,
                   'payload.loc_olc': '8Q6RF5M7+37V', 'payload.lnd_7318u': 44,
                   'payload.dev_test': True,
                   'payload.service_uploaded': '2020-03-10T23:58:51Z',
                   'payload.service_transport': 'geigiecast:61.205.85.144',
                   'payload.service_md5': 'b5b622d94f501074111ff6051a833e79',
                   'payload.service_handler': 'i-051cab8ec0fe30bcd',
                   'payload.ip_address': '61.205.85.144',
                   'payload.ip_country_code': 'JP',
                   'payload.ip_city': 'Kashihara-shi',
                   'payload.ip_country_name': 'Japan',
                   'payload.ip_subdivision': 'Nara',
                   'payload.location': '34.48273,136.16316',
                   'origin': 'arn:aws:sns:us-west-2:985752656544:ingest-measurements-prd'},
              'metadata': {'fields': {'payload.service_transport': {'ner': {
                  'labels': [
                      {'start': 11, 'end': 24, 'label': 'ip_address', 'score': 1,
                       'source': 'regex', 'text': '61.205.85.144'}]}},
                  'payload.ip_address': {'ner': {'labels': [
                      {'start': 0, 'end': 13,
                       'label': 'ip_address', 'score': 1,
                       'source': 'regex',
                       'text': '61.205.85.144'}]}},
                  'payload.loc_lat': {'ner': {'labels': [
                      {'start': 0, 'end': 8,
                       'label': 'latitude', 'score': 1,
                       'source': 'regex',
                       'text': '34.48273'}]}},
                  'payload.loc_lon': {'ner': {'labels': [
                      {'start': 0, 'end': 9,
                       'label': 'longitude', 'score': 1,
                       'source': 'regex',
                       'text': '136.16316'}]}}}}},
             {'id': 'o6vnl2lrypt1',
              'ingest_time': 'foo',
              'data':
                  {'payload.device_urn': 'pointcast:10042', 'payload.device_class': 'pointcast',
                   'payload.device_sn': 'Pointcast #10042', 'payload.device': 10042,
                   'payload.when_captured': '2020-03-10T23:58:45Z', 'payload.loc_lat': 37.7233303,
                   'payload.loc_lon': 140.4767968, 'payload.loc_alt': 145, 'payload.loc_olc': '8R92PFFG+8PM',
                   'payload.env_temp': 23.8, 'payload.bat_voltage': 5.03, 'payload.dev_comms_failures': 5990,
                   'payload.dev_restarts': 1542, 'payload.dev_free_memory': 50588,
                   'payload.dev_last_failure': 'no EPOCH', 'payload.service_uploaded': '2020-03-10T23:58:44Z',
                   'payload.service_transport': 'pointcast:103.67.223.44',
                   'payload.service_md5': '044cb5d5e2cd9a873d35c7bce29ddd8d',
                   'payload.service_handler': 'i-051a2a353509414f0', 'payload.ip_address': '103.67.223.44',
                   'payload.ip_country_code': 'JP', 'payload.ip_city': None, 'payload.ip_country_name': 'Japan',
                   'payload.ip_subdivision': None, 'payload.location': '37.7233303,140.4767968',
                   'origin': 'arn:aws:sns:us-west-2:985752656544:ingest-measurements-prd'}, 'metadata': {'fields': {
                 'payload.service_transport': {'ner': {'labels': [
                     {'start': 10, 'end': 23, 'label': 'ip_address', 'score': 1, 'source': 'regex',
                      'text': '103.67.223.44'}]}}, 'payload.ip_address': {'ner': {'labels': [
                     {'start': 0, 'end': 13, 'label': 'ip_address', 'score': 1, 'source': 'regex',
                      'text': '103.67.223.44'}]}}, 'payload.loc_lat': {'ner': {'labels': [
                     {'start': 0, 'end': 10, 'label': 'latitude', 'score': 1, 'source': 'regex',
                      'text': '37.7233303'}]}}, 'payload.loc_lon': {'ner': {'labels': [
                     {'start': 0, 'end': 11, 'label': 'longitude', 'score': 1, 'source': 'regex',
                      'text': '140.4767968'}]}}}}},
             {'id': '6gwrzlzpkjhv',
              'ingest_time': 'foo',
              'data':
                  {'payload.device_urn': 'pointcast:10042',
                   'payload.device_class': 'pointcast',
                   'payload.device_sn': 'Pointcast #10042',
                   'payload.device': 10042,
                   'payload.when_captured': '2020-03-10T23:58:39Z',
                   'payload.loc_lat': 37.7233303,
                   'payload.loc_lon': 140.4767968, 'payload.loc_alt': 145,
                   'payload.loc_olc': '8R92PFFG+8PM',
                   'payload.lnd_7128ec': 15,
                   'payload.service_uploaded': '2020-03-10T23:58:39Z',
                   'payload.service_transport': 'pointcast:103.67.223.44',
                   'payload.service_md5': 'e7aa396b744d524f184830155a77ca97',
                   'payload.service_handler': 'i-051cab8ec0fe30bcd',
                   'payload.ip_address': '103.67.223.44',
                   'payload.ip_country_code': 'JP', 'payload.ip_city': None,
                   'payload.ip_country_name': 'Japan',
                   'payload.ip_subdivision': None,
                   'payload.location': '37.7233303,140.4767968',
                   'origin': 'arn:aws:sns:us-west-2:985752656544:ingest-measurements-prd'},
              'metadata': {'fields': {'payload.service_transport': {'ner': {
                  'labels': [
                      {'start': 10, 'end': 23, 'label': 'ip_address', 'score': 1,
                       'source': 'regex', 'text': '103.67.223.44'}]}},
                  'payload.ip_address': {'ner': {'labels': [
                      {'start': 0, 'end': 13,
                       'label': 'ip_address', 'score': 1,
                       'source': 'regex',
                       'text': '103.67.223.44'}]}},
                  'payload.loc_lat': {'ner': {'labels': [
                      {'start': 0, 'end': 10,
                       'label': 'latitude', 'score': 1,
                       'source': 'regex',
                       'text': '37.7233303'}]}},
                  'payload.loc_lon': {'ner': {'labels': [
                      {'start': 0, 'end': 11,
                       'label': 'longitude', 'score': 1,
                       'source': 'regex',
                       'text': '140.4767968'}]}}}}},
             {'id': 'kjzno9o6j9hz',
              'ingest_time': 'foo',
              'data':
                  {'payload.device_urn': 'pointcast:20105', 'payload.device_class': 'pointcast',
                   'payload.device_sn': 'Pointcast #20105', 'payload.device': 20105,
                   'payload.when_captured': '2020-03-10T23:58:43Z', 'payload.loc_lat': 38.3151,
                   'payload.loc_lon': -123.0752, 'payload.loc_olc': '84CR8W8F+2WV', 'payload.lnd_78017w': 95,
                   'payload.service_uploaded': '2020-03-10T23:58:43Z',
                   'payload.service_transport': 'pointcast:12.235.42.3',
                   'payload.service_md5': 'afa4110616de9bb1b9cd3930eef9b50e',
                   'payload.service_handler': 'i-0c65ac97805549e0d', 'payload.ip_address': '12.235.42.3',
                   'payload.ip_country_code': 'US', 'payload.ip_city': 'Bodega Bay',
                   'payload.ip_country_name': 'United States', 'payload.ip_subdivision': 'California',
                   'payload.location': '38.3151,-123.0752',
                   'origin': 'arn:aws:sns:us-west-2:985752656544:ingest-measurements-prd'}, 'metadata': {'fields': {
                 'payload.service_transport': {'ner': {'labels': [
                     {'start': 10, 'end': 21, 'label': 'ip_address', 'score': 1, 'source': 'regex',
                      'text': '12.235.42.3'}]}}, 'payload.ip_address': {'ner': {'labels': [
                     {'start': 0, 'end': 11, 'label': 'ip_address', 'score': 1, 'source': 'regex',
                      'text': '12.235.42.3'}]}}, 'payload.loc_lat': {'ner': {'labels': [
                     {'start': 0, 'end': 7, 'label': 'latitude', 'score': 1, 'source': 'regex', 'text': '38.3151'}]}},
                 'payload.loc_lon': {'ner': {'labels': [
                     {'start': 0, 'end': 9, 'label': 'longitude', 'score': 1, 'source': 'regex',
                      'text': '-123.0752'}]}}}}},
             {'id': 'y6yp272rglb7',
              'ingest_time': 'foo',
              'data':
                  {'payload.device_urn': 'pointcast:10024',
                   'payload.device_class': 'pointcast',
                   'payload.device_sn': 'Pointcast #10024',
                   'payload.device': 10024,
                   'payload.when_captured': '2020-03-10T23:58:33Z',
                   'payload.loc_lat': 37.54562, 'payload.loc_lon': 140.398995,
                   'payload.loc_alt': 238, 'payload.loc_olc': '8R92G9WX+6HX',
                   'payload.env_temp': 25.6, 'payload.bat_voltage': 8.36,
                   'payload.dev_comms_failures': 1155,
                   'payload.dev_restarts': 501,
                   'payload.dev_free_memory': 53348,
                   'payload.dev_ntp_count': 1, 'payload.dev_last_failure': '',
                   'payload.service_uploaded': '2020-03-10T23:58:33Z',
                   'payload.service_transport': 'pointcast:121.95.25.8',
                   'payload.service_md5': '07c33e23ea4c2d96457a5400b299abc5',
                   'payload.service_handler': 'i-0c65ac97805549e0d',
                   'payload.ip_address': '121.95.25.8',
                   'payload.ip_country_code': 'JP', 'payload.ip_city': None,
                   'payload.ip_country_name': 'Japan',
                   'payload.ip_subdivision': None,
                   'payload.location': '37.54562,140.398995',
                   'origin': 'arn:aws:sns:us-west-2:985752656544:ingest-measurements-prd'},
              'metadata': {'fields': {'payload.service_transport': {'ner': {
                  'labels': [
                      {'start': 10, 'end': 21, 'label': 'ip_address', 'score': 1,
                       'source': 'regex', 'text': '121.95.25.8'}]}},
                  'payload.ip_address': {'ner': {'labels': [
                      {'start': 0, 'end': 11,
                       'label': 'ip_address', 'score': 1,
                       'source': 'regex',
                       'text': '121.95.25.8'}]}},
                  'payload.loc_lat': {'ner': {'labels': [
                      {'start': 0, 'end': 8,
                       'label': 'latitude', 'score': 1,
                       'source': 'regex',
                       'text': '37.54562'}]}},
                  'payload.loc_lon': {'ner': {'labels': [
                      {'start': 0, 'end': 10,
                       'label': 'longitude', 'score': 1,
                       'source': 'regex',
                       'text': '140.398995'}]}}}}},
             {'id': '5gxrz17lvrt2',
              'ingest_time': 'foo',
              'data': {'payload.device_urn': 'ngeigie:74', 'payload.device_class': 'ngeigie',
                       'payload.device_sn': 'nGeigie #74', 'payload.device': 74,
                       'payload.when_captured': '2020-03-10T23:58:34Z',
                       'payload.loc_lat': 34.995197, 'payload.loc_lon': 135.764331,
                       'payload.loc_olc': '8Q6QXQW7+3PG', 'payload.lnd_7318u': 41,
                       'payload.service_uploaded': '2020-03-10T23:58:34Z',
                       'payload.service_transport': 'ngeigie:107.161.164.166',
                       'payload.service_md5': 'be9f5f5dcc6e8f308e5f44ccf2496eda',
                       'payload.service_handler': 'i-051a2a353509414f0',
                       'payload.ip_address': '107.161.164.166', 'payload.ip_country_code': 'US',
                       'payload.ip_city': 'New York', 'payload.ip_country_name': 'United States',
                       'payload.ip_subdivision': 'New York',
                       'payload.location': '34.995197,135.764331',
                       'origin': 'arn:aws:sns:us-west-2:985752656544:ingest-measurements-prd'},
              'metadata': {'fields': {'payload.service_transport': {'ner': {'labels': [
                  {'start': 8, 'end': 23, 'label': 'ip_address', 'score': 1, 'source': 'regex',
                   'text': '107.161.164.166'}]}}, 'payload.ip_address': {'ner': {'labels': [
                  {'start': 0, 'end': 15, 'label': 'ip_address', 'score': 1, 'source': 'regex',
                   'text': '107.161.164.166'}]}}, 'payload.loc_lat': {'ner': {'labels': [
                  {'start': 0, 'end': 9, 'label': 'latitude', 'score': 1, 'source': 'regex', 'text': '34.995197'}]}},
                  'payload.loc_lon': {'ner': {'labels': [
                      {'start': 0, 'end': 10, 'label': 'longitude', 'score': 1, 'source': 'regex',
                       'text': '135.764331'}]}}}}},
             {'id': 'p6vn0z4ky9h1',
              'ingest_time': 'foo',
              'data': {
                  'payload.device_urn': 'pointcast:10042', 'payload.device_class': 'pointcast',
                  'payload.device_sn': 'Pointcast #10042', 'payload.device': 10042,
                  'payload.when_captured': '2020-03-10T23:58:33Z', 'payload.loc_lat': 37.7233303,
                  'payload.loc_lon': 140.4767968, 'payload.loc_alt': 145, 'payload.loc_olc': '8R92PFFG+8PM',
                  'payload.lnd_7318u': 43, 'payload.service_uploaded': '2020-03-10T23:58:32Z',
                  'payload.service_transport': 'pointcast:103.67.223.44',
                  'payload.service_md5': '325d237c0f2fe8624e11d7baa00828a2',
                  'payload.service_handler': 'i-051a2a353509414f0', 'payload.ip_address': '103.67.223.44',
                  'payload.ip_country_code': 'JP', 'payload.ip_city': None, 'payload.ip_country_name': 'Japan',
                  'payload.ip_subdivision': None, 'payload.location': '37.7233303,140.4767968',
                  'origin': 'arn:aws:sns:us-west-2:985752656544:ingest-measurements-prd'}, 'metadata': {'fields': {
                 'payload.service_transport': {'ner': {'labels': [
                     {'start': 10, 'end': 23, 'label': 'ip_address', 'score': 1, 'source': 'regex',
                      'text': '103.67.223.44'}]}}, 'payload.ip_address': {'ner': {'labels': [
                     {'start': 0, 'end': 13, 'label': 'ip_address', 'score': 1, 'source': 'regex',
                      'text': '103.67.223.44'}]}}, 'payload.loc_lat': {'ner': {'labels': [
                     {'start': 0, 'end': 10, 'label': 'latitude', 'score': 1, 'source': 'regex',
                      'text': '37.7233303'}]}}, 'payload.loc_lon': {'ner': {'labels': [
                     {'start': 0, 'end': 11, 'label': 'longitude', 'score': 1, 'source': 'regex',
                      'text': '140.4767968'}]}}}}},
             {'id': 'g7v9gjl1xoh5',
              'ingest_time': 'foo',
              'data': {'payload.device_urn': 'pointcast:10024',
                       'payload.device_class': 'pointcast',
                       'payload.device_sn': 'Pointcast #10024',
                       'payload.device': 10024,
                       'payload.when_captured': '2020-03-10T23:58:32Z',
                       'payload.loc_lat': 37.54562,
                       'payload.loc_lon': 140.398995,
                       'payload.loc_alt': 238, 'payload.loc_olc': '8R92G9WX+6HX',
                       'payload.lnd_7318u': 45,
                       'payload.service_uploaded': '2020-03-10T23:58:32Z',
                       'payload.service_transport': 'pointcast:121.95.25.8',
                       'payload.service_md5': '0c8d1523969e1834cfa56b3472a30e31',
                       'payload.service_handler': 'i-051cab8ec0fe30bcd',
                       'payload.ip_address': '121.95.25.8',
                       'payload.ip_country_code': 'JP', 'payload.ip_city': None,
                       'payload.ip_country_name': 'Japan',
                       'payload.ip_subdivision': None,
                       'payload.location': '37.54562,140.398995',
                       'origin': 'arn:aws:sns:us-west-2:985752656544:ingest-measurements-prd'},
              'metadata': {'fields': {'payload.service_transport': {'ner': {
                  'labels': [
                      {'start': 10, 'end': 21, 'label': 'ip_address', 'score': 1,
                       'source': 'regex', 'text': '121.95.25.8'}]}},
                  'payload.ip_address': {'ner': {'labels': [
                      {'start': 0, 'end': 11,
                       'label': 'ip_address', 'score': 1,
                       'source': 'regex',
                       'text': '121.95.25.8'}]}},
                  'payload.loc_lat': {'ner': {'labels': [
                      {'start': 0, 'end': 8,
                       'label': 'latitude', 'score': 1,
                       'source': 'regex',
                       'text': '37.54562'}]}},
                  'payload.loc_lon': {'ner': {'labels': [
                      {'start': 0, 'end': 10,
                       'label': 'longitude', 'score': 1,
                       'source': 'regex',
                       'text': '140.398995'}]}}}}}

             ]
    }
    return {'data': records}
