from gretel_client.transformers.base import FieldRef
from gretel_client.transformers.data_restore_pipeline import DataRestorePipeline
from gretel_client.transformers import DataTransformPipeline, DataPath, BucketConfig
from gretel_client.transformers.data_transform_pipeline import RECORD_KEYS
from gretel_client.transformers.fpe import crypto_aes
from gretel_client.transformers import FormatConfig, CombineConfig, ConditionalConfig, \
    DateShiftConfig, DropConfig, FakeConstantConfig, RedactWithCharConfig, RedactWithLabelConfig, \
    RedactWithStringConfig, FpeFloatConfig, FpeStringConfig, SecureHashConfig, Score
from gretel_client.transformers.string_mask import StringMask
from gretel_client.transformers.transformers.bucket import Bucket

SEED = 8675309


def test_record_xf(record_and_meta_2):
    # empty transformer
    entity_xf_list = [
        # replace names with PERSON_NAM
        RedactWithLabelConfig(labels=['person_name']),

        # swap emails with fake (but consistent emails)
        FakeConstantConfig(labels=['email_address'], seed=SEED),

        # character-redact IP addresses
        RedactWithCharConfig(labels=['ip_address']),

        # this should not be run
        RedactWithCharConfig(char='N', labels=['location_city']),

        # secure hash
        SecureHashConfig(secret='rockybalboa', labels=['location_state']),

        # replace latitude
        FakeConstantConfig(labels=['latitude'], seed=SEED)
    ]
    # field redact entire city
    city_redact = RedactWithCharConfig(char='Y')

    data_paths = [
        DataPath(input='summary', xforms=entity_xf_list),
        DataPath(input='dni', xforms=entity_xf_list),
        DataPath(input='city', xforms=[entity_xf_list, city_redact]),
        DataPath(input='state', xforms=entity_xf_list),
        DataPath(input='stuff', xforms=entity_xf_list),
        DataPath(input='latitude', xforms=entity_xf_list)
    ]

    xf = DataTransformPipeline(data_paths)

    check1 = xf.transform_record(record_and_meta_2).get('record')

    data_paths = [
        DataPath(input='city', xforms=[entity_xf_list, city_redact]),
        DataPath(input='*', xforms=entity_xf_list),
    ]

    xf = DataTransformPipeline(data_paths)

    check2 = xf.transform_record(record_and_meta_2).get('record')

    assert check1 == {
        'summary': 'PERSON_NAME <pauldaniels@morrison-rosales.biz> works at Gretel. PERSON_NAME used to work at '
                   'Qualcomm.',
        'dni': 'He loves X.X.X.X for DNS',
        'city': 'YYY YYYYY',
        'state': '8896cd9f38ceac0e98f47c41a2028219f17d8ef41277e4e2138d52a08c24e0aa',
        'stuff': 'nothing labeled here',
        'latitude': -89.3146475}

    assert check2 == {
        'summary': 'PERSON_NAME <pauldaniels@morrison-rosales.biz> works at Gretel. PERSON_NAME used to work at '
                   'Qualcomm.',
        'dni': 'He loves X.X.X.X for DNS',
        'city': 'YYY YYYYY',
        'state': '8896cd9f38ceac0e98f47c41a2028219f17d8ef41277e4e2138d52a08c24e0aa',
        'stuff': 'nothing labeled here',
        'latitude': -89.3146475}
    # now add a drop field that contains an entity
    entity_xf_list.insert(0, DropConfig(labels=['ip_address']))

    data_paths = [
        DataPath(input='summary', xforms=entity_xf_list),
        DataPath(input='dni', xforms=entity_xf_list),
        DataPath(input='city', xforms=[entity_xf_list, city_redact]),
        DataPath(input='state', xforms=entity_xf_list),
        DataPath(input='stuff', xforms=entity_xf_list),
        DataPath(input='latitude', xforms=entity_xf_list)
    ]

    xf = DataTransformPipeline(data_paths)

    check = xf.transform_record(record_and_meta_2).get('record')

    assert check == {
        'summary': 'PERSON_NAME <pauldaniels@morrison-rosales.biz> works at Gretel. PERSON_NAME used to work at '
                   'Qualcomm.',
        'city': 'YYY YYYYY',
        'state': '8896cd9f38ceac0e98f47c41a2028219f17d8ef41277e4e2138d52a08c24e0aa',
        'stuff': 'nothing labeled here',
        'latitude': -89.3146475}


def test_filter_by_score(record_and_meta_2):
    entity_xf_list = [
        # Replace names with PERSON_NAME. Should be applied to all.
        RedactWithLabelConfig(labels=['person_name'], minimum_score=Score.HIGH),

        # Replace names with XXXX. Should be applied to Qualcomm but not Gretel.
        RedactWithCharConfig(labels=['company_name'], minimum_score=Score.HIGH),

        # Replace names with LOCATION_CITY. Should be applied to San Diego.
        RedactWithLabelConfig(labels=['location_city']),
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
    check = xf.transform_record(record_and_meta_2).get('record')
    assert check == {
        'summary': 'PERSON_NAME <alex@gretel.ai> works at Gretel. PERSON_NAME used to work at '
                   'XXXXXXXX.',
        'dni': 'He loves 8.8.8.8 for DNS',
        'city': 'LOCATION_CITY',
        'state': 'California',
        'stuff': 'nothing labeled here',
        'latitude': 112.221
    }


def test_record_drop_field():
    rec = {'foo': 'bar', 'drop_me': 'bye'}
    drop = DropConfig()
    data_paths = [DataPath(input='drop_me', xforms=drop),
                  DataPath(input='*')]
    xf = DataTransformPipeline(data_paths)
    assert xf.transform_record(rec) == {'foo': 'bar'}


def test_record_zero_fpe():
    rec = {'latitude': 0.0, 'longitude': -0.0, 'credit_card': '4123567891234567', 'the_dude': 100000000,
           'the_hotness': "convertme", "the_sci_notation": 1.23E-7}
    numbers_xf = [FpeStringConfig(
        secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94", radix=10)]

    float_xf = [FpeFloatConfig(
        secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94", radix=10,
        float_precision=3)]
    text_xf = [
        FpeStringConfig(secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94", radix=36)]

    data_paths = [
        DataPath(input='credit_card', xforms=numbers_xf),
        DataPath(input='latitude', xforms=float_xf),

        DataPath(input='longitude', xforms=float_xf),
        DataPath(input='the_dude', xforms=numbers_xf),
        DataPath(input='the_sci_notation', xforms=float_xf),
        DataPath(input='the_hotness', xforms=text_xf)
    ]
    xf = DataTransformPipeline(data_paths)
    rf = DataRestorePipeline(data_paths)
    xf_payload = xf.transform_record(rec)
    check = xf_payload.get('credit_card')
    assert check == '5931468769662449'
    check = xf_payload.get('longitude')
    assert check == -1.32547939979e-312
    check = xf_payload.get('latitude')
    assert check == 1.32547939979e-312
    check = xf_payload.get('the_hotness')
    assert check == '2qjuxg7ju'
    check = xf_payload.get('the_dude')
    assert check == 128994144
    check = xf_payload.get('the_sci_notation')
    assert check == 1.229570610794763e-07
    check = rf.transform_record(xf_payload)
    assert check == rec


def test_record_fpe():
    rec = {'latitude': -70.783, 'longitude': -112.221, 'credit_card': '4123567891234567', 'the_dude': 100000000,
           'the_hotness': "convertme", "the_sci_notation": 1.23E-7}
    numbers_xf = [FpeStringConfig(
        secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94", radix=10)]

    float_xf = [FpeFloatConfig(
        secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94", radix=10,
        float_precision=3)]
    text_xf = [
        FpeStringConfig(secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94", radix=36)]

    data_paths = [DataPath(input='credit_card', xforms=numbers_xf),
                  DataPath(input='longitude', xforms=float_xf),
                  DataPath(input='latitude', xforms=float_xf),
                  DataPath(input='the_dude', xforms=numbers_xf),
                  DataPath(input='the_sci_notation', xforms=float_xf),
                  DataPath(input='the_hotness', xforms=text_xf)
                  ]
    xf = DataTransformPipeline(data_paths)
    rf = DataRestorePipeline(data_paths)
    xf_payload = xf.transform_record(rec)
    check = xf_payload.get('credit_card')
    assert check == '5931468769662449'
    check = xf_payload.get('longitude')
    assert check == -112.22154173039645
    check = xf_payload.get('latitude')
    assert check == -70.78287074710897
    check = xf_payload.get('the_hotness')
    assert check == '2qjuxg7ju'
    check = xf_payload.get('the_dude')
    assert check == 128994144
    check = xf_payload.get('the_sci_notation')
    assert check == 1.229570610794763e-07
    check = rf.transform_record(xf_payload)
    assert check == rec


def test_pipe_record_fpe(record_and_meta_2):
    xf_fpe = FpeFloatConfig(secret='2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94', radix=10,
                            labels=['latitude'])
    data_paths = [DataPath(input='latitude', xforms=xf_fpe),
                  DataPath(input='*')]

    xf = DataTransformPipeline(data_paths)
    rf = DataRestorePipeline(data_paths)
    check = xf.transform_record(record_and_meta_2)
    assert check['record'] == {'summary': 'Alex Watson <alex@gretel.ai> works at Gretel. Alexander Ehrath '
                                          'used to work at Qualcomm.',
                               'dni': 'He loves 8.8.8.8 for DNS',
                               'city': 'San Diego',
                               'state': 'California', 'stuff': 'nothing labeled here',
                               'latitude': 124.10051071657566}

    check = rf.transform_record(check)
    assert check['record'] == {'summary': 'Alex Watson <alex@gretel.ai> works at Gretel. Alexander Ehrath '
                                          'used to work at Qualcomm.',
                               'dni': 'He loves 8.8.8.8 for DNS',
                               'city': 'San Diego',
                               'state': 'California', 'stuff': 'nothing labeled here',
                               'latitude': 112.221}

    check = xf.transform_record(check)
    assert check['record'] == {'summary': 'Alex Watson <alex@gretel.ai> works at Gretel. Alexander Ehrath '
                                          'used to work at Qualcomm.',
                               'dni': 'He loves 8.8.8.8 for DNS',
                               'city': 'San Diego',
                               'state': 'California', 'stuff': 'nothing labeled here',
                               'latitude': 124.10051071657566}


def test_pipe_date_shift(records_date_tweak):
    # run tests with user_id to tweak the de-identified date

    xf_user_id = FpeStringConfig(secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94", radix=10)
    xf_date = DateShiftConfig(secret='2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94',
                              lower_range_days=-10, upper_range_days=25,
                              tweak=FieldRef('user_id'))

    data_paths = [DataPath(input='user_id', xforms=xf_user_id),
                  DataPath(input='created', xforms=xf_date),
                  DataPath(input='*')
                  ]

    xf = DataTransformPipeline(data_paths)
    rf = DataRestorePipeline(data_paths)
    check_aw = xf.transform_record(records_date_tweak[0])
    check_ae = xf.transform_record(records_date_tweak[1])
    assert check_aw['created'] == '2016-07-06'
    assert check_ae['created'] == '2016-06-20'
    check_aw = rf.transform_record(check_aw)
    check_ae = rf.transform_record(check_ae)
    assert check_aw['created'] == '2016-06-17'
    assert check_ae['created'] == '2016-06-17'

    # run tests without tweaking the de-identified date
    xf_date = DateShiftConfig(secret='2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94',
                              lower_range_days=-10, upper_range_days=25)

    data_paths = [DataPath(input='created', xforms=xf_date)]

    xf = DataTransformPipeline(data_paths)
    rf = DataRestorePipeline(data_paths)
    check_aw = xf.transform_record(records_date_tweak[0])
    check_ae = xf.transform_record(records_date_tweak[1])
    assert check_aw['created'] == '2016-06-13'
    assert check_ae['created'] == '2016-06-13'
    record_and_meta_aw = check_aw
    record_and_meta_ae = check_ae
    check_aw = rf.transform_record(record_and_meta_aw)
    check_ae = rf.transform_record(record_and_meta_ae)
    assert check_aw['created'] == '2016-06-17'
    assert check_ae['created'] == '2016-06-17'


def test_pipe_date_shift_cbc_fast(records_date_tweak):
    # run tests with user_id to tweak the de-identified date

    xf_user_id = FpeStringConfig(secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94", radix=10,
                                 aes_mode=crypto_aes.Mode.CBC_FAST)
    xf_date = DateShiftConfig(secret='2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94',
                              lower_range_days=-10, upper_range_days=25,
                              tweak=FieldRef('user_id'))

    data_paths = [DataPath(input='user_id', xforms=xf_user_id),
                  DataPath(input='created', xforms=xf_date),
                  DataPath(input='*')
                  ]

    xf = DataTransformPipeline(data_paths)
    rf = DataRestorePipeline(data_paths)
    check_aw = xf.transform_record(records_date_tweak[0])
    check_ae = xf.transform_record(records_date_tweak[1])
    assert check_aw['created'] == '2016-06-18'
    assert check_ae['created'] == '2016-06-18'
    check_ae = rf.transform_record(check_ae)
    check_aw = rf.transform_record(check_aw)
    assert check_aw['created'] == '2016-06-17'
    assert check_ae['created'] == '2016-06-17'

    # run tests without tweaking the de-identified date
    xf_date = DateShiftConfig(secret='2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94',
                              lower_range_days=-10, upper_range_days=25)

    data_paths = [DataPath(input='created', xforms=xf_date)]

    xf = DataTransformPipeline(data_paths)
    rf = DataRestorePipeline(data_paths)
    check_aw = xf.transform_record(records_date_tweak[0])
    check_ae = xf.transform_record(records_date_tweak[1])
    assert check_aw['created'] == '2016-06-13'
    assert check_ae['created'] == '2016-06-13'
    record_and_meta_aw = check_aw
    record_and_meta_ae = check_ae
    check_aw = rf.transform_record(record_and_meta_aw)
    check_ae = rf.transform_record(record_and_meta_ae)
    assert check_aw['created'] == '2016-06-17'
    assert check_ae['created'] == '2016-06-17'


def test_meta_data_transform(record_meta_data_check):
    entity_xf = [
        RedactWithLabelConfig(labels=['date']),
        SecureHashConfig(secret='rockybalboa', labels=['location']),
        FpeStringConfig(labels=['credit_card_number'],
                        secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94",
                        radix=10)
    ]
    data_paths = [DataPath(input='*', xforms=entity_xf)]
    xf = DataTransformPipeline(data_paths)
    rf = DataRestorePipeline(data_paths)
    transformed = xf.transform_record(record_meta_data_check)
    assert transformed['record']['Credit Card'] == '4471585942734458'
    assert transformed['metadata']['fields']['Credit Card']['ner']['labels'][0]['text'] == '4471585942734458'
    assert transformed['metadata']['fields']['Country']['ner']['labels'][0]['start'] == 0
    assert transformed['metadata']['fields']['Country']['ner']['labels'][0]['end'] == 64
    restored = rf.transform_record(transformed)
    assert restored['record']['Credit Card'] == record_meta_data_check['record']['Credit Card']


def test_pipe_record_filter(record_meta_data_check):
    entity_xf = [
        RedactWithLabelConfig(labels=['date']),
        SecureHashConfig(secret='rockybalboa', labels=['location']),
        FpeStringConfig(labels=['credit_card_number'],
                        secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94",
                        radix=10)
    ]
    data_paths = [
        DataPath(input='Country', xforms=entity_xf),
        DataPath(input='?ddress', xforms=entity_xf),
        DataPath(input='Cr*', xforms=entity_xf)
    ]
    xf = DataTransformPipeline(data_paths)
    rf = DataRestorePipeline(data_paths)
    transformed = xf.transform_record(record_meta_data_check)
    assert transformed['record']['Credit Card'] == '4471585942734458'
    assert transformed['metadata']['fields']['Credit Card']['ner']['labels'][0]['text'] == '4471585942734458'
    assert transformed['metadata']['fields']['Country']['ner']['labels'][0]['start'] == 0
    assert transformed['metadata']['fields']['Country']['ner']['labels'][0]['end'] == 64
    # The metadata has one entry less than record entries, because Address does not have meta data in this test.
    assert len(transformed['metadata']['fields']) == 2
    assert len(transformed['record']) == 3
    restored = rf.transform_record(transformed)
    assert restored['record']['Credit Card'] == record_meta_data_check['record']['Credit Card']


def test_fpe_dirty_transform(record_dirty_fpe_check):
    field_xf = FpeStringConfig(secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94", radix=10)
    data_paths = [
        DataPath(input='Credit Card', xforms=field_xf),
        DataPath(input='Customer ID', xforms=field_xf),
        DataPath(input='*')
    ]
    xf = DataTransformPipeline(data_paths)
    rf = DataRestorePipeline(data_paths)
    transformed = xf.transform_record(record_dirty_fpe_check)
    assert transformed['Credit Card'] == '447158 5942734 458'
    assert transformed['Customer ID'] == '747/52*232 83-19'
    restored = rf.transform_record(transformed)
    assert restored == record_dirty_fpe_check


def test_record_fpe_precision():
    rec = {'latitude': -70.783, 'longitude': -112.221, 'credit_card': '4123567891234567', 'the_dude': 100000000,
           'the_hotness': "convertme", "the_sci_notation": 1.23E-7}
    int_xf = FpeStringConfig(secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94", radix=10)

    num1_xf = FpeFloatConfig(secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94", radix=10,
                             float_precision=1)

    num2_xf = FpeFloatConfig(secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94", radix=10,
                             float_precision=0)

    num3_xf = FpeFloatConfig(secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94", radix=10,
                             float_precision=1)

    num4_xf = FpeStringConfig(secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94", radix=36)

    data_paths = [
        DataPath(input='credit_card', xforms=int_xf),
        DataPath(input='latitude', xforms=num1_xf),
        DataPath(input='the_dude', xforms=int_xf),
        DataPath(input='longitude', xforms=num2_xf),
        DataPath(input='the_sci_notation', xforms=num3_xf),
        DataPath(input='the_hotness', xforms=num4_xf),
        DataPath(input='*')
    ]
    xf = DataTransformPipeline(data_paths)
    rf = DataRestorePipeline(data_paths)
    xf_payload = xf.transform_record(rec)
    check = xf_payload.get('credit_card')
    assert check == '5931468769662449'
    check = xf_payload.get('longitude')
    assert check == -112.2929577756414
    check = xf_payload.get('latitude')
    assert check == -70.78143312456855
    check = xf_payload.get('the_hotness')
    assert check == '2qjuxg7ju'
    check = xf_payload.get('the_dude')
    assert check == 128994144
    check = xf_payload.get('the_sci_notation')
    assert check == 1.2342967235924508e-07
    check = rf.transform_record(xf_payload)
    assert check == rec


def test_record_output_map_and_schemas():
    rec = {'a': 1.23, 'b': 2.34, 'c': 3.45, 'd': 4.56, 'e': 5.67}
    rec2 = {'f': 1.23, 'b': 2.34, 'c': 3.45, 'd': 4.56, 'e': 5.67}
    test_payloads = [(rec, record_key) for record_key in RECORD_KEYS]
    test_payloads.append((rec, None))
    for payload, record_key in test_payloads:
        xf_list = FpeFloatConfig(secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94", radix=10)

        data_paths = [
            DataPath(input='a', output='x'),
            DataPath(input='b', output='y'),
            DataPath(input='c', xforms=xf_list, output='z'),
            DataPath(input='d', xforms=xf_list),
            DataPath(input='e', xforms=xf_list),
            DataPath(input='*')
        ]
        xf = DataTransformPipeline(data_paths)
        rf = DataRestorePipeline(data_paths)
        xf_payload = xf.transform_record(payload)
        xf_record = xf_payload.get(record_key) or xf_payload
        check = xf_record.get('x')
        assert check == 1.23
        check = xf_record.get('y')
        assert check == 2.34
        check = xf_record.get('z')
        assert check == 3.590038584114511
        check = xf_record.get('d')
        assert check == 7.002521213914073
        check = xf_record.get('e')
        assert check == 4.9570355284951875
        check = rf.transform_record(xf_payload)
        check = check.get(record_key) or check
        assert check == rec

        # test multiple names mapping to the same output field
        xf_list = FpeStringConfig(secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94", radix=10)
        data_paths = [
            DataPath(input='a', xforms=xf_list, output='x'),
            DataPath(input='f', xforms=xf_list, output='x'),
            DataPath(input='b', xforms=xf_list, output='y'),
            DataPath(input='c', xforms=xf_list, output='z'),
            DataPath(input='*')
        ]
        xf = DataTransformPipeline(data_paths)
        xf_payload = xf.transform_record(rec)
        xf_payload2 = xf.transform_record(rec2)
        xf_record = xf_payload.get(record_key) or xf_payload
        xf_record2 = xf_payload2.get(record_key) or xf_payload2

        assert xf_record == xf_record2


def test_pipe_combine(records_date_tweak):
    xf_combine = CombineConfig(combine=FieldRef(['first_name', 'city', 'state']), separator=", ")

    data_paths = [
        DataPath(input='last_name', xforms=xf_combine, output='name_location'),
    ]

    xf = DataTransformPipeline(data_paths)

    check_aw = xf.transform_record(records_date_tweak[0])
    assert check_aw == {'name_location': 'Watson, Alex, San Diego, California'}
    check_ae = xf.transform_record(records_date_tweak[1])
    assert check_ae == {'name_location': 'Ehrath, Alex, San Marcos, California'}


def test_conditional_transformer(records_conditional):
    xf_fpe = FpeFloatConfig(secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94", radix=10)
    xf_consent = ConditionalConfig(conditional_value=FieldRef('user_consent'), regex=r"['1']",
                                   true_xform=xf_fpe,
                                   false_xform=RedactWithLabelConfig())

    data_paths_encrypt = [DataPath(input='lon', xforms=xf_fpe),
                          DataPath(input='lat', xforms=xf_fpe),
                          DataPath(input='*')
                          ]

    data_paths_decrypt = [DataPath(input='lon', xforms=xf_consent),
                          DataPath(input='lat', xforms=xf_consent),
                          DataPath(input='*')
                          ]

    xf_encrypt = DataTransformPipeline(data_paths_encrypt)
    xf_decrypt = DataRestorePipeline(data_paths_decrypt)
    check_aw = xf_encrypt.transform_record(records_conditional[0])
    check_ae = xf_encrypt.transform_record(records_conditional[1])
    assert check_ae['record']['lat'] == 50.65564864394322
    assert check_ae['record']['lon'] == 191.8142181740291
    assert check_aw['record']['lat'] == 77.00217823076872
    assert check_aw['record']['lon'] == 254.0404040486477
    check_aw = xf_decrypt.transform_record(check_aw)
    check_ae = xf_decrypt.transform_record(check_ae)
    assert check_ae['record']['lat'] == 'LATITUDE'
    assert check_ae['record']['lon'] == 'LONGITUDE'
    assert check_aw['record']['lat'] == 112.22134
    assert check_aw['record']['lon'] == 135.76433

    xf_fpe = FpeFloatConfig(secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94", radix=10)
    xf_consent = ConditionalConfig(conditional_value=FieldRef('user_consent'), regex=r"['1']",
                                   true_xform=xf_fpe)

    data_paths_encrypt = [DataPath(input='lon', xforms=xf_fpe),
                          DataPath(input='lat', xforms=xf_fpe),
                          DataPath(input='*')
                          ]

    data_paths_decrypt = [DataPath(input='lon', xforms=xf_consent),
                          DataPath(input='lat', xforms=xf_consent),
                          DataPath(input='*')
                          ]

    xf_encrypt = DataTransformPipeline(data_paths_encrypt)
    xf_decrypt = DataRestorePipeline(data_paths_decrypt)
    check_aw = xf_encrypt.transform_record(records_conditional[0])
    check_ae = xf_encrypt.transform_record(records_conditional[1])
    assert check_ae['record']['lat'] == 50.65564864394322
    assert check_ae['record']['lon'] == 191.8142181740291
    assert check_aw['record']['lat'] == 77.00217823076872
    assert check_aw['record']['lon'] == 254.0404040486477
    check_aw = xf_decrypt.transform_record(check_aw)
    check_ae = xf_decrypt.transform_record(check_ae)
    assert check_ae['record']['lat'] == 50.65564864394322
    assert check_ae['record']['lon'] == 191.8142181740291
    assert check_aw['record']['lat'] == 112.22134
    assert check_aw['record']['lon'] == 135.76433

    xf_fpe = FpeFloatConfig(secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94", radix=10)
    xf_consent = ConditionalConfig(conditional_value=FieldRef('user_consent'), regex=r"['1']",
                                   false_xform=xf_fpe)

    data_paths_decrypt = [DataPath(input='lon', xforms=xf_consent),
                          DataPath(input='lat', xforms=xf_consent),
                          DataPath(input='*')
                          ]

    xf_decrypt = DataRestorePipeline(data_paths_decrypt)
    check_aw = xf_decrypt.transform_record(check_aw)
    check_ae = xf_decrypt.transform_record(check_ae)
    assert check_ae['record']['lat'] == 35.659491
    assert check_ae['record']['lon'] == 139.72785
    assert check_aw['record']['lat'] == 112.22134
    assert check_aw['record']['lon'] == 135.76433


def test_redact_with_string(record_and_meta_2):
    xf_redact_field = RedactWithStringConfig(string="DON'T_SHOW_THIS_FIELD")
    xf_redact_entity = RedactWithStringConfig(labels=['ip_address'], string="DON'T_SHOW_THIS_ENTITY")

    data_paths = [
        DataPath(input='city', xforms=xf_redact_field),
        DataPath(input='*', xforms=xf_redact_entity)
    ]

    xf = DataTransformPipeline(data_paths)

    check = xf.transform_record(record_and_meta_2)
    assert check['record']['dni'] == 'He loves DON\'T_SHOW_THIS_ENTITY for DNS'
    assert check['record']['city'] == "DON'T_SHOW_THIS_FIELD"


def test_gretel_meta(record_and_meta_2):
    xf_fpe = FpeStringConfig(secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94", radix=10)
    xf_redact_entity = FpeStringConfig(labels=['ip_address'],
                                       secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94",
                                       radix=10)

    data_paths = [
        DataPath(input='latitude', xforms=xf_fpe),
        DataPath(input='*', xforms=xf_redact_entity)
    ]

    xf = DataTransformPipeline(data_paths)
    rf = DataRestorePipeline(data_paths)
    check = xf.transform_record(record_and_meta_2)
    assert check['metadata']['gretel_id'] == '2732c7ed44a8402f899a01e52a931985'
    check = rf.transform_record(check)
    assert check['record'] == record_and_meta_2['record']
    assert check['metadata']['gretel_id'] == '2732c7ed44a8402f899a01e52a931985'


def test_record_fpe_formatter():
    rec = {'latitude': -70.783, 'longitude': -112.221, 'credit_card': '4123 5678 9123 4567', 'the_dude': 100000000,
           'the_hotness': "convertme", "the_sci_notation": 1.23E-7}
    numbers_xf = [FpeStringConfig(
        secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94", radix=10)]

    float_xf = [FpeFloatConfig(
        secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94", radix=10,
        float_precision=3)]
    cc_xf = [FormatConfig(pattern=r'\s+', replacement=''), numbers_xf]

    text_xf = [
        FpeStringConfig(secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94", radix=36)]

    data_paths = [DataPath(input='credit_card', xforms=cc_xf),
                  DataPath(input='longitude', xforms=float_xf),
                  DataPath(input='latitude', xforms=float_xf),
                  DataPath(input='the_dude', xforms=numbers_xf),
                  DataPath(input='the_sci_notation', xforms=float_xf),
                  DataPath(input='the_hotness', xforms=text_xf)
                  ]
    xf = DataTransformPipeline(data_paths)
    xf_payload = xf.transform_record(rec)
    check = xf_payload.get('credit_card')
    assert check == '5931468769662449'


def test_record_fpe_base62():
    rec = {'latitude': -70.783, 'longitude': -112.221, 'credit_card': '4123567891234567', 'the_dude': 100000000,
           'the_hotness': "This is some awesome text with UPPER and lower case characters.",
           "the_sci_notation": 1.23E-7}
    numbers_xf = [FpeStringConfig(
        secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94", radix=10)]

    float_xf = [FpeFloatConfig(
        secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94", radix=10,
        float_precision=3)]
    cc_xf = [FormatConfig(pattern=r'\s+', replacement=''),
             FpeStringConfig(
                 secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94", radix=10)]

    text_xf = [
        FpeStringConfig(secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94", radix=62)]

    data_paths = [DataPath(input='credit_card', xforms=cc_xf),
                  DataPath(input='longitude', xforms=float_xf),
                  DataPath(input='latitude', xforms=float_xf),
                  DataPath(input='the_dude', xforms=numbers_xf),
                  DataPath(input='the_sci_notation', xforms=float_xf),
                  DataPath(input='the_hotness', xforms=text_xf)
                  ]
    xf = DataTransformPipeline(data_paths)
    rf = DataRestorePipeline(data_paths)
    xf_payload = xf.transform_record(rec)
    check = xf_payload.get('credit_card')
    assert check == '5931468769662449'
    check = rf.transform_record(xf_payload)
    assert check == rec


def test_record_fpe_mask():
    rec = {'latitude': -70.783, 'longitude': -112.221, 'credit_card': '4123 5678 9123 4567', 'the_dude': 100000000,
           'the_hotness': "convertme", "the_sci_notation": 1.23E-7}
    mask = StringMask(start_pos=1)
    cc_xf = [FormatConfig(pattern=r'\s+', replacement=''),
             FpeStringConfig(
                 secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94", radix=10,
                 mask=[mask])]

    data_paths = [DataPath(input='credit_card', xforms=cc_xf)]
    xf = DataTransformPipeline(data_paths)
    rf = DataRestorePipeline(data_paths)
    xf_payload = xf.transform_record(rec)
    check = xf_payload.get('credit_card')
    assert check == '4599631908097107'
    rf_payload = rf.transform_record(xf_payload)
    check = rf_payload.get('credit_card')
    assert check == '4123567891234567'
    cc_xf = [FpeStringConfig(
        secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94", radix=10,
        mask=[mask])]
    data_paths = [DataPath(input='credit_card', xforms=cc_xf)]
    xf = DataTransformPipeline(data_paths)
    rf = DataRestorePipeline(data_paths)
    xf_payload = xf.transform_record(rec)
    check = xf_payload.get('credit_card')
    assert check == '4599 6319 0809 7107'
    rf_payload = rf.transform_record(xf_payload)
    check = rf_payload.get('credit_card')
    assert check == '4123 5678 9123 4567'


def test_pipe_bucket(records_date_tweak):
    bucket_list = [Bucket('A', 'L', 'A-L'), Bucket('M', 'Z', 'M-Z')]
    bucket_xf = BucketConfig(buckets=bucket_list)

    data_paths = [DataPath(input='last_name', xforms=bucket_xf),
                  DataPath(input='*')]

    xf = DataTransformPipeline(data_paths)
    check_aw = xf.transform_record(records_date_tweak[0])
    check_ae = xf.transform_record(records_date_tweak[1])
    assert check_aw['last_name'] == 'M-Z'
    assert check_ae['last_name'] == 'A-L'


def test_date_shift_format():
    xf_date = DateShiftConfig(
        secret="2B7E151628AED2A6ABF7158809CF4F3CEF4359D8D580AA4F7F036D6F04FC6A94",
        lower_range_days=-10,
        upper_range_days=25,
        date_format='%m/%d/%Y',
        tweak=FieldRef("user_id")
    )
    data_paths = [DataPath(input="birthday", xforms=xf_date), DataPath(input="*")]
    pipe = DataTransformPipeline(data_paths)
    restore_pipe = DataRestorePipeline(data_paths)
    records = [
        {"user_id": "michaelj@dabulls.com", "birthday": "02/17/1963"},
        {"user_id": "michaelj@twinpinesmall.com", "birthday": "06/09/1961"},
        {"user_id": "michaelj@titostacos.com", "birthday": "08/29/1958"},
    ]
    out = [pipe.transform_record(rec) for rec in records]
    assert out == [
        {"user_id": "michaelj@dabulls.com", "birthday": "02/13/1963"},
        {"user_id": "michaelj@twinpinesmall.com", "birthday": "06/05/1961"},
        {"user_id": "michaelj@titostacos.com", "birthday": "08/25/1958"},
    ]
    restored = [restore_pipe.transform_record(rec) for rec in out]
    assert restored == [
        {"user_id": "michaelj@dabulls.com", "birthday": "02/17/1963"},
        {"user_id": "michaelj@twinpinesmall.com", "birthday": "06/09/1961"},
        {"user_id": "michaelj@titostacos.com", "birthday": "08/29/1958"},
    ]
    records = [
        {"user_id": "michaelj@dabulls.com", "birthday": "1963-02-17"},
        {"user_id": "michaelj@twinpinesmall.com", "birthday": "1961-06-09"},
        {"user_id": "michaelj@titostacos.com", "birthday": "1958-08-29"},
    ]
    out = [pipe.transform_record(rec) for rec in records]
    assert out == [
        {"user_id": "michaelj@dabulls.com", "birthday": "02/13/1963"},
        {"user_id": "michaelj@twinpinesmall.com", "birthday": "06/05/1961"},
        {"user_id": "michaelj@titostacos.com", "birthday": "08/25/1958"},
    ]
    restored = [restore_pipe.transform_record(rec) for rec in out]
    # Please note the format!
    assert restored == [
        {"user_id": "michaelj@dabulls.com", "birthday": "02/17/1963"},
        {"user_id": "michaelj@twinpinesmall.com", "birthday": "06/09/1961"},
        {"user_id": "michaelj@titostacos.com", "birthday": "08/29/1958"},
    ]
