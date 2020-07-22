from gretel_client.transformers import FpeFloatConfig, ConditionalConfig, DataPath, RedactWithLabelConfig, FieldRef, \
    DataTransformPipeline, DataRestorePipeline


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
