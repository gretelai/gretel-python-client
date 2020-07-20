from gretel_client.transformers.fakers import _Fakers


def test_deterministic_faker():
    locales = ['en-US', 'es-ES']
    _Fakers.set_locales_proxy_seed(0)
    fakers = _Fakers(seed=12345, locales=locales)
    new_value1 = fakers.constant_fake("test@mule.com", "email")
    new_value2 = fakers.constant_fake("test@mule.com", "email")
    new_value2 = fakers.constant_fake("test@mule.com", "email")
    us_value = fakers.constant_fake("test@mule.com", "email")
    new_value2 = fakers.constant_fake("test@mule.com", "email")
    new_value2 = fakers.constant_fake("test@mule.com", "email")
    _Fakers.set_locales_proxy_seed(0)
    fakers = _Fakers(seed=54321)
    new_value3 = fakers.constant_fake("test@mule.com", "email")
    assert new_value1 == new_value2 != new_value3 and new_value2 != us_value
    null_value = fakers.constant_fake("test@mule.com", "wrong")
    assert not null_value
    null_value = fakers.constant_fake("test@mule.com", "location")
    assert not null_value