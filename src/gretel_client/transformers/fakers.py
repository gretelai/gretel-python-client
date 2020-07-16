import hashlib
from collections import OrderedDict

import faker as faker_module

LOCALES = {
    "ar-EG": "Arabic (Egypt)",
    "ar-PS": "Arabic (Palestine)",
    "ar-SA": "Arabic (Saudi Arabia)",
    "bg-BG": "Bulgarian",
    "bs-BA": "Bosnian",
    "cs-CZ": "Czech",
    "de-DE": "German",
    "dk-DK": "Danish",
    "el-GR": "Greek",
    "en-AU": "English (Australia)",
    "en-CA": "English (Canada)",
    "en-GB": "English (Great Britain)",
    "en-IN": "English (India)",
    "en-NZ": "English (New Zealand)",
    "en-US": "English (United States)",
    "es-ES": "Spanish (Spain)",
    "es-MX": "Spanish (Mexico)",
    "et-EE": "Estonian",
    "fa-IR": "Persian (Iran)",
    "fi-FI": "Finnish",
    "fr-FR": "French",
    "hi-IN": "Hindi",
    "hr-HR": "Croatian",
    "hu-HU": "Hungarian",
    "hy-AM": "Armenian",
    "it-IT": "Italian",
    "ja-JP": "Japanese",
    "ka-GE": "Georgian (Georgia)",
    "ko-KR": "Korean",
    "lt-LT": "Lithuanian",
    "lv-LV": "Latvian",
    "ne-NP": "Nepali",
    "nl-NL": "Dutch (Netherlands)",
    "no-NO": "Norwegian",
    "pl-PL": "Polish",
    "pt-BR": "Portuguese (Brazil)",
    "pt-PT": "Portuguese (Portugal)",
    "ro-RO": "Romanian",
    "ru-RU": "Russian",
    "sl-SI": "Slovene",
    "sv-SE": "Swedish",
    "tr-TR": "Turkish",
    "uk-UA": "Ukrainian",
    "zh-CN": "Chinese (China)",
    "zh-TW": "Chinese (Taiwan)",
}


class _DeterministicFaker:
    """
    A wrapper around Faker that adds an additional parameter to each of the Faker methods, which is interpreted
    as the "source" object for the respective provider method, with the aim of establishing a deterministic mapping
    without requiring additional space.
    """

    def __init__(self, seed, locales, locale_seed):
        self.seed = seed
        self.faker = faker_module.Faker(locales)
        self.seed_obj = None
        if locale_seed is not None:
            Fakers.set_locales_proxy_seed(locale_seed)

    def __getattr__(self, name):
        def det_faker_method(seed_obj, *args, **kwargs):
            seed = (
                int(hashlib.sha256(str(seed_obj).encode()).hexdigest(), 16) ^ self.seed
            )
            self.faker.seed_instance(seed)
            return faker_method(*args, **kwargs)

        try:
            faker_method = getattr(self.faker, name)
        except AttributeError:
            return None

        return det_faker_method


class Fakers:
    def __init__(self, seed=None, locales=None, locale_seed=None):
        if locales is None:
            locales = ["en-US"]
        if all(locale in LOCALES for locale in locales):
            self.locales_dict = OrderedDict(
                [(key, i + 1) for i, key in enumerate(locales)]
            )
            self.random_faker = faker_module.Faker(locales=self.locales_dict)
            self.deterministic_faker = _DeterministicFaker(
                seed=seed, locales=self.locales_dict, locale_seed=locale_seed
            )

    @staticmethod
    def set_locales_proxy_seed(seed):
        faker_module.Faker.seed(seed)

    def random_fake(self, method):
        func = getattr(self.random_faker, method)
        return func()

    def constant_fake(self, seed_obj, method):
        self.deterministic_faker.seed_obj = seed_obj
        func = getattr(self.deterministic_faker, method)
        if not func:
            return None
        return func(seed_obj)
