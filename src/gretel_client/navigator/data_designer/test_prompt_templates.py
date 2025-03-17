import pytest

from gretel_client.navigator.data_designer.prompt_templates import (
    assert_valid_template,
    GenerationTemplateError,
)


# Extensive test cases for assert_valid_template.
@pytest.mark.parametrize(
    "template,should_raise",
    [
        # Valid templates:
        ("Hello, world!", False),  # No placeholders.
        ("Hello {name}", False),  # Single valid named placeholder.
        ("{greeting}, {name}!", False),  # Multiple valid placeholders.
        ("Hello {name} and {friend}", False),  # Multiple valid placeholders.
        ("{{}}", False),  # Escaped braces (literal "{}").
        ("a {b} c {{escaped}} d", False),  # Mix of placeholder and escaped braces.
        ("Just a brace: {{}}", False),  # Only escaped braces.
        ("Hello {name!s}", False),  # With conversion flag.
        ("Hello {name:>10}", False),  # With format spec.
        (
            "The set is {{1,2,3}}",
            False,
        ),  # Escaped curly braces around non-placeholder text.
        ("Value: {val:.2f}", False),  # Valid format spec for a numeric value.
        ("Broken }}", False),  # Literal "}" via escaping.
        ("Hello {first_name}", False),  # Valid identifier with underscore.
        ("Hello {name_}", False),  # Valid identifier ending with underscore.
        ("Hello {{name}}", False),  # Entire placeholder is escaped (literal).
        (
            "A{{B}}C{name}D{{E}}",
            False,
        ),  # Complex mix: literal parts and one valid placeholder.
        # Invalid templates:
        ("", True),  # Empty string.
        ("Hello {}", True),  # Positional (empty) placeholder.
        ("Hello {0}", True),  # Positional numeric placeholder.
        ("Hello {name", True),  # Missing closing brace.
        ("Hello {name} {age", True),  # One placeholder missing its closing brace.
        ("Hello {not valid}", True),  # Placeholder with a space (invalid identifier).
        ("Hello {123name}", True),  # Identifier starting with a digit.
        ("Hello {name:>", True),  # Malformed format spec.
        ("Hello }", True),  # Unmatched closing brace.
        ("Hello {n@me}", True),  # Invalid character in placeholder.
        ("Hello {name!}", True),  # Incomplete conversion specifier.
    ],
)
def test_assert_valid_template(template, should_raise):
    """
    Test that assert_valid_template either completes without error for valid templates
    or raises GenerationTemplateError for invalid ones.
    """
    if should_raise:
        with pytest.raises(GenerationTemplateError):
            assert_valid_template(template)
    else:
        # Should not raise an exception; the function returns None.
        assert assert_valid_template(template) is None
