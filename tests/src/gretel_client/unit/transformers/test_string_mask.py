from gretel_client.transformers.string_mask import StringMask

TEST_STR = "This is a test."
TEST_STR2 = "This is a test.t"

def test_string_mask():
    string_mask = StringMask(start_pos=0)
    check = string_mask.get_masked_chars(TEST_STR)
    string_mask = StringMask(start_pos=4)
    check = string_mask.get_masked_chars(TEST_STR)
    string_mask = StringMask(start_pos=4, mask_until='t')
    check = string_mask.get_masked_chars(TEST_STR)
    string_mask = StringMask(start_pos=4, mask_until='t', greedy=True)
    check = string_mask.get_masked_chars(TEST_STR)
    string_mask = StringMask(mask_after='t', mask_until='t')
    check = string_mask.get_masked_chars(TEST_STR)
    string_mask = StringMask(mask_after='t', mask_until='t', greedy=True)
    check = string_mask.get_masked_chars(TEST_STR)
    string_mask = StringMask(mask_after='.', mask_until='t')
    check = string_mask.get_masked_chars(TEST_STR)
    string_mask = StringMask(mask_after='.', mask_until='t', greedy=True)
    check = string_mask.get_masked_chars(TEST_STR)
    check = string_mask.get_masked_chars(TEST_STR2)
    string_mask = StringMask(start_pos=4, mask_until='t')
    check = string_mask.get_mask_slice(TEST_STR)
    check = string_mask.get_mask_slice(TEST_STR)
