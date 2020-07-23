"""
Module that contains masking tools for strings.
"""
from typing import Tuple


class StringMask:
    """
    Specify a masking strategy for a string. When provided to transformers that accept masks, the mask
    will determine what parts of the string are redacted, encrypted, etc.

    Args:
        start_pos: What index position in the string to start masking after, non-inclusive.
        end_pos: What index position in the string to stop masking at, inclusive.
        mask_after: Scan for this character, and once observed, start masking after it, non-inclusive.
        mask_until: Scan for this character, and once found, mask until it is reached, inclusive.
        greedy: When using ``mask_after`` or ``mask_until``, if True, will scan as far as possible
            to find the matching character. For example given the string: "this.is.the.string", if
            ``mask_after`` is "." and ``greedy`` is ``False`` then masking will start after "this.".
            If ``greedy`` is ``True``, then masking will start after "this.is.the."
    """
    start_pos: int = None
    end_pos: int = None
    mask_after: str = None
    mask_until: str = None
    greedy: bool = False

    def __init__(self, start_pos: int = None, end_pos: int = None, mask_after: chr = None, mask_until: chr = None,
                 greedy: bool = False):
        if start_pos and mask_after:
            raise ValueError("You can only specify either start_pos or mask_after")
        if end_pos and mask_until:
            raise ValueError("You can only specify either end_pos or mask_until")
        if start_pos and not isinstance(start_pos, int):
            raise ValueError("start_pos needs to be of type int.")
        if end_pos and not isinstance(end_pos, int):
            raise ValueError("end_pos needs to be of type int.")
        if mask_after and not isinstance(mask_after, str):
            raise ValueError("mask_after needs to be of type str.")
        if mask_until and not isinstance(mask_until, str):
            raise ValueError("mask_until needs to be of type str.")
        self.start_pos = start_pos
        self.end_pos = end_pos
        self.mask_after = mask_after
        self.mask_until = mask_until
        self.greedy = greedy

    def get_mask_slice(self, value: str) -> slice:
        if self.mask_after is None:
            start = self.start_pos
        else:
            start = value.rfind(self.mask_after) if self.greedy else value.find(self.mask_after) + 1
        if start is None:
            start = 0
        if self.mask_until is None:
            stop = self.end_pos
        else:
            rem_value = value[start:]
            stop = start + (rem_value.rfind(self.mask_until) if self.greedy else rem_value.find(self.mask_until))
        return slice(start, stop)

    def get_masked_chars(self, value: str) -> str:
        return value[self.get_mask_slice(value)]

    def get_masked_chars_slice(self, value: str) -> Tuple[str, slice]:
        _slice = self.get_mask_slice(value)
        return value[_slice], _slice
