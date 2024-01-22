import struct
from enum import Enum


class LpFlags(Enum):
    LP_UNUSED = 0x0
    LP_NORMAL = 0x1
    LP_REDIRECT = 0x2
    LP_DEAD = 0x3


class ItemIdData:
    _FIELD_SIZE = 4
    def __init__(self, item_id_bytes):
        _encoded_data = struct.unpack('<I', item_id_bytes)[0]
        # lower 15 bits store the item offset
        self.lp_off = _encoded_data & 0x7fff
        # middle 2 bits store the item flags
        self.lp_flags = LpFlags((_encoded_data & 0x18000) >> 15)
        # upper 15 bits store the item length
        self.lp_len = (_encoded_data & 0xfffe0000) >> 17

    def to_bytes(self):
        item_id = 0
        # upper 15 bits store the item length
        item_id = item_id | self.lp_len << 17
        # middle 2 bits store the item flags
        item_id = item_id | self.lp_flags.value << 15
        # lower 15 bits store the item offset
        item_id = item_id | self.lp_off
        return struct.pack('<I', item_id)