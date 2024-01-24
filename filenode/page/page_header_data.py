import struct
from enum import IntFlag, Enum


class PdFlags(IntFlag):
    PD_UNDEFINED = 0x0
    PD_HAS_FREE_LINES = 0x1
    PD_PAGE_FULL = 0x2
    PD_ALL_VISIBLE = 0x4


class PdPageVersion(Enum):
    PRE_POSTGRES_73 = 0x0
    POSTGRES_73_74 = 0x1
    POSTGRES_80 = 0x2
    POSTGRES_81_82 = 0x3
    POSTGRES_83_PLUS = 0x4


class PageHeaderData:
    _FIELD_SIZE = 24

    def __init__(self, header_bytes):
        # parse raw header bytes
        self.pd_lsn = struct.unpack('<Q', header_bytes[:8])[0]
        self.checksum = struct.unpack('<H', header_bytes[8:10])[0]

        self.pd_flags = PdFlags(
            struct.unpack('<H', header_bytes[10:12])[0]
        )

        self.pd_lower = struct.unpack('<H', header_bytes[12:14])[0]
        self.pd_upper = struct.unpack('<H', header_bytes[14:16])[0]
        self.pd_special = struct.unpack('<H', header_bytes[16:18])[0]

        _pd_pagesize_version = struct.unpack('<H', header_bytes[18:20])[0]
        # parse pd_pagesize_version
        self.size = self.length = _pd_pagesize_version & 0xFF00
        self.version = PdPageVersion(_pd_pagesize_version & 0x00FF)

        self.pd_prune_xid = struct.unpack('<I', header_bytes[20:24])[0]

    def to_bytes(self):
        header_bytes = b''

        header_bytes += struct.pack('<Q', self.pd_lsn)
        # zero out checksum, just to be super safe with editing data
        header_bytes += struct.pack('<H', 0)
        # pack pd_flags into 32 bit integer via bitwise or
        header_bytes += struct.pack('<H', self.pd_flags.value)
        header_bytes += struct.pack('<H', self.pd_lower)
        header_bytes += struct.pack('<H', self.pd_upper)
        header_bytes += struct.pack('<H', self.pd_special)
        # pack pd_pagesize_version
        header_bytes += struct.pack('<H', self.size | self.version.value)
        header_bytes += struct.pack('<I', self.pd_prune_xid)

        return header_bytes
