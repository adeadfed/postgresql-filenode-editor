import struct
from enum import IntFlag


class HeapT_InfomaskFlags(IntFlag):
    # https://github.com/postgres/postgres/blob/d4e66a39eb96ca514e3f49c85cf0b4b6f138854e/src/include/access/htup_details.h#L188
    HEAP_HASNULL = 0x1
    HEAP_HASVARWIDTH = 0x2
    HEAP_HASEXTERNAL = 0x4
    HEAP_HASOID_OLD = 0x8
    HEAP_XMAX_KEYSHR_LOCK = 0x10
    HEAP_COMBOCID = 0x20
    HEAP_XMAX_EXCL_LOCK = 0x40
    HEAP_XMAX_LOCK_ONLY = 0x80
    HEAP_XMIN_COMMITTED = 0x100
    HEAP_XMIN_INVALID = 0x200
    HEAP_XMAX_COMMITTED = 0x400
    HEAP_XMAX_INVALID = 0x800
    HEAP_XMAX_IS_MULTI = 0x1000
    HEAP_UPDATED = 0x2000
    HEAP_MOVED_OFF = 0x4000
    HEAP_MOVED_IN = 0x8000
    
class HeapT_Infomask2Flags(IntFlag):
    HEAP_KEYS_UPDATED = 0x2000
    HEAP_HOT_UPDATED = 0x4000
    HEAP_ONLY_TUPLE = 0x8000
    

class T_Infomask:
    HEAP_XACT_MASK = 0xFFF0
    HEAP_LOCK_MASK = (HeapT_InfomaskFlags.HEAP_XMAX_EXCL_LOCK.value | HeapT_InfomaskFlags.HEAP_XMAX_KEYSHR_LOCK).value
    
    def __init__(self, t_infomask_bytes):
        self.flags = HeapT_InfomaskFlags(
            struct.unpack('<H', t_infomask_bytes)[0]
        )
        
    def to_bytes(self):
        return struct.pack('<H', self.flags)
 
class T_Infomask2:
    HEAP_NATTS_MASK = 0x07FF
    HEAP_FLAGS_MASK = 0xF800
    HEAP2_XACT_MASK = 0xE000
    
    def __init__(self, t_infomask2_bytes):
        _t_infomask_2 = struct.unpack('<H', t_infomask2_bytes)[0]
        # get number of attributes in the item
        self.natts = _t_infomask_2 & self.HEAP_NATTS_MASK

        self.flags = HeapT_Infomask2Flags(_t_infomask_2 & self.HEAP_FLAGS_MASK) 

    def to_bytes(self):
        return struct.pack('<H', self.natts | self.flags)

